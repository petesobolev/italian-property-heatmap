#!/usr/bin/env python3
"""
OMI Property Values Ingestion Script

Loads property valuation data from Agenzia delle Entrate's OMI (Osservatorio Mercato Immobiliare)
into the Supabase database.

Data Source: https://www1.agenziaentrate.gov.it/servizi/geopoi_omi/

Usage:
    python load_omi_values.py --provinces RM MI --semesters 20242 20241
    python load_omi_values.py --all-provinces --semesters 20242
    python load_omi_values.py --test  # Run with just Roma for testing
"""

import argparse
import json
import logging
import os
import re
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor, Json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('omi_ingestion.log')
    ]
)
logger = logging.getLogger(__name__)

# Constants
OMI_BASE_URL = "https://www1.agenziaentrate.gov.it/servizi/geopoi_omi/"
REQUEST_DELAY_SECONDS = 1.5  # Respectful rate limiting
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5
HTTP_TIMEOUT_SECONDS = 20  # Shorter timeout, fail fast and retry
MUNICIPALITY_TIMEOUT_SECONDS = 300  # 5 minutes max per municipality
STALL_DETECTION_SECONDS = 120  # Log warning if no progress for 2 minutes


class ProgressTracker:
    """Track progress and detect stalls in long-running operations."""

    def __init__(self):
        self._last_activity = time.time()
        self._last_logged_stall = 0
        self._current_operation = ""
        self._lock = threading.Lock()

    def heartbeat(self, operation: str = ""):
        """Record activity to reset stall timer."""
        with self._lock:
            self._last_activity = time.time()
            if operation:
                self._current_operation = operation

    def check_stall(self) -> Optional[float]:
        """
        Check if operation has stalled.
        Returns seconds since last activity if stalled, None otherwise.
        """
        with self._lock:
            elapsed = time.time() - self._last_activity
            if elapsed > STALL_DETECTION_SECONDS:
                # Only log every STALL_DETECTION_SECONDS to avoid spam
                if time.time() - self._last_logged_stall > STALL_DETECTION_SECONDS:
                    self._last_logged_stall = time.time()
                    return elapsed
            return None

    def get_current_operation(self) -> str:
        """Get description of current operation."""
        with self._lock:
            return self._current_operation

    def seconds_since_activity(self) -> float:
        """Get seconds since last activity."""
        with self._lock:
            return time.time() - self._last_activity


class MunicipalityTimeoutError(Exception):
    """Raised when a municipality takes too long to process."""
    pass

# Property types by segment - all segments are now ingested
RESIDENTIAL_PROPERTY_TYPES = {
    'Abitazioni civili',
    'Abitazioni di tipo economico',
    'Abitazioni signorili',
    'Ville e Villini',
    'Box',
    'Posti auto coperti',
    'Posti auto scoperti',
    'Autorimesse',
}

COMMERCIAL_PROPERTY_TYPES = {
    'Negozi',
    'Uffici',
    'Uffici strutturati',
    'Centri commerciali',
}

INDUSTRIAL_PROPERTY_TYPES = {
    'Magazzini',
    'Laboratori',
    'Capannoni industriali',
    'Capannoni tipici',
}

# All property types we want to ingest
ALL_PROPERTY_TYPES = RESIDENTIAL_PROPERTY_TYPES | COMMERCIAL_PROPERTY_TYPES | INDUSTRIAL_PROPERTY_TYPES

# Property type normalization mapping (lowercase key -> normalized value)
PROPERTY_TYPE_MAPPING = {
    # Residential
    'abitazioni civili': 'residenziale',
    'abitazioni di tipo economico': 'residenziale',
    'abitazioni signorili': 'residenziale',
    'ville e villini': 'residenziale',
    'box': 'residenziale',
    'posti auto coperti': 'residenziale',
    'posti auto scoperti': 'residenziale',
    'autorimesse': 'residenziale',
    # Commercial
    'negozi': 'negozi',
    'uffici': 'uffici',
    'uffici strutturati': 'uffici',
    'centri commerciali': 'negozi',
    # Industrial
    'laboratori': 'laboratori',
    'magazzini': 'magazzini',
    'capannoni industriali': 'capannoni',
    'capannoni tipici': 'capannoni',
}

# State mapping (Italian to normalized)
STATE_MAPPING = {
    'OTTIMO': 'OTTIMO',
    'NORMALE': 'NORMALE',
    'SCADENTE': 'SCADENTE',
    'ottimo': 'OTTIMO',
    'normale': 'NORMALE',
    'scadente': 'SCADENTE',
}


@dataclass
class Province:
    """Province data from OMI API."""
    code: str  # Province abbreviation (e.g., "RM", "MI")
    name: str


@dataclass
class Comune:
    """Comune (municipality) data from OMI API."""
    codcom: str  # Cadastral code (e.g., "H501" for Roma)
    name: str
    province_code: str
    istat_code: Optional[str] = None  # 6-digit ISTAT code (resolved via mapping)


@dataclass
class OMIZone:
    """OMI zone data."""
    zone_code: str  # e.g., "B1", "C2"
    zone_type: str  # B (centrale), C (semicentrale), D (periferica), etc.
    zone_description: str
    microzone_code: Optional[str] = None
    geometry: Optional[dict] = None  # GeoJSON geometry


@dataclass
class PropertyValue:
    """Property value quotation from OMI."""
    omi_zone_id: str
    municipality_id: str
    semester_id: str
    property_type: str
    state: Optional[str]
    value_min_eur_sqm: Optional[float]
    value_max_eur_sqm: Optional[float]
    rent_min_eur_sqm_month: Optional[float]
    rent_max_eur_sqm_month: Optional[float]
    source_url: str


class OMIIngestionError(Exception):
    """Custom exception for OMI ingestion errors."""
    pass


class OMIClient:
    """Client for interacting with the OMI API and web pages."""

    def __init__(self, progress_tracker: Optional[ProgressTracker] = None):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/html, */*',
            'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
        })
        self.progress_tracker = progress_tracker

    def _make_request(self, url: str, params: Optional[dict] = None,
                      expect_json: bool = True) -> Any:
        """Make HTTP request with retries and rate limiting."""
        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(REQUEST_DELAY_SECONDS)

                # Record heartbeat before request
                if self.progress_tracker:
                    self.progress_tracker.heartbeat(f"HTTP request to {url[:60]}...")

                response = self.session.get(url, params=params, timeout=HTTP_TIMEOUT_SECONDS)
                response.raise_for_status()

                # Record heartbeat after successful response
                if self.progress_tracker:
                    self.progress_tracker.heartbeat(f"Got response from {url[:60]}...")

                if expect_json:
                    return response.json()
                return response.text

            except requests.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    # Shorter retry delay for faster recovery
                    retry_wait = min(RETRY_DELAY_SECONDS, HTTP_TIMEOUT_SECONDS / 2)
                    time.sleep(retry_wait)
                else:
                    raise OMIIngestionError(f"Failed to fetch {url}: {e}")

    def get_provinces(self) -> list[Province]:
        """Fetch list of all Italian provinces."""
        url = urljoin(OMI_BASE_URL, "zoneomi.php")
        data = self._make_request(url, params={'richiesta': '1'})

        provinces = []
        for item in data:
            # API returns list of [code, name] pairs or dicts
            if isinstance(item, list) and len(item) >= 2:
                provinces.append(Province(code=item[0], name=item[1]))
            elif isinstance(item, dict):
                # Handle OMI API format: {'PROVINCIA': 'RM', 'DIZIONE': 'ROMA'}
                code = item.get('PROVINCIA', item.get('sigla', item.get('code', '')))
                name = item.get('DIZIONE', item.get('nome', item.get('name', '')))
                if code:
                    provinces.append(Province(code=code, name=name))

        logger.info(f"Found {len(provinces)} provinces")
        return provinces

    def get_comuni(self, province_code: str) -> list[Comune]:
        """Fetch list of comuni in a province."""
        url = urljoin(OMI_BASE_URL, "zoneomi.php")
        data = self._make_request(url, params={'richiesta': '2', 'prov': province_code})

        if data is None:
            logger.warning(f"No data returned for province {province_code}")
            return []

        comuni = []
        for item in data:
            if isinstance(item, list) and len(item) >= 2:
                comuni.append(Comune(
                    codcom=str(item[0]),
                    name=item[1],
                    province_code=province_code
                ))
            elif isinstance(item, dict):
                # Handle OMI API format: {'DIZIONE': 'ROMA', 'CODCOM': 'H501'}
                code = item.get('CODCOM', item.get('codice', item.get('code', '')))
                name = item.get('DIZIONE', item.get('nome', item.get('name', '')))
                if code:
                    comuni.append(Comune(
                        codcom=str(code),  # Cadastral code (not ISTAT!)
                        name=name,
                        province_code=province_code
                    ))

        logger.info(f"Found {len(comuni)} comuni in province {province_code}")
        return comuni

    def get_zones(self, codcom: str) -> list[OMIZone]:
        """Fetch OMI zones for a municipality."""
        url = urljoin(OMI_BASE_URL, "zoneomi.php")
        data = self._make_request(url, params={'richiesta': '3', 'codcom': codcom})

        if data is None:
            logger.debug(f"No zones returned for comune {codcom}")
            return []

        zones = []
        for item in data:
            if isinstance(item, list) and len(item) >= 2:
                zone_code = item[0]
                description = item[1] if len(item) > 1 else ''
                zone_type = zone_code[0] if zone_code else ''
                zones.append(OMIZone(
                    zone_code=zone_code,
                    zone_type=zone_type,
                    zone_description=description
                ))
            elif isinstance(item, dict):
                # Handle OMI API format: {'LINK_ZONA': 'RM00000155', 'FASCIA': 'B', 'ZONA': 'B1', 'DIZIONE': '...'}
                zone_code = item.get('ZONA', item.get('codice', item.get('code', '')))
                zone_type = item.get('FASCIA', zone_code[0] if zone_code else '')
                description = item.get('DIZIONE', item.get('descrizione', item.get('description', '')))
                if zone_code:
                    zones.append(OMIZone(
                        zone_code=zone_code,
                        zone_type=zone_type,
                        zone_description=description,
                        microzone_code=item.get('LINK_ZONA', item.get('microzona'))
                    ))

        logger.debug(f"Found {len(zones)} zones in comune {codcom}")
        return zones

    def get_available_semesters(self) -> list[str]:
        """Fetch list of available semesters."""
        url = urljoin(OMI_BASE_URL, "zoneomi.php")
        data = self._make_request(url, params={'richiesta': '5'})

        semesters = []
        for item in data:
            if isinstance(item, str):
                # Format: "2024-2" -> "20242"
                sem = item.replace('-', '')
                semesters.append(sem)
            elif isinstance(item, list) and len(item) >= 1:
                sem = str(item[0]).replace('-', '')
                semesters.append(sem)

        logger.info(f"Available semesters: {semesters}")
        return semesters

    def get_zone_geometries(self, istat_code: str, semester: str) -> dict:
        """Fetch GeoJSON geometries for zones in a municipality."""
        url = urljoin(OMI_BASE_URL, "zoneomi.php")
        # Format semester: "20242" -> "2024-2"
        sem_formatted = f"{semester[:4]}-{semester[4:]}"

        try:
            data = self._make_request(
                url,
                params={'richiesta': '6', 'codcom': istat_code, 'semestre': sem_formatted}
            )
            return data if data else {}
        except OMIIngestionError:
            logger.warning(f"Could not fetch geometries for {istat_code}")
            return {}

    def get_property_types_for_zone(self, codcom: str, zone_code: str, semester: str) -> list[dict]:
        """
        Get available property types for a zone using richiesta=8.

        Args:
            codcom: Cadastral code of the comune
            zone_code: Zone code (e.g., B1, C2)
            semester: Semester in YYYYS format (e.g., 20242)

        Returns:
            List of dicts with LINK_ZONA and DESCR_TIPOLOGIA
        """
        url = urljoin(OMI_BASE_URL, "zoneomi.php")
        params = {
            'richiesta': '8',
            'codcom': codcom,
            'semestre': semester,
            'zo': zone_code,  # This is the key parameter!
        }

        try:
            data = self._make_request(url, params=params)
            if data and isinstance(data, list):
                return data
        except OMIIngestionError:
            pass

        return []

    def get_property_values(self, codcom: str, zone_code: str,
                           semester: str) -> list[PropertyValue]:
        """
        Fetch property values using the stampaomi.php endpoint.

        This is a two-step process:
        1. Get property types via richiesta=8 (returns LINK_ZONA)
        2. Scrape stampaomi.php for actual values

        Args:
            codcom: Cadastral code (e.g., H501)
            zone_code: Zone code (e.g., B1)
            semester: Semester in YYYYS format (e.g., 20242)
        """
        # Step 1: Get property types to get LINK_ZONA
        property_types = self.get_property_types_for_zone(codcom, zone_code, semester)
        if not property_types:
            logger.debug(f"No property types found for {codcom}/{zone_code}")
            return []

        values = []
        omi_zone_id = f"{codcom}_{zone_code}"

        # Step 2: Get values for each property type
        for pt in property_types:
            link_zona = pt.get('LINK_ZONA', '')
            descr = pt.get('DESCR_TIPOLOGIA', '')
            type_code = descr[0].upper() if descr else 'R'  # R=Residenziale, C=Commerciale, P=Produttiva

            # URL format: stampaomi.php?{codcom}/{link_zona}/{semester}/{type}/{zone}/{lon}/{lat}
            url = f"{OMI_BASE_URL}stampaomi.php?{codcom}/{link_zona}/{semester}/{type_code}/{zone_code}/12.5/41.9"
            source_url = url

            try:
                html = self._make_request(url, params={}, expect_json=False)
                parsed_values = self._parse_stampaomi_html(
                    html, omi_zone_id, codcom, semester, source_url
                )
                values.extend(parsed_values)
            except OMIIngestionError as e:
                logger.debug(f"Failed to get values from {url}: {e}")
                continue

        return values

    def _parse_stampaomi_html(self, html: str, omi_zone_id: str, municipality_id: str,
                              semester: str, source_url: str) -> list[PropertyValue]:
        """Parse the stampaomi.php HTML response to extract property values."""
        values = []
        soup = BeautifulSoup(html, 'html.parser')

        # Find all table rows
        rows = soup.find_all('tr')

        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 5:
                continue

            try:
                # Structure: Property Type | State | Min Value | Max Value | Surface | Min Rent | Max Rent | Surface
                property_subtype = cells[0].get_text(strip=True).replace('\xa0', '')
                state = cells[1].get_text(strip=True).replace('\xa0', '') if len(cells) > 1 else None

                # Skip header rows or empty data
                if not property_subtype or property_subtype.lower() in ['tipologia', 'tipo', 'min', 'max', '']:
                    continue

                # Parse numeric values (Italian format: 1.234,56)
                def parse_number(text: str) -> Optional[float]:
                    if not text:
                        return None
                    text = re.sub(r'[^\d,.\-]', '', text.strip())
                    if not text or text == '-':
                        return None
                    text = text.replace('.', '').replace(',', '.')
                    try:
                        return float(text)
                    except ValueError:
                        return None

                value_min = parse_number(cells[2].get_text(strip=True))
                value_max = parse_number(cells[3].get_text(strip=True))

                # Skip if no value data
                if value_min is None and value_max is None:
                    continue

                # Parse rent values (columns 5 and 6)
                rent_min = None
                rent_max = None
                if len(cells) >= 7:
                    rent_min = parse_number(cells[5].get_text(strip=True))
                    rent_max = parse_number(cells[6].get_text(strip=True))

                # Normalize property type
                normalized_type = PROPERTY_TYPE_MAPPING.get(
                    property_subtype.lower(),
                    property_subtype.lower().replace(' ', '_')
                )

                values.append(PropertyValue(
                    omi_zone_id=omi_zone_id,
                    municipality_id=municipality_id,
                    semester_id=semester,
                    property_type=normalized_type,
                    state=STATE_MAPPING.get(state, state),
                    value_min_eur_sqm=value_min,
                    value_max_eur_sqm=value_max,
                    rent_min_eur_sqm_month=rent_min,
                    rent_max_eur_sqm_month=rent_max,
                    source_url=source_url
                ))
            except (IndexError, ValueError) as e:
                logger.debug(f"Error parsing row: {e}")
                continue

        return values

    def _parse_values_html(self, html: str, istat_code: str, zone_code: str,
                          semester: str, source_url: str) -> list[PropertyValue]:
        """Parse HTML table to extract property values."""
        values = []
        soup = BeautifulSoup(html, 'html.parser')

        # Find all tables with quotation data
        tables = soup.find_all('table', class_='quotazioni') or soup.find_all('table')

        omi_zone_id = f"{istat_code}_{zone_code}"

        for table in tables:
            rows = table.find_all('tr')
            current_property_type = None

            for row in rows:
                cells = row.find_all(['td', 'th'])
                if not cells:
                    continue

                # Check if this is a header row with property type
                header = row.find('th', colspan=True) or row.find('td', class_='tipologia')
                if header:
                    current_property_type = header.get_text(strip=True)
                    continue

                # Parse data rows
                if len(cells) >= 5 and current_property_type:
                    try:
                        value = self._parse_value_row(
                            cells, current_property_type, omi_zone_id,
                            istat_code, semester, source_url
                        )
                        if value:
                            values.append(value)
                    except Exception as e:
                        logger.debug(f"Error parsing row: {e}")
                        continue

        # Alternative parsing strategy for different HTML structure
        if not values:
            values = self._parse_values_alternative(
                soup, omi_zone_id, istat_code, semester, source_url
            )

        return values

    def _parse_value_row(self, cells: list, property_type: str, omi_zone_id: str,
                        municipality_id: str, semester: str,
                        source_url: str) -> Optional[PropertyValue]:
        """Parse a single row of value data."""
        try:
            # Typical structure: State | Min Value | Max Value | Min Rent | Max Rent
            state_text = cells[0].get_text(strip=True) if len(cells) > 0 else None
            state = STATE_MAPPING.get(state_text, state_text)

            # Parse numeric values (handle Italian number format: 1.234,56)
            def parse_number(text: str) -> Optional[float]:
                if not text:
                    return None
                # Remove currency symbols and whitespace
                text = re.sub(r'[^\d,.\-]', '', text.strip())
                if not text or text == '-':
                    return None
                # Convert Italian format to float
                text = text.replace('.', '').replace(',', '.')
                try:
                    return float(text)
                except ValueError:
                    return None

            value_min = parse_number(cells[1].get_text(strip=True)) if len(cells) > 1 else None
            value_max = parse_number(cells[2].get_text(strip=True)) if len(cells) > 2 else None
            rent_min = parse_number(cells[3].get_text(strip=True)) if len(cells) > 3 else None
            rent_max = parse_number(cells[4].get_text(strip=True)) if len(cells) > 4 else None

            # Skip if no useful data
            if value_min is None and value_max is None and rent_min is None and rent_max is None:
                return None

            # Normalize property type
            normalized_type = PROPERTY_TYPE_MAPPING.get(
                property_type.lower(),
                property_type.lower().replace(' ', '_')
            )

            return PropertyValue(
                omi_zone_id=omi_zone_id,
                municipality_id=municipality_id,
                semester_id=semester,
                property_type=normalized_type,
                state=state,
                value_min_eur_sqm=value_min,
                value_max_eur_sqm=value_max,
                rent_min_eur_sqm_month=rent_min,
                rent_max_eur_sqm_month=rent_max,
                source_url=source_url
            )
        except Exception as e:
            logger.debug(f"Error parsing value row: {e}")
            return None

    def _parse_values_alternative(self, soup: BeautifulSoup, omi_zone_id: str,
                                  municipality_id: str, semester: str,
                                  source_url: str) -> list[PropertyValue]:
        """Alternative parsing strategy for different HTML structures."""
        values = []

        # Look for div-based structure or different table layouts
        # This handles cases where the HTML structure differs

        # Try to find quotation sections
        sections = soup.find_all(['div', 'section'], class_=re.compile(r'quot|valore|prezzo'))

        for section in sections:
            # Extract property type from heading
            heading = section.find(['h2', 'h3', 'h4', 'strong'])
            if not heading:
                continue

            property_type = heading.get_text(strip=True)
            normalized_type = PROPERTY_TYPE_MAPPING.get(
                property_type.lower(),
                property_type.lower().replace(' ', '_')
            )

            # Find value spans/divs
            value_elements = section.find_all(['span', 'div'], class_=re.compile(r'valore|prezzo|min|max'))

            if len(value_elements) >= 2:
                def extract_number(elem) -> Optional[float]:
                    text = elem.get_text(strip=True)
                    text = re.sub(r'[^\d,.\-]', '', text)
                    if not text or text == '-':
                        return None
                    text = text.replace('.', '').replace(',', '.')
                    try:
                        return float(text)
                    except ValueError:
                        return None

                values.append(PropertyValue(
                    omi_zone_id=omi_zone_id,
                    municipality_id=municipality_id,
                    semester_id=semester,
                    property_type=normalized_type,
                    state='NORMALE',  # Default state
                    value_min_eur_sqm=extract_number(value_elements[0]) if len(value_elements) > 0 else None,
                    value_max_eur_sqm=extract_number(value_elements[1]) if len(value_elements) > 1 else None,
                    rent_min_eur_sqm_month=extract_number(value_elements[2]) if len(value_elements) > 2 else None,
                    rent_max_eur_sqm_month=extract_number(value_elements[3]) if len(value_elements) > 3 else None,
                    source_url=source_url
                ))

        return values


class DatabaseLoader:
    """Handles loading data into PostgreSQL database via direct connection."""

    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.conn = None
        self.cursor = None
        self.ingestion_run_id: Optional[int] = None
        self._istat_cache: dict[str, Optional[str]] = {}
        self._accent_stripped_cache: dict[str, str] = {}  # stripped_name -> municipality_id
        self._connect()

    def _connect(self):
        """Establish database connection."""
        if self.cursor:
            try:
                self.cursor.close()
            except Exception:
                pass
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass

        self.conn = psycopg2.connect(**self.db_params)
        self.conn.autocommit = False
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        logger.info("Connected to database via direct PostgreSQL connection")

    def _ensure_connection(self):
        """Check connection health and reconnect if needed."""
        try:
            # Simple health check
            self.cursor.execute("SELECT 1 AS health_check")
            self.cursor.fetchone()
        except (psycopg2.InterfaceError, psycopg2.OperationalError, psycopg2.ProgrammingError) as e:
            logger.warning(f"Connection lost, reconnecting: {e}")
            self._connect()
            # Re-establish ingestion run context if we had one
            if self.ingestion_run_id:
                logger.info(f"Reconnected, continuing ingestion run {self.ingestion_run_id}")

    def municipality_has_data(self, municipality_id: str, period_id: str) -> bool:
        """Check if a municipality already has property value data for a given period."""
        self._ensure_connection()
        # Check mart table (aggregated data) since raw data is deleted after aggregation
        self.cursor.execute("""
            SELECT EXISTS(
                SELECT 1 FROM mart.municipality_values_semester
                WHERE municipality_id = %s AND period_id = %s
                LIMIT 1
            )
        """, (municipality_id, period_id))
        result = self.cursor.fetchone()
        return result['exists'] if result else False

    def get_province_completion_status(self, province_istat_prefix: str, period_id: str) -> tuple[int, int]:
        """
        Check how many municipalities in a province have data for a period.

        Args:
            province_istat_prefix: 3-digit province ISTAT prefix (e.g., '058' for Roma)
            period_id: Period to check (e.g., '2018H1')

        Returns:
            Tuple of (municipalities_with_data, total_municipalities_in_province)
        """
        self._ensure_connection()

        # Count total municipalities in province
        self.cursor.execute("""
            SELECT COUNT(*) as total FROM core.municipalities
            WHERE municipality_id LIKE %s
        """, (f"{province_istat_prefix}%",))
        total_result = self.cursor.fetchone()
        total = total_result['total'] if total_result else 0

        # Count municipalities with data for this period
        self.cursor.execute("""
            SELECT COUNT(DISTINCT municipality_id) as with_data
            FROM mart.municipality_values_semester
            WHERE municipality_id LIKE %s AND period_id = %s
        """, (f"{province_istat_prefix}%", period_id))
        data_result = self.cursor.fetchone()
        with_data = data_result['with_data'] if data_result else 0

        return with_data, total

    def is_province_complete(self, province_istat_prefix: str, period_ids: list[str], threshold: float = 0.95) -> bool:
        """
        Check if a province is essentially complete (>= threshold of municipalities have data).

        Args:
            province_istat_prefix: 3-digit province ISTAT prefix
            period_ids: List of periods to check (all must meet threshold)
            threshold: Completion threshold (default 95%)

        Returns:
            True if province is complete for all periods
        """
        for period_id in period_ids:
            with_data, total = self.get_province_completion_status(province_istat_prefix, period_id)
            if total == 0:
                return False
            completion_rate = with_data / total
            if completion_rate < threshold:
                return False
        return True

    def _normalize_name(self, name: str) -> str:
        """Normalize municipality name for matching - handle special characters."""
        # Convert to uppercase
        normalized = name.upper().strip()
        # Replace backticks with apostrophes (OMI uses ` but ISTAT uses ')
        normalized = normalized.replace('`', "'")
        return normalized

    def _strip_accents(self, name: str) -> str:
        """Strip accents and convert OMI backtick notation to base letters for matching.

        OMI uses backticks to represent accented vowels:
        - AGLIE` = Agliè
        - FORLI` = Forlì
        - CITTA` = Città
        """
        import unicodedata
        # First, remove trailing backticks that represent accents in OMI names
        result = name.replace("`", "")
        # Strip Unicode accents (è->e, ì->i, etc.)
        result = unicodedata.normalize('NFD', result)
        result = ''.join(c for c in result if unicodedata.category(c) != 'Mn')
        return result.upper().strip()

    def find_istat_code(self, codcom: str, comune_name: str, province_code: str) -> Optional[str]:
        """Find ISTAT code for a cadastral code, using mapping table and name matching."""
        # Check cache first
        if codcom in self._istat_cache:
            return self._istat_cache[codcom]

        # Retry wrapper for connection issues
        for attempt in range(3):
            try:
                return self._find_istat_code_inner(codcom, comune_name, province_code)
            except (psycopg2.ProgrammingError, psycopg2.InterfaceError, psycopg2.OperationalError) as e:
                logger.warning(f"DB error in find_istat_code (attempt {attempt + 1}/3): {e}")
                if attempt < 2:
                    time.sleep(1)
                    self._connect()
                else:
                    raise
        return None

    def _find_istat_code_inner(self, codcom: str, comune_name: str, province_code: str) -> Optional[str]:
        """Inner implementation of find_istat_code."""
        # Ensure connection is alive
        self._ensure_connection()

        # Try mapping table first
        self.cursor.execute("""
            SELECT municipality_id FROM core.cadastral_istat_mapping WHERE codcom = %s
        """, (codcom,))
        result = self.cursor.fetchone()
        if result:
            self._istat_cache[codcom] = result['municipality_id']
            return result['municipality_id']

        # Normalize name (handle backtick vs apostrophe)
        normalized = self._normalize_name(comune_name)

        # Exact match with normalized quotes
        self.cursor.execute("""
            SELECT municipality_id FROM core.municipalities WHERE UPPER(municipality_name) = %s
        """, (normalized,))
        result = self.cursor.fetchone()
        if result:
            self._save_mapping(codcom, result['municipality_id'], comune_name, province_code)
            return result['municipality_id']

        # Try matching without any quotes/apostrophes
        no_quotes = normalized.replace("'", "").replace("`", "")
        self.cursor.execute("""
            SELECT municipality_id FROM core.municipalities
            WHERE REPLACE(REPLACE(UPPER(municipality_name), '''', ''), '`', '') = %s
        """, (no_quotes,))
        result = self.cursor.fetchone()
        if result:
            self._save_mapping(codcom, result['municipality_id'], comune_name, province_code)
            return result['municipality_id']

        # Try prefix match with normalized name
        self.cursor.execute("""
            SELECT municipality_id FROM core.municipalities
            WHERE UPPER(municipality_name) LIKE %s LIMIT 1
        """, (f'{normalized}%',))
        result = self.cursor.fetchone()
        if result:
            self._save_mapping(codcom, result['municipality_id'], comune_name, province_code)
            return result['municipality_id']

        # Try accent-stripped matching (AGLIE` -> AGLIE, Agliè -> AGLIE)
        stripped = self._strip_accents(comune_name)

        # Build accent-stripped cache if empty
        if not self._accent_stripped_cache:
            self.cursor.execute("""
                SELECT municipality_id, municipality_name FROM core.municipalities
            """)
            all_munis = self.cursor.fetchall()
            for muni in all_munis:
                db_stripped = self._strip_accents(muni['municipality_name'])
                self._accent_stripped_cache[db_stripped] = muni['municipality_id']

        if stripped in self._accent_stripped_cache:
            muni_id = self._accent_stripped_cache[stripped]
            self._save_mapping(codcom, muni_id, comune_name, province_code)
            return muni_id

        # Not found
        self._istat_cache[codcom] = None
        return None

    def _save_mapping(self, codcom: str, istat_code: str, name: str, province_code: str):
        """Save cadastral to ISTAT mapping."""
        try:
            self.cursor.execute("""
                INSERT INTO core.cadastral_istat_mapping (codcom, municipality_id, municipality_name, province_code)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (codcom) DO UPDATE SET municipality_id = EXCLUDED.municipality_id
            """, (codcom, istat_code, name, province_code))
            self.conn.commit()
            self._istat_cache[codcom] = istat_code
        except Exception as e:
            self.conn.rollback()
            logger.debug(f"Could not save mapping {codcom} -> {istat_code}: {e}")

    def start_ingestion_run(self, source_name: str = "omi_values") -> int:
        """Create a new ingestion run record."""
        self.cursor.execute("""
            INSERT INTO admin.ingestion_runs (source_name, source_version, status, rows_loaded, rows_rejected)
            VALUES (%s, %s, 'started', 0, 0)
            RETURNING ingestion_run_id
        """, (source_name, datetime.now().strftime('%Y%m%d')))
        self.ingestion_run_id = self.cursor.fetchone()['ingestion_run_id']
        self.conn.commit()
        logger.info(f"Started ingestion run {self.ingestion_run_id}")
        return self.ingestion_run_id

    def complete_ingestion_run(self, rows_loaded: int, rows_rejected: int,
                               status: str = 'succeeded', error_notes: str = None):
        """Update ingestion run with final status."""
        if not self.ingestion_run_id:
            return

        self.cursor.execute("""
            UPDATE admin.ingestion_runs
            SET status = %s, rows_loaded = %s, rows_rejected = %s, error_notes = %s, finished_at = now()
            WHERE ingestion_run_id = %s
        """, (status, rows_loaded, rows_rejected, error_notes, self.ingestion_run_id))
        self.conn.commit()
        logger.info(f"Completed ingestion run {self.ingestion_run_id}: {status}")

    def ensure_time_period(self, semester_id: str):
        """Ensure time period exists for the semester."""
        year = int(semester_id[:4])
        sem = int(semester_id[4])
        period_id = f"{year}H{sem}"

        if sem == 1:
            start_date = f"{year}-01-01"
            end_date = f"{year}-06-30"
        else:
            start_date = f"{year}-07-01"
            end_date = f"{year}-12-31"

        self._ensure_connection()
        try:
            self.cursor.execute("""
                INSERT INTO core.time_periods (period_id, period_type, period_start_date, period_end_date, year, semester)
                VALUES (%s, 'semester', %s, %s, %s, %s)
                ON CONFLICT (period_id) DO NOTHING
            """, (period_id, start_date, end_date, year, sem))
            self.conn.commit()
            logger.debug(f"Ensured time period {period_id}")
        except Exception as e:
            self.conn.rollback()
            logger.warning(f"Could not ensure time period {period_id}: {e}")

    def upsert_omi_zone(self, zone: OMIZone, municipality_id: str):
        """Insert or update an OMI zone."""
        omi_zone_id = f"{municipality_id}_{zone.zone_code}"
        self._ensure_connection()

        try:
            self.cursor.execute("""
                INSERT INTO core.omi_zones (omi_zone_id, municipality_id, zone_code, zone_type, zone_description, microzone_code)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (omi_zone_id) DO UPDATE SET
                    zone_type = EXCLUDED.zone_type,
                    zone_description = EXCLUDED.zone_description,
                    microzone_code = EXCLUDED.microzone_code,
                    updated_at = now()
            """, (omi_zone_id, municipality_id, zone.zone_code, zone.zone_type, zone.zone_description, zone.microzone_code))
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            logger.warning(f"Failed to upsert zone {omi_zone_id}: {e}")
            return False

    def insert_property_values(self, values: list[PropertyValue]) -> tuple[int, int]:
        """Insert property values into raw.omi_property_values."""
        if not values:
            return 0, 0

        self._ensure_connection()
        loaded = 0
        rejected = 0

        for value in values:
            period_id = f"{value.semester_id[:4]}H{value.semester_id[4]}"
            # Normalize property type using mapping, fallback to lowercase original
            property_type = PROPERTY_TYPE_MAPPING.get(
                value.property_type.lower(),
                value.property_type.lower().replace(' ', '_')
            )

            raw_data = {
                'original_property_type': value.property_type,
                'source_url': value.source_url,
                'ingestion_timestamp': datetime.now().isoformat(),
            }

            try:
                self.cursor.execute("""
                    INSERT INTO raw.omi_property_values (
                        ingestion_run_id, omi_zone_id, municipality_id, period_id,
                        property_type, property_subtype, state,
                        value_min_eur_sqm, value_max_eur_sqm,
                        rent_min_eur_sqm_month, rent_max_eur_sqm_month,
                        source_file, raw_data
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    self.ingestion_run_id, value.omi_zone_id, value.municipality_id, period_id,
                    property_type, value.property_type, value.state,
                    value.value_min_eur_sqm, value.value_max_eur_sqm,
                    value.rent_min_eur_sqm_month, value.rent_max_eur_sqm_month,
                    value.source_url, Json(raw_data)
                ))
                loaded += 1
            except Exception as e:
                logger.debug(f"Failed to insert value: {e}")
                rejected += 1

        self.conn.commit()
        return loaded, rejected

    def aggregate_municipality_values(self, municipality_id: str, period_id: str):
        """Aggregate zone values to municipality level and insert into mart table for all segments."""
        self._ensure_connection()

        # Define property type to segment mappings
        segment_mappings = {
            'residential': ['residenziale'],
            'commercial': ['negozi', 'uffici', 'uffici_strutturati', 'centri_commerciali'],
            'industrial': ['capannoni', 'magazzini', 'laboratori'],
        }

        for segment, property_types in segment_mappings.items():
            try:
                # Build query with IN clause for property types - include state for condition premium
                placeholders = ','.join(['%s'] * len(property_types))
                self.cursor.execute(f"""
                    SELECT value_min_eur_sqm, value_max_eur_sqm, rent_min_eur_sqm_month, rent_max_eur_sqm_month, state
                    FROM raw.omi_property_values
                    WHERE municipality_id = %s AND period_id = %s AND property_type IN ({placeholders})
                """, (municipality_id, period_id, *property_types))
                rows = self.cursor.fetchall()

                if not rows:
                    continue

                val_mins = [r['value_min_eur_sqm'] for r in rows if r['value_min_eur_sqm'] is not None]
                val_maxs = [r['value_max_eur_sqm'] for r in rows if r['value_max_eur_sqm'] is not None]
                rent_mins = [r['rent_min_eur_sqm_month'] for r in rows if r['rent_min_eur_sqm_month'] is not None]
                rent_maxs = [r['rent_max_eur_sqm_month'] for r in rows if r['rent_max_eur_sqm_month'] is not None]

                value_mid = (sum(val_mins + val_maxs) / (len(val_mins) + len(val_maxs))) if (val_mins or val_maxs) else None
                rent_mid = (sum(rent_mins + rent_maxs) / (len(rent_mins) + len(rent_maxs))) if (rent_mins or rent_maxs) else None

                # Calculate condition premium (OTTIMO vs NORMALE)
                condition_premium = None
                ottimo_values = []
                normale_values = []
                for r in rows:
                    val = None
                    if r['value_min_eur_sqm'] is not None and r['value_max_eur_sqm'] is not None:
                        val = (r['value_min_eur_sqm'] + r['value_max_eur_sqm']) / 2
                    elif r['value_min_eur_sqm'] is not None:
                        val = r['value_min_eur_sqm']
                    elif r['value_max_eur_sqm'] is not None:
                        val = r['value_max_eur_sqm']

                    if val is not None:
                        if r['state'] == 'OTTIMO':
                            ottimo_values.append(val)
                        elif r['state'] == 'NORMALE':
                            normale_values.append(val)

                if ottimo_values and normale_values:
                    ottimo_avg = sum(ottimo_values) / len(ottimo_values)
                    normale_avg = sum(normale_values) / len(normale_values)
                    if normale_avg > 0:
                        condition_premium = ((ottimo_avg - normale_avg) / normale_avg) * 100

                self.cursor.execute("""
                    INSERT INTO mart.municipality_values_semester (
                        municipality_id, period_id, property_segment,
                        value_min_eur_sqm, value_max_eur_sqm, value_mid_eur_sqm,
                        rent_min_eur_sqm_month, rent_max_eur_sqm_month, rent_mid_eur_sqm_month,
                        zones_count, zones_with_data, condition_premium_pct, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (municipality_id, period_id, property_segment) DO UPDATE SET
                        value_min_eur_sqm = EXCLUDED.value_min_eur_sqm,
                        value_max_eur_sqm = EXCLUDED.value_max_eur_sqm,
                        value_mid_eur_sqm = EXCLUDED.value_mid_eur_sqm,
                        rent_min_eur_sqm_month = EXCLUDED.rent_min_eur_sqm_month,
                        rent_max_eur_sqm_month = EXCLUDED.rent_max_eur_sqm_month,
                        rent_mid_eur_sqm_month = EXCLUDED.rent_mid_eur_sqm_month,
                        zones_count = EXCLUDED.zones_count,
                        zones_with_data = EXCLUDED.zones_with_data,
                        condition_premium_pct = EXCLUDED.condition_premium_pct,
                        updated_at = now()
                """, (
                    municipality_id, period_id, segment,
                    min(val_mins) if val_mins else None,
                    max(val_maxs) if val_maxs else None,
                    value_mid,
                    min(rent_mins) if rent_mins else None,
                    max(rent_maxs) if rent_maxs else None,
                    rent_mid,
                    len(rows),
                    len(val_mins),
                    condition_premium
                ))
                self.conn.commit()
                logger.debug(f"Aggregated {segment} values for {municipality_id}/{period_id}")
            except Exception as e:
                self.conn.rollback()
                logger.warning(f"Failed to aggregate {segment} for {municipality_id}/{period_id}: {e}")

    def aggregate_zone_values(self, municipality_id: str, period_id: str):
        """Aggregate raw values to zone level for mart table for all segments."""
        self._ensure_connection()

        # Same segment mappings as municipality aggregation
        segment_mappings = {
            'residential': ['residenziale'],
            'commercial': ['negozi', 'uffici'],
            'industrial': ['capannoni', 'magazzini', 'laboratori'],
        }

        for segment, property_types in segment_mappings.items():
            try:
                placeholders = ','.join(['%s'] * len(property_types))
                self.cursor.execute(f"""
                    INSERT INTO mart.omi_zone_values_semester (
                        omi_zone_id, period_id, property_segment,
                        value_min_eur_sqm, value_max_eur_sqm, value_mid_eur_sqm,
                        rent_min_eur_sqm_month, rent_max_eur_sqm_month, rent_mid_eur_sqm_month,
                        zone_type, gross_yield_pct, data_quality_score, updated_at
                    )
                    SELECT
                        r.omi_zone_id,
                        r.period_id,
                        %s AS property_segment,
                        MIN(r.value_min_eur_sqm) AS value_min_eur_sqm,
                        MAX(r.value_max_eur_sqm) AS value_max_eur_sqm,
                        AVG((COALESCE(r.value_min_eur_sqm, 0) + COALESCE(r.value_max_eur_sqm, 0)) / 2)
                            FILTER (WHERE r.value_min_eur_sqm IS NOT NULL OR r.value_max_eur_sqm IS NOT NULL) AS value_mid_eur_sqm,
                        MIN(r.rent_min_eur_sqm_month) AS rent_min_eur_sqm_month,
                        MAX(r.rent_max_eur_sqm_month) AS rent_max_eur_sqm_month,
                        AVG((COALESCE(r.rent_min_eur_sqm_month, 0) + COALESCE(r.rent_max_eur_sqm_month, 0)) / 2)
                            FILTER (WHERE r.rent_min_eur_sqm_month IS NOT NULL OR r.rent_max_eur_sqm_month IS NOT NULL) AS rent_mid_eur_sqm_month,
                        z.zone_type,
                        CASE
                            WHEN AVG((COALESCE(r.value_min_eur_sqm, 0) + COALESCE(r.value_max_eur_sqm, 0)) / 2)
                                FILTER (WHERE r.value_min_eur_sqm IS NOT NULL OR r.value_max_eur_sqm IS NOT NULL) > 0
                            THEN ROUND(
                                (AVG((COALESCE(r.rent_min_eur_sqm_month, 0) + COALESCE(r.rent_max_eur_sqm_month, 0)) / 2)
                                    FILTER (WHERE r.rent_min_eur_sqm_month IS NOT NULL OR r.rent_max_eur_sqm_month IS NOT NULL) * 12
                                / AVG((COALESCE(r.value_min_eur_sqm, 0) + COALESCE(r.value_max_eur_sqm, 0)) / 2)
                                    FILTER (WHERE r.value_min_eur_sqm IS NOT NULL OR r.value_max_eur_sqm IS NOT NULL)) * 100
                            , 2)
                            ELSE NULL
                        END AS gross_yield_pct,
                        100.0 AS data_quality_score,
                        NOW() AS updated_at
                    FROM raw.omi_property_values r
                    LEFT JOIN core.omi_zones z ON r.omi_zone_id = z.omi_zone_id
                    WHERE r.municipality_id = %s
                      AND r.period_id = %s
                      AND r.property_type IN ({placeholders})
                      AND r.state IN ('NORMALE', 'normale', NULL)
                      AND r.omi_zone_id IS NOT NULL
                    GROUP BY r.omi_zone_id, r.period_id, z.zone_type
                    ON CONFLICT (omi_zone_id, period_id, property_segment)
                    DO UPDATE SET
                        value_min_eur_sqm = EXCLUDED.value_min_eur_sqm,
                        value_max_eur_sqm = EXCLUDED.value_max_eur_sqm,
                        value_mid_eur_sqm = EXCLUDED.value_mid_eur_sqm,
                        rent_min_eur_sqm_month = EXCLUDED.rent_min_eur_sqm_month,
                        rent_max_eur_sqm_month = EXCLUDED.rent_max_eur_sqm_month,
                        rent_mid_eur_sqm_month = EXCLUDED.rent_mid_eur_sqm_month,
                        zone_type = EXCLUDED.zone_type,
                        gross_yield_pct = EXCLUDED.gross_yield_pct,
                        data_quality_score = EXCLUDED.data_quality_score,
                        updated_at = EXCLUDED.updated_at
                """, (segment, municipality_id, period_id, *property_types))

                zones_aggregated = self.cursor.rowcount
                self.conn.commit()
                if zones_aggregated > 0:
                    logger.debug(f"Aggregated {zones_aggregated} {segment} zones for {municipality_id}/{period_id}")
            except Exception as e:
                self.conn.rollback()
                logger.warning(f"Failed to aggregate {segment} zones for {municipality_id}/{period_id}: {e}")

    def delete_raw_municipality_data(self, municipality_id: str, period_id: str):
        """Delete raw data for a municipality after aggregation to save storage."""
        self._ensure_connection()
        try:
            self.cursor.execute("""
                DELETE FROM raw.omi_property_values
                WHERE municipality_id = %s AND period_id = %s
            """, (municipality_id, period_id))
            deleted = self.cursor.rowcount
            self.conn.commit()
            if deleted > 0:
                logger.debug(f"Deleted {deleted} raw records for {municipality_id}/{period_id}")
        except Exception as e:
            self.conn.rollback()
            logger.warning(f"Failed to delete raw data for {municipality_id}/{period_id}: {e}")

    def close(self):
        """Close database connection."""
        self.cursor.close()
        self.conn.close()

    def calculate_zone_change_metrics(self):
        """Calculate value_pct_change_1s and value_pct_change_2s for all zone values."""
        self._ensure_connection()

        logger.info("Calculating zone change metrics...")

        try:
            # Update zone premium vs municipality
            logger.info("  Calculating zone premium vs municipality...")
            self.cursor.execute("""
                WITH municipality_avg AS (
                    SELECT
                        m.municipality_id,
                        m.period_id,
                        m.property_segment,
                        m.value_mid_eur_sqm AS muni_avg
                    FROM mart.municipality_values_semester m
                )
                UPDATE mart.omi_zone_values_semester z
                SET zone_premium_vs_municipality = ROUND(
                    ((z.value_mid_eur_sqm - ma.muni_avg) / NULLIF(ma.muni_avg, 0)) * 100, 2
                )
                FROM core.omi_zones oz, municipality_avg ma
                WHERE z.omi_zone_id = oz.omi_zone_id
                  AND oz.municipality_id = ma.municipality_id
                  AND z.period_id = ma.period_id
                  AND z.property_segment = ma.property_segment
                  AND z.value_mid_eur_sqm IS NOT NULL
                  AND ma.muni_avg IS NOT NULL
            """)
            premium_updated = self.cursor.rowcount
            logger.info(f"  Updated {premium_updated} zone premium values")

            # Calculate change metrics (vs prior semester and prior year)
            logger.info("  Calculating semester-over-semester and year-over-year changes...")
            self.cursor.execute("""
                WITH zone_values AS (
                    SELECT
                        omi_zone_id,
                        period_id,
                        property_segment,
                        value_mid_eur_sqm,
                        -- Previous semester (1 semester back)
                        CASE
                            WHEN period_id LIKE '%H1' THEN CONCAT(CAST(LEFT(period_id, 4)::int - 1 AS text), 'H2')
                            WHEN period_id LIKE '%H2' THEN CONCAT(LEFT(period_id, 4), 'H1')
                        END AS prev_1s_period,
                        -- Previous year same semester (2 semesters back)
                        CASE
                            WHEN period_id LIKE '%H1' THEN CONCAT(CAST(LEFT(period_id, 4)::int - 1 AS text), 'H1')
                            WHEN period_id LIKE '%H2' THEN CONCAT(CAST(LEFT(period_id, 4)::int - 1 AS text), 'H2')
                        END AS prev_2s_period
                    FROM mart.omi_zone_values_semester
                ),
                with_prev_values AS (
                    SELECT
                        zv.omi_zone_id,
                        zv.period_id,
                        zv.property_segment,
                        zv.value_mid_eur_sqm,
                        prev1.value_mid_eur_sqm AS prev_1s_value,
                        prev2.value_mid_eur_sqm AS prev_2s_value
                    FROM zone_values zv
                    LEFT JOIN mart.omi_zone_values_semester prev1
                        ON zv.omi_zone_id = prev1.omi_zone_id
                        AND zv.prev_1s_period = prev1.period_id
                        AND zv.property_segment = prev1.property_segment
                    LEFT JOIN mart.omi_zone_values_semester prev2
                        ON zv.omi_zone_id = prev2.omi_zone_id
                        AND zv.prev_2s_period = prev2.period_id
                        AND zv.property_segment = prev2.property_segment
                )
                UPDATE mart.omi_zone_values_semester m
                SET
                    value_pct_change_1s = ROUND(((wpv.value_mid_eur_sqm - wpv.prev_1s_value) / NULLIF(wpv.prev_1s_value, 0)) * 100, 2),
                    value_pct_change_2s = ROUND(((wpv.value_mid_eur_sqm - wpv.prev_2s_value) / NULLIF(wpv.prev_2s_value, 0)) * 100, 2)
                FROM with_prev_values wpv
                WHERE m.omi_zone_id = wpv.omi_zone_id
                  AND m.period_id = wpv.period_id
                  AND m.property_segment = wpv.property_segment
            """)
            changes_updated = self.cursor.rowcount
            self.conn.commit()
            logger.info(f"  Updated {changes_updated} zone change metrics")

            # Summary stats
            self.cursor.execute("""
                SELECT
                    COUNT(DISTINCT omi_zone_id) AS zones,
                    COUNT(DISTINCT period_id) AS periods,
                    COUNT(*) AS total_rows,
                    COUNT(value_pct_change_1s) AS with_1s_change,
                    COUNT(value_pct_change_2s) AS with_2s_change
                FROM mart.omi_zone_values_semester
            """)
            stats = self.cursor.fetchone()
            logger.info(f"Zone values summary: {stats[0]} zones, {stats[1]} periods, {stats[2]} total rows")
            logger.info(f"  With 1-semester change: {stats[3]}, with YoY change: {stats[4]}")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to calculate zone change metrics: {e}")
            raise


def load_env_variables() -> dict:
    """Load database connection parameters from environment."""
    # Try multiple env file locations
    env_paths = [
        Path(__file__).parent.parent.parent / 'frontend' / '.env.local',
        Path(__file__).parent.parent.parent / '.env',
        Path(__file__).parent / '.env',
    ]

    for env_path in env_paths:
        if env_path.exists():
            load_dotenv(env_path)
            logger.info(f"Loaded environment from {env_path}")
            break

    # Check for DB_* parameters (preferred for Supabase pooler)
    db_host = os.getenv('DB_HOST')
    db_password = os.getenv('DB_PASSWORD')

    if db_host and db_password:
        return {
            'host': db_host,
            'port': os.getenv('DB_PORT', '5432'),
            'dbname': os.getenv('DB_NAME', 'postgres'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': db_password,
        }

    raise OMIIngestionError(
        "Missing database credentials. Set DB_HOST, DB_PASSWORD, DB_PORT, DB_NAME, DB_USER "
        "environment variables in frontend/.env.local"
    )


def run_ingestion(
    provinces: list[str] = None,
    semesters: list[str] = None,
    test_mode: bool = False,
    skip_geometries: bool = False,
    skip_values: bool = False,
    skip_loaded: bool = False,
    municipalities: list[str] = None,
    max_retries: int = 3,
    municipality_timeout: int = MUNICIPALITY_TIMEOUT_SECONDS,
    force_zone_reload: bool = False,
):
    """
    Main ingestion function.

    Args:
        provinces: List of province codes to process (e.g., ['RM', 'MI'])
        semesters: List of semesters to load (e.g., ['20242', '20241'])
        test_mode: If True, only process Roma with limited data
        skip_geometries: Skip fetching zone geometries
        skip_values: Skip scraping property values (only load zones)
        skip_loaded: Skip municipalities that already have data for the requested semester
        municipalities: List of specific ISTAT codes to process (bypasses province iteration)
        max_retries: Maximum retry attempts for failed municipalities
        municipality_timeout: Max seconds per municipality before auto-skip
    """
    logger.info("=" * 60)
    logger.info("Starting OMI Property Values Ingestion")
    logger.info(f"Municipality timeout: {municipality_timeout}s, Stall detection: {STALL_DETECTION_SECONDS}s")
    logger.info("=" * 60)

    # Load credentials
    db_params = load_env_variables()

    # Initialize progress tracker and clients
    progress_tracker = ProgressTracker()
    omi_client = OMIClient(progress_tracker=progress_tracker)
    db_loader = DatabaseLoader(db_params)

    # Start ingestion run
    db_loader.start_ingestion_run("omi_values")

    total_loaded = 0
    total_rejected = 0
    failed_municipalities = []  # Track failed municipalities for end-of-run retry
    timed_out_municipalities = []  # Track municipalities that timed out

    def process_municipality_inner(comune, istat_code: str, result_holder: dict):
        """
        Inner function to process a municipality.
        Results stored in result_holder dict for thread-safe access.
        """
        nonlocal db_loader
        loaded = 0
        rejected = 0

        try:
            # Record start of municipality processing
            progress_tracker.heartbeat(f"Starting {comune.name}")

            # Ensure connection is healthy before starting
            db_loader._ensure_connection()

            # Get zones for this comune (using cadastral code for API)
            progress_tracker.heartbeat(f"Getting zones for {comune.name}")
            zones = omi_client.get_zones(comune.codcom)

            if not zones:
                logger.debug(f"    No zones found for {comune.codcom}")
                result_holder['result'] = (0, 0, True)
                return

            # Process each zone
            for zone_idx, zone in enumerate(zones):
                progress_tracker.heartbeat(f"{comune.name} zone {zone_idx + 1}/{len(zones)}: {zone.zone_code}")

                # Store zone definition (using ISTAT code for database)
                db_loader.upsert_omi_zone(zone, istat_code)

                if skip_values:
                    continue

                # For each semester, get property values
                for semester in semesters:
                    try:
                        progress_tracker.heartbeat(f"{comune.name}/{zone.zone_code}/{semester}")
                        values = omi_client.get_property_values(
                            comune.codcom, zone.zone_code, semester
                        )

                        # Update municipality_id to use ISTAT code
                        for val in values:
                            val.municipality_id = istat_code
                            val.omi_zone_id = f"{istat_code}_{zone.zone_code}"

                        if values:
                            zone_loaded, zone_rejected = db_loader.insert_property_values(values)
                            loaded += zone_loaded
                            rejected += zone_rejected

                            if zone_loaded > 0:
                                logger.debug(f"      Zone {zone.zone_code}/{semester}: {zone_loaded} values")

                    except OMIIngestionError as e:
                        logger.debug(f"      Error for zone {zone.zone_code}: {e}")
                        rejected += 1

            # Aggregate to municipality level and clean up raw data
            if not skip_values:
                for semester in semesters:
                    progress_tracker.heartbeat(f"Aggregating {comune.name}/{semester}")
                    period_id = f"{semester[:4]}H{semester[4]}"
                    db_loader.aggregate_municipality_values(istat_code, period_id)
                    db_loader.aggregate_zone_values(istat_code, period_id)
                    # Delete raw data after aggregation to save storage
                    db_loader.delete_raw_municipality_data(istat_code, period_id)

            result_holder['result'] = (loaded, rejected, True)

        except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
            result_holder['error'] = ('connection', e)
        except OMIIngestionError as e:
            result_holder['error'] = ('omi', e)
        except Exception as e:
            result_holder['error'] = ('unexpected', e)

    def process_municipality(comune, istat_code: str, com_idx: int, total: int) -> tuple[int, int, bool]:
        """
        Process a single municipality with retry logic and timeout.
        Returns (loaded_count, rejected_count, success).
        """
        nonlocal db_loader

        for attempt in range(1, max_retries + 1):
            result_holder = {}
            start_time = time.time()

            # Run processing in a thread so we can enforce timeout
            thread = threading.Thread(
                target=process_municipality_inner,
                args=(comune, istat_code, result_holder)
            )
            thread.daemon = True
            thread.start()

            # Wait with periodic stall checks
            check_interval = min(30, STALL_DETECTION_SECONDS / 2)
            elapsed = 0

            while thread.is_alive() and elapsed < municipality_timeout:
                thread.join(timeout=check_interval)
                elapsed = time.time() - start_time

                # Check for stalls
                stall_time = progress_tracker.check_stall()
                if stall_time:
                    current_op = progress_tracker.get_current_operation()
                    logger.warning(f"    STALL DETECTED: No progress for {stall_time:.0f}s - last operation: {current_op}")

            # Check if thread is still running (timeout occurred)
            if thread.is_alive():
                logger.error(f"    TIMEOUT: {comune.name} exceeded {municipality_timeout}s limit after {elapsed:.0f}s")
                logger.error(f"    Last operation: {progress_tracker.get_current_operation()}")
                # Thread will be abandoned (daemon thread will be killed when main thread exits)
                # Force reconnect with timeout to prevent infinite hangs
                logger.info(f"    Reconnecting to database after timeout...")
                reconnect_start = time.time()
                reconnect_timeout = 30  # Max 30 seconds to reconnect
                try:
                    # Close existing connections first
                    try:
                        if db_loader.conn:
                            db_loader.conn.close()
                    except Exception:
                        pass
                    db_loader.conn = None
                    db_loader.cursor = None

                    # Reconnect with timeout
                    db_loader._connect()
                    logger.info(f"    Reconnected in {time.time() - reconnect_start:.1f}s")
                except Exception as e:
                    logger.warning(f"    Reconnect failed after {time.time() - reconnect_start:.1f}s: {e}")
                    # Continue anyway - next municipality will retry connection

                logger.info(f"    Continuing to next municipality...")
                return 0, 0, False

            # Thread completed - check results
            if 'result' in result_holder:
                return result_holder['result']

            if 'error' in result_holder:
                error_type, error = result_holder['error']

                if error_type == 'connection':
                    logger.warning(f"    Connection error for {comune.name} (attempt {attempt}/{max_retries}): {error}")
                    if attempt < max_retries:
                        wait_time = 2 ** attempt
                        logger.info(f"    Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        db_loader._connect()
                        continue
                    else:
                        logger.error(f"    Failed after {max_retries} attempts: {comune.name}")
                        return 0, 0, False

                elif error_type == 'omi':
                    logger.warning(f"    OMI error processing {comune.name}: {error}")
                    return 0, 0, True  # OMI errors are not retryable

                else:  # unexpected
                    logger.warning(f"    Unexpected error for {comune.name} (attempt {attempt}/{max_retries}): {error}")
                    if attempt < max_retries:
                        wait_time = 2 ** attempt
                        logger.info(f"    Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        try:
                            db_loader._connect()
                        except Exception:
                            pass
                        continue
                    else:
                        logger.error(f"    Failed after {max_retries} attempts: {comune.name}")
                        return 0, 0, False

        return 0, 0, False

    try:
        # Get available semesters if not specified
        if not semesters:
            available_semesters = omi_client.get_available_semesters()
            # Default to latest 2 semesters
            semesters = available_semesters[:2] if available_semesters else ['20242']

        logger.info(f"Processing semesters: {semesters}")

        # Ensure time periods exist
        for sem in semesters:
            db_loader.ensure_time_period(sem)

        # Get provinces
        all_provinces = omi_client.get_provinces()

        # Handle specific municipalities mode
        if municipalities:
            logger.info(f"Processing {len(municipalities)} specific municipalities by ISTAT code")

            # Build lookup of all comuni across all provinces
            all_comuni = {}
            for province in all_provinces:
                try:
                    comuni = omi_client.get_comuni(province.code)
                    for comune in comuni:
                        # Find the ISTAT code for this comune
                        istat_code = db_loader.find_istat_code(comune.codcom, comune.name, comune.province_code)
                        if istat_code:
                            all_comuni[istat_code] = comune
                            comune.istat_code = istat_code
                except OMIIngestionError:
                    continue

            # Process requested municipalities
            for idx, istat_code in enumerate(municipalities):
                if istat_code not in all_comuni:
                    logger.warning(f"  [{idx + 1}/{len(municipalities)}] ISTAT {istat_code} not found in OMI data")
                    continue

                comune = all_comuni[istat_code]
                logger.info(f"  [{idx + 1}/{len(municipalities)}] {comune.name} ({comune.codcom} -> {istat_code})")

                start_time = time.time()
                loaded, rejected, success = process_municipality(comune, istat_code, idx, len(municipalities))
                elapsed = time.time() - start_time
                total_loaded += loaded
                total_rejected += rejected

                if not success:
                    # Timed out municipalities get separate tracking (may need different retry approach)
                    if elapsed >= municipality_timeout * 0.9:
                        logger.warning(f"    Municipality {comune.name} added to timeout list")
                        timed_out_municipalities.append((comune, istat_code))
                    else:
                        failed_municipalities.append((comune, istat_code))

        else:
            # Standard province-based processing
            if test_mode:
                # In test mode, just use Roma
                provinces = ['RM']
                logger.info("Test mode: processing only Roma")
            elif provinces:
                # Filter to requested provinces
                province_codes = set(p.upper() for p in provinces)
                all_provinces = [p for p in all_provinces if p.code.upper() in province_codes]
                logger.info(f"Processing {len(all_provinces)} provinces: {[p.code for p in all_provinces]}")
            else:
                logger.info(f"Processing all {len(all_provinces)} provinces")

            # Process each province
            for prov_idx, province in enumerate(all_provinces):
                logger.info(f"\n[{prov_idx + 1}/{len(all_provinces)}] Processing province: {province.name} ({province.code})")

                try:
                    comuni = omi_client.get_comuni(province.code)
                except OMIIngestionError as e:
                    logger.error(f"Failed to get comuni for {province.code}: {e}")
                    continue

                # In test mode, limit to first 5 comuni
                if test_mode:
                    comuni = comuni[:5]

                # Check if province is already complete (skip entire province if so)
                # Skip unless force_zone_reload is set (which forces re-ingestion for zone data)
                if skip_loaded and not skip_values and comuni and not force_zone_reload:
                    # Get the ISTAT prefix from the first comune we can resolve
                    province_istat_prefix = None
                    for sample_comune in comuni[:5]:  # Check first few comuni
                        sample_istat = db_loader.find_istat_code(sample_comune.codcom, sample_comune.name, sample_comune.province_code)
                        if sample_istat and len(sample_istat) >= 3:
                            province_istat_prefix = sample_istat[:3]
                            break

                    if province_istat_prefix:
                        period_ids = [f"{sem[:4]}H{sem[4]}" for sem in semesters]
                        if db_loader.is_province_complete(province_istat_prefix, period_ids):
                            logger.info(f"  Province {province.name} is already complete (>=95%), skipping entirely")
                            continue

                for com_idx, comune in enumerate(comuni):
                    # Look up ISTAT code from cadastral code
                    istat_code = db_loader.find_istat_code(comune.codcom, comune.name, comune.province_code)
                    if not istat_code:
                        logger.debug(f"  Skipping {comune.name} ({comune.codcom}) - no ISTAT mapping found")
                        continue

                    comune.istat_code = istat_code

                    # Check if municipality already has data for requested semesters
                    # Skip unless force_zone_reload is set (which forces re-ingestion for zone data)
                    if skip_loaded and not skip_values and not force_zone_reload:
                        period_id = f"{semesters[0][:4]}H{semesters[0][4]}"
                        if db_loader.municipality_has_data(istat_code, period_id):
                            logger.debug(f"  [{com_idx + 1}/{len(comuni)}] {comune.name} - skipping (already has data)")
                            continue

                    logger.info(f"  [{com_idx + 1}/{len(comuni)}] {comune.name} ({comune.codcom} -> {istat_code})")

                    start_time = time.time()
                    loaded, rejected, success = process_municipality(comune, istat_code, com_idx, len(comuni))
                    elapsed = time.time() - start_time
                    total_loaded += loaded
                    total_rejected += rejected

                    if not success:
                        if elapsed >= municipality_timeout * 0.9:
                            logger.warning(f"    Municipality {comune.name} added to timeout list")
                            timed_out_municipalities.append((comune, istat_code))
                        else:
                            failed_municipalities.append((comune, istat_code))

        # Retry failed municipalities at the end (but not timed-out ones)
        if failed_municipalities:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"Retrying {len(failed_municipalities)} failed municipalities...")
            logger.info("=" * 60)

            still_failed = []
            for idx, (comune, istat_code) in enumerate(failed_municipalities):
                logger.info(f"  [Retry {idx + 1}/{len(failed_municipalities)}] {comune.name} ({istat_code})")

                # Wait a bit before retrying
                time.sleep(3)

                start_time = time.time()
                loaded, rejected, success = process_municipality(comune, istat_code, idx, len(failed_municipalities))
                elapsed = time.time() - start_time
                total_loaded += loaded
                total_rejected += rejected

                if not success:
                    if elapsed >= municipality_timeout * 0.9:
                        timed_out_municipalities.append((comune, istat_code))
                    else:
                        still_failed.append((comune.name, istat_code))

            if still_failed:
                logger.error(f"\n{len(still_failed)} municipalities still failed after retry:")
                for name, istat in still_failed:
                    logger.error(f"  - {name} ({istat})")

        # Log timed-out municipalities separately
        if timed_out_municipalities:
            logger.warning(f"\n{len(timed_out_municipalities)} municipalities timed out (exceeded {municipality_timeout}s):")
            for comune, istat in timed_out_municipalities:
                logger.warning(f"  - {comune.name} ({istat}) - may need manual retry with longer timeout")

        # Complete ingestion run
        db_loader.complete_ingestion_run(
            rows_loaded=total_loaded,
            rows_rejected=total_rejected,
            status='succeeded'
        )

        logger.info("\n" + "=" * 60)
        logger.info("Ingestion Complete!")
        logger.info(f"  Rows loaded: {total_loaded}")
        logger.info(f"  Rows rejected: {total_rejected}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        db_loader.complete_ingestion_run(
            rows_loaded=total_loaded,
            rows_rejected=total_rejected,
            status='failed',
            error_notes=str(e)
        )
        raise
    finally:
        db_loader.close()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Load OMI property values from Agenzia delle Entrate'
    )

    parser.add_argument(
        '--provinces', '-p',
        nargs='+',
        help='Province codes to process (e.g., RM MI TO). If not specified, all provinces are processed.'
    )

    parser.add_argument(
        '--semesters', '-s',
        nargs='+',
        help='Semesters to load (e.g., 20242 20241). Format: YYYYS where S is 1 or 2.'
    )

    parser.add_argument(
        '--test', '-t',
        action='store_true',
        help='Run in test mode (only Roma, limited data)'
    )

    parser.add_argument(
        '--skip-geometries',
        action='store_true',
        help='Skip fetching zone geometries'
    )

    parser.add_argument(
        '--skip-values',
        action='store_true',
        help='Skip scraping property values (only load zone definitions)'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    parser.add_argument(
        '--skip-loaded',
        action='store_true',
        help='Skip municipalities that already have data for the requested semester'
    )

    parser.add_argument(
        '--municipalities', '-m',
        nargs='+',
        help='Specific municipality ISTAT codes to process (e.g., 052032 058104). Bypasses province-level iteration.'
    )

    parser.add_argument(
        '--max-retries',
        type=int,
        default=3,
        help='Maximum retries for failed municipalities (default: 3)'
    )

    parser.add_argument(
        '--municipality-timeout',
        type=int,
        default=MUNICIPALITY_TIMEOUT_SECONDS,
        help=f'Maximum seconds per municipality before auto-skip (default: {MUNICIPALITY_TIMEOUT_SECONDS})'
    )

    parser.add_argument(
        '--force-zone-reload',
        action='store_true',
        help='Force re-ingestion to populate zone-level data (ignores existing municipality data)'
    )

    parser.add_argument(
        '--calculate-zone-changes',
        action='store_true',
        help='Calculate zone change metrics (run after historical data is fully ingested)'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        # Handle zone change calculation separately
        if args.calculate_zone_changes:
            db_params = load_env_variables()
            db_loader = DatabaseLoader(db_params)
            try:
                db_loader.calculate_zone_change_metrics()
                logger.info("Zone change metrics calculation complete!")
            finally:
                db_loader.close()
            sys.exit(0)

        run_ingestion(
            provinces=args.provinces,
            semesters=args.semesters,
            test_mode=args.test,
            skip_geometries=args.skip_geometries,
            skip_values=args.skip_values,
            skip_loaded=args.skip_loaded,
            municipalities=args.municipalities,
            max_retries=args.max_retries,
            municipality_timeout=args.municipality_timeout,
            force_zone_reload=args.force_zone_reload,
        )
    except KeyboardInterrupt:
        logger.info("\nIngestion interrupted by user")
        sys.exit(1)
    except OMIIngestionError as e:
        logger.error(f"Ingestion error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
