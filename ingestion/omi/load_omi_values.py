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
import sys
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

# Property types to focus on (residential)
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

# Property type normalization mapping
PROPERTY_TYPE_MAPPING = {
    'abitazioni civili': 'residenziale_civile',
    'abitazioni di tipo economico': 'residenziale_economico',
    'abitazioni signorili': 'residenziale_signorile',
    'ville e villini': 'ville_villini',
    'box': 'box',
    'posti auto coperti': 'posto_auto_coperto',
    'posti auto scoperti': 'posto_auto_scoperto',
    'autorimesse': 'autorimessa',
    'negozi': 'negozi',
    'uffici': 'uffici',
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
    istat_code: str  # 6-digit ISTAT code
    name: str
    province_code: str


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

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/html, */*',
            'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
        })

    def _make_request(self, url: str, params: Optional[dict] = None,
                      expect_json: bool = True) -> Any:
        """Make HTTP request with retries and rate limiting."""
        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(REQUEST_DELAY_SECONDS)
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()

                if expect_json:
                    return response.json()
                return response.text

            except requests.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY_SECONDS)
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
                    istat_code=str(item[0]),
                    name=item[1],
                    province_code=province_code
                ))
            elif isinstance(item, dict):
                # Handle OMI API format: {'DIZIONE': 'ROMA', 'CODCOM': 'H501'}
                code = item.get('CODCOM', item.get('codice', item.get('code', '')))
                name = item.get('DIZIONE', item.get('nome', item.get('name', '')))
                if code:
                    comuni.append(Comune(
                        istat_code=str(code),  # This is actually the cadastral code
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

    def get_property_values(self, istat_code: str, zone_code: str,
                           semester: str) -> list[PropertyValue]:
        """
        Scrape property values from the risultato.php page.

        This is the main scraping function that extracts min/max values
        from the HTML tables on the OMI website.
        """
        # Build the URL for the results page
        # Format semester: "20242" -> "2024-2"
        sem_formatted = f"{semester[:4]}-{semester[4:]}"

        url = urljoin(OMI_BASE_URL, "risultato.php")
        params = {
            'codcom': istat_code,
            'zona': zone_code,
            'semestre': sem_formatted,
        }

        source_url = f"{url}?codcom={istat_code}&zona={zone_code}&semestre={sem_formatted}"

        try:
            html = self._make_request(url, params=params, expect_json=False)
            return self._parse_values_html(
                html, istat_code, zone_code, semester, source_url
            )
        except OMIIngestionError as e:
            logger.warning(f"Failed to get values for {istat_code}/{zone_code}: {e}")
            return []

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
        self.conn = psycopg2.connect(**db_params)
        self.conn.autocommit = False
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        self.ingestion_run_id: Optional[int] = None
        logger.info("Connected to database via direct PostgreSQL connection")

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

        loaded = 0
        rejected = 0

        for value in values:
            period_id = f"{value.semester_id[:4]}H{value.semester_id[4]}"
            property_type = 'residenziale' if any(x in value.property_type for x in ['residen', 'abitaz', 'ville']) else value.property_type

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
        """Aggregate zone values to municipality level and insert into mart table."""
        try:
            self.cursor.execute("""
                SELECT value_min_eur_sqm, value_max_eur_sqm, rent_min_eur_sqm_month, rent_max_eur_sqm_month
                FROM raw.omi_property_values
                WHERE municipality_id = %s AND period_id = %s AND property_type = 'residenziale'
            """, (municipality_id, period_id))
            rows = self.cursor.fetchall()

            if not rows:
                return

            val_mins = [r['value_min_eur_sqm'] for r in rows if r['value_min_eur_sqm'] is not None]
            val_maxs = [r['value_max_eur_sqm'] for r in rows if r['value_max_eur_sqm'] is not None]
            rent_mins = [r['rent_min_eur_sqm_month'] for r in rows if r['rent_min_eur_sqm_month'] is not None]
            rent_maxs = [r['rent_max_eur_sqm_month'] for r in rows if r['rent_max_eur_sqm_month'] is not None]

            value_mid = (sum(val_mins + val_maxs) / (len(val_mins) + len(val_maxs))) if (val_mins or val_maxs) else None
            rent_mid = (sum(rent_mins + rent_maxs) / (len(rent_mins) + len(rent_maxs))) if (rent_mins or rent_maxs) else None

            self.cursor.execute("""
                INSERT INTO mart.municipality_values_semester (
                    municipality_id, period_id, property_segment,
                    value_min_eur_sqm, value_max_eur_sqm, value_mid_eur_sqm,
                    rent_min_eur_sqm_month, rent_max_eur_sqm_month, rent_mid_eur_sqm_month,
                    zones_count, zones_with_data, updated_at
                ) VALUES (%s, %s, 'residential', %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (municipality_id, period_id, property_segment) DO UPDATE SET
                    value_min_eur_sqm = EXCLUDED.value_min_eur_sqm,
                    value_max_eur_sqm = EXCLUDED.value_max_eur_sqm,
                    value_mid_eur_sqm = EXCLUDED.value_mid_eur_sqm,
                    rent_min_eur_sqm_month = EXCLUDED.rent_min_eur_sqm_month,
                    rent_max_eur_sqm_month = EXCLUDED.rent_max_eur_sqm_month,
                    rent_mid_eur_sqm_month = EXCLUDED.rent_mid_eur_sqm_month,
                    zones_count = EXCLUDED.zones_count,
                    zones_with_data = EXCLUDED.zones_with_data,
                    updated_at = now()
            """, (
                municipality_id, period_id,
                min(val_mins) if val_mins else None,
                max(val_maxs) if val_maxs else None,
                value_mid,
                min(rent_mins) if rent_mins else None,
                max(rent_maxs) if rent_maxs else None,
                rent_mid,
                len(rows),
                len(val_mins)
            ))
            self.conn.commit()
            logger.debug(f"Aggregated values for {municipality_id}/{period_id}")
        except Exception as e:
            self.conn.rollback()
            logger.warning(f"Failed to aggregate {municipality_id}/{period_id}: {e}")

    def close(self):
        """Close database connection."""
        self.cursor.close()
        self.conn.close()


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
):
    """
    Main ingestion function.

    Args:
        provinces: List of province codes to process (e.g., ['RM', 'MI'])
        semesters: List of semesters to load (e.g., ['20242', '20241'])
        test_mode: If True, only process Roma with limited data
        skip_geometries: Skip fetching zone geometries
        skip_values: Skip scraping property values (only load zones)
    """
    logger.info("=" * 60)
    logger.info("Starting OMI Property Values Ingestion")
    logger.info("=" * 60)

    # Load credentials
    db_params = load_env_variables()

    # Initialize clients
    omi_client = OMIClient()
    db_loader = DatabaseLoader(db_params)

    # Start ingestion run
    db_loader.start_ingestion_run("omi_values")

    total_loaded = 0
    total_rejected = 0

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

            for com_idx, comune in enumerate(comuni):
                logger.info(f"  [{com_idx + 1}/{len(comuni)}] {comune.name} ({comune.istat_code})")

                try:
                    # Get zones for this comune
                    zones = omi_client.get_zones(comune.istat_code)

                    if not zones:
                        logger.debug(f"    No zones found for {comune.istat_code}")
                        continue

                    # Process each zone
                    for zone in zones:
                        # Store zone definition
                        db_loader.upsert_omi_zone(zone, comune.istat_code)

                        if skip_values:
                            continue

                        # For each semester, get property values
                        for semester in semesters:
                            try:
                                values = omi_client.get_property_values(
                                    comune.istat_code, zone.zone_code, semester
                                )

                                if values:
                                    loaded, rejected = db_loader.insert_property_values(values)
                                    total_loaded += loaded
                                    total_rejected += rejected

                                    if loaded > 0:
                                        logger.debug(f"      Zone {zone.zone_code}/{semester}: {loaded} values")

                            except OMIIngestionError as e:
                                logger.debug(f"      Error for zone {zone.zone_code}: {e}")
                                total_rejected += 1

                    # Aggregate to municipality level
                    if not skip_values:
                        for semester in semesters:
                            period_id = f"{semester[:4]}H{semester[4]}"
                            db_loader.aggregate_municipality_values(comune.istat_code, period_id)

                except OMIIngestionError as e:
                    logger.warning(f"    Error processing {comune.name}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"    Unexpected error for {comune.name}: {e}")
                    continue

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

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        run_ingestion(
            provinces=args.provinces,
            semesters=args.semesters,
            test_mode=args.test,
            skip_geometries=args.skip_geometries,
            skip_values=args.skip_values,
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
