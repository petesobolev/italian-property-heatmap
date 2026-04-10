#!/usr/bin/env python3
"""
OMI Zone Boundaries Ingestion Script

Loads OMI zone boundaries (geometries) from Agenzia delle Entrate into the database.
This provides sub-municipality zone divisions that can be displayed on the map.

Note: Property values API (risultato.php) is currently returning 404 errors.
This script focuses on zone boundaries only.

Data Source: https://www1.agenziaentrate.gov.it/servizi/geopoi_omi/

Usage:
    python load_omi_zones.py --province RM        # Load zones for Roma province
    python load_omi_zones.py --province RM MI TO  # Load multiple provinces
    python load_omi_zones.py --all                # Load all provinces (slow!)
"""

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Constants
OMI_BASE_URL = "https://www1.agenziaentrate.gov.it/servizi/geopoi_omi/zoneomi.php"
REQUEST_DELAY = 0.5  # seconds between requests


@dataclass
class Province:
    code: str  # e.g., "RM"
    name: str  # e.g., "ROMA"


@dataclass
class Comune:
    codcom: str  # Cadastral code, e.g., "H501"
    name: str    # e.g., "ROMA"
    province_code: str


@dataclass
class Zone:
    link_zona: str  # e.g., "RM00000155"
    fascia: str     # e.g., "B" (central), "C", "D", "E", "R"
    zona: str       # e.g., "B1"
    description: str


class OMIClient:
    """Client for OMI API with retry logic."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        })

        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get_provinces(self) -> list[Province]:
        """Get list of all provinces."""
        r = self.session.get(OMI_BASE_URL, params={'richiesta': '1'}, timeout=30)
        r.raise_for_status()
        data = r.json()

        provinces = []
        for item in data:
            if isinstance(item, dict):
                provinces.append(Province(
                    code=item.get('PROVINCIA', ''),
                    name=item.get('DIZIONE', '')
                ))

        logger.info(f"Found {len(provinces)} provinces")
        return provinces

    def get_comuni(self, province_code: str) -> list[Comune]:
        """Get list of comuni in a province."""
        r = self.session.get(OMI_BASE_URL, params={'richiesta': '2', 'prov': province_code}, timeout=30)
        r.raise_for_status()
        data = r.json()

        if not data:
            return []

        comuni = []
        for item in data:
            if isinstance(item, dict):
                comuni.append(Comune(
                    codcom=item.get('CODCOM', ''),
                    name=item.get('DIZIONE', ''),
                    province_code=province_code
                ))

        return comuni

    def get_zones(self, codcom: str) -> list[Zone]:
        """Get list of zones in a comune with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                r = self.session.get(OMI_BASE_URL, params={'richiesta': '3', 'codcom': codcom}, timeout=60)
                r.raise_for_status()
                data = r.json()

                if not data:
                    return []

                zones = []
                for item in data:
                    if isinstance(item, dict):
                        zones.append(Zone(
                            link_zona=item.get('LINK_ZONA', ''),
                            fascia=item.get('FASCIA', ''),
                            zona=item.get('ZONA', ''),
                            description=item.get('DIZIONE', '')
                        ))

                return zones

            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    logger.warning(f"Timeout fetching zones for {codcom}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch zones for {codcom} after {max_retries} attempts")
                    return []
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching zones for {codcom}: {e}")
                return []

    def get_zone_geometry(self, codcom: str, zona: str, semester: str = '20242') -> Optional[dict]:
        """Get GeoJSON geometry for a zone with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                r = self.session.get(OMI_BASE_URL, params={
                    'richiesta': '6',
                    'codcom': codcom,
                    'zona': zona,
                    'semestre': semester
                }, timeout=60)  # Increased timeout
                r.raise_for_status()

                data = r.json()
                if not data or 'dat' not in data:
                    return None

                dat = data['dat']
                if isinstance(dat, str):
                    dat = json.loads(dat)

                if dat and 'features' in dat and dat['features']:
                    return dat['features'][0].get('geometry')

                return None

            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    logger.warning(f"Timeout fetching geometry for {codcom}/{zona}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch geometry for {codcom}/{zona} after {max_retries} attempts")
                    return None
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching geometry for {codcom}/{zona}: {e}")
                return None


def load_env():
    """Load environment variables."""
    env_paths = [
        Path(__file__).parent.parent.parent / 'frontend' / '.env.local',
        Path(__file__).parent.parent.parent / '.env',
    ]

    for env_path in env_paths:
        if env_path.exists():
            load_dotenv(env_path)
            logger.info(f"Loaded environment from {env_path}")
            return

    logger.warning("No .env file found")


def get_db_connection():
    """Get database connection."""
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT', '5432'),
        dbname=os.getenv('DB_NAME', 'postgres'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD'),
    )


def create_cadastral_mapping_table(conn):
    """Create table for cadastral to ISTAT code mapping."""
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS core.cadastral_istat_mapping (
            codcom VARCHAR(10) PRIMARY KEY,  -- Cadastral code (e.g., H501)
            municipality_id VARCHAR(10),      -- ISTAT code (e.g., 058091)
            municipality_name VARCHAR(255),
            province_code VARCHAR(5),
            created_at TIMESTAMPTZ DEFAULT now(),
            UNIQUE(municipality_id)
        )
    """)
    conn.commit()
    logger.info("Created cadastral_istat_mapping table")


def find_istat_code(conn, comune_name: str, province_code: str) -> Optional[str]:
    """Try to find ISTAT code for a comune by name matching."""
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Normalize name for matching
    normalized = comune_name.upper().strip()

    # Try exact match first
    cur.execute("""
        SELECT municipality_id, municipality_name
        FROM core.municipalities
        WHERE UPPER(municipality_name) = %s
    """, (normalized,))

    result = cur.fetchone()
    if result:
        return result['municipality_id']

    # Try with province filter
    cur.execute("""
        SELECT municipality_id, municipality_name
        FROM core.municipalities
        WHERE UPPER(municipality_name) = %s
        AND province_code LIKE %s
    """, (normalized, f'%{province_code}%'))

    result = cur.fetchone()
    if result:
        return result['municipality_id']

    # Try fuzzy match
    cur.execute("""
        SELECT municipality_id, municipality_name
        FROM core.municipalities
        WHERE UPPER(municipality_name) LIKE %s
        LIMIT 1
    """, (f'{normalized}%',))

    result = cur.fetchone()
    if result:
        return result['municipality_id']

    return None


def load_zones_for_province(conn, client: OMIClient, province_code: str, semester: str = '20242', start_at: int = 0):
    """Load all zones for a province."""
    cur = conn.cursor()

    comuni = client.get_comuni(province_code)
    logger.info(f"Found {len(comuni)} comuni in province {province_code}")

    if start_at > 0:
        logger.info(f"Resuming from position {start_at}")

    zones_loaded = 0
    comuni_mapped = 0

    for i, comune in enumerate(comuni):
        if i < start_at:
            continue

        if (i + 1) % 10 == 0:
            logger.info(f"  Processing {i + 1}/{len(comuni)}: {comune.name}")

        # Try to find ISTAT code
        istat_code = find_istat_code(conn, comune.name, province_code)

        if istat_code:
            # Save mapping
            cur.execute("""
                INSERT INTO core.cadastral_istat_mapping (codcom, municipality_id, municipality_name, province_code)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (codcom) DO UPDATE SET
                    municipality_id = EXCLUDED.municipality_id,
                    municipality_name = EXCLUDED.municipality_name
            """, (comune.codcom, istat_code, comune.name, province_code))
            comuni_mapped += 1

        # Get zones
        zones = client.get_zones(comune.codcom)

        for zone in zones:
            # Get geometry
            geom = client.get_zone_geometry(comune.codcom, zone.zona, semester)

            if geom:
                omi_zone_id = f"{comune.codcom}_{zone.zona}"

                # Insert zone with geometry (convert to MultiPolygon)
                cur.execute("""
                    INSERT INTO core.omi_zones (
                        omi_zone_id, municipality_id, zone_code, zone_type,
                        zone_description, microzone_code, geom
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s,
                        ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
                    )
                    ON CONFLICT (omi_zone_id) DO UPDATE SET
                        zone_type = EXCLUDED.zone_type,
                        zone_description = EXCLUDED.zone_description,
                        geom = ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)),
                        updated_at = now()
                """, (
                    omi_zone_id,
                    istat_code,  # May be None
                    zone.zona,
                    zone.fascia,
                    zone.description,
                    zone.link_zona,
                    json.dumps(geom),
                    json.dumps(geom)  # For the UPDATE clause
                ))
                zones_loaded += 1

            time.sleep(REQUEST_DELAY)

        conn.commit()

    logger.info(f"Province {province_code}: loaded {zones_loaded} zones, mapped {comuni_mapped}/{len(comuni)} comuni to ISTAT codes")
    return zones_loaded, comuni_mapped


def main():
    parser = argparse.ArgumentParser(description='Load OMI zone boundaries')
    parser.add_argument('--province', '-p', nargs='+', help='Province codes to load (e.g., RM MI TO)')
    parser.add_argument('--all', action='store_true', help='Load all provinces')
    parser.add_argument('--semester', default='20242', help='Semester for geometry (default: 20242)')
    parser.add_argument('--list-provinces', action='store_true', help='List available provinces and exit')
    parser.add_argument('--start-at', type=int, default=0, help='Start at this comune index (0-based, for resuming)')
    args = parser.parse_args()

    load_env()

    client = OMIClient()

    # List provinces mode
    if args.list_provinces:
        provinces = client.get_provinces()
        for p in provinces:
            print(f"{p.code}: {p.name}")
        return

    # Determine which provinces to load
    if args.all:
        provinces = client.get_provinces()
        province_codes = [p.code for p in provinces]
    elif args.province:
        province_codes = [p.upper() for p in args.province]
    else:
        parser.print_help()
        return

    # Connect to database
    conn = get_db_connection()
    logger.info("Connected to database")

    # Ensure mapping table exists
    create_cadastral_mapping_table(conn)

    # Load zones
    total_zones = 0
    total_mapped = 0

    for province_code in province_codes:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing province: {province_code}")
        logger.info(f"{'='*60}")

        try:
            zones, mapped = load_zones_for_province(conn, client, province_code, args.semester, args.start_at)
            total_zones += zones
            total_mapped += mapped
        except Exception as e:
            logger.error(f"Error processing {province_code}: {e}")
            conn.rollback()
        # Reset start_at after first province
        args.start_at = 0

    conn.close()

    print(f"\n{'='*60}")
    print(f"COMPLETE")
    print(f"{'='*60}")
    print(f"Total zones loaded: {total_zones}")
    print(f"Total comuni mapped: {total_mapped}")


if __name__ == '__main__':
    main()
