#!/usr/bin/env python3
"""
Load OMI zone geometries from official GEOPOI KML files.

These KML files are downloaded from the Agenzia delle Entrate GEOPOI service
and contain the official, current zone boundaries.

Usage:
    python load_kml_zones.py "docs/Provincia di Roma 20252"
    python load_kml_zones.py "docs/Provincia di Roma 20252" --dry-run
"""

import argparse
import json
import logging
import os
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# KML namespace
KML_NS = {'kml': 'http://www.opengis.net/kml/2.2'}


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


def parse_coordinates(coord_text):
    """Parse KML coordinates string to list of [lng, lat] pairs."""
    coords = []
    for point in coord_text.strip().split():
        parts = point.split(',')
        if len(parts) >= 2:
            lng, lat = float(parts[0]), float(parts[1])
            coords.append([lng, lat])
    return coords


def parse_kml_file(kml_path):
    """Parse a KML file and extract zone geometries."""
    zones = []

    tree = ET.parse(kml_path)
    root = tree.getroot()

    # Find all Placemarks
    for placemark in root.findall('.//kml:Placemark', KML_NS):
        # Get zone name
        name_elem = placemark.find('kml:name', KML_NS)
        name = name_elem.text if name_elem is not None else ''

        # Get extended data (CODCOM, CODZONA)
        codcom = None
        codzona = None
        for data in placemark.findall('.//kml:Data', KML_NS):
            data_name = data.get('name')
            value_elem = data.find('kml:value', KML_NS)
            value = value_elem.text if value_elem is not None else ''

            if data_name == 'CODCOM':
                codcom = value
            elif data_name == 'CODZONA':
                codzona = value

        if not codcom or not codzona:
            continue

        # Get polygon geometry
        polygon = placemark.find('.//kml:Polygon', KML_NS)
        if polygon is None:
            continue

        # Get outer boundary
        outer_coords_elem = polygon.find('.//kml:outerBoundaryIs//kml:coordinates', KML_NS)
        if outer_coords_elem is None or not outer_coords_elem.text:
            continue

        outer_ring = parse_coordinates(outer_coords_elem.text)
        if len(outer_ring) < 4:
            continue

        # Get inner boundaries (holes)
        inner_rings = []
        for inner in polygon.findall('.//kml:innerBoundaryIs', KML_NS):
            inner_coords_elem = inner.find('.//kml:coordinates', KML_NS)
            if inner_coords_elem is not None and inner_coords_elem.text:
                inner_ring = parse_coordinates(inner_coords_elem.text)
                if len(inner_ring) >= 4:
                    inner_rings.append(inner_ring)

        # Build GeoJSON geometry
        coordinates = [outer_ring] + inner_rings
        geometry = {
            "type": "Polygon",
            "coordinates": coordinates
        }

        zones.append({
            'codcom': codcom,
            'codzona': codzona,
            'name': name,
            'geometry': geometry
        })

    return zones


def main():
    parser = argparse.ArgumentParser(description='Load OMI zone geometries from KML files')
    parser.add_argument('kml_folder', help='Folder containing KML files')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be updated without making changes')
    args = parser.parse_args()

    load_env()

    kml_folder = Path(args.kml_folder)
    if not kml_folder.exists():
        logger.error(f"Folder not found: {kml_folder}")
        return

    # Find all KML files
    kml_files = list(kml_folder.glob('*.kml'))
    logger.info(f"Found {len(kml_files)} KML files")

    # Parse all KML files
    all_zones = {}
    for kml_file in kml_files:
        # Extract cadastral code from filename (e.g., "H501 - Comune di ROMA 2025-2.kml")
        match = re.match(r'^([A-Z]\d+)', kml_file.name)
        if not match:
            logger.warning(f"Could not extract code from: {kml_file.name}")
            continue

        zones = parse_kml_file(kml_file)
        for zone in zones:
            key = (zone['codcom'], zone['codzona'])
            all_zones[key] = zone

        logger.info(f"  {kml_file.name}: {len(zones)} zones")

    logger.info(f"Total zones parsed: {len(all_zones)}")

    if args.dry_run:
        logger.info("Dry run - showing sample zones:")
        for (codcom, codzona), zone in list(all_zones.items())[:5]:
            logger.info(f"  {codcom}_{codzona}: {zone['name']}")
        return

    # Connect to database
    conn = get_db_connection()
    cur = conn.cursor()
    logger.info("Connected to database")

    # Get existing zones
    cur.execute("SELECT omi_zone_id, zone_code FROM core.omi_zones")
    existing_zones = {row[0]: row[1] for row in cur.fetchall()}
    logger.info(f"Found {len(existing_zones)} existing zones in database")

    # Update geometries
    updated = 0
    not_found = 0

    for omi_zone_id, zone_code in existing_zones.items():
        # Extract cadastral code from omi_zone_id (e.g., "H501_B1" -> "H501")
        parts = omi_zone_id.split('_')
        if len(parts) >= 2:
            codcom = parts[0]
            codzona = zone_code

            if (codcom, codzona) in all_zones:
                zone_data = all_zones[(codcom, codzona)]
                geom_json = json.dumps(zone_data['geometry'])

                cur.execute("""
                    UPDATE core.omi_zones
                    SET geom = ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)),
                        updated_at = now()
                    WHERE omi_zone_id = %s
                """, (geom_json, omi_zone_id))
                updated += 1

                if updated % 100 == 0:
                    logger.info(f"  Updated {updated} zones...")
                    conn.commit()
            else:
                not_found += 1

    conn.commit()
    conn.close()

    logger.info(f"Complete!")
    logger.info(f"  Updated: {updated}")
    logger.info(f"  Not found in KML files: {not_found}")


if __name__ == '__main__':
    main()
