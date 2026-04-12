#!/usr/bin/env python3
"""
Load real OMI zone geometries from shapefile.

Data source: onData compiled shapefile from Agenzia delle Entrate GEOPOI
Download: http://dev.ondata.it/projs/zoneomi/zone_omi_poligoni.zip

Usage:
    python load_zone_geometries.py /path/to/zone_omi_all.shp
"""

import argparse
import json
import logging
import os
from pathlib import Path

import shapefile
import psycopg2
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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


def shape_to_geojson(shape):
    """Convert pyshp shape to GeoJSON geometry."""
    # Handle POLYGON (5) and POLYGONZ (15) and POLYGONM (25)
    if shape.shapeType in (shapefile.POLYGON, 15, 25):
        # Handle polygon with potential holes
        parts = list(shape.parts) + [len(shape.points)]
        rings = []
        for i in range(len(parts) - 1):
            ring = shape.points[parts[i]:parts[i + 1]]
            # Convert to 2D (drop Z/M if present) - GeoJSON uses [lng, lat]
            ring_2d = [[p[0], p[1]] for p in ring]
            rings.append(ring_2d)

        if len(rings) == 1:
            return {"type": "Polygon", "coordinates": [rings[0]]}
        else:
            # First ring is exterior, rest are holes
            return {"type": "Polygon", "coordinates": rings}

    elif shape.shapeType in (shapefile.POLYLINE, 13, 23):
        points_2d = [[p[0], p[1]] for p in shape.points]
        return {"type": "LineString", "coordinates": points_2d}

    elif shape.shapeType in (shapefile.POINT, 11, 21):
        return {"type": "Point", "coordinates": [shape.points[0][0], shape.points[0][1]]}

    return None


def main():
    parser = argparse.ArgumentParser(description='Load real OMI zone geometries from shapefile')
    parser.add_argument('shapefile', help='Path to zone_omi_all.shp')
    parser.add_argument('--province', '-p', help='Only load zones for this province (e.g., RM)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be updated without making changes')
    args = parser.parse_args()

    load_env()

    # Read shapefile
    logger.info(f"Reading shapefile: {args.shapefile}")
    sf = shapefile.Reader(args.shapefile, encoding='utf-8')

    # Get field names
    fields = [f[0] for f in sf.fields[1:]]  # Skip DeletionFlag
    logger.info(f"Fields: {fields}")

    # Build lookup of zone geometries
    # Key: (CODCOM, CODZONA) -> geometry
    zone_geometries = {}
    province_filter = args.province.upper() if args.province else None

    for shaperec in sf.iterShapeRecords():
        rec = dict(zip(fields, shaperec.record))

        # Filter by province if specified
        if province_filter and rec.get('QI_29457_2') != province_filter:
            continue

        codcom = rec.get('CODCOM')
        codzona = rec.get('CODZONA')

        if codcom and codzona:
            geom = shape_to_geojson(shaperec.shape)
            if geom:
                zone_geometries[(codcom, codzona)] = {
                    'geometry': geom,
                    'istat_code': rec.get('Com1991__4'),
                    'name': rec.get('Name'),
                }

    logger.info(f"Loaded {len(zone_geometries)} zone geometries from shapefile")

    if args.dry_run:
        logger.info("Dry run - not updating database")
        # Show sample
        for (codcom, codzona), data in list(zone_geometries.items())[:5]:
            logger.info(f"  {codcom}_{codzona}: {data['name']}")
        return

    # Connect to database
    conn = get_db_connection()
    cur = conn.cursor()
    logger.info("Connected to database")

    # Get existing zones that need geometry
    cur.execute("""
        SELECT omi_zone_id, zone_code
        FROM core.omi_zones
        WHERE geom IS NULL
    """)
    zones_needing_geom = cur.fetchall()
    logger.info(f"Found {len(zones_needing_geom)} zones needing geometry")

    # Update geometries
    updated = 0
    not_found = 0

    for omi_zone_id, zone_code in zones_needing_geom:
        # Extract cadastral code from omi_zone_id (e.g., "H501_B1" -> "H501")
        parts = omi_zone_id.split('_')
        if len(parts) >= 2:
            codcom = parts[0]
            codzona = zone_code

            if (codcom, codzona) in zone_geometries:
                geom_data = zone_geometries[(codcom, codzona)]
                geom_json = json.dumps(geom_data['geometry'])

                # Shapefile is in UTM Zone 32N (EPSG:32632), transform to WGS84 (EPSG:4326)
                cur.execute("""
                    UPDATE core.omi_zones
                    SET geom = ST_Multi(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s), 32632), 4326)),
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
    logger.info(f"  Not found in shapefile: {not_found}")


if __name__ == '__main__':
    main()
