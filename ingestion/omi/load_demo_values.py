#!/usr/bin/env python3
"""
Load demo property values for testing the heatmap visualization.

This script inserts realistic demo values for Roma and Milano zones
to verify the end-to-end pipeline works correctly.

Usage:
    python load_demo_values.py
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Realistic property values for Italian cities (EUR/sqm)
DEMO_VALUES = {
    # Roma zones (municipality_id: 058091)
    '058091': {
        'name': 'Roma',
        'zones': {
            'B1': {'desc': 'Centro Storico', 'min': 6500, 'max': 9500, 'rent_min': 18, 'rent_max': 28},
            'B2': {'desc': 'Prati', 'min': 5500, 'max': 7500, 'rent_min': 16, 'rent_max': 22},
            'B3': {'desc': 'Trastevere', 'min': 5800, 'max': 8200, 'rent_min': 17, 'rent_max': 25},
            'C1': {'desc': 'Testaccio', 'min': 4200, 'max': 5800, 'rent_min': 14, 'rent_max': 18},
            'C2': {'desc': 'San Giovanni', 'min': 4000, 'max': 5500, 'rent_min': 13, 'rent_max': 17},
            'C3': {'desc': 'Monteverde', 'min': 4500, 'max': 6000, 'rent_min': 14, 'rent_max': 19},
            'C4': {'desc': 'Parioli', 'min': 5000, 'max': 7000, 'rent_min': 15, 'rent_max': 21},
            'D1': {'desc': 'Tuscolano', 'min': 2800, 'max': 4000, 'rent_min': 10, 'rent_max': 14},
            'D2': {'desc': 'EUR', 'min': 3500, 'max': 5000, 'rent_min': 12, 'rent_max': 16},
            'D3': {'desc': 'Ostiense', 'min': 3000, 'max': 4200, 'rent_min': 11, 'rent_max': 15},
            'D4': {'desc': 'Tiburtina', 'min': 2600, 'max': 3800, 'rent_min': 9, 'rent_max': 13},
            'E1': {'desc': 'Tor Bella Monaca', 'min': 1800, 'max': 2600, 'rent_min': 7, 'rent_max': 10},
            'E2': {'desc': 'Corviale', 'min': 1900, 'max': 2800, 'rent_min': 7, 'rent_max': 11},
            'R1': {'desc': 'Campagna Romana', 'min': 1200, 'max': 2000, 'rent_min': 5, 'rent_max': 8},
        }
    },
    # Milano zones (municipality_id: 015146)
    '015146': {
        'name': 'Milano',
        'zones': {
            'B1': {'desc': 'Duomo - Centro', 'min': 8000, 'max': 12000, 'rent_min': 22, 'rent_max': 35},
            'B2': {'desc': 'Brera', 'min': 7500, 'max': 11000, 'rent_min': 20, 'rent_max': 32},
            'B3': {'desc': 'Quadrilatero', 'min': 9000, 'max': 14000, 'rent_min': 25, 'rent_max': 40},
            'C1': {'desc': 'Porta Venezia', 'min': 5500, 'max': 7500, 'rent_min': 16, 'rent_max': 22},
            'C2': {'desc': 'Navigli', 'min': 5800, 'max': 8000, 'rent_min': 17, 'rent_max': 24},
            'C3': {'desc': 'Isola', 'min': 5000, 'max': 7000, 'rent_min': 15, 'rent_max': 21},
            'C4': {'desc': 'Porta Romana', 'min': 5200, 'max': 7200, 'rent_min': 15, 'rent_max': 22},
            'D1': {'desc': 'Città Studi', 'min': 4000, 'max': 5500, 'rent_min': 13, 'rent_max': 17},
            'D2': {'desc': 'Lambrate', 'min': 3800, 'max': 5200, 'rent_min': 12, 'rent_max': 16},
            'D3': {'desc': 'Bovisa', 'min': 3500, 'max': 4800, 'rent_min': 11, 'rent_max': 15},
            'D4': {'desc': 'Lorenteggio', 'min': 3200, 'max': 4500, 'rent_min': 10, 'rent_max': 14},
            'E1': {'desc': 'Quarto Oggiaro', 'min': 2500, 'max': 3500, 'rent_min': 8, 'rent_max': 12},
            'E2': {'desc': 'Baggio', 'min': 2300, 'max': 3300, 'rent_min': 8, 'rent_max': 11},
            'R1': {'desc': 'Chiaravalle', 'min': 2000, 'max': 3000, 'rent_min': 7, 'rent_max': 10},
        }
    },
    # Some surrounding municipalities in RM province
    '058015': {'name': 'Bracciano', 'zones': {'B1': {'desc': 'Centro', 'min': 2200, 'max': 3000, 'rent_min': 8, 'rent_max': 11}}},
    '058022': {'name': 'Cerveteri', 'zones': {'B1': {'desc': 'Centro', 'min': 2000, 'max': 2800, 'rent_min': 7, 'rent_max': 10}}},
    '058040': {'name': 'Frascati', 'zones': {'B1': {'desc': 'Centro', 'min': 3000, 'max': 4200, 'rent_min': 10, 'rent_max': 14}}},
    '058055': {'name': 'Ladispoli', 'zones': {'B1': {'desc': 'Centro', 'min': 2500, 'max': 3500, 'rent_min': 9, 'rent_max': 12}}},
    '058058': {'name': 'Marino', 'zones': {'B1': {'desc': 'Centro', 'min': 2400, 'max': 3400, 'rent_min': 8, 'rent_max': 11}}},
    '058065': {'name': 'Nettuno', 'zones': {'B1': {'desc': 'Centro', 'min': 2300, 'max': 3200, 'rent_min': 8, 'rent_max': 11}}},
    '058078': {'name': 'Pomezia', 'zones': {'B1': {'desc': 'Centro', 'min': 2600, 'max': 3600, 'rent_min': 9, 'rent_max': 12}}},
    '058097': {'name': 'Tivoli', 'zones': {'B1': {'desc': 'Centro', 'min': 2200, 'max': 3000, 'rent_min': 8, 'rent_max': 11}}},
    '058104': {'name': 'Velletri', 'zones': {'B1': {'desc': 'Centro', 'min': 1800, 'max': 2600, 'rent_min': 6, 'rent_max': 9}}},
    '058118': {'name': 'Civitavecchia', 'zones': {'B1': {'desc': 'Centro', 'min': 2100, 'max': 3000, 'rent_min': 7, 'rent_max': 10}}},
    # Some surrounding municipalities in MI province
    '015009': {'name': 'Assago', 'zones': {'B1': {'desc': 'Centro', 'min': 3000, 'max': 4200, 'rent_min': 10, 'rent_max': 14}}},
    '015081': {'name': 'Cologno Monzese', 'zones': {'B1': {'desc': 'Centro', 'min': 2800, 'max': 3800, 'rent_min': 9, 'rent_max': 13}}},
    '015108': {'name': 'Legnano', 'zones': {'B1': {'desc': 'Centro', 'min': 2200, 'max': 3200, 'rent_min': 8, 'rent_max': 11}}},
    '015130': {'name': 'Monza', 'zones': {'B1': {'desc': 'Centro', 'min': 3200, 'max': 4500, 'rent_min': 11, 'rent_max': 15}}},
    '015194': {'name': 'Rozzano', 'zones': {'B1': {'desc': 'Centro', 'min': 2700, 'max': 3700, 'rent_min': 9, 'rent_max': 12}}},
    '015209': {'name': 'Sesto San Giovanni', 'zones': {'B1': {'desc': 'Centro', 'min': 3000, 'max': 4200, 'rent_min': 10, 'rent_max': 14}}},
}


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


def ensure_time_period(cur, period_id: str):
    """Ensure time period exists."""
    year = int(period_id[:4])
    sem = int(period_id[4]) if len(period_id) == 5 else 2

    if sem == 1:
        start_date, end_date = f"{year}-01-01", f"{year}-06-30"
    else:
        start_date, end_date = f"{year}-07-01", f"{year}-12-31"

    formatted_id = f"{year}H{sem}"

    try:
        cur.execute("""
            INSERT INTO core.time_periods (period_id, period_type, period_start_date, period_end_date, year, semester)
            VALUES (%s, 'semester', %s, %s, %s, %s)
            ON CONFLICT (period_id) DO NOTHING
        """, (formatted_id, start_date, end_date, year, sem))
        return formatted_id
    except Exception as e:
        logger.warning(f"Could not ensure time period {formatted_id}: {e}")
        return formatted_id


def load_demo_data():
    """Load demo property values."""
    load_env()
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    period_id = ensure_time_period(cur, '20242')
    conn.commit()

    logger.info(f"Loading demo data for period {period_id}")

    # Create ingestion run
    cur.execute("""
        INSERT INTO admin.ingestion_runs (source_name, source_version, status, rows_loaded, rows_rejected)
        VALUES ('demo_values', %s, 'started', 0, 0)
        RETURNING ingestion_run_id
    """, (datetime.now().strftime('%Y%m%d'),))
    ingestion_run_id = cur.fetchone()['ingestion_run_id']
    conn.commit()

    loaded_raw = 0
    loaded_zones = 0

    for muni_id, muni_data in DEMO_VALUES.items():
        muni_name = muni_data['name']

        # Check if municipality exists
        cur.execute("SELECT 1 FROM core.municipalities WHERE municipality_id = %s", (muni_id,))
        if not cur.fetchone():
            logger.warning(f"Municipality {muni_id} ({muni_name}) not found, skipping")
            continue

        for zone_code, values in muni_data['zones'].items():
            omi_zone_id = f"{muni_id}_{zone_code}"
            zone_type = zone_code[0]

            # Ensure zone exists
            cur.execute("""
                INSERT INTO core.omi_zones (omi_zone_id, municipality_id, zone_code, zone_type, zone_description)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (omi_zone_id) DO UPDATE SET
                    zone_description = EXCLUDED.zone_description,
                    updated_at = now()
            """, (omi_zone_id, muni_id, zone_code, zone_type, values['desc']))
            loaded_zones += 1

            # Insert raw values
            cur.execute("""
                INSERT INTO raw.omi_property_values (
                    ingestion_run_id, omi_zone_id, municipality_id, period_id,
                    property_type, property_subtype, state,
                    value_min_eur_sqm, value_max_eur_sqm,
                    rent_min_eur_sqm_month, rent_max_eur_sqm_month,
                    source_file
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                ingestion_run_id, omi_zone_id, muni_id, period_id,
                'residenziale', 'abitazioni_civili', 'NORMALE',
                values['min'], values['max'],
                values['rent_min'], values['rent_max'],
                'demo_data'
            ))
            loaded_raw += 1

        logger.info(f"  Loaded {muni_name} ({muni_id})")
        conn.commit()

    # Update ingestion run
    cur.execute("""
        UPDATE admin.ingestion_runs
        SET status = 'succeeded', rows_loaded = %s, finished_at = now()
        WHERE ingestion_run_id = %s
    """, (loaded_raw, ingestion_run_id))
    conn.commit()

    logger.info(f"Loaded {loaded_raw} raw values, {loaded_zones} zones")

    # Run aggregation to mart tables
    logger.info("Running aggregation to mart tables...")

    # Aggregate zone values
    cur.execute("""
        INSERT INTO mart.omi_zone_values_semester (
            omi_zone_id, period_id, property_segment,
            value_min_eur_sqm, value_max_eur_sqm, value_mid_eur_sqm,
            rent_min_eur_sqm_month, rent_max_eur_sqm_month, rent_mid_eur_sqm_month,
            gross_yield_pct, data_quality_score, updated_at
        )
        SELECT
            r.omi_zone_id, r.period_id, 'residential',
            MIN(r.value_min_eur_sqm), MAX(r.value_max_eur_sqm),
            AVG((r.value_min_eur_sqm + r.value_max_eur_sqm) / 2),
            MIN(r.rent_min_eur_sqm_month), MAX(r.rent_max_eur_sqm_month),
            AVG((r.rent_min_eur_sqm_month + r.rent_max_eur_sqm_month) / 2),
            ROUND((AVG((r.rent_min_eur_sqm_month + r.rent_max_eur_sqm_month) / 2) * 12 /
                   NULLIF(AVG((r.value_min_eur_sqm + r.value_max_eur_sqm) / 2), 0) * 100)::numeric, 2),
            100.0, NOW()
        FROM raw.omi_property_values r
        WHERE r.property_type = 'residenziale' AND r.omi_zone_id IS NOT NULL
        GROUP BY r.omi_zone_id, r.period_id
        ON CONFLICT (omi_zone_id, period_id, property_segment)
        DO UPDATE SET
            value_min_eur_sqm = EXCLUDED.value_min_eur_sqm,
            value_max_eur_sqm = EXCLUDED.value_max_eur_sqm,
            value_mid_eur_sqm = EXCLUDED.value_mid_eur_sqm,
            rent_mid_eur_sqm_month = EXCLUDED.rent_mid_eur_sqm_month,
            gross_yield_pct = EXCLUDED.gross_yield_pct,
            updated_at = NOW()
    """)
    conn.commit()
    logger.info("  Zone values aggregated")

    # Aggregate municipality values
    cur.execute("""
        INSERT INTO mart.municipality_values_semester (
            municipality_id, period_id, property_segment,
            value_min_eur_sqm, value_max_eur_sqm, value_mid_eur_sqm,
            rent_min_eur_sqm_month, rent_max_eur_sqm_month, rent_mid_eur_sqm_month,
            zones_count, zones_with_data, data_quality_score, updated_at
        )
        SELECT
            r.municipality_id, r.period_id, 'residential',
            MIN(r.value_min_eur_sqm), MAX(r.value_max_eur_sqm),
            AVG((r.value_min_eur_sqm + r.value_max_eur_sqm) / 2),
            MIN(r.rent_min_eur_sqm_month), MAX(r.rent_max_eur_sqm_month),
            AVG((r.rent_min_eur_sqm_month + r.rent_max_eur_sqm_month) / 2),
            COUNT(DISTINCT r.omi_zone_id),
            COUNT(DISTINCT r.omi_zone_id),
            100.0, NOW()
        FROM raw.omi_property_values r
        WHERE r.property_type = 'residenziale'
        GROUP BY r.municipality_id, r.period_id
        ON CONFLICT (municipality_id, period_id, property_segment)
        DO UPDATE SET
            value_min_eur_sqm = EXCLUDED.value_min_eur_sqm,
            value_max_eur_sqm = EXCLUDED.value_max_eur_sqm,
            value_mid_eur_sqm = EXCLUDED.value_mid_eur_sqm,
            rent_mid_eur_sqm_month = EXCLUDED.rent_mid_eur_sqm_month,
            zones_count = EXCLUDED.zones_count,
            zones_with_data = EXCLUDED.zones_with_data,
            updated_at = NOW()
    """)
    conn.commit()
    logger.info("  Municipality values aggregated")

    # Show summary
    cur.execute("SELECT COUNT(*) as cnt FROM mart.municipality_values_semester WHERE period_id = %s", (period_id,))
    muni_count = cur.fetchone()['cnt']
    cur.execute("SELECT COUNT(*) as cnt FROM mart.omi_zone_values_semester WHERE period_id = %s", (period_id,))
    zone_count = cur.fetchone()['cnt']

    logger.info(f"\nSummary:")
    logger.info(f"  Municipalities with values: {muni_count}")
    logger.info(f"  Zones with values: {zone_count}")

    cur.close()
    conn.close()
    logger.info("Done!")


if __name__ == '__main__':
    load_demo_data()
