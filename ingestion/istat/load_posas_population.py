#!/usr/bin/env python3
"""
Load ISTAT POSAS population data from CSV files.

The POSAS (Popolazione per Sesso, Età e Stato civile) data contains
population by age and sex for each municipality. This script aggregates
the data to get total population per municipality.

Usage:
    python load_posas_population.py docs/POSAS_2026_en_All_files/
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
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


def process_posas_files(folder_path: Path) -> pd.DataFrame:
    """Process POSAS CSV files and aggregate population by municipality."""
    all_data = []

    # Prefer the national municipalities file if it exists (contains all data)
    national_file = folder_path / 'POSAS_2026_en_Municipalities.csv'
    if national_file.exists():
        csv_files = [national_file]
        logger.info("Using national municipalities file")
    else:
        # Fall back to province files, excluding the national file
        csv_files = [f for f in folder_path.glob('POSAS_*_*.csv') if 'Municipalities' not in f.name]
        logger.info(f"Found {len(csv_files)} province CSV files")

    for csv_file in csv_files:
        try:
            # Read CSV, skip the header row with the description
            df = pd.read_csv(csv_file, skiprows=1, encoding='utf-8-sig')

            # Standardize column names
            df.columns = ['municipality_code', 'municipality_name', 'age', 'males', 'females', 'total']

            # Filter to only the total rows (age=999 contains the sum for each municipality)
            # This avoids double-counting individual ages
            totals = df[df['age'] == 999].copy()

            if len(totals) == 0:
                # Fallback: if no age=999 rows, aggregate manually (excluding any notes)
                df = df[df['municipality_code'].astype(str).str.match(r'^\d+$', na=False)]
                totals = df.groupby(['municipality_code', 'municipality_name']).agg({
                    'males': 'sum',
                    'females': 'sum',
                    'total': 'sum'
                }).reset_index()

            all_data.append(totals[['municipality_code', 'municipality_name', 'males', 'females', 'total']])

        except Exception as e:
            logger.error(f"Error processing {csv_file}: {e}")

    if not all_data:
        return pd.DataFrame()

    # Combine all provinces
    combined = pd.concat(all_data, ignore_index=True)
    logger.info(f"Total rows before deduplication: {len(combined)}")

    # Deduplicate by municipality code (some municipalities appear in multiple province files)
    # Keep the first occurrence (they should have the same data)
    combined = combined.drop_duplicates(subset=['municipality_code'], keep='first')
    logger.info(f"Total municipalities after deduplication: {len(combined)}")

    return combined


def load_to_database(conn, df: pd.DataFrame, reference_year: int):
    """Load population data into the database."""
    cur = conn.cursor()

    # First, load into raw table
    loaded = 0
    skipped = 0

    for _, row in df.iterrows():
        # Format municipality code to 6 digits (ISTAT format)
        municipality_id = str(row['municipality_code']).zfill(6)

        try:
            # Insert into raw.istat_population
            cur.execute("""
                INSERT INTO raw.istat_population (
                    municipality_id, reference_year, total_population,
                    male_population, female_population, source_file
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (municipality_id, reference_year) DO UPDATE SET
                    total_population = EXCLUDED.total_population,
                    male_population = EXCLUDED.male_population,
                    female_population = EXCLUDED.female_population,
                    source_file = EXCLUDED.source_file
            """, (
                municipality_id,
                reference_year,
                int(row['total']),
                int(row['males']),
                int(row['females']),
                'POSAS_2026_en'
            ))
            loaded += 1

        except Exception as e:
            logger.debug(f"Error loading {municipality_id}: {e}")
            skipped += 1

    conn.commit()
    logger.info(f"Loaded {loaded} rows into raw.istat_population, skipped {skipped}")

    # Now populate the mart table
    cur.execute("""
        INSERT INTO mart.municipality_demographics_year (
            municipality_id, reference_year, total_population
        )
        SELECT
            p.municipality_id,
            p.reference_year,
            p.total_population
        FROM raw.istat_population p
        WHERE p.reference_year = %s
        AND EXISTS (SELECT 1 FROM core.municipalities m WHERE m.municipality_id = p.municipality_id)
        ON CONFLICT (municipality_id, reference_year) DO UPDATE SET
            total_population = EXCLUDED.total_population,
            updated_at = now()
    """, (reference_year,))

    mart_count = cur.rowcount
    conn.commit()
    logger.info(f"Populated {mart_count} rows in mart.municipality_demographics_year")

    return loaded, mart_count


def main():
    parser = argparse.ArgumentParser(description='Load ISTAT POSAS population data')
    parser.add_argument('folder', help='Folder containing POSAS CSV files')
    parser.add_argument('--year', type=int, default=2026, help='Reference year (default: 2026)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be loaded without making changes')
    args = parser.parse_args()

    load_env()

    folder_path = Path(args.folder)
    if not folder_path.exists():
        logger.error(f"Folder not found: {folder_path}")
        return 1

    # Process CSV files
    df = process_posas_files(folder_path)

    if df.empty:
        logger.error("No data processed")
        return 1

    logger.info(f"Processed {len(df)} municipalities")
    logger.info(f"Total population: {df['total'].sum():,}")

    # Show sample
    logger.info("Sample data:")
    print(df.head(10).to_string())

    if args.dry_run:
        logger.info("Dry run - not loading to database")
        return 0

    # Load to database
    conn = get_db_connection()
    logger.info("Connected to database")

    loaded, mart_count = load_to_database(conn, df, args.year)

    conn.close()

    print(f"\n{'='*60}")
    print(f"COMPLETE")
    print(f"{'='*60}")
    print(f"Municipalities loaded to raw table: {loaded}")
    print(f"Municipalities loaded to mart table: {mart_count}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
