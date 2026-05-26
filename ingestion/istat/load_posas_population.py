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


def detect_csv_format(csv_file: Path) -> dict:
    """Detect the CSV format (delimiter, column positions) by reading the header."""
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        # Skip first line (description)
        f.readline()
        header_line = f.readline().strip()

    # Check delimiter
    if ';' in header_line:
        delimiter = ';'
        headers = [h.strip('"') for h in header_line.split(';')]
    else:
        delimiter = ','
        headers = [h.strip('"') for h in header_line.split(',')]

    # Find column indices for the data we need
    # Look for columns by common patterns
    col_indices = {}

    for i, h in enumerate(headers):
        h_lower = h.lower()
        if 'codice' in h_lower or 'municipality code' in h_lower:
            col_indices['code'] = i
        elif h_lower in ('comune', 'municipality'):
            col_indices['name'] = i
        elif h_lower in ('età', 'age'):
            col_indices['age'] = i
        elif h_lower == 'totale maschi' or h_lower == 'total males':
            col_indices['males'] = i
        elif h_lower == 'totale femmine' or h_lower == 'total females':
            col_indices['females'] = i
        elif h_lower == 'totale' or h_lower == 'total':
            col_indices['total'] = i

    return {
        'delimiter': delimiter,
        'columns': col_indices,
        'header_count': len(headers),
    }


def process_posas_files(folder_path: Path) -> pd.DataFrame:
    """Process POSAS CSV files and aggregate population by municipality."""
    all_data = []

    # Find CSV files - prefer national file, otherwise use province files
    # Support both English and Italian naming patterns
    csv_files = []
    for pattern in ['POSAS_*_Municipalities.csv', 'POSAS_*_it_Comuni.csv']:
        national_files = list(folder_path.glob(pattern))
        if national_files:
            csv_files = national_files
            logger.info(f"Using national municipalities file: {national_files[0].name}")
            break

    if not csv_files:
        # Fall back to province files
        csv_files = [f for f in folder_path.glob('POSAS_*.csv')
                     if 'Municipalities' not in f.name and 'Comuni' not in f.name]
        logger.info(f"Found {len(csv_files)} province CSV files")

    # Detect format from first file
    if not csv_files:
        logger.error("No CSV files found")
        return pd.DataFrame()

    fmt = detect_csv_format(csv_files[0])
    logger.info(f"Detected format: delimiter='{fmt['delimiter']}', columns={fmt['columns']}")

    required_cols = ['code', 'name', 'age', 'males', 'females', 'total']
    missing = [c for c in required_cols if c not in fmt['columns']]
    if missing:
        logger.error(f"Missing required columns: {missing}")
        return pd.DataFrame()

    for csv_file in csv_files:
        try:
            # Read CSV with detected delimiter, skip the description row
            df = pd.read_csv(
                csv_file,
                skiprows=1,
                encoding='utf-8-sig',
                delimiter=fmt['delimiter'],
                dtype=str,  # Read all as string first
                quotechar='"',
            )

            # Select and rename columns based on detected positions
            col_map = fmt['columns']
            df_subset = df.iloc[:, [col_map['code'], col_map['name'], col_map['age'],
                                     col_map['males'], col_map['females'], col_map['total']]].copy()
            df_subset.columns = ['municipality_code', 'municipality_name', 'age', 'males', 'females', 'total']

            # Convert numeric columns
            for col in ['age', 'males', 'females', 'total']:
                df_subset[col] = pd.to_numeric(df_subset[col], errors='coerce')

            # Filter to only the total rows (age=999 contains the sum for each municipality)
            totals = df_subset[df_subset['age'] == 999].copy()

            if len(totals) == 0:
                # Fallback: if no age=999 rows, aggregate manually (excluding any notes)
                df_clean = df_subset[df_subset['municipality_code'].str.match(r'^\d+$', na=False)]
                totals = df_clean.groupby(['municipality_code', 'municipality_name']).agg({
                    'males': 'sum',
                    'females': 'sum',
                    'total': 'sum'
                }).reset_index()

            all_data.append(totals[['municipality_code', 'municipality_name', 'males', 'females', 'total']])

        except Exception as e:
            logger.error(f"Error processing {csv_file}: {e}")
            import traceback
            logger.debug(traceback.format_exc())

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
                f'POSAS_{reference_year}'
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
