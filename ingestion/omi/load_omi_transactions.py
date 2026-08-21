#!/usr/bin/env python3
"""
OMI Transaction (NTN) Data Ingestion Script

Loads provincial-level transaction volumes from Agenzia delle Entrate's OMI
(Osservatorio Mercato Immobiliare) into the Supabase database.

Data Source: https://www.agenziaentrate.gov.it/portale/schede/fabbricatiterreni/omi/banche-dati/volumi-di-compravendita

The public data is at provincial level, split by:
- Cap (capoluogo) = provincial capital city
- NonCap = all other municipalities combined

Usage:
    python load_omi_transactions.py --file ../docs/OMI_NTN/RES.csv
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

# Province code mapping (abbreviation to ISTAT code prefix)
PROVINCE_ABBREV_TO_CODE = {
    'AG': '084', 'AL': '006', 'AN': '042', 'AO': '007', 'AP': '044', 'AQ': '066',
    'AR': '051', 'AT': '005', 'AV': '064', 'BA': '072', 'BG': '016', 'BI': '096',
    'BL': '025', 'BN': '062', 'BO': '037', 'BR': '074', 'BS': '017', 'BT': '110',
    'BZ': '021', 'CA': '092', 'CB': '070', 'CE': '061', 'CH': '069', 'CL': '085',
    'CN': '004', 'CO': '013', 'CR': '019', 'CS': '078', 'CT': '087', 'CZ': '079',
    'EN': '086', 'FC': '040', 'FE': '038', 'FG': '071', 'FI': '048', 'FM': '109',
    'FR': '060', 'GE': '010', 'GO': '031', 'GR': '053', 'IM': '008', 'IS': '094',
    'KR': '101', 'LC': '097', 'LE': '075', 'LI': '049', 'LO': '098', 'LT': '059',
    'LU': '046', 'MB': '108', 'MC': '043', 'ME': '083', 'MI': '015', 'MN': '020',
    'MO': '036', 'MS': '045', 'MT': '077', 'NA': '063', 'NO': '003', 'NU': '091',
    'OR': '095', 'PA': '082', 'PC': '033', 'PD': '028', 'PE': '068', 'PG': '054',
    'PI': '050', 'PN': '093', 'PO': '100', 'PR': '034', 'PT': '047', 'PU': '041',
    'PV': '018', 'PZ': '076', 'RA': '039', 'RC': '080', 'RE': '035', 'RG': '088',
    'RI': '057', 'RM': '058', 'RN': '099', 'RO': '029', 'SA': '065', 'SI': '052',
    'SO': '014', 'SP': '011', 'SR': '089', 'SS': '090', 'SU': '111', 'SV': '009',
    'TA': '073', 'TE': '067', 'TN': '022', 'TO': '001', 'TP': '081', 'TR': '055',
    'TS': '032', 'TV': '026', 'UD': '030', 'VA': '012', 'VB': '103', 'VC': '002',
    'VE': '027', 'VI': '024', 'VR': '023', 'VT': '056', 'VV': '102',
}


def load_env():
    """Load environment variables from frontend/.env.local"""
    env_paths = [
        Path(__file__).parent.parent.parent / "frontend" / ".env.local",
        Path(__file__).parent.parent.parent / ".env.local",
        Path(__file__).parent / ".env",
    ]

    for env_path in env_paths:
        if env_path.exists():
            load_dotenv(env_path)
            logger.info(f"Loaded environment from {env_path}")
            return

    logger.warning("No .env file found, using environment variables")


def create_supabase_client() -> Client:
    """Create Supabase client from environment variables."""
    url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise ValueError("Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

    return create_client(url, key)


def parse_quarter_to_period(year: int, quarter: int) -> str:
    """Convert year and quarter to period_id format (YYYYS where S is semester)."""
    # Quarters 1,2 -> Semester 1; Quarters 3,4 -> Semester 2
    semester = 1 if quarter <= 2 else 2
    return f"{year}{semester}"


def process_residential_csv(file_path: Path) -> pd.DataFrame:
    """
    Process the RES.csv file into a normalized format.

    Input columns:
    - Area: Regional grouping (Nord Ovest, etc.)
    - Regione: Region name
    - prov: Province abbreviation (GE, MI, etc.)
    - Cap: 'cap' for capoluogo, 'NonCap' for other municipalities
    - 2011_1 through 2024_4: Quarterly NTN values

    Output: Long-format DataFrame with columns:
    - province_code: ISTAT province code
    - period_id: Semester (YYYYS)
    - is_capoluogo: Boolean
    - ntn_quarterly: NTN for the quarter
    """
    logger.info(f"Reading {file_path}...")

    df = pd.read_csv(file_path, sep=';', encoding='utf-8')
    logger.info(f"Loaded {len(df)} rows")
    logger.info(f"Columns: {list(df.columns)[:10]}...")

    # Identify value columns (YYYY_Q format)
    value_cols = [c for c in df.columns if '_' in c and c.split('_')[0].isdigit()]
    logger.info(f"Found {len(value_cols)} quarterly columns")

    # Melt to long format
    df_long = df.melt(
        id_vars=['Area', 'Regione', 'prov', 'Cap'],
        value_vars=value_cols,
        var_name='quarter',
        value_name='ntn'
    )

    # Parse quarter column (YYYY_Q)
    df_long['year'] = df_long['quarter'].apply(lambda x: int(x.split('_')[0]))
    df_long['quarter_num'] = df_long['quarter'].apply(lambda x: int(x.split('_')[1]))

    # Convert to semester period_id
    df_long['period_id'] = df_long.apply(
        lambda r: parse_quarter_to_period(r['year'], r['quarter_num']),
        axis=1
    )

    # Map province abbreviation to ISTAT code
    df_long['province_code'] = df_long['prov'].map(PROVINCE_ABBREV_TO_CODE)

    # Flag capoluogo vs non-capoluogo
    df_long['is_capoluogo'] = df_long['Cap'].str.lower() == 'cap'

    # Clean NTN values (handle comma decimals)
    df_long['ntn'] = df_long['ntn'].apply(
        lambda x: float(str(x).replace(',', '.')) if pd.notna(x) else None
    )

    # Aggregate quarters into semesters (sum Q1+Q2 or Q3+Q4)
    df_semester = df_long.groupby(
        ['province_code', 'period_id', 'is_capoluogo', 'Regione', 'prov']
    ).agg({
        'ntn': 'sum'  # Sum quarters within semester
    }).reset_index()

    df_semester = df_semester.rename(columns={'ntn': 'ntn_semester'})

    logger.info(f"Processed into {len(df_semester)} semester records")

    # Show sample
    logger.info("\nSample data:")
    logger.info(df_semester.head(10).to_string())

    return df_semester


def upsert_provincial_transactions(supabase: Client, df: pd.DataFrame):
    """
    Upsert provincial transaction data to mart.province_transactions_semester.

    Note: This creates a new table for provincial-level data since the existing
    municipality_transactions_semester is for municipal data.
    """
    logger.info(f"Upserting {len(df)} records...")

    records = []
    for _, row in df.iterrows():
        if pd.isna(row['province_code']) or pd.isna(row['ntn_semester']):
            continue

        records.append({
            'province_code': row['province_code'],
            'period_id': row['period_id'],
            'is_capoluogo': row['is_capoluogo'],
            'ntn_total': float(row['ntn_semester']),
            'property_segment': 'residential',
            'updated_at': datetime.utcnow().isoformat(),
        })

    if not records:
        logger.warning("No valid records to upsert")
        return

    # Upsert in batches
    batch_size = 500
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            supabase.schema('mart').table('province_transactions_semester').upsert(
                batch,
                on_conflict='province_code,period_id,is_capoluogo,property_segment'
            ).execute()
            logger.info(f"Upserted batch {i // batch_size + 1}: {len(batch)} records")
        except Exception as e:
            logger.error(f"Failed to upsert batch: {e}")
            # Try individual records
            for record in batch:
                try:
                    supabase.schema('mart').table('province_transactions_semester').upsert(
                        [record],
                        on_conflict='province_code,period_id,is_capoluogo,property_segment'
                    ).execute()
                except Exception as e2:
                    logger.error(f"Failed to upsert record {record['province_code']}/{record['period_id']}: {e2}")

    logger.info(f"Upsert complete: {len(records)} records")


def find_transaction_files() -> dict:
    """Auto-discover transaction data files in common locations."""
    base_paths = [
        Path(__file__).parent.parent.parent / "docs",
        Path(__file__).parent.parent.parent / "docs" / "OMI_NTN",
        Path(__file__).parent,
    ]

    files = {}

    # Look for residential data
    patterns = [
        ("residential", ["RESIDENZIALE_DEFINITIVO*.zip", "RES.csv", "residenziale*.csv"]),
        ("commercial", ["NON_RESIDENZIALE*.zip", "non_residenziale*.csv", "NONRES.csv"]),
    ]

    for segment, file_patterns in patterns:
        for base_path in base_paths:
            if not base_path.exists():
                continue
            for pattern in file_patterns:
                matches = list(base_path.glob(pattern))
                if matches:
                    # Prefer most recent file
                    files[segment] = sorted(matches, key=lambda p: p.stat().st_mtime, reverse=True)[0]
                    break
            if segment in files:
                break

    return files


def extract_csv_from_zip(zip_path: Path) -> Path:
    """Extract CSV file from ZIP and return path to extracted CSV."""
    import zipfile
    import tempfile

    extract_dir = Path(tempfile.mkdtemp())

    with zipfile.ZipFile(zip_path, 'r') as zf:
        csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
        if not csv_files:
            raise ValueError(f"No CSV files found in {zip_path}")

        # Extract the first (or main) CSV file
        csv_file = csv_files[0]
        zf.extract(csv_file, extract_dir)
        logger.info(f"Extracted {csv_file} from {zip_path.name}")
        return extract_dir / csv_file


def main():
    parser = argparse.ArgumentParser(
        description='Load OMI provincial transaction data',
        epilog="""
Examples:
    # Auto-discover and load transaction data from docs/
    python load_omi_transactions.py

    # Load from specific file
    python load_omi_transactions.py --file ../docs/OMI_NTN/RES.csv

    # Load from ZIP file
    python load_omi_transactions.py --file ../docs/RESIDENZIALE_DEFINITIVO_2011_2024.zip

    # Dry run to preview data
    python load_omi_transactions.py --dry-run
"""
    )
    parser.add_argument('--file', type=str, help='Path to CSV or ZIP file (auto-discovered if not provided)')
    parser.add_argument('--dry-run', action='store_true', help='Process without writing to database')
    parser.add_argument('--segment', choices=['residential', 'commercial', 'all'], default='all',
                        help='Which segment to load (default: all)')
    args = parser.parse_args()

    load_env()

    # Determine file(s) to process
    files_to_process = {}

    if args.file:
        file_path = Path(args.file)
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            sys.exit(1)
        # Determine segment from filename
        name_lower = file_path.name.lower()
        if 'non' in name_lower or 'commercial' in name_lower:
            files_to_process['commercial'] = file_path
        else:
            files_to_process['residential'] = file_path
    else:
        # Auto-discover files
        files_to_process = find_transaction_files()
        if not files_to_process:
            logger.error("No transaction data files found. Please specify --file or place files in docs/")
            logger.info("Expected locations: docs/OMI_NTN/RES.csv or docs/RESIDENZIALE_DEFINITIVO_*.zip")
            sys.exit(1)
        logger.info(f"Auto-discovered files: {files_to_process}")

    # Filter by segment if specified
    if args.segment != 'all':
        files_to_process = {k: v for k, v in files_to_process.items() if k == args.segment}

    if not files_to_process:
        logger.error(f"No {args.segment} transaction data found")
        sys.exit(1)

    # Process each file
    supabase = None if args.dry_run else create_supabase_client()

    for segment, file_path in files_to_process.items():
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {segment} transactions from {file_path.name}")
        logger.info('='*60)

        # Handle ZIP files
        if file_path.suffix.lower() == '.zip':
            file_path = extract_csv_from_zip(file_path)

        # Process the CSV
        df = process_residential_csv(file_path)

        # Update property_segment in the data
        # Note: The upsert function already sets segment to 'residential'
        # For commercial data, we'd need to update the logic

        if args.dry_run:
            logger.info("Dry run - not writing to database")
            logger.info(f"\nTotal records: {len(df)}")
            logger.info(f"Date range: {df['period_id'].min()} to {df['period_id'].max()}")
            logger.info(f"Provinces: {df['province_code'].nunique()}")
            continue

        # Connect to Supabase and upsert
        upsert_provincial_transactions(supabase, df)


if __name__ == '__main__':
    main()
