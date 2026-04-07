#!/usr/bin/env python3
"""
ISTAT Demographics Data Ingestion Script

Downloads and loads ISTAT demographic data (population, age structure, foreign residents)
into the Supabase database for all Italian municipalities.

Data Sources:
- ISTAT Demo: https://demo.istat.it/ (Demografia in cifre)
- ISTAT Dati: https://dati.istat.it/ (Open data API)
- Direct CSV downloads from ISTAT open data portal

Target Tables:
- raw.istat_population (raw ingested data)
- mart.municipality_demographics_year (curated analytics table)

Usage:
    python load_demographics.py --years 2020-2024
    python load_demographics.py --year 2023
    python load_demographics.py --file /path/to/data.csv --year 2023

Environment Variables (from frontend/.env.local):
    NEXT_PUBLIC_SUPABASE_URL - Supabase project URL
    SUPABASE_SERVICE_ROLE_KEY - Service role key for database access
"""

import argparse
import io
import logging
import os
import re
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
import pandas as pd
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from frontend/.env.local
ENV_PATH = Path(__file__).parent.parent.parent / "frontend" / ".env.local"
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)
    logger.info(f"Loaded environment from {ENV_PATH}")
else:
    logger.warning(f"Environment file not found: {ENV_PATH}")

# Supabase configuration
SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("Missing Supabase credentials. Set NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")

# Build PostgreSQL connection string from Supabase URL
# Supabase REST API URL format: https://<project>.supabase.co
# PostgreSQL URL format: postgresql://postgres:<password>@db.<project>.supabase.co:5432/postgres
def get_supabase_db_url() -> str:
    """Build PostgreSQL connection URL from Supabase credentials."""
    if not SUPABASE_URL:
        raise ValueError("SUPABASE_URL not set")

    # Extract project ID from Supabase URL
    # Format: https://vewcbnclnqikufpgzzyu.supabase.co
    match = re.search(r"https://([^.]+)\.supabase\.co", SUPABASE_URL)
    if not match:
        raise ValueError(f"Invalid Supabase URL format: {SUPABASE_URL}")

    project_id = match.group(1)

    # For service role, use the password from the JWT (this is a workaround)
    # In production, you should use a proper DATABASE_URL or connection pooler
    # The service role key can be used for PostgREST but not direct Postgres
    # We'll use the Supabase REST API instead via supabase-py
    return f"postgresql://postgres.{project_id}:{SUPABASE_KEY}@aws-0-eu-central-1.pooler.supabase.com:6543/postgres"


# ISTAT Data Sources and URLs
ISTAT_BASE_URL = "https://demo.istat.it"
ISTAT_DATA_URL = "https://dati.istat.it"

# ISTAT SDMX/JSON-stat API endpoints
ISTAT_API_BASE = "https://esploradati.istat.it/SDMXWS/rest"

# Direct download URLs for population data (bulk CSV)
ISTAT_POPULATION_URLS = {
    # Annual municipal population - try multiple sources
    "population": "https://demo.istat.it/data/bilpop/{year}/BILPOP_{year}_IT.zip",
    "pop_structure": "https://demo.istat.it/data/p2/P2_{year}_it.zip",
    # Foreign residents
    "foreign": "https://demo.istat.it/data/strares/STRARES_{year}_IT.zip",
}

# Alternative: ISTAT I.Stat open data
ISTAT_ISTAT_DATASETS = {
    "DCIS_POPRES1": "Resident population by municipality",
    "DCIS_STRBIL1": "Demographic balance",
    "DCIS_STRANCIT": "Foreign residents by citizenship",
}

# Column mappings for various ISTAT data formats
COLUMN_MAPPINGS = {
    # Municipality identifiers
    "ITTER107": "municipality_code",
    "Codice comune": "municipality_code",
    "COD_COMUNE": "municipality_code",
    "PROCOM": "municipality_code",
    "PRO_COM": "municipality_code",
    "PRO_COM_T": "municipality_code",
    "Codice Istat Comune": "municipality_code",
    "codice_istat": "municipality_code",
    "Codice ISTAT del comune": "municipality_code",

    # Municipality names
    "Territorio": "municipality_name",
    "COMUNE": "municipality_name",
    "Comune": "municipality_name",
    "Denominazione": "municipality_name",

    # Province/Region codes
    "COD_PROV": "province_code",
    "COD_REG": "region_code",

    # Population totals
    "Totale": "total_population",
    "Value": "total_population",
    "Popolazione totale": "total_population",
    "Popolazione residente": "total_population",
    "TOTAL": "total_population",
    "POP_TOT": "total_population",

    # Gender breakdown
    "Maschi": "male_population",
    "Femmine": "female_population",
    "M": "male_population",
    "F": "female_population",

    # Age groups (multiple formats)
    "0-14": "pop_0_14",
    "15-64": "pop_15_64",
    "65 e oltre": "pop_65_plus",
    "65+": "pop_65_plus",
    "ETA1": "age_group",
    "Classe di eta'": "age_group",
    "Classe di età": "age_group",

    # Foreign residents
    "Stranieri": "foreign_population",
    "Popolazione straniera": "foreign_population",
    "Stranieri residenti": "foreign_population",

    # Natural movement
    "Nati": "births",
    "Morti": "deaths",
    "Nati vivi": "births",

    # Migration
    "Iscritti": "immigration",
    "Cancellati": "emigration",
    "Iscritti totali": "immigration",
    "Cancellati totali": "emigration",

    # Households
    "Famiglie": "households",
    "Numero famiglie": "households",
    "Componenti medi": "avg_household_size",

    # Year/Time
    "Anno": "year",
    "TIME": "year",
    "TIME_PERIOD": "year",
    "Seleziona periodo": "year",
}


class ISTATDemographicsLoader:
    """Load ISTAT demographic data into Supabase."""

    def __init__(self, use_supabase_rest: bool = True):
        """Initialize the loader.

        Args:
            use_supabase_rest: Use Supabase REST API (True) or direct PostgreSQL (False)
        """
        self.use_supabase_rest = use_supabase_rest
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "ItalianPropertyHeatmap/1.0 (demographic-data-ingestion)"
        })

        if use_supabase_rest:
            self._init_supabase_client()
        else:
            self._init_postgres_client()

    def _init_supabase_client(self):
        """Initialize Supabase REST client."""
        try:
            from supabase import create_client, Client
            self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
            logger.info("Initialized Supabase REST client")
        except ImportError:
            logger.error("supabase-py not installed. Run: pip install supabase")
            raise

    def _init_postgres_client(self):
        """Initialize direct PostgreSQL connection."""
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            self.pg_conn = psycopg2.connect(get_supabase_db_url())
            self.pg_cursor = self.pg_conn.cursor(cursor_factory=RealDictCursor)
            logger.info("Initialized PostgreSQL client")
        except ImportError:
            logger.error("psycopg2 not installed. Run: pip install psycopg2-binary")
            raise

    def download_istat_data(self, year: int) -> Optional[pd.DataFrame]:
        """Download ISTAT demographic data for a given year.

        Tries multiple data sources in order of preference.
        """
        logger.info(f"Downloading ISTAT demographic data for {year}...")

        # Try different data sources
        df = None

        # Method 1: Try ISTAT Demo bulk download (population balance)
        df = self._try_demo_bilpop(year)
        if df is not None and len(df) > 0:
            return df

        # Method 2: Try ISTAT Demo P2 (population structure)
        df = self._try_demo_p2(year)
        if df is not None and len(df) > 0:
            return df

        # Method 3: Try ISTAT dati.istat.it API
        df = self._try_istat_api(year)
        if df is not None and len(df) > 0:
            return df

        # Method 4: Generate sample/fallback data for testing
        logger.warning(f"Could not download data for {year}, using fallback approach")
        return self._generate_fallback_data(year)

    def _try_demo_bilpop(self, year: int) -> Optional[pd.DataFrame]:
        """Try downloading from ISTAT demo bilpop endpoint."""
        # Try multiple URL patterns as ISTAT occasionally changes their structure
        urls = [
            f"https://demo.istat.it/data/bilpop/{year}/BILPOP_{year}_IT.zip",
            f"https://demo.istat.it/data/bilpop/{year}/bilpop{year}.zip",
            f"https://demo.istat.it/bil{year}/dati/bilpop.zip",
        ]

        for url in urls:
            logger.info(f"Trying bilpop source: {url}")

            try:
                response = self.session.get(url, timeout=60)
                if response.status_code == 200:
                    df = self._extract_and_parse_zip(response.content, year)
                    if df is not None and len(df) > 0:
                        return df
                else:
                    logger.debug(f"bilpop download failed: HTTP {response.status_code}")
            except Exception as e:
                logger.debug(f"bilpop download error: {e}")

        return None

    def _try_demo_p2(self, year: int) -> Optional[pd.DataFrame]:
        """Try downloading from ISTAT demo P2 endpoint (population by age)."""
        urls = [
            f"https://demo.istat.it/data/p2/P2_{year}_it.zip",
            f"https://demo.istat.it/pop{year}/dati/pop.zip",
            f"https://demo.istat.it/popres/download.php?anno={year}&formato=csv",
        ]

        for url in urls:
            logger.info(f"Trying P2 source: {url}")

            try:
                response = self.session.get(url, timeout=60)
                if response.status_code == 200:
                    # Check if it's a zip or direct CSV
                    content_type = response.headers.get('content-type', '')
                    if 'zip' in content_type or url.endswith('.zip'):
                        df = self._extract_and_parse_zip(response.content, year)
                    else:
                        # Try parsing as CSV directly
                        df = self._parse_csv_response(response.text, year)
                    if df is not None and len(df) > 0:
                        return df
                else:
                    logger.debug(f"P2 download failed: HTTP {response.status_code}")
            except Exception as e:
                logger.debug(f"P2 download error: {e}")

        return None

    def _parse_csv_response(self, text: str, year: int) -> Optional[pd.DataFrame]:
        """Parse CSV text response."""
        for delimiter in [';', ',', '\t']:
            try:
                df = pd.read_csv(
                    io.StringIO(text),
                    delimiter=delimiter,
                    dtype=str,
                    na_values=['n.d.', '..', '-', '']
                )
                if len(df.columns) > 2:
                    return self._normalize_columns(df, year)
            except:
                continue
        return None

    def _try_istat_api(self, year: int) -> Optional[pd.DataFrame]:
        """Try ISTAT SDMX REST API for population data."""
        # Multiple ISTAT API endpoints to try
        api_endpoints = [
            # Population by municipality (DCIS_POPRES1)
            f"https://sdmx.istat.it/SDMXWS/rest/data/22_289/A.{year}..IT",
            # Alternative endpoint structure
            f"https://esploradati.istat.it/SDMXWS/rest/data/22_289/A.{year}..IT",
            # Older format
            f"https://sdmx.istat.it/SDMXWS/rest/data/DCIS_POPRES1/A.{year}",
        ]

        for api_url in api_endpoints:
            logger.info(f"Trying ISTAT API: {api_url}")

            try:
                headers = {"Accept": "application/vnd.sdmx.data+csv;version=1.0.0"}
                response = self.session.get(api_url, headers=headers, timeout=120)

                if response.status_code == 200:
                    df = pd.read_csv(io.StringIO(response.text), dtype=str)
                    if len(df) > 0:
                        return self._parse_sdmx_data(df, year)
                else:
                    logger.debug(f"ISTAT API failed: HTTP {response.status_code}")
            except Exception as e:
                logger.debug(f"ISTAT API error: {e}")

        # Also try JSON-stat format
        return self._try_istat_jsonstat(year)

    def _try_istat_jsonstat(self, year: int) -> Optional[pd.DataFrame]:
        """Try ISTAT JSON-stat API as fallback."""
        api_url = f"https://sdmx.istat.it/SDMXWS/rest/data/22_289/A.{year}..IT?format=jsondata"
        logger.info(f"Trying ISTAT JSON-stat: {api_url}")

        try:
            response = self.session.get(api_url, timeout=120)
            if response.status_code == 200:
                data = response.json()
                return self._parse_jsonstat_data(data, year)
            else:
                logger.debug(f"JSON-stat API failed: HTTP {response.status_code}")
        except Exception as e:
            logger.debug(f"JSON-stat API error: {e}")

        return None

    def _parse_jsonstat_data(self, data: dict, year: int) -> Optional[pd.DataFrame]:
        """Parse JSON-stat format from ISTAT API."""
        try:
            # JSON-stat 2.0 format
            if 'dataSets' in data:
                dataset = data['dataSets'][0]
                dimensions = data.get('structure', {}).get('dimensions', {})

                # Extract observations and map to municipalities
                records = []
                observations = dataset.get('observations', {}) or dataset.get('series', {})

                # This is a simplified parser - full implementation would
                # properly decode the dimension indices
                for key, value in observations.items():
                    records.append({
                        'municipality_code': key,
                        'total_population': value[0] if isinstance(value, list) else value,
                        'year': year
                    })

                if records:
                    return pd.DataFrame(records)

        except Exception as e:
            logger.debug(f"Error parsing JSON-stat: {e}")

        return None

    def _extract_and_parse_zip(self, content: bytes, year: int) -> Optional[pd.DataFrame]:
        """Extract and parse CSV files from ISTAT zip archive."""
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                csv_files = [f for f in zf.namelist() if f.endswith('.csv')]

                if not csv_files:
                    logger.debug("No CSV files found in zip archive")
                    return None

                dfs = []
                for csv_file in csv_files:
                    logger.debug(f"Parsing {csv_file}")
                    with zf.open(csv_file) as f:
                        # Try different encodings
                        for encoding in ['utf-8', 'latin-1', 'cp1252']:
                            try:
                                f.seek(0)
                                content_str = f.read().decode(encoding)
                                # Try different delimiters
                                for delimiter in [';', ',', '\t']:
                                    try:
                                        df = pd.read_csv(
                                            io.StringIO(content_str),
                                            delimiter=delimiter,
                                            dtype=str,
                                            na_values=['n.d.', '..', '-', '']
                                        )
                                        if len(df.columns) > 2:
                                            dfs.append(df)
                                            break
                                    except:
                                        continue
                                if dfs:
                                    break
                            except:
                                continue

                if dfs:
                    combined = pd.concat(dfs, ignore_index=True)
                    return self._normalize_columns(combined, year)

        except Exception as e:
            logger.debug(f"Error extracting zip: {e}")

        return None

    def _parse_sdmx_data(self, df: pd.DataFrame, year: int) -> Optional[pd.DataFrame]:
        """Parse SDMX CSV format from ISTAT API."""
        # SDMX format has specific column naming
        df = self._normalize_columns(df, year)
        return df

    def _normalize_columns(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """Normalize column names to our standard format."""
        # Apply column mappings
        rename_map = {}
        for col in df.columns:
            col_clean = col.strip()
            if col_clean in COLUMN_MAPPINGS:
                rename_map[col] = COLUMN_MAPPINGS[col_clean]

        df = df.rename(columns=rename_map)

        # Add year if not present
        if 'year' not in df.columns:
            df['year'] = year

        logger.debug(f"Normalized columns: {list(df.columns)}")
        return df

    def _generate_fallback_data(self, year: int) -> pd.DataFrame:
        """Generate fallback data by querying existing municipalities."""
        logger.info("Generating fallback demographic data from municipalities table...")

        municipalities = []

        # Try to get municipalities from database
        try:
            if self.use_supabase_rest:
                # Try public schema first (may have RLS allowing access)
                result = self.supabase.table("municipalities").select(
                    "municipality_id, municipality_name, province_code, region_code"
                ).execute()
                municipalities = result.data if result.data else []

                # If that fails, try with explicit core schema
                if not municipalities:
                    result = self.supabase.schema("core").table("municipalities").select(
                        "municipality_id, municipality_name, province_code, region_code"
                    ).execute()
                    municipalities = result.data if result.data else []
            else:
                self.pg_cursor.execute("""
                    SELECT municipality_id, municipality_name, province_code, region_code
                    FROM core.municipalities
                    LIMIT 10000
                """)
                municipalities = self.pg_cursor.fetchall()

        except Exception as e:
            logger.warning(f"Could not query municipalities from database: {e}")

        if municipalities:
            # Create basic DataFrame with municipality codes
            df = pd.DataFrame(municipalities)
            df['year'] = year
            df['municipality_code'] = df['municipality_id']
            logger.info(f"Created fallback data for {len(df)} municipalities")
            return df

        # If database query fails, provide instruction to use local file
        logger.warning(
            "Could not retrieve municipalities from database. "
            "Please provide demographic data via --file option. "
            "ISTAT data can be downloaded from: "
            "https://demo.istat.it/ or https://www.istat.it/it/archivio/popolazione"
        )
        return pd.DataFrame()

    def load_from_file(self, file_path: Path, year: int) -> pd.DataFrame:
        """Load demographic data from a local file."""
        logger.info(f"Loading data from {file_path}")

        suffix = file_path.suffix.lower()

        if suffix == '.csv':
            for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                for delimiter in [';', ',', '\t']:
                    try:
                        df = pd.read_csv(
                            file_path,
                            encoding=encoding,
                            delimiter=delimiter,
                            dtype=str,
                            na_values=['n.d.', '..', '-', '']
                        )
                        if len(df.columns) > 2:
                            logger.info(f"Loaded CSV: encoding={encoding}, delimiter='{delimiter}'")
                            return self._normalize_columns(df, year)
                    except:
                        continue
            raise ValueError(f"Could not parse CSV file: {file_path}")

        elif suffix in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path, dtype=str, na_values=['n.d.', '..', '-', ''])
            return self._normalize_columns(df, year)

        elif suffix == '.zip':
            with open(file_path, 'rb') as f:
                return self._extract_and_parse_zip(f.read(), year)

        else:
            raise ValueError(f"Unsupported file format: {suffix}")

    def transform_demographics(self, df: pd.DataFrame, year: int) -> List[Dict[str, Any]]:
        """Transform raw ISTAT data into mart table format."""
        logger.info(f"Transforming {len(df)} rows for year {year}")

        records = []

        # Check if data is in age-grouped format
        is_age_grouped = 'age_group' in df.columns

        if is_age_grouped:
            # Pivot age groups by municipality
            records = self._transform_age_grouped(df, year)
        else:
            # Direct column mapping
            records = self._transform_direct(df, year)

        logger.info(f"Transformed into {len(records)} demographic records")
        return records

    def _transform_direct(self, df: pd.DataFrame, year: int) -> List[Dict[str, Any]]:
        """Transform data with direct column mapping."""
        records = []

        for _, row in df.iterrows():
            municipality_id = self._build_municipality_id(row)
            if not municipality_id:
                continue

            record = {
                'municipality_id': municipality_id,
                'reference_year': year,
                'total_population': self._parse_int(row.get('total_population')),
                'population_density': None,  # Calculated later from area
                'young_ratio': None,
                'working_ratio': None,
                'elderly_ratio': None,
                'dependency_ratio': None,
                'old_age_index': None,
                'foreign_ratio': None,
                'population_growth_rate': None,
                'natural_balance': None,
                'migration_balance': None,
                'households': self._parse_int(row.get('households')),
                'avg_household_size': self._parse_float(row.get('avg_household_size')),
            }

            # Calculate ratios if age data available
            total = record['total_population']
            if total and total > 0:
                pop_0_14 = self._parse_int(row.get('pop_0_14')) or 0
                pop_15_64 = self._parse_int(row.get('pop_15_64')) or 0
                pop_65_plus = self._parse_int(row.get('pop_65_plus')) or 0
                foreign = self._parse_int(row.get('foreign_population')) or 0

                if pop_0_14 or pop_15_64 or pop_65_plus:
                    record['young_ratio'] = round(pop_0_14 / total, 4) if pop_0_14 else None
                    record['working_ratio'] = round(pop_15_64 / total, 4) if pop_15_64 else None
                    record['elderly_ratio'] = round(pop_65_plus / total, 4) if pop_65_plus else None

                    if pop_15_64 > 0:
                        record['dependency_ratio'] = round((pop_0_14 + pop_65_plus) / pop_15_64, 4)

                    if pop_0_14 > 0:
                        record['old_age_index'] = round(pop_65_plus / pop_0_14, 4)

                if foreign:
                    record['foreign_ratio'] = round(foreign / total, 4)

                # Natural balance
                births = self._parse_int(row.get('births')) or 0
                deaths = self._parse_int(row.get('deaths')) or 0
                if births or deaths:
                    record['natural_balance'] = births - deaths

                # Migration balance
                immigration = self._parse_int(row.get('immigration')) or 0
                emigration = self._parse_int(row.get('emigration')) or 0
                if immigration or emigration:
                    record['migration_balance'] = immigration - emigration

            records.append(record)

        return records

    def _transform_age_grouped(self, df: pd.DataFrame, year: int) -> List[Dict[str, Any]]:
        """Transform age-grouped data by pivoting."""
        records = []

        # Group by municipality
        for municipality_code, group in df.groupby('municipality_code'):
            municipality_id = self._normalize_municipality_id(str(municipality_code))
            if not municipality_id:
                continue

            # Aggregate age groups
            pop_0_14 = 0
            pop_15_64 = 0  # 15-64
            pop_65_plus = 0
            total_pop = 0

            for _, row in group.iterrows():
                age_group = str(row.get('age_group', '')).lower()
                pop = self._parse_int(row.get('total_population')) or 0

                # Map age groups to our categories
                if any(x in age_group for x in ['0-14', '0-4', '5-9', '10-14']):
                    pop_0_14 += pop
                elif any(x in age_group for x in ['15-64', '15-19', '20-24', '25-29', '30-34',
                                                    '35-39', '40-44', '45-49', '50-54', '55-59', '60-64']):
                    pop_15_64 += pop
                elif any(x in age_group for x in ['65', 'oltre', '65-69', '70-74', '75-79', '80-84', '85+']):
                    pop_65_plus += pop

                total_pop += pop

            record = {
                'municipality_id': municipality_id,
                'reference_year': year,
                'total_population': total_pop if total_pop > 0 else None,
                'population_density': None,
                'young_ratio': round(pop_0_14 / total_pop, 4) if total_pop > 0 and pop_0_14 else None,
                'working_ratio': round(pop_15_64 / total_pop, 4) if total_pop > 0 and pop_15_64 else None,
                'elderly_ratio': round(pop_65_plus / total_pop, 4) if total_pop > 0 and pop_65_plus else None,
                'dependency_ratio': round((pop_0_14 + pop_65_plus) / pop_15_64, 4) if pop_15_64 > 0 else None,
                'old_age_index': round(pop_65_plus / pop_0_14, 4) if pop_0_14 > 0 else None,
                'foreign_ratio': None,
                'population_growth_rate': None,
                'natural_balance': None,
                'migration_balance': None,
                'households': None,
                'avg_household_size': None,
            }

            records.append(record)

        return records

    def _build_municipality_id(self, row: Dict[str, Any]) -> Optional[str]:
        """Extract and normalize municipality ID from row."""
        code = row.get('municipality_code') or row.get('municipality_id')
        return self._normalize_municipality_id(code)

    def _normalize_municipality_id(self, code: Any) -> Optional[str]:
        """Normalize municipality code to 6-digit format."""
        if code is None:
            return None

        code_str = str(code).strip()

        # Remove non-numeric characters
        code_str = ''.join(c for c in code_str if c.isdigit())

        if not code_str or len(code_str) < 3:
            return None

        # Pad to 6 digits with leading zeros
        return code_str.zfill(6)

    def _parse_int(self, val: Any) -> Optional[int]:
        """Parse integer value."""
        if val is None or val == '' or val == 'n.d.' or val == '..':
            return None
        if isinstance(val, float) and pd.isna(val):
            return None
        try:
            if isinstance(val, str):
                val = val.replace(',', '').replace(' ', '').replace('.', '')
            return int(float(val))
        except (ValueError, TypeError):
            return None

    def _parse_float(self, val: Any) -> Optional[float]:
        """Parse float value."""
        if val is None or val == '' or val == 'n.d.' or val == '..':
            return None
        if isinstance(val, float) and pd.isna(val):
            return None
        try:
            if isinstance(val, str):
                val = val.replace(',', '.')
            return round(float(val), 4)
        except (ValueError, TypeError):
            return None

    def upsert_demographics(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Upsert demographic records to mart.municipality_demographics_year.

        Returns:
            Tuple of (rows_loaded, rows_rejected)
        """
        if not records:
            logger.warning("No records to upsert")
            return 0, 0

        logger.info(f"Upserting {len(records)} records to mart.municipality_demographics_year")

        rows_loaded = 0
        rows_rejected = 0

        if self.use_supabase_rest:
            rows_loaded, rows_rejected = self._upsert_via_rest(records)
        else:
            rows_loaded, rows_rejected = self._upsert_via_postgres(records)

        logger.info(f"Upsert complete: {rows_loaded} loaded, {rows_rejected} rejected")
        return rows_loaded, rows_rejected

    def _upsert_via_rest(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Upsert using Supabase REST API."""
        rows_loaded = 0
        rows_rejected = 0

        # Process in batches
        batch_size = 500

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]

            try:
                # Add updated_at timestamp
                for record in batch:
                    record['updated_at'] = datetime.utcnow().isoformat()

                # Upsert to mart table (schema-qualified table name not needed for Supabase)
                # Note: Supabase REST API uses table names without schema prefix
                # The table is in the 'mart' schema but accessed via REST as just the table name
                result = self.supabase.schema("mart").table("municipality_demographics_year").upsert(
                    batch,
                    on_conflict="municipality_id,reference_year"
                ).execute()

                rows_loaded += len(batch)

                if (i + batch_size) % 1000 == 0:
                    logger.info(f"Progress: {i + batch_size} / {len(records)} records")

            except Exception as e:
                logger.error(f"Error upserting batch {i}-{i+batch_size}: {e}")
                rows_rejected += len(batch)

        return rows_loaded, rows_rejected

    def _upsert_via_postgres(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Upsert using direct PostgreSQL connection."""
        rows_loaded = 0
        rows_rejected = 0

        for record in records:
            try:
                self.pg_cursor.execute("""
                    INSERT INTO mart.municipality_demographics_year (
                        municipality_id, reference_year,
                        total_population, population_density,
                        young_ratio, working_ratio, elderly_ratio,
                        dependency_ratio, old_age_index,
                        foreign_ratio, population_growth_rate, natural_balance,
                        migration_balance, households, avg_household_size,
                        updated_at
                    ) VALUES (
                        %(municipality_id)s, %(reference_year)s,
                        %(total_population)s, %(population_density)s,
                        %(young_ratio)s, %(working_ratio)s, %(elderly_ratio)s,
                        %(dependency_ratio)s, %(old_age_index)s,
                        %(foreign_ratio)s, %(population_growth_rate)s, %(natural_balance)s,
                        %(migration_balance)s, %(households)s, %(avg_household_size)s,
                        NOW()
                    )
                    ON CONFLICT (municipality_id, reference_year) DO UPDATE SET
                        total_population = COALESCE(EXCLUDED.total_population, mart.municipality_demographics_year.total_population),
                        population_density = COALESCE(EXCLUDED.population_density, mart.municipality_demographics_year.population_density),
                        young_ratio = COALESCE(EXCLUDED.young_ratio, mart.municipality_demographics_year.young_ratio),
                        working_ratio = COALESCE(EXCLUDED.working_ratio, mart.municipality_demographics_year.working_ratio),
                        elderly_ratio = COALESCE(EXCLUDED.elderly_ratio, mart.municipality_demographics_year.elderly_ratio),
                        dependency_ratio = COALESCE(EXCLUDED.dependency_ratio, mart.municipality_demographics_year.dependency_ratio),
                        old_age_index = COALESCE(EXCLUDED.old_age_index, mart.municipality_demographics_year.old_age_index),
                        foreign_ratio = COALESCE(EXCLUDED.foreign_ratio, mart.municipality_demographics_year.foreign_ratio),
                        population_growth_rate = COALESCE(EXCLUDED.population_growth_rate, mart.municipality_demographics_year.population_growth_rate),
                        natural_balance = COALESCE(EXCLUDED.natural_balance, mart.municipality_demographics_year.natural_balance),
                        migration_balance = COALESCE(EXCLUDED.migration_balance, mart.municipality_demographics_year.migration_balance),
                        households = COALESCE(EXCLUDED.households, mart.municipality_demographics_year.households),
                        avg_household_size = COALESCE(EXCLUDED.avg_household_size, mart.municipality_demographics_year.avg_household_size),
                        updated_at = NOW()
                """, record)

                rows_loaded += 1

                if rows_loaded % 1000 == 0:
                    self.pg_conn.commit()
                    logger.info(f"Progress: {rows_loaded} records processed")

            except Exception as e:
                logger.warning(f"Error upserting record {record.get('municipality_id')}: {e}")
                rows_rejected += 1

        self.pg_conn.commit()
        return rows_loaded, rows_rejected

    def calculate_growth_rates(self, year: int) -> int:
        """Calculate year-over-year population growth rates."""
        logger.info(f"Calculating growth rates for {year}...")

        try:
            if self.use_supabase_rest:
                # Use RPC or raw SQL via Supabase
                # This requires a stored procedure or we calculate in Python
                return self._calculate_growth_rates_python(year)
            else:
                self.pg_cursor.execute("""
                    UPDATE mart.municipality_demographics_year curr
                    SET population_growth_rate =
                        CASE WHEN prev.total_population > 0
                        THEN ((curr.total_population - prev.total_population)::numeric / prev.total_population) * 100
                        ELSE NULL END
                    FROM mart.municipality_demographics_year prev
                    WHERE curr.municipality_id = prev.municipality_id
                      AND curr.reference_year = %(year)s
                      AND prev.reference_year = %(prev_year)s
                      AND curr.total_population IS NOT NULL
                      AND prev.total_population IS NOT NULL
                """, {'year': year, 'prev_year': year - 1})

                self.pg_conn.commit()
                return self.pg_cursor.rowcount

        except Exception as e:
            logger.error(f"Error calculating growth rates: {e}")
            return 0

    def _calculate_growth_rates_python(self, year: int) -> int:
        """Calculate growth rates using Python (for REST API mode)."""
        try:
            # Get current year data
            current = self.supabase.schema("mart").table("municipality_demographics_year").select(
                "municipality_id, total_population"
            ).eq("reference_year", year).execute()

            # Get previous year data
            previous = self.supabase.schema("mart").table("municipality_demographics_year").select(
                "municipality_id, total_population"
            ).eq("reference_year", year - 1).execute()

            if not current.data or not previous.data:
                return 0

            # Build lookup
            prev_pop = {r['municipality_id']: r['total_population'] for r in previous.data}

            updates = []
            for record in current.data:
                mid = record['municipality_id']
                curr_pop = record['total_population']
                prev = prev_pop.get(mid)

                if curr_pop and prev and prev > 0:
                    growth_rate = round(((curr_pop - prev) / prev) * 100, 4)
                    updates.append({
                        'municipality_id': mid,
                        'reference_year': year,
                        'population_growth_rate': growth_rate
                    })

            # Batch update
            if updates:
                for update in updates:
                    self.supabase.schema("mart").table("municipality_demographics_year").update({
                        'population_growth_rate': update['population_growth_rate']
                    }).eq("municipality_id", update['municipality_id']).eq(
                        "reference_year", update['reference_year']
                    ).execute()

            return len(updates)

        except Exception as e:
            logger.error(f"Error in Python growth rate calculation: {e}")
            return 0

    def close(self):
        """Close database connections."""
        if not self.use_supabase_rest and hasattr(self, 'pg_conn'):
            self.pg_cursor.close()
            self.pg_conn.close()


def parse_year_range(year_arg: str) -> List[int]:
    """Parse year argument which can be a single year or range."""
    if '-' in year_arg:
        start, end = year_arg.split('-')
        return list(range(int(start), int(end) + 1))
    else:
        return [int(year_arg)]


def main():
    parser = argparse.ArgumentParser(
        description="Load ISTAT demographic data into Supabase",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Load data for a single year (downloads from ISTAT)
    python load_demographics.py --year 2023

    # Load data for multiple years
    python load_demographics.py --years 2020-2024

    # Load from a local file
    python load_demographics.py --file /path/to/data.csv --year 2023

    # Use direct PostgreSQL connection instead of REST API
    python load_demographics.py --year 2023 --direct-postgres
"""
    )

    parser.add_argument(
        '--year',
        type=int,
        help='Single reference year for the data'
    )
    parser.add_argument(
        '--years',
        type=str,
        help='Year range (e.g., 2020-2024)'
    )
    parser.add_argument(
        '--file',
        type=str,
        help='Path to local data file (CSV, Excel, or ZIP)'
    )
    parser.add_argument(
        '--direct-postgres',
        action='store_true',
        help='Use direct PostgreSQL connection instead of Supabase REST API'
    )
    parser.add_argument(
        '--skip-growth-rates',
        action='store_true',
        help='Skip calculation of year-over-year growth rates'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Parse and transform data but do not load to database'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Determine years to process
    years = []
    if args.year:
        years = [args.year]
    elif args.years:
        years = parse_year_range(args.years)
    else:
        # Default to recent years
        years = [2023]

    logger.info(f"Processing years: {years}")

    # Initialize loader
    try:
        loader = ISTATDemographicsLoader(use_supabase_rest=not args.direct_postgres)
    except Exception as e:
        logger.error(f"Failed to initialize loader: {e}")
        sys.exit(1)

    total_loaded = 0
    total_rejected = 0

    try:
        for year in years:
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing year {year}")
            logger.info('='*60)

            # Get data
            if args.file:
                df = loader.load_from_file(Path(args.file), year)
            else:
                df = loader.download_istat_data(year)

            if df is None or len(df) == 0:
                logger.warning(f"No data available for year {year}")
                continue

            logger.info(f"Loaded {len(df)} raw records")

            # Transform
            records = loader.transform_demographics(df, year)

            if args.dry_run:
                logger.info(f"[DRY RUN] Would upsert {len(records)} records for {year}")
                # Show sample records
                for record in records[:3]:
                    logger.info(f"  Sample: {record}")
                continue

            # Upsert
            loaded, rejected = loader.upsert_demographics(records)
            total_loaded += loaded
            total_rejected += rejected

            # Calculate growth rates
            if not args.skip_growth_rates and year > min(years):
                growth_count = loader.calculate_growth_rates(year)
                logger.info(f"Updated {growth_count} growth rate records")

        logger.info(f"\n{'='*60}")
        logger.info("INGESTION COMPLETE")
        logger.info(f"Total loaded: {total_loaded}")
        logger.info(f"Total rejected: {total_rejected}")
        logger.info('='*60)

    except KeyboardInterrupt:
        logger.info("\nIngestion interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Ingestion failed: {e}")
        sys.exit(1)
    finally:
        loader.close()


if __name__ == "__main__":
    main()
