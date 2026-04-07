#!/usr/bin/env python3
"""
Police Crime Statistics Ingestion Script

Downloads and imports arson-related crime statistics from the Italian government's
CKAN open data portal into the mart.arson_proxy_province_year table.

Data Source:
    Portal: https://dati-coll.dfp.gov.it/
    Dataset: "Delitti denunciati per capoluogo e provincia"
    License: CC BY 4.0

Usage:
    python load_crime_stats.py [--years 2018,2019,2020] [--dry-run]

Environment variables (from frontend/.env.local):
    NEXT_PUBLIC_SUPABASE_URL - Supabase project URL
    SUPABASE_SERVICE_ROLE_KEY - Service role key for database writes

Alternative environment variables:
    DATABASE_URL - Full PostgreSQL connection string, or use:
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import argparse
import csv
import io
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

# Add parent directory to path for db module
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src" / "backend" / "ingest"))

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("Error: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Data source URLs - Direct CSV download links
CRIME_DATA_URLS = {
    2018: "https://dati-coll.dfp.gov.it/dataset/7c1c26e4-bcb7-480c-8db6-89adac30dd0d/resource/5853a54a-164c-4a81-b76e-5cbd9400576a/download/delitti_2018.csv",
    2019: "https://dati-coll.dfp.gov.it/dataset/7c1c26e4-bcb7-480c-8db6-89adac30dd0d/resource/8b86ef4e-1f35-4adb-b1f1-76ad3fc4e409/download/delitti_2019.csv",
    2020: "https://dati-coll.dfp.gov.it/dataset/7c1c26e4-bcb7-480c-8db6-89adac30dd0d/resource/8b2e5f17-985c-4383-bfb9-f1d8da9445f9/download/delitti_2020.csv",
}

# Crime codes to extract (arson-related)
ARSON_CRIME_CODES = {
    "25.0 INCENDI": "incendio",  # General arson/fires
    "27.0 DANNEGGIAMENTO SEGUITO DA INCENDIO": "danneggiamento_seguito_da_incendio",  # Damage followed by fire
}

# Capoluogo (provincial capital) to ISTAT province code mapping
# ISTAT province codes are 3-digit strings (e.g., "001" for Torino)
# The capoluogo typically shares its name with the province
CAPOLUOGO_TO_PROVINCE_CODE: Dict[str, str] = {
    # Piemonte (01)
    "Torino": "001",
    "Vercelli": "002",
    "Novara": "003",
    "Cuneo": "004",
    "Asti": "005",
    "Alessandria": "006",
    "Biella": "096",
    "Verbania": "103",
    # Valle d'Aosta (02)
    "Aosta": "007",
    # Lombardia (03)
    "Varese": "012",
    "Como": "013",
    "Sondrio": "014",
    "Milano": "015",
    "Bergamo": "016",
    "Brescia": "017",
    "Pavia": "018",
    "Cremona": "019",
    "Mantova": "020",
    "Lecco": "097",
    "Lodi": "098",
    "Monza": "108",  # Monza e Brianza
    # Trentino-Alto Adige (04)
    "Bolzano": "021",
    "Trento": "022",
    # Veneto (05)
    "Verona": "023",
    "Vicenza": "024",
    "Belluno": "025",
    "Treviso": "026",
    "Venezia": "027",
    "Padova": "028",
    "Rovigo": "029",
    # Friuli-Venezia Giulia (06)
    "Udine": "030",
    "Gorizia": "031",
    "Trieste": "032",
    "Pordenone": "093",
    # Liguria (07)
    "Imperia": "008",
    "Savona": "009",
    "Genova": "010",
    "La Spezia": "011",
    # Emilia-Romagna (08)
    "Piacenza": "033",
    "Parma": "034",
    "Reggio Emilia": "035",
    "Modena": "036",
    "Bologna": "037",
    "Ferrara": "038",
    "Ravenna": "039",
    "Forli'": "040",  # Note: apostrophe in CSV
    "Rimini": "099",
    # Toscana (09)
    "Massa": "045",  # Massa-Carrara province
    "Lucca": "046",
    "Pistoia": "047",
    "Firenze": "048",
    "Livorno": "049",
    "Pisa": "050",
    "Arezzo": "051",
    "Siena": "052",
    "Grosseto": "053",
    "Prato": "100",
    # Umbria (10)
    "Perugia": "054",
    "Terni": "055",
    # Marche (11)
    "Pesaro": "041",  # Pesaro e Urbino province
    "Ancona": "042",
    "Macerata": "043",
    "Ascoli Piceno": "044",
    "Fermo": "109",
    # Lazio (12)
    "Viterbo": "056",
    "Rieti": "057",
    "Roma": "058",
    "Latina": "059",
    "Frosinone": "060",
    # Abruzzo (13)
    "L'Aquila": "066",
    "Teramo": "067",
    "Pescara": "068",
    "Chieti": "069",
    # Molise (14)
    "Campobasso": "070",
    "Isernia": "094",
    # Campania (15)
    "Caserta": "061",
    "Benevento": "062",
    "Napoli": "063",
    "Avellino": "064",
    "Salerno": "065",
    # Puglia (16)
    "Foggia": "071",
    "Bari": "072",
    "Taranto": "073",
    "Brindisi": "074",
    "Lecce": "075",
    "Barletta-Andria-Trani": "110",
    # Basilicata (17)
    "Potenza": "076",
    "Matera": "077",
    # Calabria (18)
    "Cosenza": "078",
    "Catanzaro": "079",
    "Reggio Calabria": "080",
    "Crotone": "101",
    "Vibo Valentia": "102",
    # Sicilia (19)
    "Trapani": "081",
    "Palermo": "082",
    "Messina": "083",
    "Agrigento": "084",
    "Caltanissetta": "085",
    "Enna": "086",
    "Catania": "087",
    "Ragusa": "088",
    "Siracusa": "089",
    # Sardegna (20)
    "Sassari": "090",
    "Nuoro": "091",
    "Cagliari": "092",
    "Oristano": "095",
    # Note: Sud Sardegna (province code 111) was created in 2016 from parts of other provinces
    # The crime data may still use the old capoluogo names
}


def get_connection_string() -> str:
    """
    Get database connection string from environment variables.

    Supports Supabase connection format and standard PostgreSQL format.
    """
    # Try DATABASE_URL first
    conn_string = os.getenv("DATABASE_URL")
    if conn_string:
        return conn_string

    # Check for individual DB_* components (preferred for Supabase pooler)
    host = os.getenv("DB_HOST")
    password = os.getenv("DB_PASSWORD")

    if host and password:
        port = os.getenv("DB_PORT", "5432")
        database = os.getenv("DB_NAME", "postgres")
        user = os.getenv("DB_USER", "postgres")
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    # Fall back to Supabase direct connection format (requires IPv4 add-on)
    supabase_url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if supabase_url and service_key:
        # Parse Supabase URL to get host
        # Format: https://<project-ref>.supabase.co
        parsed = urlparse(supabase_url)
        project_ref = parsed.hostname.split(".")[0]  # e.g., "vewcbnclnqikufpgzzyu"

        # Supabase PostgreSQL connection format
        # Host: db.<project-ref>.supabase.co
        # Port: 5432
        # Database: postgres
        # User: postgres
        # Password: service_role_key (for service role access)
        host = f"db.{project_ref}.supabase.co"
        return f"postgresql://postgres:{service_key}@{host}:5432/postgres"

    raise ValueError("No database connection configuration found. Set DB_HOST + DB_PASSWORD or DATABASE_URL")


def load_env_file(env_path: Path) -> None:
    """Load environment variables from a .env file."""
    if not env_path.exists():
        logger.warning(f"Environment file not found: {env_path}")
        return

    logger.info(f"Loading environment from: {env_path}")
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                # Remove quotes if present
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                os.environ.setdefault(key, value)


def download_crime_csv(year: int) -> str:
    """
    Download crime statistics CSV for a given year.

    Returns the CSV content as a string.
    """
    url = CRIME_DATA_URLS.get(year)
    if not url:
        raise ValueError(f"No URL configured for year {year}. Available years: {list(CRIME_DATA_URLS.keys())}")

    logger.info(f"Downloading crime data for {year} from {url}")

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        # Try different encodings
        for encoding in ["utf-8", "latin-1", "cp1252"]:
            try:
                return response.content.decode(encoding)
            except UnicodeDecodeError:
                continue

        # Fallback
        return response.content.decode("utf-8", errors="replace")

    except requests.RequestException as e:
        logger.error(f"Failed to download data for {year}: {e}")
        raise


def parse_crime_csv(csv_content: str, year: int) -> List[Dict]:
    """
    Parse crime CSV and extract arson-related records.

    Returns a list of dictionaries with:
    - capoluogo: Province capital name
    - crime_type: 'incendio' or 'danneggiamento_seguito_da_incendio'
    - count: Number of reported crimes
    - year: Year of the data
    """
    records = []

    reader = csv.DictReader(io.StringIO(csv_content))

    for row in reader:
        # Get the crime type (reato)
        reato = row.get("reato", "").strip()

        # Check if this is an arson-related crime
        crime_type = None
        for code, crime_name in ARSON_CRIME_CODES.items():
            if code.lower() == reato.lower():
                crime_type = crime_name
                break

        if not crime_type:
            continue

        # Get capoluogo and count
        capoluogo = row.get("capoluogo", "").strip()
        try:
            count = int(row.get("totale_delitti", 0))
        except (ValueError, TypeError):
            count = 0

        records.append({
            "capoluogo": capoluogo,
            "crime_type": crime_type,
            "count": count,
            "year": year,
            "regione": row.get("regione", "").strip(),
        })

    logger.info(f"Parsed {len(records)} arson-related records for {year}")
    return records


def aggregate_by_province(records: List[Dict]) -> Dict[Tuple[str, int], Dict]:
    """
    Aggregate crime records by province and year.

    Returns a dictionary keyed by (province_code, year) with aggregated counts.
    """
    aggregated = {}
    unmapped = set()

    for record in records:
        capoluogo = record["capoluogo"]
        year = record["year"]
        crime_type = record["crime_type"]
        count = record["count"]

        # Map capoluogo to province code
        province_code = CAPOLUOGO_TO_PROVINCE_CODE.get(capoluogo)

        if not province_code:
            unmapped.add(capoluogo)
            continue

        key = (province_code, year)

        if key not in aggregated:
            aggregated[key] = {
                "province_code": province_code,
                "year": year,
                "count_incendio": 0,
                "count_danneggiamento_seguito_da_incendio": 0,
            }

        if crime_type == "incendio":
            aggregated[key]["count_incendio"] += count
        elif crime_type == "danneggiamento_seguito_da_incendio":
            aggregated[key]["count_danneggiamento_seguito_da_incendio"] += count

    if unmapped:
        logger.warning(f"Could not map the following capoluoghi to province codes: {unmapped}")

    # Calculate totals
    for data in aggregated.values():
        data["count_total_arson_related"] = (
            data["count_incendio"] + data["count_danneggiamento_seguito_da_incendio"]
        )

    logger.info(f"Aggregated to {len(aggregated)} province-year records")
    return aggregated


def get_province_populations(cursor, years: List[int]) -> Dict[Tuple[str, int], int]:
    """
    Fetch province populations from the database for rate calculation.

    Returns a dictionary keyed by (province_code, year) with population counts.
    """
    populations = {}

    try:
        # Try to get from mart.municipality_demographics_year aggregated to province level
        cursor.execute(
            """
            SELECT
                m.province_code,
                d.reference_year,
                SUM(d.total_population) as province_population
            FROM mart.municipality_demographics_year d
            JOIN core.municipalities m ON d.municipality_id = m.municipality_id
            WHERE d.reference_year = ANY(%s)
              AND d.total_population IS NOT NULL
            GROUP BY m.province_code, d.reference_year
            """,
            (years,)
        )

        for row in cursor.fetchall():
            key = (row["province_code"], row["reference_year"])
            populations[key] = row["province_population"]

        logger.info(f"Retrieved population data for {len(populations)} province-year combinations")

    except Exception as e:
        logger.warning(f"Could not fetch population data: {e}")

    return populations


def upsert_arson_data(
    cursor,
    aggregated_data: Dict[Tuple[str, int], Dict],
    populations: Dict[Tuple[str, int], int],
    source_version: str,
    dry_run: bool = False,
) -> int:
    """
    Upsert aggregated arson data into mart.arson_proxy_province_year.

    Returns the number of rows upserted.
    """
    rows_upserted = 0

    for key, data in aggregated_data.items():
        province_code = data["province_code"]
        year = data["year"]

        # Calculate rate per 100k residents if population is available
        population = populations.get(key)
        rate_per_100k = None
        if population and population > 0:
            rate_per_100k = (data["count_total_arson_related"] / population) * 100000

        if dry_run:
            logger.info(
                f"[DRY RUN] Would upsert: province={province_code}, year={year}, "
                f"incendio={data['count_incendio']}, dsi={data['count_danneggiamento_seguito_da_incendio']}, "
                f"total={data['count_total_arson_related']}, rate={rate_per_100k:.2f if rate_per_100k else 'N/A'}"
            )
            rows_upserted += 1
            continue

        cursor.execute(
            """
            INSERT INTO mart.arson_proxy_province_year (
                province_code,
                year,
                count_incendio,
                count_danneggiamento_seguito_da_incendio,
                count_total_arson_related,
                rate_per_100k_residents,
                source_version,
                updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (province_code, year) DO UPDATE SET
                count_incendio = EXCLUDED.count_incendio,
                count_danneggiamento_seguito_da_incendio = EXCLUDED.count_danneggiamento_seguito_da_incendio,
                count_total_arson_related = EXCLUDED.count_total_arson_related,
                rate_per_100k_residents = EXCLUDED.rate_per_100k_residents,
                source_version = EXCLUDED.source_version,
                updated_at = now()
            """,
            (
                province_code,
                year,
                data["count_incendio"],
                data["count_danneggiamento_seguito_da_incendio"],
                data["count_total_arson_related"],
                rate_per_100k,
                source_version,
            )
        )
        rows_upserted += 1

    return rows_upserted


def main():
    parser = argparse.ArgumentParser(
        description="Import arson-related crime statistics from Italian government open data"
    )
    parser.add_argument(
        "--years",
        type=str,
        default=",".join(str(y) for y in CRIME_DATA_URLS.keys()),
        help=f"Comma-separated list of years to import (default: {','.join(str(y) for y in CRIME_DATA_URLS.keys())})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be inserted without actually inserting",
    )
    parser.add_argument(
        "--env-file",
        type=str,
        default=None,
        help="Path to .env file (default: frontend/.env.local)",
    )
    args = parser.parse_args()

    # Parse years
    try:
        years = [int(y.strip()) for y in args.years.split(",")]
    except ValueError:
        logger.error(f"Invalid years format: {args.years}")
        sys.exit(1)

    # Load environment file
    if args.env_file:
        env_path = Path(args.env_file)
    else:
        # Default to frontend/.env.local relative to project root
        project_root = Path(__file__).parent.parent.parent
        env_path = project_root / "frontend" / ".env.local"

    load_env_file(env_path)

    # Validate years
    invalid_years = [y for y in years if y not in CRIME_DATA_URLS]
    if invalid_years:
        logger.error(f"No data available for years: {invalid_years}. Available: {list(CRIME_DATA_URLS.keys())}")
        sys.exit(1)

    logger.info(f"Starting crime statistics ingestion for years: {years}")

    # Download and parse all years
    all_records = []
    for year in years:
        try:
            csv_content = download_crime_csv(year)
            records = parse_crime_csv(csv_content, year)
            all_records.extend(records)
        except Exception as e:
            logger.error(f"Failed to process year {year}: {e}")
            continue

    if not all_records:
        logger.error("No records parsed. Exiting.")
        sys.exit(1)

    # Aggregate by province
    aggregated = aggregate_by_province(all_records)

    # Connect to database
    try:
        conn_string = get_connection_string()
        logger.info("Connecting to database...")

        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        try:
            # Get population data for rate calculation
            populations = get_province_populations(cursor, years)

            # Source version for tracking
            source_version = f"CKAN_DFP_{min(years)}-{max(years)}"

            # Upsert data
            rows_upserted = upsert_arson_data(
                cursor, aggregated, populations, source_version, dry_run=args.dry_run
            )

            if not args.dry_run:
                conn.commit()
                logger.info(f"Successfully upserted {rows_upserted} rows")
            else:
                logger.info(f"[DRY RUN] Would have upserted {rows_upserted} rows")

            # Print summary statistics
            print_summary(aggregated, years)

        except Exception as e:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

    logger.info("Ingestion completed successfully")


def print_summary(aggregated: Dict, years: List[int]) -> None:
    """Print summary statistics of the imported data."""
    print("\n" + "=" * 60)
    print("SUMMARY: Arson-Related Crime Statistics by Year")
    print("=" * 60)

    for year in sorted(years):
        year_data = [v for k, v in aggregated.items() if k[1] == year]
        if not year_data:
            continue

        total_incendio = sum(d["count_incendio"] for d in year_data)
        total_dsi = sum(d["count_danneggiamento_seguito_da_incendio"] for d in year_data)
        total_all = sum(d["count_total_arson_related"] for d in year_data)
        provinces_count = len(year_data)

        print(f"\nYear {year}:")
        print(f"  Provinces with data: {provinces_count}")
        print(f"  Total INCENDI (arson): {total_incendio:,}")
        print(f"  Total DANNEGGIAMENTO SEGUITO DA INCENDIO: {total_dsi:,}")
        print(f"  Total arson-related crimes: {total_all:,}")

        # Top 5 provinces by total arson-related crimes
        top_provinces = sorted(year_data, key=lambda x: x["count_total_arson_related"], reverse=True)[:5]
        print(f"  Top 5 provinces:")
        for i, p in enumerate(top_provinces, 1):
            print(f"    {i}. Province {p['province_code']}: {p['count_total_arson_related']} crimes")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
