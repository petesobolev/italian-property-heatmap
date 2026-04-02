#!/usr/bin/env python3
"""
ISTAT Population Ingestion Script

Imports ISTAT population demographics data into the database.

Data sources:
- http://demo.istat.it/ (Demographic data)
- https://www.istat.it/it/archivio/popolazione (Population statistics)

Usage:
    python istat_population.py --file <path_to_csv_or_xlsx> --year 2023

Environment variables:
    DATABASE_URL - Full PostgreSQL connection string, or use:
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, Optional, Any

import pandas as pd

from db import (
    create_ingestion_run,
    complete_ingestion_run,
    get_db_cursor,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ISTAT column mappings - various formats used across different ISTAT datasets
COLUMN_MAPPINGS = {
    # Identifiers
    "ITTER107": "municipality_id",
    "Codice comune": "municipality_id",
    "Cod_Comune": "municipality_id",
    "PROCOM": "municipality_id",
    "PRO_COM": "municipality_id",
    "Territorio": "municipality_name",
    # Population totals
    "Value": "total_population",
    "Totale": "total_population",
    "Popolazione": "total_population",
    "Popolazione totale": "total_population",
    "Total": "total_population",
    # Gender breakdown
    "Maschi": "male_population",
    "Femmine": "female_population",
    "Males": "male_population",
    "Females": "female_population",
    # Age groups
    "0-14": "population_0_14",
    "15-24": "population_15_24",
    "25-44": "population_25_44",
    "45-64": "population_45_64",
    "65+": "population_65_plus",
    "65 e oltre": "population_65_plus",
    # Alternative age columns
    "ETA1": "age_group",
    "Età": "age_group",
    # Foreign population
    "Stranieri": "foreign_population",
    "Popolazione straniera": "foreign_population",
    "Foreign": "foreign_population",
    # Natural movement
    "Nati": "births",
    "Morti": "deaths",
    "Births": "births",
    "Deaths": "deaths",
    # Migration
    "Iscritti": "immigration",
    "Cancellati": "emigration",
    "Immigration": "immigration",
    "Emigration": "emigration",
    # Households
    "Famiglie": "households",
    "Numero medio componenti": "avg_household_size",
    # Year
    "Anno": "year",
    "TIME": "year",
    "Year": "year",
    "Seleziona periodo": "year",
}


def parse_value(val: Any) -> Optional[int]:
    """Parse a population value field."""
    if val is None or val == "" or val == "n.d." or val == "..":
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    try:
        if isinstance(val, str):
            val = val.replace(",", "").replace(" ", "").replace(".", "")
        return int(float(val))
    except (ValueError, TypeError):
        return None


def parse_float(val: Any) -> Optional[float]:
    """Parse a float value field."""
    if val is None or val == "" or val == "n.d." or val == "..":
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    try:
        if isinstance(val, str):
            val = val.replace(",", ".")
        return float(val)
    except (ValueError, TypeError):
        return None


def build_municipality_id(row: Dict[str, Any]) -> str:
    """Build 6-digit ISTAT municipality code."""
    municipality_id = row.get("municipality_id", "")
    if municipality_id:
        # Handle various formats
        mid = str(municipality_id).strip()
        # Remove any non-numeric characters
        mid = "".join(c for c in mid if c.isdigit())
        if mid:
            return mid.zfill(6)
    return ""


def load_population_data(file_path: Path) -> pd.DataFrame:
    """Load population data from CSV or Excel file."""
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        for encoding in ["utf-8", "latin-1", "cp1252", "iso-8859-1"]:
            for delimiter in [";", ",", "\t"]:
                try:
                    df = pd.read_csv(
                        file_path,
                        encoding=encoding,
                        delimiter=delimiter,
                        dtype=str,
                        na_values=["n.d.", "..", "-", ""],
                    )
                    if len(df.columns) > 2:
                        logger.info(f"Loaded CSV with encoding={encoding}, delimiter='{delimiter}'")
                        return df
                except Exception:
                    continue
        raise ValueError(f"Could not parse CSV file: {file_path}")

    elif suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path, dtype=str, na_values=["n.d.", "..", "-", ""])
        return df

    else:
        raise ValueError(f"Unsupported file format: {suffix}")


def process_population_data(
    df: pd.DataFrame,
    year: int,
    ingestion_run_id: int,
    source_file: str,
) -> tuple[int, int]:
    """Process and insert population data into the database."""
    # Rename columns
    df = df.rename(columns={k: v for k, v in COLUMN_MAPPINGS.items() if k in df.columns})

    logger.info(f"Columns after mapping: {list(df.columns)}")
    logger.info(f"Sample row: {df.iloc[0].to_dict() if len(df) > 0 else 'empty'}")

    rows_loaded = 0
    rows_rejected = 0

    # Check if this is age-group pivoted data (common ISTAT format)
    is_age_grouped = "age_group" in df.columns

    with get_db_cursor() as cursor:
        if is_age_grouped:
            # Need to pivot/aggregate age groups by municipality
            rows_loaded, rows_rejected = process_age_grouped_data(
                df, year, ingestion_run_id, source_file, cursor
            )
        else:
            # Standard format with columns per metric
            for idx, row in df.iterrows():
                try:
                    row_dict = row.to_dict()

                    municipality_id = build_municipality_id(row_dict)
                    if not municipality_id or len(municipality_id) < 6:
                        rows_rejected += 1
                        continue

                    # Extract year from data if available
                    data_year = parse_value(row_dict.get("year")) or year

                    cursor.execute(
                        """
                        INSERT INTO raw.istat_population (
                            ingestion_run_id, municipality_id, reference_year,
                            total_population, male_population, female_population,
                            population_0_14, population_15_24, population_25_44,
                            population_45_64, population_65_plus,
                            foreign_population, births, deaths,
                            immigration, emigration,
                            households, avg_household_size,
                            source_file
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (municipality_id, reference_year) DO UPDATE SET
                            total_population = COALESCE(EXCLUDED.total_population, raw.istat_population.total_population),
                            male_population = COALESCE(EXCLUDED.male_population, raw.istat_population.male_population),
                            female_population = COALESCE(EXCLUDED.female_population, raw.istat_population.female_population),
                            population_0_14 = COALESCE(EXCLUDED.population_0_14, raw.istat_population.population_0_14),
                            population_15_24 = COALESCE(EXCLUDED.population_15_24, raw.istat_population.population_15_24),
                            population_25_44 = COALESCE(EXCLUDED.population_25_44, raw.istat_population.population_25_44),
                            population_45_64 = COALESCE(EXCLUDED.population_45_64, raw.istat_population.population_45_64),
                            population_65_plus = COALESCE(EXCLUDED.population_65_plus, raw.istat_population.population_65_plus),
                            foreign_population = COALESCE(EXCLUDED.foreign_population, raw.istat_population.foreign_population),
                            source_file = EXCLUDED.source_file
                        """,
                        (
                            ingestion_run_id,
                            municipality_id,
                            data_year,
                            parse_value(row_dict.get("total_population")),
                            parse_value(row_dict.get("male_population")),
                            parse_value(row_dict.get("female_population")),
                            parse_value(row_dict.get("population_0_14")),
                            parse_value(row_dict.get("population_15_24")),
                            parse_value(row_dict.get("population_25_44")),
                            parse_value(row_dict.get("population_45_64")),
                            parse_value(row_dict.get("population_65_plus")),
                            parse_value(row_dict.get("foreign_population")),
                            parse_value(row_dict.get("births")),
                            parse_value(row_dict.get("deaths")),
                            parse_value(row_dict.get("immigration")),
                            parse_value(row_dict.get("emigration")),
                            parse_value(row_dict.get("households")),
                            parse_float(row_dict.get("avg_household_size")),
                            source_file,
                        ),
                    )
                    rows_loaded += 1

                    if rows_loaded % 1000 == 0:
                        logger.info(f"Processed {rows_loaded} rows...")

                except Exception as e:
                    logger.warning(f"Error processing row {idx}: {e}")
                    rows_rejected += 1

    return rows_loaded, rows_rejected


def process_age_grouped_data(
    df: pd.DataFrame,
    year: int,
    ingestion_run_id: int,
    source_file: str,
    cursor,
) -> tuple[int, int]:
    """Process age-grouped ISTAT format where each row is a municipality + age group."""
    # Group by municipality and pivot age groups
    rows_loaded = 0
    rows_rejected = 0

    # Get unique municipalities
    municipalities = df.groupby("municipality_id")

    for municipality_id, group in municipalities:
        try:
            mid = build_municipality_id({"municipality_id": municipality_id})
            if not mid or len(mid) < 6:
                rows_rejected += len(group)
                continue

            # Aggregate population by age group
            pop_0_14 = 0
            pop_15_24 = 0
            pop_25_44 = 0
            pop_45_64 = 0
            pop_65_plus = 0
            total_pop = 0

            for _, row in group.iterrows():
                age_group = str(row.get("age_group", "")).lower()
                pop = parse_value(row.get("total_population", 0)) or 0

                if "0-14" in age_group or age_group in ["0", "1-4", "5-9", "10-14"]:
                    pop_0_14 += pop
                elif "15-24" in age_group or age_group in ["15-19", "20-24"]:
                    pop_15_24 += pop
                elif "25-44" in age_group or age_group in ["25-29", "30-34", "35-39", "40-44"]:
                    pop_25_44 += pop
                elif "45-64" in age_group or age_group in ["45-49", "50-54", "55-59", "60-64"]:
                    pop_45_64 += pop
                elif "65" in age_group or "oltre" in age_group:
                    pop_65_plus += pop
                else:
                    # Try to parse age range
                    import re

                    match = re.search(r"(\d+)", age_group)
                    if match:
                        age = int(match.group(1))
                        if age < 15:
                            pop_0_14 += pop
                        elif age < 25:
                            pop_15_24 += pop
                        elif age < 45:
                            pop_25_44 += pop
                        elif age < 65:
                            pop_45_64 += pop
                        else:
                            pop_65_plus += pop

                total_pop += pop

            cursor.execute(
                """
                INSERT INTO raw.istat_population (
                    ingestion_run_id, municipality_id, reference_year,
                    total_population,
                    population_0_14, population_15_24, population_25_44,
                    population_45_64, population_65_plus,
                    source_file
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (municipality_id, reference_year) DO UPDATE SET
                    total_population = EXCLUDED.total_population,
                    population_0_14 = EXCLUDED.population_0_14,
                    population_15_24 = EXCLUDED.population_15_24,
                    population_25_44 = EXCLUDED.population_25_44,
                    population_45_64 = EXCLUDED.population_45_64,
                    population_65_plus = EXCLUDED.population_65_plus,
                    source_file = EXCLUDED.source_file
                """,
                (
                    ingestion_run_id,
                    mid,
                    year,
                    total_pop if total_pop > 0 else None,
                    pop_0_14 if pop_0_14 > 0 else None,
                    pop_15_24 if pop_15_24 > 0 else None,
                    pop_25_44 if pop_25_44 > 0 else None,
                    pop_45_64 if pop_45_64 > 0 else None,
                    pop_65_plus if pop_65_plus > 0 else None,
                    source_file,
                ),
            )
            rows_loaded += 1

        except Exception as e:
            logger.warning(f"Error processing municipality {municipality_id}: {e}")
            rows_rejected += 1

    return rows_loaded, rows_rejected


def aggregate_to_mart(year: int) -> int:
    """Aggregate raw population data to mart.municipality_demographics_year."""
    logger.info(f"Aggregating demographics to mart for {year}...")

    with get_db_cursor() as cursor:
        # Delete existing mart records for this year
        cursor.execute(
            "DELETE FROM mart.municipality_demographics_year WHERE reference_year = %s",
            (year,),
        )

        # Calculate derived metrics and insert to mart
        cursor.execute(
            """
            INSERT INTO mart.municipality_demographics_year (
                municipality_id, reference_year,
                total_population, population_density,
                young_ratio, working_ratio, elderly_ratio,
                dependency_ratio, old_age_index,
                foreign_ratio,
                natural_balance, migration_balance,
                households, avg_household_size
            )
            SELECT
                p.municipality_id,
                p.reference_year,
                p.total_population,
                -- Population density requires area from municipalities table
                NULL AS population_density,
                -- Age ratios
                CASE WHEN p.total_population > 0
                    THEN COALESCE(p.population_0_14, 0)::numeric / p.total_population
                    ELSE NULL END AS young_ratio,
                CASE WHEN p.total_population > 0
                    THEN (COALESCE(p.population_15_24, 0) + COALESCE(p.population_25_44, 0) + COALESCE(p.population_45_64, 0))::numeric / p.total_population
                    ELSE NULL END AS working_ratio,
                CASE WHEN p.total_population > 0
                    THEN COALESCE(p.population_65_plus, 0)::numeric / p.total_population
                    ELSE NULL END AS elderly_ratio,
                -- Dependency ratio: (0-14 + 65+) / 15-64
                CASE WHEN (COALESCE(p.population_15_24, 0) + COALESCE(p.population_25_44, 0) + COALESCE(p.population_45_64, 0)) > 0
                    THEN (COALESCE(p.population_0_14, 0) + COALESCE(p.population_65_plus, 0))::numeric /
                         (COALESCE(p.population_15_24, 0) + COALESCE(p.population_25_44, 0) + COALESCE(p.population_45_64, 0))
                    ELSE NULL END AS dependency_ratio,
                -- Old age index: 65+ / 0-14
                CASE WHEN COALESCE(p.population_0_14, 0) > 0
                    THEN COALESCE(p.population_65_plus, 0)::numeric / p.population_0_14
                    ELSE NULL END AS old_age_index,
                -- Foreign ratio
                CASE WHEN p.total_population > 0
                    THEN COALESCE(p.foreign_population, 0)::numeric / p.total_population
                    ELSE NULL END AS foreign_ratio,
                -- Natural balance
                COALESCE(p.births, 0) - COALESCE(p.deaths, 0) AS natural_balance,
                -- Migration balance
                COALESCE(p.immigration, 0) - COALESCE(p.emigration, 0) AS migration_balance,
                p.households,
                p.avg_household_size
            FROM raw.istat_population p
            WHERE p.reference_year = %s
              AND p.total_population IS NOT NULL
            """,
            (year,),
        )

        cursor.execute(
            "SELECT COUNT(*) as cnt FROM mart.municipality_demographics_year WHERE reference_year = %s",
            (year,),
        )
        result = cursor.fetchone()
        count = result["cnt"] if result else 0

    logger.info(f"Created {count} mart demographic records for {year}")
    return count


def main():
    parser = argparse.ArgumentParser(description="Import ISTAT population data")
    parser.add_argument("--file", type=str, required=True, help="Path to population data file (CSV or Excel)")
    parser.add_argument("--year", type=int, required=True, help="Reference year for the data")
    parser.add_argument("--skip-mart", action="store_true", help="Skip mart aggregation")
    args = parser.parse_args()

    file_path = Path(args.file)
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        sys.exit(1)

    logger.info(f"Processing population data for year: {args.year}")

    ingestion_run_id = create_ingestion_run(
        source_name="istat_population",
        source_version=str(args.year),
    )
    logger.info(f"Started ingestion run {ingestion_run_id}")

    try:
        df = load_population_data(file_path)
        logger.info(f"Loaded {len(df)} rows from {file_path}")

        rows_loaded, rows_rejected = process_population_data(
            df, args.year, ingestion_run_id, str(file_path)
        )

        if not args.skip_mart:
            aggregate_to_mart(args.year)

        complete_ingestion_run(
            ingestion_run_id,
            rows_loaded=rows_loaded,
            rows_rejected=rows_rejected,
            success=True,
        )
        logger.info(f"Ingestion completed. Loaded: {rows_loaded}, Rejected: {rows_rejected}")

    except Exception as e:
        logger.exception(f"Ingestion failed: {e}")
        complete_ingestion_run(
            ingestion_run_id,
            rows_loaded=0,
            error_notes=str(e),
            success=False,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
