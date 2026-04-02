#!/usr/bin/env python3
"""
OMI Property Values Ingestion Script

Imports OMI (Osservatorio del Mercato Immobiliare) property value data
from Agenzia delle Entrate into the database.

Data source: https://www.agenziaentrate.gov.it/portale/web/guest/schede/fabbricatiterreni/omi/banche-dati/quotazioni-immobiliari

OMI data is published semiannually and contains property values by zone.

Usage:
    python omi_values.py --file <path_to_csv_or_xlsx> --semester 2024S1

Environment variables:
    DATABASE_URL - Full PostgreSQL connection string, or use:
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import argparse
import csv
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd

from db import (
    create_ingestion_run,
    complete_ingestion_run,
    get_db_cursor,
    get_db_connection,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# OMI column mappings (Italian to English)
# OMI files vary by region and format, these are common column names
COLUMN_MAPPINGS = {
    # Region/Province/Municipality identifiers
    "Cod_Regione": "region_code",
    "Cod_Provincia": "province_code",
    "Cod_Comune": "municipality_code",
    "Comune_cat": "municipality_code",  # Alternative name
    "Comune_ISTAT": "municipality_id",
    "PRO_COM": "municipality_id",
    # Zone information
    "Zona": "zone_code",
    "Cod_Zona": "zone_code",
    "Fascia": "zone_type",
    "Descr_zona": "zone_description",
    "Microzona": "microzone_code",
    # Property type
    "Cod_tip": "property_type_code",
    "Descr_Tipologia": "property_type",
    "Destinazione": "property_type",
    "Stato": "state",
    "Stato_prev": "state",
    # Values
    "Compr_min": "value_min",
    "Compr_max": "value_max",
    "Val_min": "value_min",
    "Val_max": "value_max",
    "Loc_min": "rent_min",
    "Loc_max": "rent_max",
    "Sup_NL_compr": "surface_range",
    # Semester
    "Semestre": "semester",
    "Anno": "year",
}

# Property type normalization
PROPERTY_TYPE_MAP = {
    # Residential
    "abitazioni civili": "residenziale",
    "abitazioni di tipo economico": "residenziale",
    "abitazioni signorili": "residenziale",
    "ville e villini": "residenziale",
    "abitazioni": "residenziale",
    "residenziale": "residenziale",
    # Commercial
    "negozi": "commerciale",
    "uffici": "terziario",
    "terziario": "terziario",
    "commerciale": "commerciale",
    # Industrial
    "capannoni industriali": "produttivo",
    "capannoni tipici": "produttivo",
    "produttivo": "produttivo",
    # Parking
    "box": "box",
    "autorimesse": "box",
    "posti auto": "box",
}


def normalize_property_type(raw_type: str) -> str:
    """Normalize property type to standard categories."""
    if not raw_type:
        return "altro"
    normalized = raw_type.lower().strip()
    return PROPERTY_TYPE_MAP.get(normalized, "altro")


def parse_semester(semester_str: str, year: Optional[int] = None) -> str:
    """Parse semester string into standard format (e.g., '2024S1')."""
    if not semester_str:
        return ""

    s = str(semester_str).strip().upper()

    # Handle formats like "2024S1", "2024-S1", "1° sem 2024"
    if "S1" in s or "1" in s:
        sem = "S1"
    elif "S2" in s or "2" in s:
        sem = "S2"
    else:
        sem = "S1"  # Default

    # Extract year
    import re

    year_match = re.search(r"20\d{2}", s)
    if year_match:
        year = int(year_match.group())
    elif year:
        pass  # Use provided year
    else:
        year = 2024  # Default

    return f"{year}{sem}"


def parse_value(val: Any) -> Optional[float]:
    """Parse a value field that might be string or numeric."""
    if val is None or val == "" or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        # Handle Italian number format (comma as decimal separator)
        if isinstance(val, str):
            val = val.replace(",", ".").replace(" ", "")
        return float(val)
    except (ValueError, TypeError):
        return None


def build_municipality_id(row: Dict[str, Any]) -> str:
    """Build 6-digit ISTAT municipality code from various column combinations."""
    # Try direct municipality_id first
    if row.get("municipality_id"):
        return str(row["municipality_id"]).zfill(6)

    # Try combining province and municipality codes
    prov = str(row.get("province_code", "")).zfill(3)
    muni = str(row.get("municipality_code", "")).zfill(3)

    if prov and muni and prov != "000" and muni != "000":
        return prov + muni

    return ""


def ensure_time_period(cursor, period_id: str) -> None:
    """Ensure the time period exists in core.time_periods."""
    # Parse period_id (e.g., "2024S1")
    year = int(period_id[:4])
    semester = int(period_id[-1])

    if semester == 1:
        start_date = f"{year}-01-01"
        end_date = f"{year}-06-30"
    else:
        start_date = f"{year}-07-01"
        end_date = f"{year}-12-31"

    cursor.execute(
        """
        INSERT INTO core.time_periods (period_id, period_type, period_start_date, period_end_date, year, semester)
        VALUES (%s, 'semester', %s, %s, %s, %s)
        ON CONFLICT (period_id) DO NOTHING
        """,
        (period_id, start_date, end_date, year, semester),
    )


def load_omi_data(file_path: Path, semester: str) -> pd.DataFrame:
    """Load OMI data from CSV or Excel file."""
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        # Try different encodings and delimiters
        for encoding in ["utf-8", "latin-1", "cp1252"]:
            for delimiter in [";", ",", "\t"]:
                try:
                    df = pd.read_csv(
                        file_path,
                        encoding=encoding,
                        delimiter=delimiter,
                        dtype=str,
                    )
                    if len(df.columns) > 3:  # Reasonable column count
                        logger.info(f"Loaded CSV with encoding={encoding}, delimiter='{delimiter}'")
                        return df
                except Exception:
                    continue
        raise ValueError(f"Could not parse CSV file: {file_path}")

    elif suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path, dtype=str)
        return df

    else:
        raise ValueError(f"Unsupported file format: {suffix}")


def process_omi_values(
    df: pd.DataFrame,
    semester: str,
    ingestion_run_id: int,
    source_file: str,
) -> tuple[int, int]:
    """Process and insert OMI values into the database."""
    # Rename columns to standard names
    df = df.rename(columns={k: v for k, v in COLUMN_MAPPINGS.items() if k in df.columns})

    logger.info(f"Columns after mapping: {list(df.columns)}")

    rows_loaded = 0
    rows_rejected = 0

    with get_db_cursor() as cursor:
        # Ensure time period exists
        ensure_time_period(cursor, semester)

        for idx, row in df.iterrows():
            try:
                row_dict = row.to_dict()

                # Build municipality ID
                municipality_id = build_municipality_id(row_dict)
                if not municipality_id or municipality_id == "000000":
                    rows_rejected += 1
                    continue

                # Get values
                value_min = parse_value(row_dict.get("value_min"))
                value_max = parse_value(row_dict.get("value_max"))
                rent_min = parse_value(row_dict.get("rent_min"))
                rent_max = parse_value(row_dict.get("rent_max"))

                # Skip if no value data
                if value_min is None and value_max is None:
                    rows_rejected += 1
                    continue

                # Normalize property type
                property_type = normalize_property_type(
                    row_dict.get("property_type", "") or row_dict.get("property_type_code", "")
                )

                # Build zone ID
                zone_code = str(row_dict.get("zone_code", "")).strip()
                omi_zone_id = f"{municipality_id}_{zone_code}" if zone_code else None

                cursor.execute(
                    """
                    INSERT INTO raw.omi_property_values (
                        ingestion_run_id, omi_zone_id, municipality_id, period_id,
                        property_type, property_subtype, state,
                        value_min_eur_sqm, value_max_eur_sqm,
                        rent_min_eur_sqm_month, rent_max_eur_sqm_month,
                        source_file, raw_data
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        ingestion_run_id,
                        omi_zone_id,
                        municipality_id,
                        semester,
                        property_type,
                        row_dict.get("property_type"),  # Keep original as subtype
                        row_dict.get("state"),
                        value_min,
                        value_max,
                        rent_min,
                        rent_max,
                        source_file,
                        None,  # Could store full row as JSON if needed
                    ),
                )
                rows_loaded += 1

                if rows_loaded % 1000 == 0:
                    logger.info(f"Processed {rows_loaded} rows...")

            except Exception as e:
                logger.warning(f"Error processing row {idx}: {e}")
                rows_rejected += 1

    return rows_loaded, rows_rejected


def aggregate_to_mart(semester: str) -> int:
    """Aggregate raw OMI values to municipality-level mart table."""
    logger.info(f"Aggregating values to mart for {semester}...")

    with get_db_cursor() as cursor:
        # Delete existing mart records for this semester
        cursor.execute(
            "DELETE FROM mart.municipality_values_semester WHERE period_id = %s",
            (semester,),
        )

        # Aggregate from raw to mart
        cursor.execute(
            """
            INSERT INTO mart.municipality_values_semester (
                municipality_id, period_id, property_segment,
                value_min_eur_sqm, value_max_eur_sqm, value_mid_eur_sqm,
                zones_count, zones_with_data
            )
            SELECT
                municipality_id,
                period_id,
                CASE
                    WHEN property_type = 'residenziale' THEN 'residential'
                    WHEN property_type IN ('commerciale', 'terziario') THEN 'commercial'
                    WHEN property_type = 'produttivo' THEN 'industrial'
                    ELSE 'other'
                END AS property_segment,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value_min_eur_sqm) AS value_min_eur_sqm,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value_max_eur_sqm) AS value_max_eur_sqm,
                AVG((COALESCE(value_min_eur_sqm, 0) + COALESCE(value_max_eur_sqm, 0)) / 2) AS value_mid_eur_sqm,
                COUNT(DISTINCT omi_zone_id) AS zones_count,
                COUNT(*) AS zones_with_data
            FROM raw.omi_property_values
            WHERE period_id = %s
                AND (value_min_eur_sqm IS NOT NULL OR value_max_eur_sqm IS NOT NULL)
            GROUP BY municipality_id, period_id, property_segment
            """,
            (semester,),
        )

        cursor.execute(
            "SELECT COUNT(*) as cnt FROM mart.municipality_values_semester WHERE period_id = %s",
            (semester,),
        )
        result = cursor.fetchone()
        count = result["cnt"] if result else 0

    logger.info(f"Created {count} mart records for {semester}")
    return count


def update_forecasts_from_mart(semester: str) -> int:
    """Update model.forecasts_municipality with latest values from mart."""
    logger.info(f"Updating forecasts from mart for {semester}...")

    with get_db_cursor() as cursor:
        # Parse semester to get a date
        year = int(semester[:4])
        sem = int(semester[-1])
        forecast_date = f"{year}-{'01' if sem == 1 else '07'}-01"

        cursor.execute(
            """
            INSERT INTO model.forecasts_municipality (
                municipality_id, forecast_date, horizon_months,
                property_segment, model_version,
                value_mid_eur_sqm, publishable_flag
            )
            SELECT
                mv.municipality_id,
                %s::date AS forecast_date,
                12 AS horizon_months,
                mv.property_segment,
                'omi_import_v1' AS model_version,
                mv.value_mid_eur_sqm,
                true AS publishable_flag
            FROM mart.municipality_values_semester mv
            WHERE mv.period_id = %s
                AND mv.value_mid_eur_sqm IS NOT NULL
            ON CONFLICT (municipality_id, forecast_date, horizon_months, property_segment, model_version)
            DO UPDATE SET
                value_mid_eur_sqm = EXCLUDED.value_mid_eur_sqm,
                created_at = now()
            """,
            (forecast_date, semester),
        )

        cursor.execute(
            """
            SELECT COUNT(*) as cnt
            FROM model.forecasts_municipality
            WHERE forecast_date = %s::date AND model_version = 'omi_import_v1'
            """,
            (forecast_date,),
        )
        result = cursor.fetchone()
        count = result["cnt"] if result else 0

    logger.info(f"Updated {count} forecast records")
    return count


def main():
    parser = argparse.ArgumentParser(description="Import OMI property values")
    parser.add_argument("--file", type=str, required=True, help="Path to OMI data file (CSV or Excel)")
    parser.add_argument("--semester", type=str, required=True, help="Semester (e.g., 2024S1)")
    parser.add_argument("--skip-mart", action="store_true", help="Skip mart aggregation")
    parser.add_argument("--skip-forecasts", action="store_true", help="Skip forecast updates")
    args = parser.parse_args()

    file_path = Path(args.file)
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        sys.exit(1)

    semester = parse_semester(args.semester)
    logger.info(f"Processing OMI data for semester: {semester}")

    # Start ingestion run
    ingestion_run_id = create_ingestion_run(
        source_name="omi_values",
        source_version=semester,
    )
    logger.info(f"Started ingestion run {ingestion_run_id}")

    try:
        # Load data
        df = load_omi_data(file_path, semester)
        logger.info(f"Loaded {len(df)} rows from {file_path}")

        # Process and insert
        rows_loaded, rows_rejected = process_omi_values(
            df, semester, ingestion_run_id, str(file_path)
        )

        # Aggregate to mart
        if not args.skip_mart:
            aggregate_to_mart(semester)

        # Update forecasts
        if not args.skip_forecasts:
            update_forecasts_from_mart(semester)

        # Mark success
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
