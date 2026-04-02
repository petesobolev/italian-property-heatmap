#!/usr/bin/env python3
"""
OMI Transactions Ingestion Script

Imports OMI (Osservatorio del Mercato Immobiliare) transaction data (NTN - Numero Transazioni Normalizzate)
from Agenzia delle Entrate into the database.

Data source: https://www.agenziaentrate.gov.it/portale/web/guest/schede/fabbricatiterreni/omi/statistiche

NTN data is published semiannually and contains transaction counts by municipality.

Usage:
    python omi_transactions.py --file <path_to_csv_or_xlsx> --semester 2024S1

Environment variables:
    DATABASE_URL - Full PostgreSQL connection string, or use:
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

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

# OMI NTN column mappings
COLUMN_MAPPINGS = {
    # Identifiers
    "Cod_Regione": "region_code",
    "Cod_Provincia": "province_code",
    "Cod_Comune": "municipality_code",
    "Comune_ISTAT": "municipality_id",
    "PRO_COM": "municipality_id",
    "Cod_ISTAT": "municipality_id",
    # Transactions
    "NTN": "ntn",
    "NTN_RES": "ntn",  # Residential NTN
    "NTN_COM": "ntn_commercial",
    "NTN_TER": "ntn_tertiary",
    "NTN_PRO": "ntn_industrial",
    # Market indicators
    "IMI": "imt",
    "IMT": "imt",
    "Quotazione_stock": "quotation_stock",
    "Stock": "quotation_stock",
    # Property type
    "Destinazione": "property_type",
    "Tipologia": "property_type",
    # Period
    "Semestre": "semester",
    "Anno": "year",
}


def parse_semester(semester_str: str, year: Optional[int] = None) -> str:
    """Parse semester string into standard format (e.g., '2024S1')."""
    if not semester_str:
        return ""

    s = str(semester_str).strip().upper()
    import re

    # Extract semester number
    if "S1" in s or "1" in s.split()[-1] if len(s.split()) > 0 else False:
        sem = "S1"
    elif "S2" in s or "2" in s.split()[-1] if len(s.split()) > 0 else False:
        sem = "S2"
    else:
        # Default based on common patterns
        sem = "S1" if "1" in s else "S2" if "2" in s else "S1"

    # Extract year
    year_match = re.search(r"20\d{2}", s)
    if year_match:
        year = int(year_match.group())
    elif year:
        pass
    else:
        year = 2024

    return f"{year}{sem}"


def parse_value(val: Any) -> Optional[float]:
    """Parse a value field that might be string or numeric."""
    if val is None or val == "" or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        if isinstance(val, str):
            val = val.replace(",", ".").replace(" ", "")
        return float(val)
    except (ValueError, TypeError):
        return None


def build_municipality_id(row: Dict[str, Any]) -> str:
    """Build 6-digit ISTAT municipality code from various column combinations."""
    if row.get("municipality_id"):
        return str(row["municipality_id"]).zfill(6)

    prov = str(row.get("province_code", "")).zfill(3)
    muni = str(row.get("municipality_code", "")).zfill(3)

    if prov and muni and prov != "000" and muni != "000":
        return prov + muni

    return ""


def ensure_time_period(cursor, period_id: str) -> None:
    """Ensure the time period exists in core.time_periods."""
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


def load_ntn_data(file_path: Path) -> pd.DataFrame:
    """Load NTN data from CSV or Excel file."""
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        for encoding in ["utf-8", "latin-1", "cp1252"]:
            for delimiter in [";", ",", "\t"]:
                try:
                    df = pd.read_csv(
                        file_path,
                        encoding=encoding,
                        delimiter=delimiter,
                        dtype=str,
                    )
                    if len(df.columns) > 3:
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


def process_ntn_data(
    df: pd.DataFrame,
    semester: str,
    ingestion_run_id: int,
    source_file: str,
) -> tuple[int, int]:
    """Process and insert NTN transaction data into the database."""
    # Rename columns
    df = df.rename(columns={k: v for k, v in COLUMN_MAPPINGS.items() if k in df.columns})

    logger.info(f"Columns after mapping: {list(df.columns)}")

    rows_loaded = 0
    rows_rejected = 0

    with get_db_cursor() as cursor:
        ensure_time_period(cursor, semester)

        for idx, row in df.iterrows():
            try:
                row_dict = row.to_dict()

                municipality_id = build_municipality_id(row_dict)
                if not municipality_id or municipality_id == "000000":
                    rows_rejected += 1
                    continue

                ntn = parse_value(row_dict.get("ntn"))
                imt = parse_value(row_dict.get("imt"))
                quotation_stock = parse_value(row_dict.get("quotation_stock"))

                # Skip if no transaction data
                if ntn is None and imt is None:
                    rows_rejected += 1
                    continue

                # Determine property type
                property_type = row_dict.get("property_type", "residenziale")
                if not property_type:
                    property_type = "residenziale"

                cursor.execute(
                    """
                    INSERT INTO raw.omi_transactions (
                        ingestion_run_id, municipality_id, period_id,
                        property_type, ntn, imt, quotation_stock,
                        source_file
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        ingestion_run_id,
                        municipality_id,
                        semester,
                        property_type.lower().strip(),
                        ntn,
                        imt,
                        int(quotation_stock) if quotation_stock else None,
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


def aggregate_to_mart(semester: str) -> int:
    """Aggregate raw NTN data to municipality-level mart table."""
    logger.info(f"Aggregating transactions to mart for {semester}...")

    with get_db_cursor() as cursor:
        # Delete existing mart records for this semester
        cursor.execute(
            "DELETE FROM mart.municipality_transactions_semester WHERE period_id = %s",
            (semester,),
        )

        # Aggregate from raw to mart
        cursor.execute(
            """
            INSERT INTO mart.municipality_transactions_semester (
                municipality_id, period_id, property_segment,
                ntn_total, imt_avg, quotation_stock_total
            )
            SELECT
                t.municipality_id,
                t.period_id,
                CASE
                    WHEN t.property_type = 'residenziale' THEN 'residential'
                    WHEN t.property_type IN ('commerciale', 'terziario') THEN 'commercial'
                    WHEN t.property_type = 'produttivo' THEN 'industrial'
                    ELSE 'other'
                END AS property_segment,
                SUM(t.ntn) AS ntn_total,
                AVG(t.imt) AS imt_avg,
                SUM(t.quotation_stock) AS quotation_stock_total
            FROM raw.omi_transactions t
            WHERE t.period_id = %s
            GROUP BY t.municipality_id, t.period_id, property_segment
            """,
            (semester,),
        )

        # Calculate ntn_per_1000_pop if population data exists
        cursor.execute(
            """
            UPDATE mart.municipality_transactions_semester mt
            SET ntn_per_1000_pop = (mt.ntn_total / md.total_population) * 1000
            FROM mart.municipality_demographics_year md
            WHERE mt.municipality_id = md.municipality_id
              AND mt.period_id = %s
              AND md.total_population > 0
            """,
            (semester,),
        )

        cursor.execute(
            "SELECT COUNT(*) as cnt FROM mart.municipality_transactions_semester WHERE period_id = %s",
            (semester,),
        )
        result = cursor.fetchone()
        count = result["cnt"] if result else 0

    logger.info(f"Created {count} mart transaction records for {semester}")
    return count


def main():
    parser = argparse.ArgumentParser(description="Import OMI transaction data (NTN)")
    parser.add_argument("--file", type=str, required=True, help="Path to NTN data file (CSV or Excel)")
    parser.add_argument("--semester", type=str, required=True, help="Semester (e.g., 2024S1)")
    parser.add_argument("--skip-mart", action="store_true", help="Skip mart aggregation")
    args = parser.parse_args()

    file_path = Path(args.file)
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        sys.exit(1)

    semester = parse_semester(args.semester)
    logger.info(f"Processing NTN data for semester: {semester}")

    ingestion_run_id = create_ingestion_run(
        source_name="omi_transactions",
        source_version=semester,
    )
    logger.info(f"Started ingestion run {ingestion_run_id}")

    try:
        df = load_ntn_data(file_path)
        logger.info(f"Loaded {len(df)} rows from {file_path}")

        rows_loaded, rows_rejected = process_ntn_data(
            df, semester, ingestion_run_id, str(file_path)
        )

        if not args.skip_mart:
            aggregate_to_mart(semester)

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
