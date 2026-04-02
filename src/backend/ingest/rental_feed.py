#!/usr/bin/env python3
"""
Rental Data Ingestion Pipeline

Ingests rental listing data from various sources and aggregates
to municipality-level monthly statistics.

Usage:
    python rental_feed.py --source omi --period 2024S1
    python rental_feed.py --generate-demo --municipalities 1000

Environment variables:
    DATABASE_URL or SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY
"""

import argparse
import json
import logging
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd
import numpy as np

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent))
from db import get_db_cursor, create_ingestion_run, complete_ingestion_run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_municipality_ids() -> List[str]:
    """Get all municipality IDs from core table."""
    with get_db_cursor() as cursor:
        cursor.execute("SELECT municipality_id FROM core.municipalities")
        rows = cursor.fetchall()
        return [r["municipality_id"] for r in rows]


def get_omi_rental_data(period_id: str) -> pd.DataFrame:
    """
    Extract rental data from OMI property values.
    OMI data includes rent ranges for different property types.
    """
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                municipality_id,
                property_type,
                property_subtype,
                rent_min_eur_sqm_month,
                rent_max_eur_sqm_month,
                (rent_min_eur_sqm_month + rent_max_eur_sqm_month) / 2 as rent_mid_eur_sqm_month
            FROM raw.omi_property_values
            WHERE period_id = %s
              AND rent_min_eur_sqm_month IS NOT NULL
            """,
            (period_id,)
        )
        rows = cursor.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()


def aggregate_rental_data(
    df: pd.DataFrame,
    period_id: str,
    property_segment: str = "residential"
) -> pd.DataFrame:
    """Aggregate rental listings to municipality-month level."""
    if df.empty:
        return pd.DataFrame()

    # Group by municipality
    agg = df.groupby("municipality_id").agg({
        "rent_mid_eur_sqm_month": ["min", "max", "mean", "median", "std", "count"],
    }).reset_index()

    # Flatten column names
    agg.columns = [
        "municipality_id",
        "rent_min_eur_sqm_month",
        "rent_max_eur_sqm_month",
        "rent_mid_eur_sqm_month",
        "rent_median_eur_sqm_month",
        "rent_std",
        "listings_count",
    ]

    # Calculate percentiles
    percentiles = df.groupby("municipality_id")["rent_mid_eur_sqm_month"].quantile([0.25, 0.75]).unstack()
    percentiles.columns = ["rent_percentile_25", "rent_percentile_75"]
    agg = agg.merge(percentiles, on="municipality_id", how="left")

    # Add period and segment
    agg["period_id"] = period_id
    agg["property_segment"] = property_segment
    agg["data_source"] = "omi"

    return agg


def generate_demo_rental_data(
    municipality_ids: List[str],
    num_municipalities: int = 1000,
    months: int = 12
) -> pd.DataFrame:
    """
    Generate synthetic rental data for demo purposes.
    Creates realistic rent distributions based on Italian market patterns.
    """
    if not municipality_ids:
        logger.warning("No municipality IDs provided")
        return pd.DataFrame()

    # Sample municipalities
    sample_ids = random.sample(municipality_ids, min(num_municipalities, len(municipality_ids)))

    # Regional rent multipliers (Southern Italy generally lower)
    region_multipliers = {
        "01": 1.0,   # Piemonte
        "02": 1.1,   # Valle d'Aosta
        "03": 1.4,   # Lombardia
        "04": 1.2,   # Trentino
        "05": 1.1,   # Veneto
        "06": 1.0,   # Friuli
        "07": 1.1,   # Liguria
        "08": 1.0,   # Emilia-Romagna
        "09": 1.2,   # Toscana
        "10": 0.9,   # Umbria
        "11": 0.9,   # Marche
        "12": 1.3,   # Lazio
        "13": 0.7,   # Abruzzo
        "14": 0.6,   # Molise
        "15": 0.8,   # Campania
        "16": 0.7,   # Puglia
        "17": 0.6,   # Basilicata
        "18": 0.6,   # Calabria
        "19": 0.7,   # Sicilia
        "20": 0.8,   # Sardegna
    }

    # Generate monthly data
    records = []
    base_date = datetime.now().replace(day=1) - timedelta(days=30 * months)

    for month_offset in range(months):
        period_date = base_date + timedelta(days=30 * month_offset)
        period_id = period_date.strftime("%Y-%m")

        for muni_id in sample_ids:
            # Extract region code from municipality ID
            region_code = muni_id[:2] if len(muni_id) >= 2 else "01"
            multiplier = region_multipliers.get(region_code, 1.0)

            # Base rent with regional adjustment
            base_rent = random.gauss(8, 3) * multiplier  # €/sqm/month
            base_rent = max(3, min(25, base_rent))  # Clamp to realistic range

            # Add some noise and trend
            trend = 0.002 * month_offset  # Slight upward trend
            noise = random.gauss(0, 0.5)
            rent_mid = base_rent * (1 + trend + noise / 10)

            # Generate min/max around mid
            rent_min = rent_mid * random.uniform(0.7, 0.9)
            rent_max = rent_mid * random.uniform(1.1, 1.3)

            records.append({
                "municipality_id": muni_id,
                "period_id": period_id,
                "property_segment": "residential",
                "rent_min_eur_sqm_month": round(rent_min, 2),
                "rent_max_eur_sqm_month": round(rent_max, 2),
                "rent_mid_eur_sqm_month": round(rent_mid, 2),
                "rent_median_eur_sqm_month": round(rent_mid * random.uniform(0.95, 1.05), 2),
                "rent_percentile_25": round(rent_min * 1.1, 2),
                "rent_percentile_75": round(rent_max * 0.9, 2),
                "listings_count": random.randint(5, 200),
                "avg_days_on_market": random.randint(20, 90),
                "data_source": "demo",
                "sample_size": random.randint(10, 100),
            })

    return pd.DataFrame(records)


def calculate_rental_changes(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate period-over-period changes in rent."""
    if df.empty or "period_id" not in df.columns:
        return df

    # Sort by municipality and period
    df = df.sort_values(["municipality_id", "period_id"])

    # Calculate changes
    df["rent_pct_change_1m"] = df.groupby("municipality_id")["rent_mid_eur_sqm_month"].pct_change() * 100

    # 3-month change (shift by 3)
    df["rent_pct_change_3m"] = df.groupby("municipality_id")["rent_mid_eur_sqm_month"].transform(
        lambda x: (x / x.shift(3) - 1) * 100
    )

    # 12-month change
    df["rent_pct_change_12m"] = df.groupby("municipality_id")["rent_mid_eur_sqm_month"].transform(
        lambda x: (x / x.shift(12) - 1) * 100
    )

    return df


def save_rental_data(df: pd.DataFrame) -> int:
    """Save rental data to mart.municipality_rents_month."""
    if df.empty:
        return 0

    # Columns to save
    columns = [
        "municipality_id", "period_id", "property_segment",
        "rent_min_eur_sqm_month", "rent_max_eur_sqm_month",
        "rent_mid_eur_sqm_month", "rent_median_eur_sqm_month",
        "rent_percentile_25", "rent_percentile_75",
        "listings_count", "avg_days_on_market",
        "rent_pct_change_1m", "rent_pct_change_3m", "rent_pct_change_12m",
        "data_source", "sample_size",
    ]

    # Filter to available columns
    available_cols = [c for c in columns if c in df.columns]
    save_df = df[available_cols].copy()

    # Replace NaN with None
    save_df = save_df.where(pd.notnull(save_df), None)

    rows_saved = 0
    with get_db_cursor() as cursor:
        for _, row in save_df.iterrows():
            try:
                # Build upsert query
                cols = [c for c in available_cols if row.get(c) is not None or c in ["municipality_id", "period_id", "property_segment"]]
                values = [row[c] for c in cols]
                placeholders = ", ".join(["%s"] * len(cols))
                col_names = ", ".join(cols)

                # Upsert
                update_cols = [c for c in cols if c not in ["municipality_id", "period_id", "property_segment"]]
                update_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

                cursor.execute(
                    f"""
                    INSERT INTO mart.municipality_rents_month ({col_names})
                    VALUES ({placeholders})
                    ON CONFLICT (municipality_id, period_id, property_segment)
                    DO UPDATE SET {update_clause}, updated_at = now()
                    """,
                    values
                )
                rows_saved += 1
            except Exception as e:
                logger.warning(f"Error saving rental data for {row.get('municipality_id')}: {e}")

    return rows_saved


def update_yield_in_forecasts() -> int:
    """
    Update gross yield in forecasts based on latest rental and value data.
    """
    with get_db_cursor() as cursor:
        # Update forecasts with calculated yield
        cursor.execute(
            """
            UPDATE model.forecasts_municipality f
            SET
                forecast_gross_yield_pct = model.calculate_gross_yield(
                    r.rent_mid_eur_sqm_month * 12,
                    f.value_mid_eur_sqm
                ),
                forecast_rent_eur_sqm_month = r.rent_mid_eur_sqm_month
            FROM mart.municipality_rents_month r
            WHERE f.municipality_id = r.municipality_id
              AND f.property_segment = r.property_segment
              AND r.period_id = (
                  SELECT MAX(period_id)
                  FROM mart.municipality_rents_month
                  WHERE municipality_id = f.municipality_id
                    AND property_segment = f.property_segment
              )
              AND f.value_mid_eur_sqm IS NOT NULL
              AND f.value_mid_eur_sqm > 0
            """
        )
        return cursor.rowcount


def main():
    parser = argparse.ArgumentParser(description="Ingest rental data")
    parser.add_argument(
        "--source",
        type=str,
        choices=["omi", "listings", "demo"],
        default="demo",
        help="Data source"
    )
    parser.add_argument("--period", type=str, help="Period ID (e.g., 2024S1 for OMI)")
    parser.add_argument("--generate-demo", action="store_true", help="Generate demo rental data")
    parser.add_argument("--municipalities", type=int, default=1000, help="Number of municipalities for demo")
    parser.add_argument("--months", type=int, default=12, help="Months of data for demo")
    parser.add_argument("--update-yields", action="store_true", help="Update yields in forecasts table")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    args = parser.parse_args()

    # Create ingestion run
    run_id = None
    if not args.dry_run:
        run_id = create_ingestion_run("rental_feed", args.source)
        logger.info(f"Created ingestion run {run_id}")

    try:
        if args.source == "omi" and args.period:
            # Extract from OMI data
            logger.info(f"Extracting rental data from OMI for {args.period}")
            raw_df = get_omi_rental_data(args.period)
            logger.info(f"Found {len(raw_df)} OMI rental records")

            if not raw_df.empty:
                # Convert semester to month (use first month of semester)
                year = int(args.period[:4])
                semester = int(args.period[-1])
                month_id = f"{year}-{'01' if semester == 1 else '07'}"

                df = aggregate_rental_data(raw_df, month_id)
                df = calculate_rental_changes(df)
            else:
                df = pd.DataFrame()

        elif args.generate_demo or args.source == "demo":
            # Generate demo data
            logger.info(f"Generating demo rental data for {args.municipalities} municipalities")
            municipality_ids = get_municipality_ids()

            if not municipality_ids:
                # Fallback: generate IDs based on GeoJSON
                logger.info("No municipalities in database, generating IDs from range")
                # Generate sample municipality IDs matching Italian format
                municipality_ids = [f"{r:02d}{p:03d}" for r in range(1, 21) for p in range(1, 500)]

            df = generate_demo_rental_data(
                municipality_ids,
                num_municipalities=args.municipalities,
                months=args.months
            )
            df = calculate_rental_changes(df)
            logger.info(f"Generated {len(df)} rental records")

        else:
            logger.error("No valid data source specified")
            sys.exit(1)

        # Summary stats
        if not df.empty:
            stats = {
                "total_records": len(df),
                "municipalities": df["municipality_id"].nunique(),
                "periods": df["period_id"].nunique() if "period_id" in df.columns else 0,
                "avg_rent": round(df["rent_mid_eur_sqm_month"].mean(), 2),
                "min_rent": round(df["rent_mid_eur_sqm_month"].min(), 2),
                "max_rent": round(df["rent_mid_eur_sqm_month"].max(), 2),
            }
            logger.info(f"Summary: {stats}")

            if args.dry_run:
                logger.info("Dry run - not saving to database")
                print(df.head(20))
            else:
                rows_saved = save_rental_data(df)
                logger.info(f"Saved {rows_saved} rental records")

                if args.update_yields:
                    yields_updated = update_yield_in_forecasts()
                    logger.info(f"Updated yields for {yields_updated} forecasts")

                if run_id:
                    complete_ingestion_run(run_id, rows_saved, stats)

        logger.info("Rental ingestion complete")

    except Exception as e:
        logger.exception(f"Rental ingestion failed: {e}")
        if run_id:
            complete_ingestion_run(run_id, 0, {"error": str(e)}, success=False)
        sys.exit(1)


if __name__ == "__main__":
    main()
