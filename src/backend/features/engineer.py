#!/usr/bin/env python3
"""
Feature Engineering Pipeline

Computes features for ML models from raw and mart data.
Populates model.features_municipality_semester table.

Usage:
    python engineer.py --period 2024S1

Environment variables:
    DATABASE_URL or SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd
import numpy as np

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "ingest"))
from db import get_db_cursor, get_db_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_municipality_values(period_id: str, property_segment: str = "residential") -> pd.DataFrame:
    """Fetch municipality values for a period."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                municipality_id,
                value_min_eur_sqm,
                value_max_eur_sqm,
                value_mid_eur_sqm,
                rent_mid_eur_sqm_month,
                zones_count,
                zones_with_data,
                value_pct_change_1s,
                value_pct_change_2s
            FROM mart.municipality_values_semester
            WHERE period_id = %s AND property_segment = %s
            """,
            (period_id, property_segment),
        )
        rows = cursor.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_municipality_transactions(period_id: str, property_segment: str = "residential") -> pd.DataFrame:
    """Fetch municipality transactions for a period."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                municipality_id,
                ntn_total,
                ntn_per_1000_pop,
                imt_avg,
                quotation_stock_total,
                ntn_pct_change_1s,
                absorption_rate
            FROM mart.municipality_transactions_semester
            WHERE period_id = %s AND property_segment = %s
            """,
            (period_id, property_segment),
        )
        rows = cursor.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_municipality_demographics(year: int) -> pd.DataFrame:
    """Fetch municipality demographics for a year."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                municipality_id,
                total_population,
                population_density,
                young_ratio,
                working_ratio,
                elderly_ratio,
                dependency_ratio,
                old_age_index,
                foreign_ratio,
                population_growth_rate,
                natural_balance,
                migration_balance
            FROM mart.municipality_demographics_year
            WHERE reference_year = %s
            """,
            (year,),
        )
        rows = cursor.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_municipality_metadata() -> pd.DataFrame:
    """Fetch municipality metadata (region, province, flags)."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                municipality_id,
                municipality_name,
                province_code,
                region_code,
                coastal_flag,
                mountain_flag
            FROM core.municipalities
            """
        )
        rows = cursor.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_historical_values(
    municipality_ids: List[str],
    property_segment: str = "residential",
    num_periods: int = 4,
) -> pd.DataFrame:
    """Fetch historical values for volatility calculation."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                municipality_id,
                period_id,
                value_mid_eur_sqm
            FROM mart.municipality_values_semester
            WHERE property_segment = %s
            ORDER BY period_id DESC
            """,
            (property_segment,),
        )
        rows = cursor.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()


def calculate_spatial_features(
    df: pd.DataFrame,
    metadata_df: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate spatial aggregation features."""
    # Merge metadata
    df = df.merge(metadata_df, on="municipality_id", how="left")

    # Calculate province averages
    if "value_mid_eur_sqm" in df.columns:
        province_avg = df.groupby("province_code")["value_mid_eur_sqm"].transform("mean")
        df["province_avg_value"] = province_avg

        # Calculate region averages
        region_avg = df.groupby("region_code")["value_mid_eur_sqm"].transform("mean")
        df["region_avg_value"] = region_avg

        # Calculate premium/discount vs province and region
        df["value_vs_province_pct"] = np.where(
            df["province_avg_value"] > 0,
            ((df["value_mid_eur_sqm"] / df["province_avg_value"]) - 1) * 100,
            None,
        )
        df["value_vs_region_pct"] = np.where(
            df["region_avg_value"] > 0,
            ((df["value_mid_eur_sqm"] / df["region_avg_value"]) - 1) * 100,
            None,
        )

    return df


def calculate_yield_features(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate rental yield features."""
    if "rent_mid_eur_sqm_month" in df.columns and "value_mid_eur_sqm" in df.columns:
        # Annual rent / property value
        annual_rent = df["rent_mid_eur_sqm_month"] * 12
        df["gross_yield_pct"] = np.where(
            df["value_mid_eur_sqm"] > 0,
            (annual_rent / df["value_mid_eur_sqm"]) * 100,
            None,
        )
        df["rent_value_ratio"] = np.where(
            df["value_mid_eur_sqm"] > 0,
            df["rent_mid_eur_sqm_month"] / df["value_mid_eur_sqm"],
            None,
        )
    return df


def calculate_volatility(historical_df: pd.DataFrame, municipality_ids: List[str]) -> Dict[str, float]:
    """Calculate price volatility over historical periods."""
    volatility = {}

    if historical_df.empty:
        return volatility

    for muni_id in municipality_ids:
        muni_data = historical_df[historical_df["municipality_id"] == muni_id]
        if len(muni_data) >= 2:
            values = muni_data["value_mid_eur_sqm"].dropna()
            if len(values) >= 2:
                # Calculate percentage changes
                pct_changes = values.pct_change().dropna()
                volatility[muni_id] = pct_changes.std() * 100  # As percentage
        else:
            volatility[muni_id] = None

    return volatility


def calculate_feature_completeness(df: pd.DataFrame, feature_columns: List[str]) -> pd.Series:
    """Calculate what percentage of features are populated for each row."""
    non_null_counts = df[feature_columns].notna().sum(axis=1)
    total_features = len(feature_columns)
    return (non_null_counts / total_features) * 100


def compute_features(
    period_id: str,
    property_segment: str = "residential",
) -> pd.DataFrame:
    """Compute all features for a given period."""
    logger.info(f"Computing features for {period_id}, segment={property_segment}")

    # Parse year from period
    year = int(period_id[:4])

    # Fetch data
    values_df = get_municipality_values(period_id, property_segment)
    transactions_df = get_municipality_transactions(period_id, property_segment)
    demographics_df = get_municipality_demographics(year)
    metadata_df = get_municipality_metadata()

    logger.info(f"Fetched: {len(values_df)} values, {len(transactions_df)} transactions, {len(demographics_df)} demographics, {len(metadata_df)} municipalities")

    # Start with metadata as base
    features_df = metadata_df.copy()

    # Merge values
    if not values_df.empty:
        features_df = features_df.merge(values_df, on="municipality_id", how="left")

    # Merge transactions
    if not transactions_df.empty:
        features_df = features_df.merge(transactions_df, on="municipality_id", how="left")

    # Merge demographics
    if not demographics_df.empty:
        features_df = features_df.merge(demographics_df, on="municipality_id", how="left")

    # Calculate spatial features
    features_df = calculate_spatial_features(features_df, metadata_df)

    # Calculate yield features
    features_df = calculate_yield_features(features_df)

    # Calculate volatility
    if not values_df.empty:
        historical_df = get_historical_values(
            features_df["municipality_id"].tolist(),
            property_segment,
        )
        volatility = calculate_volatility(historical_df, features_df["municipality_id"].tolist())
        features_df["value_volatility_4s"] = features_df["municipality_id"].map(volatility)

    # Add period and segment
    features_df["period_id"] = period_id
    features_df["property_segment"] = property_segment

    # Calculate feature completeness
    feature_columns = [
        "value_mid_eur_sqm", "value_pct_change_1s", "value_pct_change_2s",
        "ntn_total", "ntn_per_1000_pop", "gross_yield_pct",
        "total_population", "young_ratio", "elderly_ratio", "foreign_ratio",
        "province_avg_value", "region_avg_value",
    ]
    existing_cols = [c for c in feature_columns if c in features_df.columns]
    if existing_cols:
        features_df["feature_completeness_score"] = calculate_feature_completeness(
            features_df, existing_cols
        )

    logger.info(f"Computed {len(features_df)} feature rows")
    return features_df


def save_features(features_df: pd.DataFrame) -> int:
    """Save features to model.features_municipality_semester."""
    if features_df.empty:
        return 0

    # Column mapping to database
    db_columns = [
        "municipality_id", "period_id", "property_segment",
        "value_mid_eur_sqm", "value_pct_change_1s", "value_pct_change_2s",
        "value_volatility_4s", "ntn_total", "ntn_per_1000_pop",
        "ntn_pct_change_1s", "absorption_rate", "gross_yield_pct",
        "rent_value_ratio", "total_population", "population_growth_rate",
        "young_ratio", "elderly_ratio", "foreign_ratio", "dependency_ratio",
        "coastal_flag", "mountain_flag", "province_avg_value", "region_avg_value",
        "value_vs_province_pct", "value_vs_region_pct",
        "feature_completeness_score",
    ]

    # Filter to existing columns
    available_columns = [c for c in db_columns if c in features_df.columns]
    save_df = features_df[available_columns].copy()

    # Replace NaN with None for database
    save_df = save_df.where(pd.notnull(save_df), None)

    rows_saved = 0
    with get_db_cursor() as cursor:
        # Delete existing features for this period/segment
        period_id = features_df["period_id"].iloc[0]
        property_segment = features_df["property_segment"].iloc[0]

        cursor.execute(
            "DELETE FROM model.features_municipality_semester WHERE period_id = %s AND property_segment = %s",
            (period_id, property_segment),
        )

        # Insert new features
        for _, row in save_df.iterrows():
            try:
                columns = [c for c in available_columns if row.get(c) is not None or c in ["municipality_id", "period_id", "property_segment"]]
                values = [row[c] for c in columns]
                placeholders = ", ".join(["%s"] * len(columns))
                column_names = ", ".join(columns)

                cursor.execute(
                    f"INSERT INTO model.features_municipality_semester ({column_names}) VALUES ({placeholders})",
                    values,
                )
                rows_saved += 1
            except Exception as e:
                logger.warning(f"Error saving features for {row.get('municipality_id')}: {e}")

    logger.info(f"Saved {rows_saved} feature rows")
    return rows_saved


def main():
    parser = argparse.ArgumentParser(description="Compute features for ML models")
    parser.add_argument("--period", type=str, required=True, help="Period ID (e.g., 2024S1)")
    parser.add_argument("--segment", type=str, default="residential", help="Property segment")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    args = parser.parse_args()

    try:
        features_df = compute_features(args.period, args.segment)

        if args.dry_run:
            logger.info("Dry run - not saving to database")
            print(features_df.head(10))
        else:
            rows_saved = save_features(features_df)
            logger.info(f"Feature engineering complete. Saved {rows_saved} rows.")

    except Exception as e:
        logger.exception(f"Feature engineering failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
