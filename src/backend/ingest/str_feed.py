#!/usr/bin/env python3
"""
Short-Term Rental (STR) Data Ingestion Pipeline

Ingests STR data (Airbnb, VRBO metrics) and aggregates to municipality-level
monthly statistics including ADR, occupancy, RevPAR, and seasonality.

Usage:
    python str_feed.py --generate-demo --municipalities 500 --months 24
    python str_feed.py --source airdna --file data/str_export.csv
    python str_feed.py --update-yields

Environment variables:
    DATABASE_URL or SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY
"""

import argparse
import json
import logging
import math
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


# Tourism seasonality patterns for different municipality types
SEASONALITY_PATTERNS = {
    "coastal_south": {  # Puglia, Calabria, Sicily coastal
        "peak": [6, 7, 8],
        "shoulder": [5, 9],
        "off_peak": [1, 2, 3, 4, 10, 11, 12],
        "peak_multiplier": 2.5,
        "shoulder_multiplier": 1.4,
        "base_occupancy": 0.35,
        "peak_occupancy": 0.85,
    },
    "coastal_north": {  # Liguria, Romagna, Versilia
        "peak": [6, 7, 8],
        "shoulder": [4, 5, 9, 10],
        "off_peak": [1, 2, 3, 11, 12],
        "peak_multiplier": 2.0,
        "shoulder_multiplier": 1.3,
        "base_occupancy": 0.40,
        "peak_occupancy": 0.80,
    },
    "mountain_ski": {  # Alps, Dolomites
        "peak": [1, 2, 7, 8, 12],
        "shoulder": [3, 6, 9],
        "off_peak": [4, 5, 10, 11],
        "peak_multiplier": 2.2,
        "shoulder_multiplier": 1.3,
        "base_occupancy": 0.30,
        "peak_occupancy": 0.75,
    },
    "city_cultural": {  # Rome, Florence, Venice
        "peak": [4, 5, 6, 9, 10],
        "shoulder": [3, 7, 8, 11],
        "off_peak": [1, 2, 12],
        "peak_multiplier": 1.6,
        "shoulder_multiplier": 1.2,
        "base_occupancy": 0.55,
        "peak_occupancy": 0.85,
    },
    "rural_agritourism": {  # Tuscany hills, Umbria
        "peak": [5, 6, 7, 8, 9],
        "shoulder": [4, 10],
        "off_peak": [1, 2, 3, 11, 12],
        "peak_multiplier": 1.8,
        "shoulder_multiplier": 1.3,
        "base_occupancy": 0.30,
        "peak_occupancy": 0.70,
    },
    "lake": {  # Como, Garda, Maggiore
        "peak": [6, 7, 8],
        "shoulder": [4, 5, 9, 10],
        "off_peak": [1, 2, 3, 11, 12],
        "peak_multiplier": 2.0,
        "shoulder_multiplier": 1.4,
        "base_occupancy": 0.35,
        "peak_occupancy": 0.80,
    },
    "urban_business": {  # Milan, Turin, Bologna
        "peak": [3, 4, 5, 9, 10, 11],
        "shoulder": [2, 6],
        "off_peak": [1, 7, 8, 12],
        "peak_multiplier": 1.3,
        "shoulder_multiplier": 1.1,
        "base_occupancy": 0.55,
        "peak_occupancy": 0.70,
    },
    "default": {
        "peak": [6, 7, 8],
        "shoulder": [4, 5, 9, 10],
        "off_peak": [1, 2, 3, 11, 12],
        "peak_multiplier": 1.5,
        "shoulder_multiplier": 1.2,
        "base_occupancy": 0.40,
        "peak_occupancy": 0.65,
    },
}


def get_municipality_ids() -> List[str]:
    """Get all municipality IDs from core table."""
    with get_db_cursor() as cursor:
        cursor.execute("""
            SELECT municipality_id, coastal_flag, mountain_flag, region_code
            FROM core.municipalities
        """)
        rows = cursor.fetchall()
        return rows if rows else []


def get_tourism_municipalities() -> List[Dict[str, Any]]:
    """Get municipalities with tourism data to identify tourism-heavy areas."""
    with get_db_cursor() as cursor:
        cursor.execute("""
            SELECT
                m.municipality_id,
                m.municipality_name,
                m.coastal_flag,
                m.mountain_flag,
                m.region_code,
                COALESCE(AVG(t.presences_total), 0) as avg_presences,
                COALESCE(AVG(t.arrivals_total), 0) as avg_arrivals
            FROM core.municipalities m
            LEFT JOIN mart.municipality_tourism_month t ON m.municipality_id = t.municipality_id
            GROUP BY m.municipality_id, m.municipality_name, m.coastal_flag, m.mountain_flag, m.region_code
        """)
        rows = cursor.fetchall()
        return rows if rows else []


def get_property_values() -> Dict[str, float]:
    """Get latest property values per municipality for yield calculations."""
    with get_db_cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT ON (municipality_id)
                municipality_id,
                value_mid_eur_sqm
            FROM mart.municipality_values_semester
            WHERE property_segment = 'residential'
              AND value_mid_eur_sqm IS NOT NULL
            ORDER BY municipality_id, period_id DESC
        """)
        rows = cursor.fetchall()
        return {r["municipality_id"]: r["value_mid_eur_sqm"] for r in rows} if rows else {}


def classify_municipality(muni: Dict[str, Any]) -> str:
    """Classify municipality into a seasonality pattern category."""
    region = muni.get("region_code", "")
    coastal = muni.get("coastal_flag", False)
    mountain = muni.get("mountain_flag", False)
    presences = muni.get("avg_presences", 0) or 0

    # Major cities
    major_cities = ["058091", "015146", "027042", "048017", "037006"]  # Roma, Milano, Venezia, Firenze, Bologna
    if muni.get("municipality_id") in major_cities:
        return "city_cultural"

    # Business cities
    business_cities = ["001272", "082053", "006183"]  # Torino, Palermo, Genova
    if muni.get("municipality_id") in business_cities:
        return "urban_business"

    # Mountain ski areas
    ski_regions = ["04", "21", "25"]  # Trentino, VdA, Lombardia mountains
    if mountain and region in ski_regions:
        return "mountain_ski"

    # Lake areas
    lake_provinces = ["013", "017", "097"]  # Como, Brescia, Verbania
    province = muni.get("municipality_id", "")[:3]
    if province in lake_provinces:
        return "lake"

    # Coastal south
    south_regions = ["15", "16", "17", "18", "19"]
    if coastal and region in south_regions:
        return "coastal_south"

    # Coastal north
    north_regions = ["03", "07", "08", "09", "10", "11"]
    if coastal and region in north_regions:
        return "coastal_north"

    # Rural agritourism (Tuscany, Umbria)
    rural_regions = ["09", "10"]
    if region in rural_regions and not coastal and presences > 0:
        return "rural_agritourism"

    return "default"


def generate_demo_str_data(
    municipalities: List[Dict[str, Any]],
    num_municipalities: int = 500,
    months: int = 24,
    property_values: Dict[str, float] = None
) -> pd.DataFrame:
    """
    Generate synthetic STR data for demo purposes.
    Creates realistic ADR, occupancy, and revenue patterns based on Italian tourism.
    """
    if not municipalities:
        logger.warning("No municipalities provided")
        return pd.DataFrame()

    property_values = property_values or {}

    # Filter to tourism-relevant municipalities (coastal, mountain, or with tourism)
    tourism_munis = [
        m for m in municipalities
        if m.get("coastal_flag") or m.get("mountain_flag") or (m.get("avg_presences") or 0) > 100
    ]

    if len(tourism_munis) < num_municipalities:
        # Add random other municipalities
        other_munis = [m for m in municipalities if m not in tourism_munis]
        tourism_munis.extend(random.sample(other_munis, min(num_municipalities - len(tourism_munis), len(other_munis))))

    sample_munis = random.sample(tourism_munis, min(num_municipalities, len(tourism_munis)))

    # Regional base ADR (average daily rate in EUR)
    region_base_adr = {
        "01": 80,   "02": 110,  "03": 120,  "04": 130,  "05": 95,
        "06": 75,   "07": 100,  "08": 85,   "09": 120,  "10": 80,
        "11": 75,   "12": 110,  "13": 65,   "14": 55,   "15": 75,
        "16": 70,   "17": 60,   "18": 55,   "19": 70,   "20": 85,
    }

    records = []
    base_date = datetime.now().replace(day=1) - timedelta(days=30 * months)

    for muni in sample_munis:
        muni_id = muni.get("municipality_id")
        region = muni.get("region_code", "01")
        category = classify_municipality(muni)
        pattern = SEASONALITY_PATTERNS[category]

        # Base ADR for this municipality
        base_adr = region_base_adr.get(region, 80)
        # Add some per-municipality variation
        muni_adr_factor = random.uniform(0.7, 1.5)
        base_adr *= muni_adr_factor

        # Property value for yield calculation
        prop_value = property_values.get(muni_id)

        # Base listings count
        base_listings = random.randint(10, 300)

        for month_offset in range(months):
            period_date = base_date + timedelta(days=30 * month_offset)
            month = period_date.month
            period_id = period_date.strftime("%Y-%m")

            # Determine season
            if month in pattern["peak"]:
                season = "peak"
                adr_mult = pattern["peak_multiplier"]
                occ_base = pattern["peak_occupancy"]
            elif month in pattern["shoulder"]:
                season = "shoulder"
                adr_mult = pattern["shoulder_multiplier"]
                occ_base = (pattern["peak_occupancy"] + pattern["base_occupancy"]) / 2
            else:
                season = "off_peak"
                adr_mult = 1.0
                occ_base = pattern["base_occupancy"]

            # Add year-over-year growth trend
            yoy_growth = 0.03 * (month_offset / 12)  # 3% annual ADR growth

            # Calculate ADR with seasonality and noise
            adr = base_adr * adr_mult * (1 + yoy_growth)
            adr += random.gauss(0, adr * 0.1)  # 10% noise
            adr = max(30, min(400, adr))  # Clamp

            # Calculate occupancy
            occupancy = occ_base + random.gauss(0, 0.08)
            occupancy = max(0.15, min(0.95, occupancy))

            # RevPAR = ADR * Occupancy
            rev_par = adr * occupancy

            # Monthly revenue estimate (for 30-day month)
            monthly_revenue = rev_par * 30

            # Annual revenue estimate (extrapolate from this month's data with seasonality adjustment)
            if season == "peak":
                annual_revenue = monthly_revenue * 12 * 0.7  # Peak months inflate annual
            elif season == "shoulder":
                annual_revenue = monthly_revenue * 12 * 0.9
            else:
                annual_revenue = monthly_revenue * 12 * 1.3  # Off-peak underestimates annual

            # Calculate yields if we have property value
            str_gross_yield = None
            str_net_yield = None
            if prop_value and prop_value > 0:
                # Gross yield = annual revenue / (property value * avg sqm)
                avg_sqm = random.uniform(50, 80)  # Typical apartment size
                property_total = prop_value * avg_sqm
                str_gross_yield = (annual_revenue / property_total) * 100
                # Simple net yield estimate (after ~35% costs)
                str_net_yield = str_gross_yield * 0.65

            # Listings with growth trend
            listings_growth = 1 + 0.05 * (month_offset / 12)  # 5% annual listing growth
            listings = int(base_listings * listings_growth * random.uniform(0.9, 1.1))

            records.append({
                "municipality_id": muni_id,
                "period_id": period_id,
                "adr_eur": round(adr, 2),
                "adr_median_eur": round(adr * random.uniform(0.9, 1.0), 2),
                "adr_percentile_25": round(adr * 0.7, 2),
                "adr_percentile_75": round(adr * 1.3, 2),
                "occupancy_rate": round(occupancy, 3),
                "occupancy_median": round(occupancy * random.uniform(0.95, 1.0), 3),
                "rev_par_eur": round(rev_par, 2),
                "monthly_revenue_avg_eur": round(monthly_revenue, 2),
                "annual_revenue_estimate_eur": round(annual_revenue, 2),
                "active_listings_count": listings,
                "new_listings_count": random.randint(0, max(1, listings // 10)),
                "entire_home_pct": round(random.uniform(0.6, 0.85), 2),
                "avg_bedrooms": round(random.uniform(1.5, 3.0), 1),
                "avg_guests_capacity": round(random.uniform(3, 6), 1),
                "avg_minimum_nights": random.randint(1, 4),
                "is_peak_season": season == "peak",
                "seasonality_factor": round(adr_mult, 2),
                "peak_month_flag": season == "peak",
                "shoulder_month_flag": season == "shoulder",
                "off_peak_month_flag": season == "off_peak",
                "review_score_avg": round(random.uniform(4.2, 4.9), 2),
                "superhost_pct": round(random.uniform(0.15, 0.40), 2),
                "instant_book_pct": round(random.uniform(0.50, 0.85), 2),
                "str_gross_yield_pct": round(str_gross_yield, 2) if str_gross_yield else None,
                "str_net_yield_pct": round(str_net_yield, 2) if str_net_yield else None,
                "data_source": "demo",
                "sample_size": listings,
            })

    return pd.DataFrame(records)


def calculate_str_changes(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate period-over-period changes in STR metrics."""
    if df.empty or "period_id" not in df.columns:
        return df

    df = df.sort_values(["municipality_id", "period_id"])

    # ADR changes
    df["adr_pct_change_1m"] = df.groupby("municipality_id")["adr_eur"].pct_change() * 100
    df["adr_pct_change_12m"] = df.groupby("municipality_id")["adr_eur"].transform(
        lambda x: (x / x.shift(12) - 1) * 100
    )

    # Occupancy changes
    df["occupancy_pct_change_1m"] = df.groupby("municipality_id")["occupancy_rate"].transform(
        lambda x: (x - x.shift(1)) * 100  # Absolute point change
    )
    df["occupancy_pct_change_12m"] = df.groupby("municipality_id")["occupancy_rate"].transform(
        lambda x: (x - x.shift(12)) * 100
    )

    # Listings changes
    df["listings_pct_change_12m"] = df.groupby("municipality_id")["active_listings_count"].transform(
        lambda x: (x / x.shift(12) - 1) * 100
    )

    return df


def calculate_seasonality_profiles(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate annual seasonality profiles per municipality."""
    if df.empty:
        return pd.DataFrame()

    # Get latest year's data
    df["year"] = pd.to_datetime(df["period_id"] + "-01").dt.year
    df["month"] = pd.to_datetime(df["period_id"] + "-01").dt.month

    latest_year = df["year"].max()
    yearly_df = df[df["year"] == latest_year].copy()

    # Group by municipality
    profiles = []
    for muni_id in yearly_df["municipality_id"].unique():
        muni_data = yearly_df[yearly_df["municipality_id"] == muni_id].sort_values("month")

        if len(muni_data) < 6:  # Need at least 6 months of data
            continue

        monthly_adr = muni_data.set_index("month")["adr_eur"].to_dict()
        monthly_occ = muni_data.set_index("month")["occupancy_rate"].to_dict()
        monthly_rev = muni_data.set_index("month")["monthly_revenue_avg_eur"].to_dict()

        # Calculate averages
        avg_adr = muni_data["adr_eur"].mean()
        avg_occ = muni_data["occupancy_rate"].mean()
        avg_rev_par = muni_data["rev_par_eur"].mean()

        # Identify peak/off-peak months
        adr_by_month = muni_data.groupby("month")["adr_eur"].mean()
        threshold_high = adr_by_month.quantile(0.75)
        threshold_low = adr_by_month.quantile(0.25)

        peak_months = [str(m).zfill(2) for m in adr_by_month[adr_by_month >= threshold_high].index]
        off_peak_months = [str(m).zfill(2) for m in adr_by_month[adr_by_month <= threshold_low].index]
        shoulder_months = [str(m).zfill(2) for m in adr_by_month.index if str(m).zfill(2) not in peak_months + off_peak_months]

        # Calculate seasonality score (coefficient of variation)
        seasonality_score = (adr_by_month.std() / avg_adr) * 100 if avg_adr > 0 else 0

        # Peak to off-peak ratios
        peak_adr = adr_by_month[adr_by_month >= threshold_high].mean() if len(peak_months) > 0 else avg_adr
        offpeak_adr = adr_by_month[adr_by_month <= threshold_low].mean() if len(off_peak_months) > 0 else avg_adr
        adr_ratio = peak_adr / offpeak_adr if offpeak_adr > 0 else 1.0

        occ_by_month = muni_data.groupby("month")["occupancy_rate"].mean()
        peak_occ = occ_by_month[occ_by_month >= occ_by_month.quantile(0.75)].mean()
        offpeak_occ = occ_by_month[occ_by_month <= occ_by_month.quantile(0.25)].mean()
        occ_ratio = peak_occ / offpeak_occ if offpeak_occ > 0 else 1.0

        profiles.append({
            "municipality_id": muni_id,
            "reference_year": latest_year,
            "annual_avg_adr_eur": round(avg_adr, 2),
            "annual_avg_occupancy": round(avg_occ, 3),
            "annual_avg_rev_par_eur": round(avg_rev_par, 2),
            "total_annual_revenue_estimate_eur": round(avg_rev_par * 365, 2),
            "seasonality_score": round(min(seasonality_score, 100), 1),
            "peak_months": peak_months,
            "shoulder_months": shoulder_months,
            "off_peak_months": off_peak_months,
            "peak_to_offpeak_adr_ratio": round(adr_ratio, 2),
            "peak_to_offpeak_occupancy_ratio": round(occ_ratio, 2),
            "peak_to_offpeak_revenue_ratio": round(adr_ratio * occ_ratio, 2),
            "monthly_adr_profile": {str(k).zfill(2): round(v, 2) for k, v in monthly_adr.items()},
            "monthly_occupancy_profile": {str(k).zfill(2): round(v, 3) for k, v in monthly_occ.items()},
            "monthly_revenue_profile": {str(k).zfill(2): round(v, 2) for k, v in monthly_rev.items()},
        })

    return pd.DataFrame(profiles)


def save_str_data(df: pd.DataFrame) -> int:
    """Save STR data to mart.municipality_str_month."""
    if df.empty:
        return 0

    columns = [
        "municipality_id", "period_id",
        "adr_eur", "adr_median_eur", "adr_percentile_25", "adr_percentile_75",
        "occupancy_rate", "occupancy_median",
        "rev_par_eur", "monthly_revenue_avg_eur", "annual_revenue_estimate_eur",
        "active_listings_count", "new_listings_count", "entire_home_pct",
        "avg_bedrooms", "avg_guests_capacity", "avg_minimum_nights",
        "is_peak_season", "seasonality_factor",
        "peak_month_flag", "shoulder_month_flag", "off_peak_month_flag",
        "review_score_avg", "superhost_pct", "instant_book_pct",
        "adr_pct_change_1m", "adr_pct_change_12m",
        "occupancy_pct_change_1m", "occupancy_pct_change_12m",
        "listings_pct_change_12m",
        "str_gross_yield_pct", "str_net_yield_pct",
        "data_source", "sample_size",
    ]

    available_cols = [c for c in columns if c in df.columns]
    save_df = df[available_cols].copy()
    save_df = save_df.where(pd.notnull(save_df), None)

    # Replace numpy types
    for col in save_df.columns:
        if save_df[col].dtype == 'bool':
            save_df[col] = save_df[col].astype(object)

    rows_saved = 0
    with get_db_cursor() as cursor:
        for _, row in save_df.iterrows():
            try:
                cols = [c for c in available_cols if row.get(c) is not None or c in ["municipality_id", "period_id"]]
                values = []
                for c in cols:
                    val = row[c]
                    if isinstance(val, (np.integer, np.floating)):
                        val = float(val) if np.isfinite(val) else None
                    elif isinstance(val, np.bool_):
                        val = bool(val)
                    values.append(val)

                placeholders = ", ".join(["%s"] * len(cols))
                col_names = ", ".join(cols)

                update_cols = [c for c in cols if c not in ["municipality_id", "period_id"]]
                update_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

                cursor.execute(
                    f"""
                    INSERT INTO mart.municipality_str_month ({col_names})
                    VALUES ({placeholders})
                    ON CONFLICT (municipality_id, period_id)
                    DO UPDATE SET {update_clause}, updated_at = now()
                    """,
                    values
                )
                rows_saved += 1
            except Exception as e:
                logger.warning(f"Error saving STR data for {row.get('municipality_id')}/{row.get('period_id')}: {e}")

    return rows_saved


def save_seasonality_profiles(df: pd.DataFrame) -> int:
    """Save seasonality profiles to mart.municipality_str_seasonality."""
    if df.empty:
        return 0

    rows_saved = 0
    with get_db_cursor() as cursor:
        for _, row in df.iterrows():
            try:
                cursor.execute(
                    """
                    INSERT INTO mart.municipality_str_seasonality (
                        municipality_id, reference_year,
                        annual_avg_adr_eur, annual_avg_occupancy, annual_avg_rev_par_eur,
                        total_annual_revenue_estimate_eur, seasonality_score,
                        peak_months, shoulder_months, off_peak_months,
                        peak_to_offpeak_adr_ratio, peak_to_offpeak_occupancy_ratio,
                        peak_to_offpeak_revenue_ratio,
                        monthly_adr_profile, monthly_occupancy_profile, monthly_revenue_profile
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (municipality_id, reference_year)
                    DO UPDATE SET
                        annual_avg_adr_eur = EXCLUDED.annual_avg_adr_eur,
                        annual_avg_occupancy = EXCLUDED.annual_avg_occupancy,
                        annual_avg_rev_par_eur = EXCLUDED.annual_avg_rev_par_eur,
                        total_annual_revenue_estimate_eur = EXCLUDED.total_annual_revenue_estimate_eur,
                        seasonality_score = EXCLUDED.seasonality_score,
                        peak_months = EXCLUDED.peak_months,
                        shoulder_months = EXCLUDED.shoulder_months,
                        off_peak_months = EXCLUDED.off_peak_months,
                        peak_to_offpeak_adr_ratio = EXCLUDED.peak_to_offpeak_adr_ratio,
                        peak_to_offpeak_occupancy_ratio = EXCLUDED.peak_to_offpeak_occupancy_ratio,
                        peak_to_offpeak_revenue_ratio = EXCLUDED.peak_to_offpeak_revenue_ratio,
                        monthly_adr_profile = EXCLUDED.monthly_adr_profile,
                        monthly_occupancy_profile = EXCLUDED.monthly_occupancy_profile,
                        monthly_revenue_profile = EXCLUDED.monthly_revenue_profile,
                        updated_at = now()
                    """,
                    (
                        row["municipality_id"], row["reference_year"],
                        row["annual_avg_adr_eur"], row["annual_avg_occupancy"],
                        row["annual_avg_rev_par_eur"], row["total_annual_revenue_estimate_eur"],
                        row["seasonality_score"],
                        row["peak_months"], row["shoulder_months"], row["off_peak_months"],
                        row["peak_to_offpeak_adr_ratio"], row["peak_to_offpeak_occupancy_ratio"],
                        row["peak_to_offpeak_revenue_ratio"],
                        json.dumps(row["monthly_adr_profile"]),
                        json.dumps(row["monthly_occupancy_profile"]),
                        json.dumps(row["monthly_revenue_profile"]),
                    )
                )
                rows_saved += 1
            except Exception as e:
                logger.warning(f"Error saving seasonality profile for {row.get('municipality_id')}: {e}")

    return rows_saved


def update_str_yields_in_forecasts() -> int:
    """Update STR yield forecasts based on latest STR and property value data."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            UPDATE model.forecasts_municipality f
            SET
                forecast_str_yield_pct = s.str_gross_yield_pct,
                str_seasonality_score = ss.seasonality_score,
                str_vs_ltr_premium_pct = CASE
                    WHEN f.forecast_gross_yield_pct IS NOT NULL AND f.forecast_gross_yield_pct > 0
                    THEN ((s.str_gross_yield_pct / f.forecast_gross_yield_pct) - 1) * 100
                    ELSE NULL
                END
            FROM mart.municipality_str_month s
            LEFT JOIN mart.municipality_str_seasonality ss
                ON s.municipality_id = ss.municipality_id
            WHERE f.municipality_id = s.municipality_id
              AND s.period_id = (
                  SELECT MAX(period_id)
                  FROM mart.municipality_str_month
                  WHERE municipality_id = f.municipality_id
              )
              AND s.str_gross_yield_pct IS NOT NULL
            """
        )
        return cursor.rowcount


def main():
    parser = argparse.ArgumentParser(description="Ingest short-term rental data")
    parser.add_argument(
        "--source",
        type=str,
        choices=["airdna", "mashvisor", "demo"],
        default="demo",
        help="Data source"
    )
    parser.add_argument("--file", type=str, help="Path to source data file")
    parser.add_argument("--generate-demo", action="store_true", help="Generate demo STR data")
    parser.add_argument("--municipalities", type=int, default=500, help="Number of municipalities for demo")
    parser.add_argument("--months", type=int, default=24, help="Months of data for demo")
    parser.add_argument("--update-yields", action="store_true", help="Update STR yields in forecasts")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    args = parser.parse_args()

    run_id = None
    if not args.dry_run:
        run_id = create_ingestion_run("str_feed", args.source)
        logger.info(f"Created ingestion run {run_id}")

    try:
        if args.generate_demo or args.source == "demo":
            logger.info(f"Generating demo STR data for {args.municipalities} municipalities, {args.months} months")

            # Get municipalities with tourism context
            municipalities = get_tourism_municipalities()
            if not municipalities:
                # Fallback to basic municipality list
                muni_ids = get_municipality_ids() if hasattr(get_municipality_ids, '__call__') else []
                municipalities = [{"municipality_id": m["municipality_id"] if isinstance(m, dict) else m} for m in muni_ids]

            # Get property values for yield calculation
            property_values = get_property_values()

            df = generate_demo_str_data(
                municipalities,
                num_municipalities=args.municipalities,
                months=args.months,
                property_values=property_values
            )
            df = calculate_str_changes(df)
            logger.info(f"Generated {len(df)} STR records")

            # Calculate seasonality profiles
            seasonality_df = calculate_seasonality_profiles(df)
            logger.info(f"Calculated {len(seasonality_df)} seasonality profiles")

        else:
            logger.error("Only demo source is currently implemented")
            sys.exit(1)

        # Summary
        if not df.empty:
            stats = {
                "total_records": len(df),
                "municipalities": df["municipality_id"].nunique(),
                "periods": df["period_id"].nunique(),
                "avg_adr": round(df["adr_eur"].mean(), 2),
                "avg_occupancy": round(df["occupancy_rate"].mean(), 3),
                "avg_rev_par": round(df["rev_par_eur"].mean(), 2),
            }
            logger.info(f"Summary: {stats}")

            if args.dry_run:
                logger.info("Dry run - not saving to database")
                print("\nSample STR data:")
                print(df.head(10).to_string())
                print("\nSample seasonality profiles:")
                print(seasonality_df.head(5).to_string())
            else:
                rows_saved = save_str_data(df)
                logger.info(f"Saved {rows_saved} STR records")

                if not seasonality_df.empty:
                    profiles_saved = save_seasonality_profiles(seasonality_df)
                    logger.info(f"Saved {profiles_saved} seasonality profiles")

                if args.update_yields:
                    yields_updated = update_str_yields_in_forecasts()
                    logger.info(f"Updated STR yields for {yields_updated} forecasts")

                if run_id:
                    complete_ingestion_run(run_id, rows_saved, stats)

        logger.info("STR ingestion complete")

    except Exception as e:
        logger.exception(f"STR ingestion failed: {e}")
        if run_id:
            complete_ingestion_run(run_id, 0, {"error": str(e)}, success=False)
        sys.exit(1)


if __name__ == "__main__":
    main()
