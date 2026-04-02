#!/usr/bin/env python3
"""
Opportunity Score Calculator

Computes composite opportunity scores for Italian municipalities.
Combines appreciation forecast, yield, confidence, and risk factors.

Usage:
    python scoring.py --period 2024S1

Environment variables:
    DATABASE_URL or SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY
"""

import argparse
import json
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

MODEL_VERSION = "scoring_v1.0"


# Investment strategy weights
STRATEGY_WEIGHTS = {
    "balanced": {
        "appreciation": 0.35,
        "yield": 0.30,
        "confidence": 0.20,
        "risk_penalty": 0.15,
    },
    "growth": {
        "appreciation": 0.55,
        "yield": 0.15,
        "confidence": 0.20,
        "risk_penalty": 0.10,
    },
    "yield": {
        "appreciation": 0.15,
        "yield": 0.55,
        "confidence": 0.20,
        "risk_penalty": 0.10,
    },
}


def get_forecasts(forecast_date: str, horizon_months: int = 12) -> pd.DataFrame:
    """Fetch latest forecasts for scoring."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                municipality_id,
                forecast_appreciation_pct,
                confidence_score,
                value_mid_eur_sqm,
                drivers,
                risks
            FROM model.forecasts_municipality
            WHERE forecast_date = %s
              AND horizon_months = %s
              AND property_segment = 'residential'
              AND model_version = (
                  SELECT model_version
                  FROM model.forecasts_municipality
                  WHERE forecast_date = %s AND horizon_months = %s
                  ORDER BY created_at DESC
                  LIMIT 1
              )
            """,
            (forecast_date, horizon_months, forecast_date, horizon_months),
        )
        rows = cursor.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_features(period_id: str) -> pd.DataFrame:
    """Fetch features including yield data."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                municipality_id,
                gross_yield_pct,
                value_volatility_4s,
                population_growth_rate,
                elderly_ratio,
                feature_completeness_score
            FROM model.features_municipality_semester
            WHERE period_id = %s AND property_segment = 'residential'
            """,
            (period_id,),
        )
        rows = cursor.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_municipality_metadata() -> pd.DataFrame:
    """Fetch municipality metadata for regional context."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                municipality_id,
                region_code,
                coastal_flag,
                mountain_flag
            FROM core.municipalities
            """
        )
        rows = cursor.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()


def normalize_score(values: pd.Series, higher_is_better: bool = True) -> pd.Series:
    """Normalize values to 0-100 scale using percentile ranking."""
    if values.isna().all():
        return pd.Series([50.0] * len(values), index=values.index)

    # Use percentile ranking
    ranks = values.rank(pct=True, na_option="keep")

    if not higher_is_better:
        ranks = 1 - ranks

    return ranks * 100


def calculate_appreciation_score(appreciation_pct: pd.Series) -> pd.Series:
    """
    Convert appreciation forecast to 0-100 score.

    Mapping:
    - -10% or worse -> 0
    - 0% -> 40
    - +5% -> 70
    - +10% or better -> 100
    """
    # Clip to reasonable range
    clipped = appreciation_pct.clip(-10, 15)

    # Linear transformation: -10 -> 0, +10 -> 100
    score = ((clipped + 10) / 20) * 100

    return score.clip(0, 100)


def calculate_yield_score(yield_pct: pd.Series) -> pd.Series:
    """
    Convert gross yield to 0-100 score.

    Mapping:
    - 0% -> 0
    - 4% -> 50 (typical Italian yield)
    - 8% or higher -> 100
    """
    # Clip to reasonable range
    clipped = yield_pct.clip(0, 10)

    # Linear transformation: 0 -> 0, 8 -> 100
    score = (clipped / 8) * 100

    return score.clip(0, 100)


def calculate_risk_score(
    volatility: pd.Series,
    elderly_ratio: pd.Series,
    pop_growth: pd.Series,
) -> pd.Series:
    """
    Calculate risk score (0-100, higher = more risk).

    Factors:
    - High volatility increases risk
    - High elderly ratio increases risk (demographic decline)
    - Negative population growth increases risk
    """
    risk = pd.Series(0.0, index=volatility.index)

    # Volatility component (0-40 points)
    if volatility.notna().any():
        vol_norm = volatility.clip(0, 20) / 20 * 40
        risk = risk.add(vol_norm.fillna(10), fill_value=0)

    # Elderly ratio component (0-30 points)
    if elderly_ratio.notna().any():
        # > 30% elderly is high risk
        elderly_risk = ((elderly_ratio - 0.2).clip(0, 0.2) / 0.2) * 30
        risk = risk.add(elderly_risk.fillna(10), fill_value=0)

    # Population decline component (0-30 points)
    if pop_growth.notna().any():
        # Negative growth is risky
        decline_risk = ((-pop_growth).clip(0, 2) / 2) * 30
        risk = risk.add(decline_risk.fillna(10), fill_value=0)

    return risk.clip(0, 100)


def compute_opportunity_scores(
    forecasts_df: pd.DataFrame,
    features_df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    strategy: str = "balanced",
) -> pd.DataFrame:
    """Compute opportunity scores for all municipalities."""
    if forecasts_df.empty:
        logger.warning("No forecasts available")
        return pd.DataFrame()

    weights = STRATEGY_WEIGHTS.get(strategy, STRATEGY_WEIGHTS["balanced"])

    # Merge data
    df = forecasts_df.merge(features_df, on="municipality_id", how="left")
    df = df.merge(metadata_df, on="municipality_id", how="left")

    # Calculate component scores
    df["appreciation_score"] = calculate_appreciation_score(
        df["forecast_appreciation_pct"].fillna(0)
    )

    df["yield_score"] = calculate_yield_score(
        df["gross_yield_pct"].fillna(0)
    )

    df["risk_score"] = calculate_risk_score(
        df.get("value_volatility_4s", pd.Series()),
        df.get("elderly_ratio", pd.Series()),
        df.get("population_growth_rate", pd.Series()),
    )

    # Use confidence directly (already 0-100)
    df["confidence_score_norm"] = df["confidence_score"].fillna(50).clip(0, 100)

    # Compute weighted opportunity score
    df["opportunity_score"] = (
        weights["appreciation"] * df["appreciation_score"]
        + weights["yield"] * df["yield_score"]
        + weights["confidence"] * df["confidence_score_norm"]
        - weights["risk_penalty"] * df["risk_score"]
    )

    # Normalize to 0-100
    df["opportunity_score"] = df["opportunity_score"].clip(0, 100)

    # Round scores
    df["opportunity_score"] = df["opportunity_score"].round(1)
    df["appreciation_score"] = df["appreciation_score"].round(1)
    df["yield_score"] = df["yield_score"].round(1)
    df["risk_score"] = df["risk_score"].round(1)

    # Identify top factors
    def get_top_factors(row):
        factors = []
        if row.get("appreciation_score", 0) > 70:
            factors.append({"factor": "high_appreciation", "score": row["appreciation_score"]})
        if row.get("yield_score", 0) > 70:
            factors.append({"factor": "high_yield", "score": row["yield_score"]})
        if row.get("risk_score", 0) < 30:
            factors.append({"factor": "low_risk", "score": 100 - row["risk_score"]})
        if row.get("confidence_score", 0) > 70:
            factors.append({"factor": "high_confidence", "score": row["confidence_score"]})

        # Add geographic factors
        if row.get("coastal_flag"):
            factors.append({"factor": "coastal_location", "score": None})

        return factors[:3]  # Top 3 factors

    df["score_factors"] = df.apply(get_top_factors, axis=1)

    return df


def save_opportunity_scores(
    scores_df: pd.DataFrame,
    forecast_date: str,
    horizon_months: int,
    strategy: str,
    model_version: str,
) -> int:
    """Save opportunity scores to model.forecasts_municipality."""
    if scores_df.empty:
        return 0

    rows_updated = 0

    with get_db_cursor() as cursor:
        for _, row in scores_df.iterrows():
            try:
                # Update existing forecast with opportunity score
                cursor.execute(
                    """
                    UPDATE model.forecasts_municipality
                    SET
                        opportunity_score = %s,
                        opportunity_strategy = %s,
                        score_factors = %s
                    WHERE municipality_id = %s
                      AND forecast_date = %s
                      AND horizon_months = %s
                      AND property_segment = 'residential'
                    """,
                    (
                        row["opportunity_score"],
                        strategy,
                        json.dumps(row.get("score_factors", [])),
                        row["municipality_id"],
                        forecast_date,
                        horizon_months,
                    ),
                )
                if cursor.rowcount > 0:
                    rows_updated += 1
            except Exception as e:
                logger.warning(f"Error updating score for {row['municipality_id']}: {e}")

    return rows_updated


def get_top_opportunities(
    scores_df: pd.DataFrame,
    n: int = 20,
    min_confidence: float = 50.0,
) -> pd.DataFrame:
    """Get top N opportunities filtered by confidence."""
    filtered = scores_df[scores_df["confidence_score"] >= min_confidence]
    return filtered.nlargest(n, "opportunity_score")


def main():
    parser = argparse.ArgumentParser(description="Calculate opportunity scores")
    parser.add_argument("--period", type=str, required=True, help="Period ID (e.g., 2024S1)")
    parser.add_argument("--horizon", type=int, default=12, help="Forecast horizon in months")
    parser.add_argument(
        "--strategy",
        type=str,
        default="balanced",
        choices=["balanced", "growth", "yield"],
        help="Investment strategy",
    )
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    args = parser.parse_args()

    # Calculate forecast date from period
    year = int(args.period[:4])
    semester = int(args.period[-1])
    forecast_date = f"{year}-{'01' if semester == 1 else '07'}-01"

    logger.info(f"Calculating opportunity scores for {forecast_date}, strategy={args.strategy}")

    try:
        # Load data
        forecasts_df = get_forecasts(forecast_date, args.horizon)
        features_df = get_features(args.period)
        metadata_df = get_municipality_metadata()

        logger.info(
            f"Loaded: {len(forecasts_df)} forecasts, {len(features_df)} features, "
            f"{len(metadata_df)} municipalities"
        )

        if forecasts_df.empty:
            logger.warning("No forecasts found. Run appreciation model first.")
            sys.exit(1)

        # Calculate scores
        scores_df = compute_opportunity_scores(
            forecasts_df, features_df, metadata_df, args.strategy
        )
        logger.info(f"Calculated {len(scores_df)} opportunity scores")

        # Summary stats
        metrics = {
            "n_scores": len(scores_df),
            "mean_score": round(scores_df["opportunity_score"].mean(), 1),
            "std_score": round(scores_df["opportunity_score"].std(), 1),
            "top_score": round(scores_df["opportunity_score"].max(), 1),
        }
        logger.info(f"Metrics: {metrics}")

        # Show top opportunities
        top_opps = get_top_opportunities(scores_df, n=10)
        if not top_opps.empty:
            logger.info("Top 10 opportunities:")
            for _, row in top_opps.iterrows():
                logger.info(
                    f"  {row['municipality_id']}: "
                    f"score={row['opportunity_score']:.1f}, "
                    f"appreciation={row.get('forecast_appreciation_pct', 0):.1f}%, "
                    f"confidence={row.get('confidence_score', 0):.0f}"
                )

        if args.dry_run:
            logger.info("Dry run - not saving to database")
            print(scores_df[["municipality_id", "opportunity_score", "appreciation_score",
                           "yield_score", "risk_score"]].head(20))
        else:
            rows_updated = save_opportunity_scores(
                scores_df, forecast_date, args.horizon, args.strategy, MODEL_VERSION
            )
            logger.info(f"Updated {rows_updated} forecast records with opportunity scores")

        logger.info("Scoring complete")

    except Exception as e:
        logger.exception(f"Scoring failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
