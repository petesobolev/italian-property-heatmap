#!/usr/bin/env python3
"""
Appreciation Prediction Model

Predicts property value appreciation for Italian municipalities.
Uses historical trends and features to forecast 12-month appreciation.

Usage:
    python appreciation.py --period 2024S1 --horizon 12

Environment variables:
    DATABASE_URL or SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

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

MODEL_VERSION = "appreciation_v1.0"


def get_features(period_id: str, property_segment: str = "residential") -> pd.DataFrame:
    """Fetch features for prediction."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT *
            FROM model.features_municipality_semester
            WHERE period_id = %s AND property_segment = %s
            """,
            (period_id, property_segment),
        )
        rows = cursor.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_historical_appreciation() -> pd.DataFrame:
    """Fetch historical appreciation data for model training."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                municipality_id,
                period_id,
                value_mid_eur_sqm,
                value_pct_change_1s,
                value_pct_change_2s
            FROM mart.municipality_values_semester
            WHERE property_segment = 'residential'
            ORDER BY municipality_id, period_id
            """
        )
        rows = cursor.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()


def create_model_run(model_name: str, model_version: str, horizon_months: int) -> int:
    """Create a model run record."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO admin.model_runs (model_name, model_version, horizon_months, status)
            VALUES (%s, %s, %s, 'started')
            RETURNING model_run_id
            """,
            (model_name, model_version, horizon_months),
        )
        result = cursor.fetchone()
        return result["model_run_id"]


def complete_model_run(model_run_id: int, metrics: Dict, success: bool = True) -> None:
    """Complete a model run record."""
    status = "succeeded" if success else "failed"
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            UPDATE admin.model_runs
            SET status = %s, metrics = %s, finished_at = now()
            WHERE model_run_id = %s
            """,
            (status, json.dumps(metrics), model_run_id),
        )


class SimpleAppreciationModel:
    """
    Simple appreciation prediction model.

    Uses a combination of:
    1. Historical momentum (recent price changes)
    2. Mean reversion (deviation from regional average)
    3. Market activity signals (transaction velocity)
    4. Demographic trends (population growth)
    """

    def __init__(self):
        self.weights = {
            "momentum": 0.4,      # Recent price trends continue
            "mean_reversion": 0.2,  # Prices revert toward regional mean
            "market_activity": 0.2,  # High activity = appreciation
            "demographics": 0.2,  # Population growth = appreciation
        }
        self.baseline_appreciation = 2.0  # Default annual appreciation %

    def predict(self, features_df: pd.DataFrame) -> pd.DataFrame:
        """Generate appreciation predictions."""
        predictions = []

        for _, row in features_df.iterrows():
            prediction = self._predict_single(row)
            predictions.append(prediction)

        return pd.DataFrame(predictions)

    def _predict_single(self, row: pd.Series) -> Dict:
        """Predict appreciation for a single municipality."""
        municipality_id = row.get("municipality_id")

        # Initialize components
        momentum_score = 0.0
        mean_reversion_score = 0.0
        market_score = 0.0
        demo_score = 0.0
        confidence = 0.0
        confidence_factors = []

        # 1. Momentum component (based on recent price changes)
        pct_change_1s = row.get("value_pct_change_1s")
        pct_change_2s = row.get("value_pct_change_2s")

        if pd.notna(pct_change_1s):
            # Project forward with dampening
            momentum_score = float(pct_change_1s) * 0.7
            confidence += 20
            confidence_factors.append("recent_trend")

        if pd.notna(pct_change_2s):
            # Weight longer-term trend
            momentum_score += float(pct_change_2s) * 0.3
            confidence += 10
            confidence_factors.append("annual_trend")

        # 2. Mean reversion component
        value_vs_region = row.get("value_vs_region_pct")
        if pd.notna(value_vs_region):
            # If undervalued vs region, expect appreciation
            # If overvalued, expect slower growth
            mean_reversion_score = -float(value_vs_region) * 0.1
            confidence += 15
            confidence_factors.append("regional_comparison")

        # 3. Market activity component
        ntn_pct_change = row.get("ntn_pct_change_1s")
        absorption_rate = row.get("absorption_rate")

        if pd.notna(ntn_pct_change):
            # Increasing transactions = positive signal
            market_score = float(ntn_pct_change) * 0.3
            confidence += 15
            confidence_factors.append("transaction_trend")

        if pd.notna(absorption_rate):
            # High absorption = seller's market
            if absorption_rate > 0.5:
                market_score += 1.0
            confidence += 10
            confidence_factors.append("absorption")

        # 4. Demographics component
        pop_growth = row.get("population_growth_rate")
        migration = row.get("migration_balance")

        if pd.notna(pop_growth):
            # Population growth drives demand
            demo_score = float(pop_growth) * 2.0
            confidence += 15
            confidence_factors.append("population_trend")

        if pd.notna(migration) and pd.notna(row.get("total_population")):
            pop = row.get("total_population")
            if pop and pop > 0:
                migration_rate = float(migration) / float(pop) * 100
                demo_score += migration_rate * 0.5
                confidence += 5
                confidence_factors.append("migration")

        # Feature completeness boost
        completeness = row.get("feature_completeness_score", 0)
        if pd.notna(completeness):
            confidence += float(completeness) * 0.1

        # Combine components
        forecast_appreciation = (
            self.baseline_appreciation
            + self.weights["momentum"] * momentum_score
            + self.weights["mean_reversion"] * mean_reversion_score
            + self.weights["market_activity"] * market_score
            + self.weights["demographics"] * demo_score
        )

        # Clamp to reasonable range
        forecast_appreciation = max(-15.0, min(25.0, forecast_appreciation))

        # Normalize confidence to 0-100
        confidence = min(100.0, max(0.0, confidence))

        # Identify key drivers
        drivers = []
        if abs(momentum_score) > 1:
            drivers.append({
                "factor": "price_momentum",
                "direction": "positive" if momentum_score > 0 else "negative",
                "strength": abs(momentum_score),
            })
        if abs(mean_reversion_score) > 0.5:
            drivers.append({
                "factor": "valuation",
                "direction": "undervalued" if mean_reversion_score > 0 else "overvalued",
                "strength": abs(mean_reversion_score),
            })
        if abs(market_score) > 0.5:
            drivers.append({
                "factor": "market_activity",
                "direction": "increasing" if market_score > 0 else "decreasing",
                "strength": abs(market_score),
            })

        # Identify risks
        risks = []
        volatility = row.get("value_volatility_4s")
        if pd.notna(volatility) and volatility > 5:
            risks.append({
                "factor": "price_volatility",
                "severity": "high" if volatility > 10 else "medium",
            })

        elderly_ratio = row.get("elderly_ratio")
        if pd.notna(elderly_ratio) and elderly_ratio > 0.3:
            risks.append({
                "factor": "aging_population",
                "severity": "medium",
            })

        return {
            "municipality_id": municipality_id,
            "forecast_appreciation_pct": round(forecast_appreciation, 2),
            "confidence_score": round(confidence, 1),
            "drivers": drivers,
            "risks": risks,
        }


def calculate_metrics(predictions_df: pd.DataFrame, actuals_df: pd.DataFrame) -> Dict:
    """Calculate model performance metrics."""
    # Merge predictions with actuals
    merged = predictions_df.merge(
        actuals_df,
        on="municipality_id",
        how="inner",
        suffixes=("_pred", "_actual"),
    )

    if merged.empty:
        return {"mae": None, "rmse": None, "directional_accuracy": None, "n_samples": 0}

    # Calculate metrics
    errors = merged["forecast_appreciation_pct"] - merged["actual_appreciation_pct"]
    mae = abs(errors).mean()
    rmse = np.sqrt((errors ** 2).mean())

    # Directional accuracy
    correct_direction = (
        (merged["forecast_appreciation_pct"] > 0) == (merged["actual_appreciation_pct"] > 0)
    ).mean()

    return {
        "mae": round(mae, 3),
        "rmse": round(rmse, 3),
        "directional_accuracy": round(correct_direction * 100, 1),
        "n_samples": len(merged),
    }


def save_forecasts(
    predictions_df: pd.DataFrame,
    forecast_date: str,
    horizon_months: int,
    property_segment: str,
    model_version: str,
    features_df: pd.DataFrame,
) -> int:
    """Save forecasts to model.forecasts_municipality."""
    rows_saved = 0

    with get_db_cursor() as cursor:
        for _, pred in predictions_df.iterrows():
            municipality_id = pred["municipality_id"]

            # Get value from features
            feature_row = features_df[features_df["municipality_id"] == municipality_id]
            value_mid = None
            if not feature_row.empty:
                value_mid = feature_row.iloc[0].get("value_mid_eur_sqm")

            try:
                cursor.execute(
                    """
                    INSERT INTO model.forecasts_municipality (
                        municipality_id, forecast_date, horizon_months,
                        property_segment, model_version,
                        value_mid_eur_sqm, forecast_appreciation_pct,
                        confidence_score, drivers, risks, publishable_flag
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (municipality_id, forecast_date, horizon_months, property_segment, model_version)
                    DO UPDATE SET
                        value_mid_eur_sqm = EXCLUDED.value_mid_eur_sqm,
                        forecast_appreciation_pct = EXCLUDED.forecast_appreciation_pct,
                        confidence_score = EXCLUDED.confidence_score,
                        drivers = EXCLUDED.drivers,
                        risks = EXCLUDED.risks,
                        created_at = now()
                    """,
                    (
                        municipality_id,
                        forecast_date,
                        horizon_months,
                        property_segment,
                        model_version,
                        float(value_mid) if pd.notna(value_mid) else None,
                        pred["forecast_appreciation_pct"],
                        pred["confidence_score"],
                        json.dumps(pred.get("drivers", [])),
                        json.dumps(pred.get("risks", [])),
                        True,  # publishable_flag
                    ),
                )
                rows_saved += 1
            except Exception as e:
                logger.warning(f"Error saving forecast for {municipality_id}: {e}")

    return rows_saved


def main():
    parser = argparse.ArgumentParser(description="Run appreciation prediction model")
    parser.add_argument("--period", type=str, required=True, help="Period ID (e.g., 2024S1)")
    parser.add_argument("--horizon", type=int, default=12, help="Forecast horizon in months")
    parser.add_argument("--segment", type=str, default="residential", help="Property segment")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    args = parser.parse_args()

    # Create model run
    model_run_id = None
    if not args.dry_run:
        model_run_id = create_model_run("appreciation", MODEL_VERSION, args.horizon)
        logger.info(f"Created model run {model_run_id}")

    try:
        # Load features
        features_df = get_features(args.period, args.segment)
        logger.info(f"Loaded {len(features_df)} feature rows")

        if features_df.empty:
            logger.warning("No features found. Run feature engineering first.")
            # Generate predictions from municipality data directly
            with get_db_cursor() as cursor:
                cursor.execute("SELECT municipality_id FROM core.municipalities")
                rows = cursor.fetchall()
                features_df = pd.DataFrame(rows)
                logger.info(f"Using {len(features_df)} municipalities from core table")

        # Initialize model
        model = SimpleAppreciationModel()

        # Generate predictions
        predictions_df = model.predict(features_df)
        logger.info(f"Generated {len(predictions_df)} predictions")

        # Calculate forecast date
        year = int(args.period[:4])
        semester = int(args.period[-1])
        forecast_date = f"{year}-{'01' if semester == 1 else '07'}-01"

        # Summary stats
        metrics = {
            "n_predictions": len(predictions_df),
            "mean_appreciation": round(predictions_df["forecast_appreciation_pct"].mean(), 2),
            "std_appreciation": round(predictions_df["forecast_appreciation_pct"].std(), 2),
            "mean_confidence": round(predictions_df["confidence_score"].mean(), 1),
        }
        logger.info(f"Metrics: {metrics}")

        if args.dry_run:
            logger.info("Dry run - not saving to database")
            print(predictions_df.head(20))
        else:
            # Save forecasts
            rows_saved = save_forecasts(
                predictions_df,
                forecast_date,
                args.horizon,
                args.segment,
                MODEL_VERSION,
                features_df,
            )
            logger.info(f"Saved {rows_saved} forecasts")
            metrics["rows_saved"] = rows_saved

            # Complete model run
            complete_model_run(model_run_id, metrics, success=True)

        logger.info("Model run complete")

    except Exception as e:
        logger.exception(f"Model run failed: {e}")
        if model_run_id:
            complete_model_run(model_run_id, {"error": str(e)}, success=False)
        sys.exit(1)


if __name__ == "__main__":
    main()
