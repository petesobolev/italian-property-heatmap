"use client";

import type { MetricType } from "./FiltersSidebar";

interface MapLegendProps {
  metric: MetricType;
  min: number;
  max: number;
  isLoading?: boolean;
  showFlatTaxEligible?: boolean;
}

const METRIC_CONFIG: Record<
  MetricType,
  {
    label: string;
    unit: string;
    format: (v: number) => string;
    colors: string[];
    fixedRange?: { min: number; max: number }; // Use fixed range instead of data-driven
  }
> = {
  value_mid_eur_sqm: {
    label: "Property Value",
    unit: "€/m²",
    format: (v) => `€${Math.round(v).toLocaleString()}`,
    colors: ["#1e3a5f", "#2d5a87", "#4a90b5", "#7cc4d4", "#b8e0ec"],
  },
  rent_mid_eur_sqm_month: {
    label: "Monthly Rent",
    unit: "€/m²/mo",
    format: (v) => `€${v.toFixed(1)}`,
    colors: ["#f0f9e8", "#bae4bc", "#7bccc4", "#43a2ca", "#0868ac"],
  },
  gross_yield_pct: {
    label: "Gross Yield",
    unit: "%",
    format: (v) => `${v.toFixed(1)}%`,
    colors: ["#fef0d9", "#fdcc8a", "#fc8d59", "#e34a33", "#b30000"],
    fixedRange: { min: 2, max: 8 }, // Fixed range for consistent colors
  },
  price_variance_pct: {
    label: "Price Spread",
    unit: "%",
    format: (v) => `${v.toFixed(0)}%`,
    colors: ["#1a9850", "#91cf60", "#ffffbf", "#fc8d59", "#d73027"],
    fixedRange: { min: 0, max: 100 },
  },
  forecast_appreciation_pct: {
    label: "Appreciation Forecast",
    unit: "%",
    format: (v) => `${v > 0 ? "+" : ""}${v.toFixed(1)}%`,
    colors: ["#7f1d1d", "#b45309", "#f5f5f4", "#16a34a", "#166534"],
    fixedRange: { min: -10, max: 10 },
  },
  forecast_gross_yield_pct: {
    label: "Gross Yield",
    unit: "%",
    format: (v) => `${v.toFixed(1)}%`,
    colors: ["#fef3c7", "#fcd34d", "#f59e0b", "#d97706", "#92400e"],
    fixedRange: { min: 0, max: 10 },
  },
  opportunity_score: {
    label: "Opportunity Score",
    unit: "pts",
    format: (v) => Math.round(v).toString(),
    colors: ["#1a1a2e", "#4a3f6b", "#c4785c", "#e8c4a0", "#f5ebe0"],
    fixedRange: { min: 0, max: 100 },
  },
  confidence_score: {
    label: "Data Confidence",
    unit: "%",
    format: (v) => `${Math.round(v)}%`,
    colors: ["#374151", "#4b5563", "#6b7280", "#9ca3af", "#d1d5db"],
    fixedRange: { min: 0, max: 100 },
  },
  foreign_ratio: {
    label: "Foreign Residents",
    unit: "%",
    format: (v) => `${v.toFixed(1)}%`,
    colors: ["#f0f9ff", "#bae6fd", "#38bdf8", "#0284c7", "#075985"],
    fixedRange: { min: 0, max: 25 },
  },
  population_growth_rate: {
    label: "Population Trend",
    unit: "% YoY",
    format: (v) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}%`,
    colors: ["#b43232", "#dc7864", "#b4b4b4", "#64b464", "#328c32"],
    // No fixedRange - use dynamic range based on viewport data
  },
  value_pct_change: {
    label: "Value Change",
    unit: "%",
    format: (v) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}%`,
    colors: ["#d73027", "#fc8d59", "#a8a8a8", "#91cf60", "#1a9850"],
    fixedRange: { min: -15, max: 15 },
  },
};

export function MapLegend({ metric, min, max, isLoading, showFlatTaxEligible }: MapLegendProps) {
  const config = METRIC_CONFIG[metric];
  const steps = 5;

  // Use fixed range if defined, otherwise use data-driven range
  const displayMin = config.fixedRange?.min ?? min;
  const displayMax = config.fixedRange?.max ?? max;
  const range = displayMax - displayMin;

  // Special legend for flat tax eligibility
  if (showFlatTaxEligible) {
    return (
      <div className="map-legend map-legend--flat-tax">
        <div className="map-legend__header">
          <span className="map-legend__title">7% Flat Tax Eligibility</span>
        </div>

        <div className="flat-tax-legend">
          <div className="flat-tax-legend__item">
            <div
              className="flat-tax-legend__swatch"
              style={{ backgroundColor: "rgba(34, 197, 94, 0.5)", borderColor: "#22c55e" }}
            />
            <span className="flat-tax-legend__label">Southern Italy (Mezzogiorno)</span>
          </div>
          <div className="flat-tax-legend__item">
            <div
              className="flat-tax-legend__swatch"
              style={{ backgroundColor: "rgba(202, 138, 4, 0.6)", borderColor: "#eab308" }}
            />
            <span className="flat-tax-legend__label">Sisma 2016 (Earthquake Zone)</span>
          </div>
          <div className="flat-tax-legend__item">
            <div
              className="flat-tax-legend__swatch"
              style={{ backgroundColor: "rgba(249, 115, 22, 0.6)", borderColor: "#f97316" }}
            />
            <span className="flat-tax-legend__label">Both (Southern + Sisma)</span>
          </div>
          <div className="flat-tax-legend__item">
            <div
              className="flat-tax-legend__swatch flat-tax-legend__swatch--ineligible"
              style={{ backgroundColor: "rgba(100, 100, 100, 0.4)", borderColor: "rgba(255, 255, 255, 0.2)" }}
            />
            <span className="flat-tax-legend__label">Not Eligible</span>
          </div>
        </div>

        <style jsx>{`
          .map-legend--flat-tax {
            position: absolute;
            bottom: 24px;
            left: 50%;
            transform: translateX(-50%);
            z-index: 1000;
            background: linear-gradient(165deg,
              rgba(22, 25, 32, 0.95) 0%,
              rgba(13, 15, 18, 0.97) 100%
            );
            backdrop-filter: blur(16px);
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 12px;
            padding: 12px 16px;
            min-width: 260px;
            box-shadow:
              0 4px 20px rgba(0, 0, 0, 0.3),
              inset 0 1px 0 rgba(255, 255, 255, 0.04);
          }

          .map-legend__header {
            margin-bottom: 12px;
          }

          .map-legend__title {
            font-size: 0.7rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            color: #a8b3c7;
          }

          .flat-tax-legend {
            display: flex;
            flex-direction: column;
            gap: 8px;
          }

          .flat-tax-legend__item {
            display: flex;
            align-items: center;
            gap: 10px;
          }

          .flat-tax-legend__swatch {
            width: 20px;
            height: 14px;
            border-radius: 3px;
            border: 2px solid;
            flex-shrink: 0;
          }

          .flat-tax-legend__label {
            font-size: 0.7rem;
            color: #c8d3e3;
          }
        `}</style>
      </div>
    );
  }

  return (
    <div className="map-legend">
      <div className="map-legend__header">
        <span className="map-legend__title">{config.label}</span>
        <span className="map-legend__unit">{config.unit}</span>
      </div>

      <div className="map-legend__bar-container">
        <div
          className="map-legend__bar"
          style={{
            background: `linear-gradient(to right, ${config.colors.join(", ")})`,
          }}
        />
        {isLoading && <div className="map-legend__loading" />}
      </div>

      <div className="map-legend__labels">
        {Array.from({ length: steps }).map((_, i) => {
          const value = displayMin + (range * i) / (steps - 1);
          return (
            <span key={i} className="map-legend__label">
              {range > 0 ? config.format(value) : "—"}
            </span>
          );
        })}
      </div>

      <style jsx>{`
        .map-legend {
          position: absolute;
          bottom: 24px;
          left: 50%;
          transform: translateX(-50%);
          z-index: 1000;
          background: linear-gradient(165deg,
            rgba(22, 25, 32, 0.95) 0%,
            rgba(13, 15, 18, 0.97) 100%
          );
          backdrop-filter: blur(16px);
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 12px;
          padding: 12px 16px;
          min-width: 280px;
          box-shadow:
            0 4px 20px rgba(0, 0, 0, 0.3),
            inset 0 1px 0 rgba(255, 255, 255, 0.04);
        }

        .map-legend__header {
          display: flex;
          align-items: center;
          justify-content: space-between;
          margin-bottom: 10px;
        }

        .map-legend__title {
          font-size: 0.7rem;
          font-weight: 600;
          text-transform: uppercase;
          letter-spacing: 0.1em;
          color: #a8b3c7;
        }

        .map-legend__unit {
          font-size: 0.65rem;
          color: #6b7a90;
          padding: 2px 8px;
          background: rgba(255, 255, 255, 0.04);
          border-radius: 4px;
        }

        .map-legend__bar-container {
          position: relative;
          margin-bottom: 8px;
        }

        .map-legend__bar {
          height: 10px;
          border-radius: 5px;
          box-shadow: inset 0 1px 2px rgba(0, 0, 0, 0.2);
        }

        .map-legend__loading {
          position: absolute;
          inset: 0;
          background: linear-gradient(
            90deg,
            transparent 0%,
            rgba(255, 255, 255, 0.1) 50%,
            transparent 100%
          );
          animation: shimmer 1.5s infinite;
          border-radius: 5px;
        }

        @keyframes shimmer {
          0% {
            transform: translateX(-100%);
          }
          100% {
            transform: translateX(100%);
          }
        }

        .map-legend__labels {
          display: flex;
          justify-content: space-between;
        }

        .map-legend__label {
          font-size: 0.65rem;
          color: #8b9bb4;
          font-variant-numeric: tabular-nums;
        }

        .map-legend__label:first-child {
          text-align: left;
        }

        .map-legend__label:last-child {
          text-align: right;
        }
      `}</style>
    </div>
  );
}
