"use client";

import type { MetricType } from "./FiltersSidebar";

interface MapLegendProps {
  metric: MetricType;
  min: number;
  max: number;
  isLoading?: boolean;
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
  vehicle_arson_rate: {
    label: "Vehicle Arson Rate",
    unit: "per 100k",
    format: (v) => v.toFixed(1),
    colors: ["#fef3c7", "#fde68a", "#f8924f", "#d7301f", "#7f1d1d"],
    fixedRange: { min: 0, max: 150 },
  },
};

export function MapLegend({ metric, min, max, isLoading }: MapLegendProps) {
  const config = METRIC_CONFIG[metric];
  const steps = 5;

  // Use fixed range if defined, otherwise use data-driven range
  const displayMin = config.fixedRange?.min ?? min;
  const displayMax = config.fixedRange?.max ?? max;
  const range = displayMax - displayMin;

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
