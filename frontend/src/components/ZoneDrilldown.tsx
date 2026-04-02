"use client";

import { useEffect, useState } from "react";

interface ZoneProperties {
  zoneId: string;
  zoneCode: string;
  zoneName: string;
  zoneType: string;
  microzoneCode: string | null;
  zoneClassification: string | null;
  values: {
    periodId: string;
    valueMidEurSqm: number | null;
    valueMinEurSqm: number | null;
    valueMaxEurSqm: number | null;
    rentMidEurSqmMonth: number | null;
    pctChange1s: number | null;
  } | null;
  forecast: null; // No zone-level forecasts in current schema
}

interface ZoneFeature {
  type: "Feature";
  properties: ZoneProperties;
  geometry: unknown;
}

interface ZoneData {
  type: "FeatureCollection";
  features: ZoneFeature[];
  stats: {
    minValue: number;
    maxValue: number;
    avgValue: number;
    zonesWithData: number;
    totalZones: number;
  } | null;
  municipality_id: string;
  segment: string;
}

interface ZoneDrilldownProps {
  municipalityId: string;
  segment?: string;
}

function formatCurrency(value: number | null | undefined): string {
  if (value == null) return "—";
  return `€${Math.round(value).toLocaleString("it-IT")}`;
}

function formatPercent(value: number | null | undefined): string {
  if (value == null) return "—";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(1)}%`;
}

function getValueColor(
  value: number | null,
  min: number,
  max: number
): string {
  if (value == null) return "rgba(139, 155, 180, 0.3)";
  const normalized = (value - min) / (max - min || 1);
  // Color scale from blue (low) through teal to terracotta (high)
  if (normalized < 0.5) {
    const t = normalized * 2;
    return `rgba(${82 + t * 114}, ${139 - t * 19}, ${153 - t * 61}, 0.7)`;
  } else {
    const t = (normalized - 0.5) * 2;
    return `rgba(${196 - t * 50}, ${120 - t * 40}, ${92 - t * 20}, 0.7)`;
  }
}

function getZoneTypeLabel(type: string): string {
  const labels: Record<string, string> = {
    B: "Central",
    C: "Semi-central",
    D: "Peripheral",
    E: "Suburban",
    R: "Rural",
  };
  return labels[type] || type;
}

export function ZoneDrilldown({
  municipalityId,
  segment = "residential",
}: ZoneDrilldownProps) {
  const [data, setData] = useState<ZoneData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedZone, setSelectedZone] = useState<string | null>(null);
  const [sortBy, setSortBy] = useState<"value" | "change">("value");

  useEffect(() => {
    async function fetchZones() {
      try {
        const response = await fetch(
          `/api/municipality/${municipalityId}/zones?segment=${segment}`
        );
        if (!response.ok) {
          setError("Failed to load zone data");
          return;
        }
        const json = await response.json();
        setData(json);
      } catch {
        setError("Failed to connect to server");
      } finally {
        setLoading(false);
      }
    }
    fetchZones();
  }, [municipalityId, segment]);

  if (loading) {
    return (
      <div className="zone-drilldown zone-drilldown--loading">
        <div className="zone-drilldown__spinner" />
        <span>Loading zone data...</span>
        <style jsx>{`
          .zone-drilldown--loading {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            gap: 16px;
            padding: 48px;
            color: #6b7a90;
          }
          .zone-drilldown__spinner {
            width: 32px;
            height: 32px;
            border: 3px solid rgba(196, 120, 92, 0.2);
            border-top-color: #c4785c;
            border-radius: 50%;
            animation: spin 1s linear infinite;
          }
          @keyframes spin {
            to {
              transform: rotate(360deg);
            }
          }
        `}</style>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="zone-drilldown zone-drilldown--error">
        <span>{error || "No zone data available"}</span>
        <style jsx>{`
          .zone-drilldown--error {
            padding: 32px;
            text-align: center;
            color: #6b7a90;
            font-size: 0.9rem;
          }
        `}</style>
      </div>
    );
  }

  if (data.features.length === 0) {
    return (
      <div className="zone-drilldown zone-drilldown--empty">
        <span>No OMI zones available for this municipality</span>
        <style jsx>{`
          .zone-drilldown--empty {
            padding: 32px;
            text-align: center;
            color: #6b7a90;
            font-size: 0.9rem;
          }
        `}</style>
      </div>
    );
  }

  const { stats } = data;
  const minValue = stats?.minValue ?? 0;
  const maxValue = stats?.maxValue ?? 10000;

  // Sort zones
  const sortedZones = [...data.features].sort((a, b) => {
    const aVal = a.properties.values;
    const bVal = b.properties.values;

    switch (sortBy) {
      case "value":
        return (bVal?.valueMidEurSqm ?? 0) - (aVal?.valueMidEurSqm ?? 0);
      case "change":
        return (bVal?.pctChange1s ?? -999) - (aVal?.pctChange1s ?? -999);
      default:
        return 0;
    }
  });

  const selectedZoneData = selectedZone
    ? data.features.find((f) => f.properties.zoneId === selectedZone)
    : null;

  return (
    <div className="zone-drilldown">
      {/* Header with stats */}
      <div className="zone-drilldown__header">
        <div className="zone-drilldown__stats">
          <div className="zone-stat">
            <span className="zone-stat__value">{stats?.totalZones ?? 0}</span>
            <span className="zone-stat__label">Total Zones</span>
          </div>
          <div className="zone-stat">
            <span className="zone-stat__value">
              {stats?.zonesWithData ?? 0}
            </span>
            <span className="zone-stat__label">With Data</span>
          </div>
          <div className="zone-stat">
            <span className="zone-stat__value">
              {formatCurrency(stats?.avgValue)}
            </span>
            <span className="zone-stat__label">Avg Value/m²</span>
          </div>
        </div>
        <div className="zone-drilldown__sort">
          <span className="sort-label">Sort by:</span>
          <select
            value={sortBy}
            onChange={(e) =>
              setSortBy(e.target.value as "value" | "change")
            }
            className="sort-select"
          >
            <option value="value">Value (High to Low)</option>
            <option value="change">Price Change</option>
          </select>
        </div>
      </div>

      {/* Zone Grid */}
      <div className="zone-grid">
        {sortedZones.map((zone) => {
          const { properties: p } = zone;
          const isSelected = selectedZone === p.zoneId;
          const bgColor = getValueColor(
            p.values?.valueMidEurSqm ?? null,
            minValue,
            maxValue
          );

          return (
            <button
              key={p.zoneId}
              className={`zone-card ${isSelected ? "zone-card--selected" : ""}`}
              onClick={() =>
                setSelectedZone(isSelected ? null : p.zoneId)
              }
              style={{ borderLeftColor: bgColor }}
            >
              <div className="zone-card__header">
                <span className="zone-card__code">{p.zoneCode}</span>
                <span className="zone-card__type">
                  {getZoneTypeLabel(p.zoneType)}
                </span>
              </div>
              <span className="zone-card__name">{p.zoneName || "—"}</span>
              <div className="zone-card__values">
                <span className="zone-card__price">
                  {formatCurrency(p.values?.valueMidEurSqm)}
                  <span className="zone-card__unit">/m²</span>
                </span>
                {p.values?.pctChange1s != null && (
                  <span
                    className={`zone-card__change ${
                      p.values.pctChange1s > 0
                        ? "zone-card__change--up"
                        : p.values.pctChange1s < 0
                        ? "zone-card__change--down"
                        : ""
                    }`}
                  >
                    {formatPercent(p.values.pctChange1s)}
                  </span>
                )}
              </div>
            </button>
          );
        })}
      </div>

      {/* Selected Zone Detail */}
      {selectedZoneData && (
        <div className="zone-detail">
          <div className="zone-detail__header">
            <h4 className="zone-detail__title">
              {selectedZoneData.properties.zoneCode} -{" "}
              {selectedZoneData.properties.zoneName || "Zone Details"}
            </h4>
            <button
              className="zone-detail__close"
              onClick={() => setSelectedZone(null)}
            >
              ×
            </button>
          </div>
          <div className="zone-detail__content">
            <div className="zone-detail__section">
              <h5 className="zone-detail__section-title">Zone Information</h5>
              <div className="zone-detail__grid">
                <div className="zone-detail__item">
                  <span className="zone-detail__label">Type</span>
                  <span className="zone-detail__value">
                    {getZoneTypeLabel(selectedZoneData.properties.zoneType)}
                  </span>
                </div>
                {selectedZoneData.properties.microzoneCode && (
                  <div className="zone-detail__item">
                    <span className="zone-detail__label">Microzone</span>
                    <span className="zone-detail__value">
                      {selectedZoneData.properties.microzoneCode}
                    </span>
                  </div>
                )}
              </div>
            </div>

            {selectedZoneData.properties.values && (
              <div className="zone-detail__section">
                <h5 className="zone-detail__section-title">Current Values</h5>
                <div className="zone-detail__grid">
                  <div className="zone-detail__item">
                    <span className="zone-detail__label">Value (Mid)</span>
                    <span className="zone-detail__value zone-detail__value--large">
                      {formatCurrency(
                        selectedZoneData.properties.values.valueMidEurSqm
                      )}
                      /m²
                    </span>
                  </div>
                  <div className="zone-detail__item">
                    <span className="zone-detail__label">Range</span>
                    <span className="zone-detail__value">
                      {formatCurrency(
                        selectedZoneData.properties.values.valueMinEurSqm
                      )}{" "}
                      -{" "}
                      {formatCurrency(
                        selectedZoneData.properties.values.valueMaxEurSqm
                      )}
                    </span>
                  </div>
                  {selectedZoneData.properties.values.rentMidEurSqmMonth !=
                    null && (
                    <div className="zone-detail__item">
                      <span className="zone-detail__label">Rent</span>
                      <span className="zone-detail__value">
                        {formatCurrency(
                          selectedZoneData.properties.values.rentMidEurSqmMonth
                        )}
                        /m²/month
                      </span>
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      <style jsx>{`
        .zone-drilldown {
          display: flex;
          flex-direction: column;
          gap: 24px;
        }

        .zone-drilldown__header {
          display: flex;
          justify-content: space-between;
          align-items: flex-start;
          gap: 16px;
          flex-wrap: wrap;
        }

        .zone-drilldown__stats {
          display: flex;
          gap: 24px;
        }

        .zone-stat {
          display: flex;
          flex-direction: column;
          gap: 2px;
        }

        .zone-stat__value {
          font-family: "Cormorant Garamond", Georgia, serif;
          font-size: 1.5rem;
          font-weight: 600;
          color: #f0f2f5;
        }

        .zone-stat__label {
          font-size: 0.7rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.08em;
        }

        .zone-drilldown__sort {
          display: flex;
          align-items: center;
          gap: 8px;
        }

        .sort-label {
          font-size: 0.8rem;
          color: #6b7a90;
        }

        .sort-select {
          background: rgba(22, 25, 32, 0.8);
          border: 1px solid rgba(255, 255, 255, 0.1);
          border-radius: 6px;
          color: #f0f2f5;
          font-size: 0.8rem;
          padding: 6px 12px;
          cursor: pointer;
        }

        .sort-select:focus {
          outline: none;
          border-color: rgba(196, 120, 92, 0.5);
        }

        .zone-grid {
          display: grid;
          grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
          gap: 12px;
        }

        .zone-card {
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-left: 4px solid;
          border-radius: 8px;
          padding: 16px;
          text-align: left;
          cursor: pointer;
          transition: all 0.2s;
          display: flex;
          flex-direction: column;
          gap: 8px;
        }

        .zone-card:hover {
          background: rgba(26, 29, 35, 0.9);
          border-color: rgba(255, 255, 255, 0.08);
          border-left-color: inherit;
        }

        .zone-card--selected {
          background: rgba(196, 120, 92, 0.1);
          border-color: rgba(196, 120, 92, 0.3);
        }

        .zone-card__header {
          display: flex;
          justify-content: space-between;
          align-items: center;
        }

        .zone-card__code {
          font-family: monospace;
          font-size: 0.75rem;
          font-weight: 600;
          color: #c4785c;
        }

        .zone-card__type {
          font-size: 0.65rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.06em;
          padding: 2px 6px;
          background: rgba(255, 255, 255, 0.05);
          border-radius: 4px;
        }

        .zone-card__name {
          font-size: 0.85rem;
          color: #a8b3c7;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }

        .zone-card__values {
          display: flex;
          align-items: baseline;
          gap: 8px;
        }

        .zone-card__price {
          font-family: "Cormorant Garamond", Georgia, serif;
          font-size: 1.25rem;
          font-weight: 600;
          color: #f0f2f5;
        }

        .zone-card__unit {
          font-size: 0.7rem;
          color: #6b7a90;
        }

        .zone-card__change {
          font-size: 0.75rem;
          font-weight: 500;
        }

        .zone-card__change--up {
          color: #4ade80;
        }
        .zone-card__change--down {
          color: #f87171;
        }

        .zone-card__score {
          display: flex;
          align-items: center;
          gap: 6px;
          font-size: 0.7rem;
          color: #6b7a90;
        }

        .zone-card__score-value {
          color: #c4785c;
          font-weight: 600;
        }

        /* Zone Detail Panel */
        .zone-detail {
          background: rgba(22, 25, 32, 0.8);
          border: 1px solid rgba(196, 120, 92, 0.2);
          border-radius: 12px;
          overflow: hidden;
          animation: slideIn 0.3s ease;
        }

        @keyframes slideIn {
          from {
            opacity: 0;
            transform: translateY(10px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }

        .zone-detail__header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          padding: 16px 20px;
          background: rgba(196, 120, 92, 0.1);
          border-bottom: 1px solid rgba(196, 120, 92, 0.15);
        }

        .zone-detail__title {
          font-family: "Cormorant Garamond", Georgia, serif;
          font-size: 1.1rem;
          font-weight: 600;
          color: #f0f2f5;
          margin: 0;
        }

        .zone-detail__close {
          background: none;
          border: none;
          color: #6b7a90;
          font-size: 1.5rem;
          cursor: pointer;
          padding: 0;
          line-height: 1;
          transition: color 0.2s;
        }

        .zone-detail__close:hover {
          color: #f0f2f5;
        }

        .zone-detail__content {
          padding: 20px;
          display: flex;
          flex-direction: column;
          gap: 20px;
        }

        .zone-detail__section {
          display: flex;
          flex-direction: column;
          gap: 12px;
        }

        .zone-detail__section-title {
          font-size: 0.7rem;
          font-weight: 600;
          color: #8b9bb4;
          text-transform: uppercase;
          letter-spacing: 0.08em;
          margin: 0;
        }

        .zone-detail__grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
          gap: 16px;
        }

        .zone-detail__item {
          display: flex;
          flex-direction: column;
          gap: 4px;
        }

        .zone-detail__label {
          font-size: 0.7rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.06em;
        }

        .zone-detail__value {
          font-size: 0.9rem;
          color: #f0f2f5;
        }

        .zone-detail__value--large {
          font-family: "Cormorant Garamond", Georgia, serif;
          font-size: 1.5rem;
          font-weight: 600;
        }

        @media (max-width: 640px) {
          .zone-drilldown__header {
            flex-direction: column;
          }

          .zone-drilldown__stats {
            width: 100%;
            justify-content: space-between;
          }

          .zone-drilldown__sort {
            width: 100%;
          }

          .sort-select {
            flex: 1;
          }

          .zone-grid {
            grid-template-columns: 1fr;
          }
        }
      `}</style>
    </div>
  );
}
