"use client";

import { useEffect, useState } from "react";

export interface MunicipalityData {
  municipalityId: string;
  name: string;
  provinceCode?: string;
  provinceName?: string;
  regionCode?: string;
  regionName?: string;
  coastalFlag?: boolean;
  mountainFlag?: boolean;
  // Values
  valueMidEurSqm?: number;
  valueMinEurSqm?: number;
  valueMaxEurSqm?: number;
  // Forecasts
  forecastAppreciationPct?: number;
  forecastGrossYieldPct?: number;
  opportunityScore?: number;
  confidenceScore?: number;
  // Demographics
  population?: number;
  populationDensity?: number;
  youngRatio?: number;
  elderlyRatio?: number;
  foreignRatio?: number;
  // Transactions
  ntnTotal?: number;
  ntnPer1000Pop?: number;
}

interface MunicipalityDrawerProps {
  municipality: MunicipalityData | null;
  isOpen: boolean;
  onClose: () => void;
  onAddToCompare?: (municipality: MunicipalityData) => void;
  isInCompareList?: boolean;
}

function formatNumber(value: number | undefined | null, decimals = 0): string {
  if (value == null) return "—";
  return value.toLocaleString("it-IT", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function formatPercent(value: number | undefined | null): string {
  if (value == null) return "—";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(1)}%`;
}

function formatCurrency(value: number | undefined | null): string {
  if (value == null) return "—";
  return `€${formatNumber(value)}`;
}

function ScoreRing({
  value,
  max = 100,
  size = 64,
  strokeWidth = 4,
  color = "#c4785c",
  bgColor = "rgba(255,255,255,0.06)",
}: {
  value: number | undefined | null;
  max?: number;
  size?: number;
  strokeWidth?: number;
  color?: string;
  bgColor?: string;
}) {
  const radius = (size - strokeWidth) / 2;
  const circumference = radius * 2 * Math.PI;
  const normalizedValue = value != null ? Math.min(value, max) : 0;
  const progress = (normalizedValue / max) * circumference;

  return (
    <div className="score-ring" style={{ width: size, height: size }}>
      <svg width={size} height={size}>
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke={bgColor}
          strokeWidth={strokeWidth}
        />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke={color}
          strokeWidth={strokeWidth}
          strokeDasharray={`${progress} ${circumference}`}
          strokeLinecap="round"
          transform={`rotate(-90 ${size / 2} ${size / 2})`}
          style={{ transition: "stroke-dasharray 0.6s ease" }}
        />
      </svg>
      <span className="score-ring__value">
        {value != null ? Math.round(value) : "—"}
      </span>
      <style jsx>{`
        .score-ring {
          position: relative;
          display: inline-flex;
          align-items: center;
          justify-content: center;
        }
        .score-ring__value {
          position: absolute;
          font-size: 1rem;
          font-weight: 600;
          color: #f0f2f5;
          font-variant-numeric: tabular-nums;
        }
      `}</style>
    </div>
  );
}

function StatCard({
  label,
  value,
  subValue,
  trend,
  icon,
}: {
  label: string;
  value: string;
  subValue?: string;
  trend?: "up" | "down" | "neutral";
  icon?: string;
}) {
  return (
    <div className="stat-card">
      {icon && <span className="stat-card__icon">{icon}</span>}
      <div className="stat-card__content">
        <span className="stat-card__label">{label}</span>
        <span className="stat-card__value">
          {value}
          {trend && (
            <span className={`stat-card__trend stat-card__trend--${trend}`}>
              {trend === "up" ? "↑" : trend === "down" ? "↓" : "→"}
            </span>
          )}
        </span>
        {subValue && <span className="stat-card__sub">{subValue}</span>}
      </div>
      <style jsx>{`
        .stat-card {
          display: flex;
          align-items: flex-start;
          gap: 10px;
          padding: 12px;
          background: rgba(255, 255, 255, 0.02);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 10px;
          transition: all 0.2s ease;
        }
        .stat-card:hover {
          background: rgba(255, 255, 255, 0.04);
          border-color: rgba(255, 255, 255, 0.08);
        }
        .stat-card__icon {
          font-size: 1.25rem;
          opacity: 0.6;
        }
        .stat-card__content {
          display: flex;
          flex-direction: column;
          min-width: 0;
        }
        .stat-card__label {
          font-size: 0.65rem;
          text-transform: uppercase;
          letter-spacing: 0.08em;
          color: #6b7a90;
          margin-bottom: 2px;
        }
        .stat-card__value {
          font-size: 1.1rem;
          font-weight: 600;
          color: #f0f2f5;
          display: flex;
          align-items: center;
          gap: 6px;
          font-variant-numeric: tabular-nums;
        }
        .stat-card__trend {
          font-size: 0.85rem;
          font-weight: 500;
        }
        .stat-card__trend--up {
          color: #4ade80;
        }
        .stat-card__trend--down {
          color: #f87171;
        }
        .stat-card__trend--neutral {
          color: #8b9bb4;
        }
        .stat-card__sub {
          font-size: 0.7rem;
          color: #8b9bb4;
          margin-top: 2px;
        }
      `}</style>
    </div>
  );
}

export function MunicipalityDrawer({
  municipality,
  isOpen,
  onClose,
  onAddToCompare,
  isInCompareList = false,
}: MunicipalityDrawerProps) {
  const [isVisible, setIsVisible] = useState(false);
  const [isAnimating, setIsAnimating] = useState(false);

  useEffect(() => {
    if (isOpen) {
      setIsVisible(true);
      // Use double requestAnimationFrame for reliable animation start
      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          setIsAnimating(true);
        });
      });
    } else {
      setIsAnimating(false);
      const timeout = setTimeout(() => setIsVisible(false), 300);
      return () => clearTimeout(timeout);
    }
  }, [isOpen]);

  // Don't render if not visible
  if (!isVisible) return null;

  const m = municipality;

  return (
    <div className={`drawer-overlay ${isAnimating ? "drawer-overlay--open" : ""}`}>
      <div className="drawer-backdrop" onClick={onClose} />
      <div className={`drawer ${isAnimating ? "drawer--open" : ""}`}>
        {/* Header */}
        <div className="drawer__header">
          <button onClick={onClose} className="drawer__close">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
              <path
                d="M15 5L5 15M5 5L15 15"
                stroke="currentColor"
                strokeWidth="1.5"
                strokeLinecap="round"
              />
            </svg>
          </button>

          {m && (
            <>
              <div className="drawer__header-content">
                <div className="drawer__badges">
                  {m.coastalFlag && (
                    <span className="drawer__badge drawer__badge--coastal">🌊 Coastal</span>
                  )}
                  {m.mountainFlag && (
                    <span className="drawer__badge drawer__badge--mountain">⛰️ Mountain</span>
                  )}
                </div>
                <h2 className="drawer__title">{m.name || "Unknown Municipality"}</h2>
                <p className="drawer__subtitle">
                  {[m.provinceName, m.regionName].filter(Boolean).join(", ") ||
                    `Code: ${m.municipalityId}`}
                </p>
              </div>

              {/* Key Scores */}
              <div className="drawer__scores">
                <div className="drawer__score-item">
                  <ScoreRing value={m.opportunityScore} color="#c4785c" />
                  <span className="drawer__score-label">Opportunity</span>
                </div>
                <div className="drawer__score-item">
                  <ScoreRing value={m.confidenceScore} color="#528b99" />
                  <span className="drawer__score-label">Confidence</span>
                </div>
              </div>
            </>
          )}
        </div>

        {/* Content */}
        <div className="drawer__content">
          {m ? (
            <>
              {/* Property Values Section */}
              <section className="drawer__section">
                <h3 className="drawer__section-title">
                  <span className="drawer__section-icon">€</span>
                  Property Values
                </h3>
                <div className="drawer__stats-grid">
                  <StatCard
                    label="Median Value"
                    value={formatCurrency(m.valueMidEurSqm)}
                    subValue="per m²"
                    icon="◈"
                  />
                  <StatCard
                    label="Range"
                    value={`${formatCurrency(m.valueMinEurSqm)} – ${formatCurrency(m.valueMaxEurSqm)}`}
                    subValue="min – max"
                    icon="↔"
                  />
                </div>
              </section>

              {/* Forecasts Section */}
              <section className="drawer__section">
                <h3 className="drawer__section-title">
                  <span className="drawer__section-icon">📈</span>
                  12-Month Forecast
                </h3>
                <div className="drawer__stats-grid">
                  <StatCard
                    label="Appreciation"
                    value={formatPercent(m.forecastAppreciationPct)}
                    trend={
                      m.forecastAppreciationPct != null
                        ? m.forecastAppreciationPct > 0
                          ? "up"
                          : m.forecastAppreciationPct < 0
                            ? "down"
                            : "neutral"
                        : undefined
                    }
                    icon="↗"
                  />
                  <StatCard
                    label="Gross Yield"
                    value={formatPercent(m.forecastGrossYieldPct)}
                    subValue="annual rental"
                    icon="%"
                  />
                </div>
              </section>

              {/* Demographics Section */}
              <section className="drawer__section">
                <h3 className="drawer__section-title">
                  <span className="drawer__section-icon">👥</span>
                  Demographics
                </h3>
                <div className="drawer__stats-grid drawer__stats-grid--3col">
                  <StatCard
                    label="Population"
                    value={formatNumber(m.population)}
                    subValue={
                      m.populationDensity
                        ? `${formatNumber(m.populationDensity)} /km²`
                        : undefined
                    }
                  />
                  <StatCard
                    label="Young (0-14)"
                    value={m.youngRatio != null ? `${(m.youngRatio * 100).toFixed(1)}%` : "—"}
                  />
                  <StatCard
                    label="Elderly (65+)"
                    value={m.elderlyRatio != null ? `${(m.elderlyRatio * 100).toFixed(1)}%` : "—"}
                  />
                </div>
                {m.foreignRatio != null && (
                  <div className="drawer__foreign-bar">
                    <div className="drawer__foreign-label">
                      <span>Foreign Residents</span>
                      <span>{(m.foreignRatio * 100).toFixed(1)}%</span>
                    </div>
                    <div className="drawer__foreign-track">
                      <div
                        className="drawer__foreign-fill"
                        style={{ width: `${Math.min(m.foreignRatio * 100, 100)}%` }}
                      />
                    </div>
                  </div>
                )}
              </section>

              {/* Market Activity Section */}
              <section className="drawer__section">
                <h3 className="drawer__section-title">
                  <span className="drawer__section-icon">📊</span>
                  Market Activity
                </h3>
                <div className="drawer__stats-grid">
                  <StatCard
                    label="Transactions (NTN)"
                    value={formatNumber(m.ntnTotal, 1)}
                    subValue="normalized count"
                    icon="⇄"
                  />
                  <StatCard
                    label="NTN per 1000 pop"
                    value={formatNumber(m.ntnPer1000Pop, 2)}
                    subValue="market intensity"
                    icon="⚡"
                  />
                </div>
              </section>
            </>
          ) : (
            <div className="drawer__empty">
              <span className="drawer__empty-icon">🗺️</span>
              <p>Select a municipality on the map to view details</p>
            </div>
          )}
        </div>

        {/* Footer */}
        {m && (
          <div className="drawer__footer">
            <button
              className={`drawer__action drawer__action--primary ${isInCompareList ? "drawer__action--added" : ""}`}
              onClick={() => onAddToCompare?.(m)}
              disabled={isInCompareList}
            >
              {isInCompareList ? (
                <>
                  <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                    <path
                      d="M3 8L7 12L13 4"
                      stroke="currentColor"
                      strokeWidth="1.5"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                  </svg>
                  Added to Compare
                </>
              ) : (
                <>
                  <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                    <path
                      d="M8 1V15M1 8H15"
                      stroke="currentColor"
                      strokeWidth="1.5"
                      strokeLinecap="round"
                    />
                  </svg>
                  Add to Compare
                </>
              )}
            </button>
            <a href={`/municipality/${m.municipalityId}`} className="drawer__action">
              <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                <path
                  d="M2 2H6L8 5H14V13H2V2Z"
                  stroke="currentColor"
                  strokeWidth="1.25"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
              Full Details
            </a>
          </div>
        )}
      </div>

      <style jsx>{`
        .drawer-overlay {
          position: fixed;
          top: 0;
          left: 0;
          right: 0;
          bottom: 0;
          z-index: 1100;
          pointer-events: none;
          overflow: visible;
        }

        .drawer-overlay--open {
          pointer-events: auto;
        }

        .drawer-backdrop {
          position: absolute;
          top: 0;
          left: 0;
          right: 0;
          bottom: 0;
          background: rgba(0, 0, 0, 0);
          transition: background 0.3s ease;
        }

        .drawer-overlay--open .drawer-backdrop {
          background: rgba(0, 0, 0, 0.3);
        }

        .drawer {
          position: fixed;
          top: 72px;
          right: 16px;
          bottom: 16px;
          width: 380px;
          max-width: calc(100vw - 32px);
          background: linear-gradient(165deg,
            rgba(22, 25, 32, 0.98) 0%,
            rgba(13, 15, 18, 0.99) 100%
          );
          backdrop-filter: blur(32px);
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 20px;
          box-shadow:
            0 8px 48px rgba(0, 0, 0, 0.5),
            0 2px 8px rgba(0, 0, 0, 0.3),
            inset 0 1px 0 rgba(255, 255, 255, 0.05);
          display: flex;
          flex-direction: column;
          transform: translateX(calc(100% + 32px));
          opacity: 0;
          transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1),
                      opacity 0.3s ease;
        }

        .drawer--open {
          transform: translateX(0);
          opacity: 1;
        }

        .drawer__close {
          position: absolute;
          top: 16px;
          right: 16px;
          width: 36px;
          height: 36px;
          background: rgba(255, 255, 255, 0.04);
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 10px;
          color: #8b9bb4;
          cursor: pointer;
          display: flex;
          align-items: center;
          justify-content: center;
          transition: all 0.2s ease;
          z-index: 10;
        }

        .drawer__close:hover {
          background: rgba(255, 255, 255, 0.08);
          color: #f0f2f5;
        }

        .drawer__header {
          padding: 24px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.06);
          background: linear-gradient(180deg,
            rgba(255, 255, 255, 0.02) 0%,
            transparent 100%
          );
        }

        .drawer__header-content {
          margin-bottom: 20px;
        }

        .drawer__badges {
          display: flex;
          gap: 8px;
          margin-bottom: 8px;
        }

        .drawer__badge {
          display: inline-flex;
          align-items: center;
          gap: 4px;
          padding: 4px 10px;
          font-size: 0.65rem;
          font-weight: 500;
          text-transform: uppercase;
          letter-spacing: 0.05em;
          border-radius: 20px;
        }

        .drawer__badge--coastal {
          background: rgba(82, 139, 153, 0.2);
          color: #7cc4d4;
          border: 1px solid rgba(82, 139, 153, 0.3);
        }

        .drawer__badge--mountain {
          background: rgba(139, 155, 180, 0.15);
          color: #a8b3c7;
          border: 1px solid rgba(139, 155, 180, 0.25);
        }

        .drawer__title {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 1.75rem;
          font-weight: 600;
          color: #f0f2f5;
          margin: 0 0 4px 0;
          letter-spacing: 0.01em;
          line-height: 1.2;
        }

        .drawer__subtitle {
          font-size: 0.8rem;
          color: #6b7a90;
          margin: 0;
        }

        .drawer__scores {
          display: flex;
          gap: 24px;
        }

        .drawer__score-item {
          display: flex;
          flex-direction: column;
          align-items: center;
          gap: 6px;
        }

        .drawer__score-label {
          font-size: 0.65rem;
          text-transform: uppercase;
          letter-spacing: 0.08em;
          color: #6b7a90;
        }

        .drawer__content {
          flex: 1;
          overflow-y: auto;
          padding: 0 24px;
        }

        .drawer__section {
          padding: 20px 0;
          border-bottom: 1px solid rgba(255, 255, 255, 0.04);
        }

        .drawer__section:last-child {
          border-bottom: none;
        }

        .drawer__section-title {
          display: flex;
          align-items: center;
          gap: 8px;
          font-size: 0.7rem;
          font-weight: 600;
          text-transform: uppercase;
          letter-spacing: 0.1em;
          color: #8b9bb4;
          margin: 0 0 12px 0;
        }

        .drawer__section-icon {
          font-size: 0.9rem;
        }

        .drawer__stats-grid {
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: 10px;
        }

        .drawer__stats-grid--3col {
          grid-template-columns: repeat(3, 1fr);
        }

        .drawer__foreign-bar {
          margin-top: 12px;
        }

        .drawer__foreign-label {
          display: flex;
          justify-content: space-between;
          font-size: 0.7rem;
          color: #8b9bb4;
          margin-bottom: 6px;
        }

        .drawer__foreign-track {
          height: 6px;
          background: rgba(255, 255, 255, 0.06);
          border-radius: 3px;
          overflow: hidden;
        }

        .drawer__foreign-fill {
          height: 100%;
          background: linear-gradient(90deg, #528b99 0%, #7cc4d4 100%);
          border-radius: 3px;
          transition: width 0.4s ease;
        }

        .drawer__empty {
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          height: 100%;
          text-align: center;
          color: #6b7a90;
        }

        .drawer__empty-icon {
          font-size: 3rem;
          margin-bottom: 16px;
          opacity: 0.5;
        }

        .drawer__footer {
          padding: 16px 24px;
          border-top: 1px solid rgba(255, 255, 255, 0.06);
          display: flex;
          gap: 10px;
        }

        .drawer__action {
          flex: 1;
          display: flex;
          align-items: center;
          justify-content: center;
          gap: 8px;
          padding: 12px 16px;
          font-size: 0.8rem;
          font-weight: 500;
          border-radius: 10px;
          cursor: pointer;
          transition: all 0.2s ease;
          background: rgba(255, 255, 255, 0.04);
          border: 1px solid rgba(255, 255, 255, 0.08);
          color: #a8b3c7;
        }

        .drawer__action:hover {
          background: rgba(255, 255, 255, 0.08);
          border-color: rgba(255, 255, 255, 0.12);
          color: #f0f2f5;
        }

        .drawer__action--primary {
          background: linear-gradient(135deg, #c4785c 0%, #a85d3f 100%);
          border: none;
          color: #fff;
        }

        .drawer__action--primary:hover {
          background: linear-gradient(135deg, #d4886c 0%, #b86d4f 100%);
          color: #fff;
        }

        .drawer__action--added {
          background: linear-gradient(135deg, #3d6b4f 0%, #2d5a3f 100%);
          cursor: default;
          opacity: 0.9;
        }

        .drawer__action--added:hover {
          background: linear-gradient(135deg, #3d6b4f 0%, #2d5a3f 100%);
        }
      `}</style>
    </div>
  );
}
