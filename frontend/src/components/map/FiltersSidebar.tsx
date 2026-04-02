"use client";

import { useState, useCallback } from "react";

export type MetricType =
  | "value_mid_eur_sqm"
  | "forecast_appreciation_pct"
  | "forecast_gross_yield_pct"
  | "opportunity_score"
  | "confidence_score";

export interface FiltersState {
  metric: MetricType;
  region: string | null;
  province: string | null;
  confidenceThreshold: number;
  propertySegment: "residential" | "commercial" | "industrial";
  showFlatTaxEligible: boolean;
}

interface FiltersSidebarProps {
  filters: FiltersState;
  onFiltersChange: (filters: FiltersState) => void;
  regions?: { code: string; name: string }[];
  provinces?: { code: string; name: string; regionCode: string }[];
  isCollapsed?: boolean;
  onToggleCollapse?: () => void;
}

const METRICS: { value: MetricType; label: string; icon: string; description: string }[] = [
  {
    value: "value_mid_eur_sqm",
    label: "Property Value",
    icon: "€",
    description: "Median price per m²",
  },
  {
    value: "forecast_appreciation_pct",
    label: "Appreciation",
    icon: "↗",
    description: "12-month forecast",
  },
  {
    value: "forecast_gross_yield_pct",
    label: "Rental Yield",
    icon: "%",
    description: "Gross annual yield",
  },
  {
    value: "opportunity_score",
    label: "Opportunity",
    icon: "◆",
    description: "Composite score",
  },
  {
    value: "confidence_score",
    label: "Confidence",
    icon: "●",
    description: "Data reliability",
  },
];

const SEGMENTS = [
  { value: "residential", label: "Residential", icon: "⌂" },
  { value: "commercial", label: "Commercial", icon: "◫" },
  { value: "industrial", label: "Industrial", icon: "⚙" },
] as const;

export function FiltersSidebar({
  filters,
  onFiltersChange,
  regions = [],
  provinces = [],
  isCollapsed = false,
  onToggleCollapse,
}: FiltersSidebarProps) {
  const [isAnimating, setIsAnimating] = useState(false);

  const handleMetricChange = useCallback(
    (metric: MetricType) => {
      onFiltersChange({ ...filters, metric });
    },
    [filters, onFiltersChange]
  );

  const handleRegionChange = useCallback(
    (region: string | null) => {
      onFiltersChange({ ...filters, region, province: null });
    },
    [filters, onFiltersChange]
  );

  const handleProvinceChange = useCallback(
    (province: string | null) => {
      onFiltersChange({ ...filters, province });
    },
    [filters, onFiltersChange]
  );

  const handleConfidenceChange = useCallback(
    (confidenceThreshold: number) => {
      onFiltersChange({ ...filters, confidenceThreshold });
    },
    [filters, onFiltersChange]
  );

  const handleSegmentChange = useCallback(
    (propertySegment: FiltersState["propertySegment"]) => {
      onFiltersChange({ ...filters, propertySegment });
    },
    [filters, onFiltersChange]
  );

  const filteredProvinces = filters.region
    ? provinces.filter((p) => p.regionCode === filters.region)
    : provinces;

  const handleToggle = () => {
    setIsAnimating(true);
    onToggleCollapse?.();
    setTimeout(() => setIsAnimating(false), 300);
  };

  return (
    <div
      className={`
        filters-sidebar
        ${isCollapsed ? "filters-sidebar--collapsed" : ""}
        ${isAnimating ? "filters-sidebar--animating" : ""}
      `}
    >
      {/* Collapse Toggle */}
      <button
        onClick={handleToggle}
        className="filters-sidebar__toggle"
        aria-label={isCollapsed ? "Expand filters" : "Collapse filters"}
      >
        <svg
          width="20"
          height="20"
          viewBox="0 0 20 20"
          fill="none"
          className={`transition-transform duration-300 ${isCollapsed ? "rotate-180" : ""}`}
        >
          <path
            d="M12.5 15L7.5 10L12.5 5"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </button>

      <div className="filters-sidebar__content">
        {/* Header */}
        <div className="filters-sidebar__header">
          <div className="filters-sidebar__header-icon">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none">
              <path
                d="M3 4.5H21M6 9.5H18M9 14.5H15M11 19.5H13"
                stroke="currentColor"
                strokeWidth="1.5"
                strokeLinecap="round"
              />
            </svg>
          </div>
          <div>
            <h2 className="filters-sidebar__title">Analisi</h2>
            <p className="filters-sidebar__subtitle">Configure your view</p>
          </div>
        </div>

        {/* Metric Selector */}
        <div className="filters-section">
          <label className="filters-section__label">
            <span className="filters-section__label-text">Metric</span>
            <span className="filters-section__label-ornament" />
          </label>
          <div className="metric-grid">
            {METRICS.map((m, idx) => (
              <button
                key={m.value}
                onClick={() => handleMetricChange(m.value)}
                className={`metric-card ${filters.metric === m.value ? "metric-card--active" : ""}`}
                style={{ animationDelay: `${idx * 50}ms` }}
              >
                <span className="metric-card__icon">{m.icon}</span>
                <span className="metric-card__label">{m.label}</span>
                <span className="metric-card__description">{m.description}</span>
              </button>
            ))}
          </div>
        </div>

        {/* Property Segment */}
        <div className="filters-section">
          <label className="filters-section__label">
            <span className="filters-section__label-text">Property Type</span>
            <span className="filters-section__label-ornament" />
          </label>
          <div className="segment-row">
            {SEGMENTS.map((s) => (
              <button
                key={s.value}
                onClick={() => handleSegmentChange(s.value)}
                className={`segment-btn ${filters.propertySegment === s.value ? "segment-btn--active" : ""}`}
              >
                <span className="segment-btn__icon">{s.icon}</span>
                <span className="segment-btn__label">{s.label}</span>
              </button>
            ))}
          </div>
        </div>

        {/* Region Dropdown */}
        <div className="filters-section">
          <label className="filters-section__label">
            <span className="filters-section__label-text">Region</span>
            <span className="filters-section__label-ornament" />
          </label>
          <div className="select-wrapper">
            <select
              value={filters.region || ""}
              onChange={(e) => handleRegionChange(e.target.value || null)}
              className="select-input"
            >
              <option value="">All Regions</option>
              {regions.map((r) => (
                <option key={r.code} value={r.code}>
                  {r.name}
                </option>
              ))}
            </select>
            <span className="select-arrow">
              <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                <path d="M3 4.5L6 7.5L9 4.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
              </svg>
            </span>
          </div>
        </div>

        {/* Province Dropdown */}
        <div className="filters-section">
          <label className="filters-section__label">
            <span className="filters-section__label-text">Province</span>
            <span className="filters-section__label-ornament" />
          </label>
          <div className="select-wrapper">
            <select
              value={filters.province || ""}
              onChange={(e) => handleProvinceChange(e.target.value || null)}
              className="select-input"
              disabled={!filters.region && provinces.length > 50}
            >
              <option value="">All Provinces</option>
              {filteredProvinces.map((p) => (
                <option key={p.code} value={p.code}>
                  {p.name}
                </option>
              ))}
            </select>
            <span className="select-arrow">
              <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                <path d="M3 4.5L6 7.5L9 4.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
              </svg>
            </span>
          </div>
        </div>

        {/* Confidence Slider */}
        <div className="filters-section">
          <label className="filters-section__label">
            <span className="filters-section__label-text">Minimum Confidence</span>
            <span className="filters-section__label-value">{filters.confidenceThreshold}%</span>
          </label>
          <div className="slider-wrapper">
            <input
              type="range"
              min="0"
              max="100"
              step="5"
              value={filters.confidenceThreshold}
              onChange={(e) => handleConfidenceChange(Number(e.target.value))}
              className="slider-input"
            />
            <div className="slider-track">
              <div
                className="slider-fill"
                style={{ width: `${filters.confidenceThreshold}%` }}
              />
            </div>
            <div className="slider-labels">
              <span>Low</span>
              <span>High</span>
            </div>
          </div>
        </div>

        {/* Tax Regime Overlay */}
        <div className="filters-section">
          <label className="filters-section__label">
            <span className="filters-section__label-text">Tax Overlay</span>
            <span className="filters-section__label-ornament" />
          </label>
          <button
            onClick={() => onFiltersChange({ ...filters, showFlatTaxEligible: !filters.showFlatTaxEligible })}
            className={`tax-toggle ${filters.showFlatTaxEligible ? "tax-toggle--active" : ""}`}
          >
            <span className="tax-toggle__icon">🇮🇹</span>
            <div className="tax-toggle__content">
              <span className="tax-toggle__label">7% Flat Tax Regime</span>
              <span className="tax-toggle__description">
                Highlight eligible municipalities (Southern Italy, pop. &lt;20k)
              </span>
            </div>
            <span className={`tax-toggle__switch ${filters.showFlatTaxEligible ? "tax-toggle__switch--on" : ""}`}>
              <span className="tax-toggle__switch-knob" />
            </span>
          </button>
        </div>

        {/* Footer */}
        <div className="filters-sidebar__footer">
          <button
            onClick={() =>
              onFiltersChange({
                metric: "value_mid_eur_sqm",
                region: null,
                province: null,
                confidenceThreshold: 0,
                propertySegment: "residential",
                showFlatTaxEligible: false,
              })
            }
            className="reset-btn"
          >
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
              <path
                d="M1.5 7C1.5 10.0376 3.96243 12.5 7 12.5C10.0376 12.5 12.5 10.0376 12.5 7C12.5 3.96243 10.0376 1.5 7 1.5C4.87827 1.5 3.03106 2.68047 2.09251 4.41667M2.09251 4.41667V1.5M2.09251 4.41667H5"
                stroke="currentColor"
                strokeWidth="1.25"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
            Reset Filters
          </button>
        </div>
      </div>

      <style jsx>{`
        .filters-sidebar {
          position: absolute;
          top: 16px;
          left: 16px;
          bottom: 16px;
          width: 280px;
          z-index: 1000;
          display: flex;
          transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1),
                      width 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }

        .filters-sidebar--collapsed {
          transform: translateX(-260px);
        }

        .filters-sidebar__toggle {
          position: absolute;
          right: -20px;
          top: 50%;
          transform: translateY(-50%);
          width: 40px;
          height: 56px;
          background: linear-gradient(135deg, #1a1d23 0%, #0d0f12 100%);
          border: 1px solid rgba(255, 255, 255, 0.1);
          border-left: none;
          border-radius: 0 12px 12px 0;
          color: #a8b3c7;
          cursor: pointer;
          display: flex;
          align-items: center;
          justify-content: center;
          transition: all 0.2s ease;
          z-index: 10;
          box-shadow: 4px 0 12px rgba(0, 0, 0, 0.3);
        }

        .filters-sidebar--collapsed .filters-sidebar__toggle {
          background: linear-gradient(135deg, #c4785c 0%, #a85d3f 100%);
          border-color: rgba(196, 120, 92, 0.5);
          color: #fff;
          box-shadow: 4px 0 20px rgba(196, 120, 92, 0.3);
        }

        .filters-sidebar__toggle:hover {
          background: linear-gradient(135deg, #22262e 0%, #13161b 100%);
          color: #e8c4a0;
        }

        .filters-sidebar--collapsed .filters-sidebar__toggle:hover {
          background: linear-gradient(135deg, #d4886c 0%, #b86d4f 100%);
          color: #fff;
        }

        .filters-sidebar__content {
          flex: 1;
          background: linear-gradient(165deg,
            rgba(22, 25, 32, 0.97) 0%,
            rgba(13, 15, 18, 0.98) 100%
          );
          backdrop-filter: blur(24px);
          border: 1px solid rgba(255, 255, 255, 0.06);
          border-radius: 16px;
          box-shadow:
            0 4px 24px rgba(0, 0, 0, 0.4),
            0 1px 2px rgba(0, 0, 0, 0.2),
            inset 0 1px 0 rgba(255, 255, 255, 0.04);
          overflow-y: auto;
          overflow-x: hidden;
          display: flex;
          flex-direction: column;
        }

        .filters-sidebar__header {
          padding: 20px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.06);
          display: flex;
          align-items: center;
          gap: 12px;
          background: linear-gradient(180deg,
            rgba(255, 255, 255, 0.02) 0%,
            transparent 100%
          );
        }

        .filters-sidebar__header-icon {
          width: 36px;
          height: 36px;
          background: linear-gradient(135deg, #c4785c 0%, #a85d3f 100%);
          border-radius: 10px;
          display: flex;
          align-items: center;
          justify-content: center;
          color: #fff;
          box-shadow: 0 2px 8px rgba(168, 93, 63, 0.3);
        }

        .filters-sidebar__title {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 1.25rem;
          font-weight: 600;
          color: #f0f2f5;
          letter-spacing: 0.02em;
          margin: 0;
        }

        .filters-sidebar__subtitle {
          font-size: 0.7rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.1em;
          margin: 2px 0 0 0;
        }

        .filters-section {
          padding: 16px 20px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.04);
        }

        .filters-section:last-of-type {
          border-bottom: none;
        }

        .filters-section__label {
          display: flex;
          align-items: center;
          justify-content: space-between;
          margin-bottom: 12px;
        }

        .filters-section__label-text {
          font-size: 0.65rem;
          font-weight: 600;
          text-transform: uppercase;
          letter-spacing: 0.12em;
          color: #8b9bb4;
        }

        .filters-section__label-value {
          font-size: 0.75rem;
          font-weight: 500;
          color: #e8c4a0;
          font-variant-numeric: tabular-nums;
        }

        .filters-section__label-ornament {
          flex: 1;
          height: 1px;
          background: linear-gradient(90deg,
            rgba(139, 155, 180, 0.3) 0%,
            transparent 100%
          );
          margin-left: 12px;
        }

        .metric-grid {
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: 8px;
        }

        .metric-card {
          background: rgba(255, 255, 255, 0.02);
          border: 1px solid rgba(255, 255, 255, 0.06);
          border-radius: 10px;
          padding: 12px 10px;
          cursor: pointer;
          transition: all 0.2s ease;
          display: flex;
          flex-direction: column;
          align-items: flex-start;
          text-align: left;
          animation: fadeSlideIn 0.4s ease backwards;
        }

        @keyframes fadeSlideIn {
          from {
            opacity: 0;
            transform: translateY(8px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }

        .metric-card:hover {
          background: rgba(255, 255, 255, 0.04);
          border-color: rgba(255, 255, 255, 0.1);
          transform: translateY(-1px);
        }

        .metric-card--active {
          background: linear-gradient(135deg,
            rgba(196, 120, 92, 0.15) 0%,
            rgba(168, 93, 63, 0.1) 100%
          );
          border-color: rgba(196, 120, 92, 0.4);
          box-shadow: 0 0 20px rgba(196, 120, 92, 0.1);
        }

        .metric-card__icon {
          font-size: 1.1rem;
          color: #c4785c;
          margin-bottom: 6px;
          opacity: 0.8;
        }

        .metric-card--active .metric-card__icon {
          opacity: 1;
        }

        .metric-card__label {
          font-size: 0.75rem;
          font-weight: 600;
          color: #d0d7e2;
          margin-bottom: 2px;
        }

        .metric-card__description {
          font-size: 0.65rem;
          color: #6b7a90;
        }

        .segment-row {
          display: flex;
          gap: 6px;
        }

        .segment-btn {
          flex: 1;
          background: rgba(255, 255, 255, 0.02);
          border: 1px solid rgba(255, 255, 255, 0.06);
          border-radius: 8px;
          padding: 10px 8px;
          cursor: pointer;
          transition: all 0.2s ease;
          display: flex;
          flex-direction: column;
          align-items: center;
          gap: 4px;
        }

        .segment-btn:hover {
          background: rgba(255, 255, 255, 0.04);
          border-color: rgba(255, 255, 255, 0.1);
        }

        .segment-btn--active {
          background: linear-gradient(135deg,
            rgba(82, 139, 153, 0.2) 0%,
            rgba(62, 107, 119, 0.15) 100%
          );
          border-color: rgba(82, 139, 153, 0.5);
        }

        .segment-btn__icon {
          font-size: 1rem;
          color: #528b99;
          opacity: 0.7;
        }

        .segment-btn--active .segment-btn__icon {
          opacity: 1;
        }

        .segment-btn__label {
          font-size: 0.65rem;
          color: #a8b3c7;
          font-weight: 500;
        }

        .select-wrapper {
          position: relative;
        }

        .select-input {
          width: 100%;
          appearance: none;
          background: rgba(0, 0, 0, 0.2);
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 8px;
          padding: 10px 32px 10px 12px;
          font-size: 0.8rem;
          color: #d0d7e2;
          cursor: pointer;
          transition: all 0.2s ease;
        }

        .select-input:hover {
          border-color: rgba(255, 255, 255, 0.15);
        }

        .select-input:focus {
          outline: none;
          border-color: rgba(196, 120, 92, 0.5);
          box-shadow: 0 0 0 3px rgba(196, 120, 92, 0.1);
        }

        .select-input:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }

        .select-input option {
          background: #1a1d23;
          color: #d0d7e2;
        }

        .select-arrow {
          position: absolute;
          right: 12px;
          top: 50%;
          transform: translateY(-50%);
          color: #6b7a90;
          pointer-events: none;
        }

        .slider-wrapper {
          position: relative;
          padding-top: 4px;
        }

        .slider-input {
          width: 100%;
          height: 24px;
          appearance: none;
          background: transparent;
          cursor: pointer;
          position: relative;
          z-index: 2;
        }

        .slider-input::-webkit-slider-thumb {
          appearance: none;
          width: 18px;
          height: 18px;
          background: linear-gradient(135deg, #e8c4a0 0%, #c4785c 100%);
          border-radius: 50%;
          cursor: pointer;
          box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
          transition: transform 0.15s ease;
        }

        .slider-input::-webkit-slider-thumb:hover {
          transform: scale(1.1);
        }

        .slider-track {
          position: absolute;
          top: 50%;
          left: 0;
          right: 0;
          height: 4px;
          transform: translateY(-50%);
          background: rgba(255, 255, 255, 0.08);
          border-radius: 2px;
          overflow: hidden;
        }

        .slider-fill {
          height: 100%;
          background: linear-gradient(90deg, #c4785c 0%, #e8c4a0 100%);
          border-radius: 2px;
          transition: width 0.1s ease;
        }

        .slider-labels {
          display: flex;
          justify-content: space-between;
          margin-top: 6px;
          font-size: 0.6rem;
          color: #5a6677;
          text-transform: uppercase;
          letter-spacing: 0.05em;
        }

        .filters-sidebar__footer {
          margin-top: auto;
          padding: 16px 20px;
          border-top: 1px solid rgba(255, 255, 255, 0.04);
        }

        .reset-btn {
          width: 100%;
          display: flex;
          align-items: center;
          justify-content: center;
          gap: 8px;
          padding: 10px 16px;
          background: transparent;
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 8px;
          color: #8b9bb4;
          font-size: 0.75rem;
          font-weight: 500;
          cursor: pointer;
          transition: all 0.2s ease;
        }

        .reset-btn:hover {
          background: rgba(255, 255, 255, 0.04);
          border-color: rgba(255, 255, 255, 0.12);
          color: #d0d7e2;
        }

        .tax-toggle {
          width: 100%;
          display: flex;
          align-items: center;
          gap: 12px;
          padding: 12px;
          background: rgba(255, 255, 255, 0.02);
          border: 1px solid rgba(255, 255, 255, 0.06);
          border-radius: 10px;
          cursor: pointer;
          transition: all 0.2s ease;
          text-align: left;
        }

        .tax-toggle:hover {
          background: rgba(255, 255, 255, 0.04);
          border-color: rgba(255, 255, 255, 0.1);
        }

        .tax-toggle--active {
          background: linear-gradient(135deg,
            rgba(34, 197, 94, 0.15) 0%,
            rgba(22, 163, 74, 0.1) 100%
          );
          border-color: rgba(34, 197, 94, 0.4);
        }

        .tax-toggle__icon {
          font-size: 1.5rem;
        }

        .tax-toggle__content {
          flex: 1;
          display: flex;
          flex-direction: column;
          gap: 2px;
        }

        .tax-toggle__label {
          font-size: 0.8rem;
          font-weight: 600;
          color: #d0d7e2;
        }

        .tax-toggle__description {
          font-size: 0.65rem;
          color: #6b7a90;
        }

        .tax-toggle__switch {
          width: 40px;
          height: 22px;
          background: rgba(255, 255, 255, 0.1);
          border-radius: 11px;
          position: relative;
          transition: background 0.2s ease;
        }

        .tax-toggle__switch--on {
          background: linear-gradient(135deg, #22c55e 0%, #16a34a 100%);
        }

        .tax-toggle__switch-knob {
          position: absolute;
          top: 2px;
          left: 2px;
          width: 18px;
          height: 18px;
          background: white;
          border-radius: 50%;
          transition: transform 0.2s ease;
          box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
        }

        .tax-toggle__switch--on .tax-toggle__switch-knob {
          transform: translateX(18px);
        }
      `}</style>
    </div>
  );
}
