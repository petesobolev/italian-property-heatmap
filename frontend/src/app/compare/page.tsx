"use client";

import { useEffect, useState, Suspense } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";

interface ComparisonData {
  municipalities: Array<{
    id: string;
    name: string;
    regionCode: string;
    provinceCode: string;
    isCoastal: boolean;
    isMountain: boolean;
    areaSqKm: number | null;
    forecast: {
      date: string;
      valueMidEurSqm: number | null;
      appreciationPct: number | null;
      grossYieldPct: number | null;
      opportunityScore: number | null;
      confidenceScore: number | null;
      drivers: Array<{ factor: string; direction: string; strength: number }>;
      risks: Array<{ factor: string; severity: string }>;
    } | null;
    historicalValues: Array<{
      periodId: string;
      valueMidEurSqm: number | null;
      rentMidEurSqmMonth: number | null;
    }>;
    demographics: {
      year: number;
      totalPopulation: number | null;
      populationDensity: number | null;
      youngRatio: number | null;
      elderlyRatio: number | null;
      foreignRatio: number | null;
      populationGrowthRate: number | null;
    } | null;
  }>;
  metrics: {
    valueMidEurSqm: {
      values: (number | null)[];
      min: number;
      max: number;
      avg: number;
    };
    appreciationPct: {
      values: (number | null)[];
      best: string;
    };
    grossYieldPct: {
      values: (number | null)[];
      best: string;
    };
    opportunityScore: {
      values: (number | null)[];
      best: string;
    };
  };
  meta: {
    segment: string;
    horizonMonths: number;
    comparedAt: string;
  };
}

interface MunicipalityOption {
  id: string;
  name: string;
  provinceCode: string;
}

function formatCurrency(value: number | null | undefined): string {
  if (value == null) return "—";
  return `€${Math.round(value).toLocaleString("it-IT")}`;
}

function formatPercent(value: number | null | undefined, showSign = true): string {
  if (value == null) return "—";
  const sign = showSign && value > 0 ? "+" : "";
  return `${sign}${value.toFixed(1)}%`;
}

function formatNumber(value: number | null | undefined, decimals = 0): string {
  if (value == null) return "—";
  return value.toLocaleString("it-IT", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

// Radial comparison chart for a single metric
function RadialCompare({
  values,
  labels,
  colors,
  unit,
  title,
}: {
  values: (number | null)[];
  labels: string[];
  colors: string[];
  unit: string;
  title: string;
}) {
  const validValues = values.filter((v): v is number => v != null);
  const maxValue = Math.max(...validValues, 1);
  const size = 200;
  const center = size / 2;
  const maxRadius = 80;
  const minRadius = 30;

  return (
    <div className="radial-compare">
      <h4 className="radial-compare__title">{title}</h4>
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
        <defs>
          {colors.map((color, i) => (
            <linearGradient key={i} id={`radial-grad-${i}`} x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stopColor={color} stopOpacity="0.8" />
              <stop offset="100%" stopColor={color} stopOpacity="0.4" />
            </linearGradient>
          ))}
        </defs>
        {/* Background rings */}
        {[0.25, 0.5, 0.75, 1].map((ratio, i) => (
          <circle
            key={i}
            cx={center}
            cy={center}
            r={minRadius + (maxRadius - minRadius) * ratio}
            fill="none"
            stroke="rgba(255,255,255,0.04)"
            strokeWidth="1"
          />
        ))}
        {/* Data arcs */}
        {values.map((value, i) => {
          if (value == null) return null;
          const angleStep = (2 * Math.PI) / values.length;
          const startAngle = i * angleStep - Math.PI / 2;
          const endAngle = startAngle + angleStep * 0.8;
          const radius = minRadius + ((value / maxValue) * (maxRadius - minRadius));

          const x1 = center + Math.cos(startAngle) * minRadius;
          const y1 = center + Math.sin(startAngle) * minRadius;
          const x2 = center + Math.cos(startAngle) * radius;
          const y2 = center + Math.sin(startAngle) * radius;
          const x3 = center + Math.cos(endAngle) * radius;
          const y3 = center + Math.sin(endAngle) * radius;
          const x4 = center + Math.cos(endAngle) * minRadius;
          const y4 = center + Math.sin(endAngle) * minRadius;

          const largeArc = endAngle - startAngle > Math.PI ? 1 : 0;

          const path = `
            M ${x1} ${y1}
            L ${x2} ${y2}
            A ${radius} ${radius} 0 ${largeArc} 1 ${x3} ${y3}
            L ${x4} ${y4}
            A ${minRadius} ${minRadius} 0 ${largeArc} 0 ${x1} ${y1}
          `;

          return (
            <path
              key={i}
              d={path}
              fill={`url(#radial-grad-${i})`}
              stroke={colors[i]}
              strokeWidth="1"
              opacity="0.9"
              className="radial-compare__arc"
              style={{ animationDelay: `${i * 100}ms` }}
            />
          );
        })}
        <text x={center} y={center - 4} textAnchor="middle" fill="#6b7a90" fontSize="10">
          {unit}
        </text>
      </svg>
      <div className="radial-compare__legend">
        {labels.map((label, i) => (
          <div key={i} className="radial-compare__legend-item">
            <span className="radial-compare__dot" style={{ background: colors[i] }} />
            <span className="radial-compare__label">{label}</span>
            <span className="radial-compare__value" style={{ color: colors[i] }}>
              {values[i] != null ? (unit.includes("€") ? formatCurrency(values[i]) : formatPercent(values[i], true)) : "—"}
            </span>
          </div>
        ))}
      </div>
      <style jsx>{`
        .radial-compare {
          padding: 24px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 16px;
          display: flex;
          flex-direction: column;
          align-items: center;
          gap: 16px;
        }
        .radial-compare__title {
          font-size: 0.75rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.1em;
          margin: 0;
          font-weight: 600;
        }
        .radial-compare__arc {
          opacity: 0;
          animation: fadeInArc 0.6s ease forwards;
        }
        @keyframes fadeInArc {
          from { opacity: 0; transform-origin: center; transform: scale(0.8); }
          to { opacity: 0.9; transform: scale(1); }
        }
        .radial-compare__legend {
          display: flex;
          flex-direction: column;
          gap: 8px;
          width: 100%;
        }
        .radial-compare__legend-item {
          display: flex;
          align-items: center;
          gap: 10px;
          padding: 8px 12px;
          background: rgba(255, 255, 255, 0.02);
          border-radius: 8px;
        }
        .radial-compare__dot {
          width: 10px;
          height: 10px;
          border-radius: 50%;
          flex-shrink: 0;
        }
        .radial-compare__label {
          flex: 1;
          font-size: 0.8rem;
          color: #a8b3c7;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        .radial-compare__value {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 1rem;
          font-weight: 600;
        }
      `}</style>
    </div>
  );
}

// Horizontal bar comparison
function BarCompare({
  values,
  labels,
  colors,
  title,
  formatFn,
  highlightBest = true,
}: {
  values: (number | null)[];
  labels: string[];
  colors: string[];
  title: string;
  formatFn: (v: number | null) => string;
  highlightBest?: boolean;
}) {
  const validValues = values.filter((v): v is number => v != null);
  const maxValue = Math.max(...validValues, 1);
  const bestIdx = highlightBest ? values.indexOf(Math.max(...validValues)) : -1;

  return (
    <div className="bar-compare">
      <h4 className="bar-compare__title">{title}</h4>
      <div className="bar-compare__bars">
        {values.map((value, i) => {
          const pct = value != null ? (value / maxValue) * 100 : 0;
          const isBest = i === bestIdx;
          return (
            <div key={i} className={`bar-compare__row ${isBest ? "bar-compare__row--best" : ""}`}>
              <span className="bar-compare__label">{labels[i]}</span>
              <div className="bar-compare__track">
                <div
                  className="bar-compare__fill"
                  style={{
                    width: `${pct}%`,
                    background: `linear-gradient(90deg, ${colors[i]} 0%, ${colors[i]}99 100%)`,
                    animationDelay: `${i * 100}ms`,
                  }}
                />
              </div>
              <span className="bar-compare__value" style={{ color: isBest ? colors[i] : "#a8b3c7" }}>
                {formatFn(value)}
              </span>
              {isBest && <span className="bar-compare__badge">Best</span>}
            </div>
          );
        })}
      </div>
      <style jsx>{`
        .bar-compare {
          padding: 24px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 16px;
        }
        .bar-compare__title {
          font-size: 0.75rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.1em;
          margin: 0 0 20px;
          font-weight: 600;
        }
        .bar-compare__bars {
          display: flex;
          flex-direction: column;
          gap: 16px;
        }
        .bar-compare__row {
          display: grid;
          grid-template-columns: 120px 1fr 80px auto;
          align-items: center;
          gap: 16px;
        }
        .bar-compare__row--best {
          transform: scale(1.02);
        }
        .bar-compare__label {
          font-size: 0.85rem;
          color: #8b9bb4;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        .bar-compare__track {
          height: 12px;
          background: rgba(255, 255, 255, 0.04);
          border-radius: 6px;
          overflow: hidden;
        }
        .bar-compare__fill {
          height: 100%;
          border-radius: 6px;
          animation: growBar 0.8s ease forwards;
          transform-origin: left;
        }
        @keyframes growBar {
          from { transform: scaleX(0); }
          to { transform: scaleX(1); }
        }
        .bar-compare__value {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 1.1rem;
          font-weight: 600;
          text-align: right;
        }
        .bar-compare__badge {
          padding: 3px 8px;
          font-size: 0.6rem;
          font-weight: 600;
          text-transform: uppercase;
          letter-spacing: 0.1em;
          color: #4ade80;
          background: rgba(74, 222, 128, 0.1);
          border: 1px solid rgba(74, 222, 128, 0.2);
          border-radius: 10px;
        }
        @media (max-width: 640px) {
          .bar-compare__row {
            grid-template-columns: 80px 1fr 60px;
          }
          .bar-compare__badge {
            display: none;
          }
        }
      `}</style>
    </div>
  );
}

// Line chart for historical comparison
function HistoryChart({
  series,
  labels,
  colors,
}: {
  series: Array<Array<{ periodId: string; value: number | null }>>;
  labels: string[];
  colors: string[];
}) {
  // Find all unique periods
  const allPeriods = [...new Set(series.flatMap((s) => s.map((d) => d.periodId)))].sort();

  if (allPeriods.length < 2) {
    return (
      <div className="history-chart history-chart--empty">
        <span>Insufficient historical data</span>
        <style jsx>{`
          .history-chart--empty {
            padding: 48px;
            text-align: center;
            color: #5a6677;
            background: rgba(22, 25, 32, 0.6);
            border: 1px solid rgba(255, 255, 255, 0.04);
            border-radius: 16px;
          }
        `}</style>
      </div>
    );
  }

  const width = 600;
  const height = 250;
  const padding = { top: 20, right: 20, bottom: 40, left: 60 };
  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;

  // Get min/max values across all series
  const allValues = series.flatMap((s) => s.map((d) => d.value).filter((v): v is number => v != null));
  const minVal = Math.min(...allValues) * 0.95;
  const maxVal = Math.max(...allValues) * 1.05;
  const valueRange = maxVal - minVal || 1;

  return (
    <div className="history-chart">
      <h4 className="history-chart__title">Price History Comparison</h4>
      <svg viewBox={`0 0 ${width} ${height}`} className="history-chart__svg">
        <defs>
          {colors.map((color, i) => (
            <linearGradient key={i} id={`line-grad-${i}`} x1="0%" y1="0%" x2="0%" y2="100%">
              <stop offset="0%" stopColor={color} stopOpacity="0.3" />
              <stop offset="100%" stopColor={color} stopOpacity="0" />
            </linearGradient>
          ))}
        </defs>

        {/* Grid lines */}
        {[0, 0.25, 0.5, 0.75, 1].map((ratio, i) => {
          const y = padding.top + chartHeight * (1 - ratio);
          const val = minVal + valueRange * ratio;
          return (
            <g key={i}>
              <line
                x1={padding.left}
                y1={y}
                x2={padding.left + chartWidth}
                y2={y}
                stroke="rgba(255,255,255,0.04)"
              />
              <text x={padding.left - 8} y={y + 4} textAnchor="end" fill="#5a6677" fontSize="10">
                €{Math.round(val).toLocaleString()}
              </text>
            </g>
          );
        })}

        {/* X-axis labels */}
        {allPeriods.filter((_, i) => i % Math.ceil(allPeriods.length / 6) === 0).map((period, i, arr) => {
          const x = padding.left + (allPeriods.indexOf(period) / (allPeriods.length - 1)) * chartWidth;
          return (
            <text
              key={i}
              x={x}
              y={height - 10}
              textAnchor="middle"
              fill="#5a6677"
              fontSize="10"
            >
              {period}
            </text>
          );
        })}

        {/* Data lines */}
        {series.map((data, seriesIdx) => {
          const points: string[] = [];
          const areaPoints: string[] = [];

          allPeriods.forEach((period, i) => {
            const dataPoint = data.find((d) => d.periodId === period);
            if (dataPoint?.value != null) {
              const x = padding.left + (i / (allPeriods.length - 1)) * chartWidth;
              const y = padding.top + chartHeight * (1 - (dataPoint.value - minVal) / valueRange);
              points.push(`${x},${y}`);
              areaPoints.push(`${x},${y}`);
            }
          });

          if (points.length < 2) return null;

          const linePath = `M ${points.join(" L ")}`;
          const firstX = parseFloat(points[0].split(",")[0]);
          const lastX = parseFloat(points[points.length - 1].split(",")[0]);
          const areaPath = `${linePath} L ${lastX},${padding.top + chartHeight} L ${firstX},${padding.top + chartHeight} Z`;

          return (
            <g key={seriesIdx}>
              <path d={areaPath} fill={`url(#line-grad-${seriesIdx})`} />
              <path
                d={linePath}
                fill="none"
                stroke={colors[seriesIdx]}
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
                className="history-chart__line"
                style={{ animationDelay: `${seriesIdx * 150}ms` }}
              />
              {/* End point */}
              {points.length > 0 && (
                <circle
                  cx={parseFloat(points[points.length - 1].split(",")[0])}
                  cy={parseFloat(points[points.length - 1].split(",")[1])}
                  r="5"
                  fill={colors[seriesIdx]}
                  stroke="#0d0f12"
                  strokeWidth="2"
                />
              )}
            </g>
          );
        })}
      </svg>

      <div className="history-chart__legend">
        {labels.map((label, i) => (
          <div key={i} className="history-chart__legend-item">
            <span className="history-chart__legend-line" style={{ background: colors[i] }} />
            <span>{label}</span>
          </div>
        ))}
      </div>

      <style jsx>{`
        .history-chart {
          padding: 24px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 16px;
        }
        .history-chart__title {
          font-size: 0.75rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.1em;
          margin: 0 0 16px;
          font-weight: 600;
        }
        .history-chart__svg {
          width: 100%;
          height: auto;
        }
        .history-chart__line {
          stroke-dasharray: 1000;
          stroke-dashoffset: 1000;
          animation: drawLine 1.5s ease forwards;
        }
        @keyframes drawLine {
          to { stroke-dashoffset: 0; }
        }
        .history-chart__legend {
          display: flex;
          gap: 24px;
          justify-content: center;
          margin-top: 16px;
          flex-wrap: wrap;
        }
        .history-chart__legend-item {
          display: flex;
          align-items: center;
          gap: 8px;
          font-size: 0.8rem;
          color: #8b9bb4;
        }
        .history-chart__legend-line {
          width: 20px;
          height: 3px;
          border-radius: 2px;
        }
      `}</style>
    </div>
  );
}

// Municipality selector with search
function MunicipalitySelector({
  selected,
  onSelect,
  onRemove,
  maxSelections = 5,
}: {
  selected: MunicipalityOption[];
  onSelect: (m: MunicipalityOption) => void;
  onRemove: (id: string) => void;
  maxSelections?: number;
}) {
  const [search, setSearch] = useState("");
  const [options, setOptions] = useState<MunicipalityOption[]>([]);
  const [loading, setLoading] = useState(false);
  const [isOpen, setIsOpen] = useState(false);

  useEffect(() => {
    if (search.length < 2) {
      setOptions([]);
      return;
    }

    const controller = new AbortController();
    setLoading(true);

    fetch(`/api/municipality/search?q=${encodeURIComponent(search)}&limit=10`, {
      signal: controller.signal,
    })
      .then((res) => res.json())
      .then((data) => {
        setOptions(
          (data.results || []).filter(
            (m: MunicipalityOption) => !selected.some((s) => s.id === m.id)
          )
        );
        setLoading(false);
      })
      .catch(() => setLoading(false));

    return () => controller.abort();
  }, [search, selected]);

  return (
    <div className="selector">
      <div className="selector__selected">
        {selected.map((m, i) => (
          <div
            key={m.id}
            className="selector__chip"
            style={{ animationDelay: `${i * 50}ms` }}
          >
            <span className="selector__chip-name">{m.name}</span>
            <button
              className="selector__chip-remove"
              onClick={() => onRemove(m.id)}
              aria-label={`Remove ${m.name}`}
            >
              ×
            </button>
          </div>
        ))}
        {selected.length < maxSelections && (
          <div className="selector__input-wrap">
            <input
              type="text"
              className="selector__input"
              placeholder={selected.length === 0 ? "Search for a municipality..." : "Add another..."}
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              onFocus={() => setIsOpen(true)}
              onBlur={() => setTimeout(() => setIsOpen(false), 200)}
            />
            {loading && <span className="selector__loading" />}
          </div>
        )}
      </div>

      {isOpen && options.length > 0 && (
        <div className="selector__dropdown">
          {options.map((m) => (
            <button
              key={m.id}
              className="selector__option"
              onClick={() => {
                onSelect(m);
                setSearch("");
              }}
            >
              <span className="selector__option-name">{m.name}</span>
              <span className="selector__option-province">{m.provinceCode}</span>
            </button>
          ))}
        </div>
      )}

      <div className="selector__hint">
        {selected.length < 2
          ? `Select at least ${2 - selected.length} more to compare`
          : selected.length < maxSelections
          ? `You can add ${maxSelections - selected.length} more`
          : "Maximum selections reached"}
      </div>

      <style jsx>{`
        .selector {
          position: relative;
          margin-bottom: 32px;
        }
        .selector__selected {
          display: flex;
          flex-wrap: wrap;
          gap: 10px;
          padding: 16px;
          background: rgba(22, 25, 32, 0.8);
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 12px;
          min-height: 60px;
        }
        .selector__chip {
          display: flex;
          align-items: center;
          gap: 8px;
          padding: 8px 12px;
          background: linear-gradient(135deg, rgba(196, 120, 92, 0.2) 0%, rgba(196, 120, 92, 0.1) 100%);
          border: 1px solid rgba(196, 120, 92, 0.3);
          border-radius: 20px;
          animation: chipIn 0.3s ease forwards;
          opacity: 0;
        }
        @keyframes chipIn {
          from { opacity: 0; transform: scale(0.8); }
          to { opacity: 1; transform: scale(1); }
        }
        .selector__chip-name {
          font-size: 0.85rem;
          font-weight: 500;
          color: #e8c4a0;
        }
        .selector__chip-remove {
          display: flex;
          align-items: center;
          justify-content: center;
          width: 18px;
          height: 18px;
          padding: 0;
          border: none;
          background: rgba(255, 255, 255, 0.1);
          color: #8b9bb4;
          border-radius: 50%;
          cursor: pointer;
          font-size: 1rem;
          line-height: 1;
          transition: all 0.2s;
        }
        .selector__chip-remove:hover {
          background: rgba(248, 113, 113, 0.3);
          color: #f87171;
        }
        .selector__input-wrap {
          flex: 1;
          min-width: 200px;
          position: relative;
        }
        .selector__input {
          width: 100%;
          padding: 8px 12px;
          border: none;
          background: transparent;
          color: #f0f2f5;
          font-size: 0.9rem;
          outline: none;
        }
        .selector__input::placeholder {
          color: #5a6677;
        }
        .selector__loading {
          position: absolute;
          right: 12px;
          top: 50%;
          transform: translateY(-50%);
          width: 16px;
          height: 16px;
          border: 2px solid rgba(196, 120, 92, 0.2);
          border-top-color: #c4785c;
          border-radius: 50%;
          animation: spin 0.8s linear infinite;
        }
        @keyframes spin {
          to { transform: translateY(-50%) rotate(360deg); }
        }
        .selector__dropdown {
          position: absolute;
          top: 100%;
          left: 0;
          right: 0;
          margin-top: 4px;
          background: #1a1d23;
          border: 1px solid rgba(255, 255, 255, 0.1);
          border-radius: 12px;
          overflow: hidden;
          box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
          z-index: 100;
        }
        .selector__option {
          display: flex;
          justify-content: space-between;
          align-items: center;
          width: 100%;
          padding: 14px 16px;
          border: none;
          background: transparent;
          color: #f0f2f5;
          cursor: pointer;
          transition: all 0.15s;
          text-align: left;
        }
        .selector__option:hover {
          background: rgba(196, 120, 92, 0.15);
        }
        .selector__option-name {
          font-size: 0.9rem;
        }
        .selector__option-province {
          font-size: 0.75rem;
          color: #6b7a90;
          text-transform: uppercase;
        }
        .selector__hint {
          margin-top: 8px;
          font-size: 0.75rem;
          color: #5a6677;
          text-align: center;
        }
      `}</style>
    </div>
  );
}

// Demographics comparison table
function DemographicsTable({
  municipalities,
  colors,
}: {
  municipalities: ComparisonData["municipalities"];
  colors: string[];
}) {
  const metrics = [
    { key: "totalPopulation", label: "Population", format: (v: number | null) => formatNumber(v) },
    { key: "populationDensity", label: "Density (per km²)", format: (v: number | null) => formatNumber(v, 1) },
    { key: "youngRatio", label: "Young (0-14)", format: (v: number | null) => v != null ? `${(v * 100).toFixed(1)}%` : "—" },
    { key: "elderlyRatio", label: "Elderly (65+)", format: (v: number | null) => v != null ? `${(v * 100).toFixed(1)}%` : "—" },
    { key: "foreignRatio", label: "Foreign Residents", format: (v: number | null) => v != null ? `${(v * 100).toFixed(1)}%` : "—" },
    { key: "populationGrowthRate", label: "Growth Rate", format: (v: number | null) => formatPercent(v, true) },
  ];

  return (
    <div className="demo-table">
      <h4 className="demo-table__title">Demographics Comparison</h4>
      <div className="demo-table__scroll">
        <table className="demo-table__table">
          <thead>
            <tr>
              <th></th>
              {municipalities.map((m, i) => (
                <th key={m.id} style={{ borderBottomColor: colors[i] }}>
                  {m.name}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {metrics.map((metric) => (
              <tr key={metric.key}>
                <td className="demo-table__metric">{metric.label}</td>
                {municipalities.map((m) => (
                  <td key={m.id}>
                    {m.demographics
                      ? metric.format((m.demographics as Record<string, number | null>)[metric.key])
                      : "—"}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <style jsx>{`
        .demo-table {
          padding: 24px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 16px;
        }
        .demo-table__title {
          font-size: 0.75rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.1em;
          margin: 0 0 16px;
          font-weight: 600;
        }
        .demo-table__scroll {
          overflow-x: auto;
        }
        .demo-table__table {
          width: 100%;
          border-collapse: collapse;
        }
        .demo-table__table th {
          padding: 12px 16px;
          text-align: center;
          font-size: 0.85rem;
          font-weight: 600;
          color: #f0f2f5;
          border-bottom: 3px solid;
          white-space: nowrap;
        }
        .demo-table__table td {
          padding: 12px 16px;
          text-align: center;
          font-size: 0.9rem;
          color: #a8b3c7;
          border-bottom: 1px solid rgba(255, 255, 255, 0.04);
        }
        .demo-table__metric {
          text-align: left !important;
          color: #6b7a90 !important;
          font-size: 0.8rem !important;
        }
        .demo-table__table tbody tr:hover td {
          background: rgba(255, 255, 255, 0.02);
        }
      `}</style>
    </div>
  );
}

function ComparePageContent() {
  const searchParams = useSearchParams();

  const [selected, setSelected] = useState<MunicipalityOption[]>([]);
  const [data, setData] = useState<ComparisonData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [initialized, setInitialized] = useState(false);

  // Color palette for municipalities
  const colors = ["#c4785c", "#528b99", "#8b9bb4", "#7cc4d4", "#e8c4a0"];

  // Load initial municipalities from URL (only once on mount)
  useEffect(() => {
    const idsParam = searchParams.get("ids");
    if (idsParam && !initialized) {
      const ids = idsParam.split(",").filter(Boolean);
      setLoading(true);
      // Fetch municipality names for URL IDs
      Promise.all(
        ids.map((id) =>
          fetch(`/api/municipality/${id}`)
            .then((res) => res.json())
            .then((data) => ({
              id,
              name: data.municipality?.name ?? id,
              provinceCode: data.municipality?.provinceCode ?? "",
            }))
            .catch(() => ({ id, name: id, provinceCode: "" }))
        )
      ).then((municipalities) => {
        setSelected(municipalities);
        setInitialized(true);
      });
    } else if (!idsParam) {
      setInitialized(true);
    }
  }, []); // Empty deps - only run on mount

  // Fetch comparison data when we have 2+ selections
  useEffect(() => {
    if (!initialized) return;

    if (selected.length < 2) {
      setData(null);
      setLoading(false);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);

    const ids = selected.map((s) => s.id).join(",");

    fetch(`/api/compare?ids=${ids}`)
      .then((response) => {
        if (!response.ok) {
          return response.json().then((err) => {
            throw new Error(err.error || "Failed to fetch comparison");
          });
        }
        return response.json();
      })
      .then((json) => {
        if (!cancelled) {
          setData(json);
          // Update URL without triggering re-render
          const newUrl = `/compare?ids=${ids}`;
          window.history.replaceState({}, "", newUrl);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load comparison");
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [selected, initialized]);

  const handleSelect = (m: MunicipalityOption) => {
    if (selected.length < 5 && !selected.some((s) => s.id === m.id)) {
      setSelected([...selected, m]);
    }
  };

  const handleRemove = (id: string) => {
    setSelected(selected.filter((s) => s.id !== id));
  };

  const copyShareLink = () => {
    const url = `${window.location.origin}/compare?ids=${selected.map((s) => s.id).join(",")}`;
    navigator.clipboard.writeText(url);
  };

  return (
    <div className="compare-page">
      {/* Navigation */}
      <nav className="nav">
        <Link href="/" className="nav__logo">
          <span className="nav__logo-icon">◆</span>
          <span className="nav__logo-text">Italia Immobiliare</span>
        </Link>
        <div className="nav__breadcrumb">
          <Link href="/map">Map</Link>
          <span className="nav__separator">/</span>
          <span>Compare</span>
        </div>
        <div className="nav__actions">
          {selected.length >= 2 && (
            <button className="nav__btn" onClick={copyShareLink}>
              Copy Link
            </button>
          )}
        </div>
      </nav>

      {/* Header */}
      <header className="header">
        <div className="header__pattern" />
        <div className="header__content">
          <span className="header__eyebrow">Municipality Comparison</span>
          <h1 className="header__title">Compare Investments</h1>
          <p className="header__subtitle">
            Select 2-5 Italian municipalities to compare property values, appreciation forecasts, and demographics side by side.
          </p>
        </div>
      </header>

      {/* Main Content */}
      <main className="main">
        {/* Municipality Selector */}
        <section className="section section--selector">
          <MunicipalitySelector
            selected={selected}
            onSelect={handleSelect}
            onRemove={handleRemove}
            maxSelections={5}
          />
        </section>

        {/* Loading State */}
        {loading && (
          <div className="loading">
            <div className="loading__spinner" />
            <span>Comparing municipalities...</span>
          </div>
        )}

        {/* Error State */}
        {error && (
          <div className="error">
            <span className="error__icon">⚠</span>
            <span>{error}</span>
          </div>
        )}

        {/* Comparison Results */}
        {data && !loading && (
          <div className="results">
            {/* Quick Stats */}
            <section className="section section--stats">
              <div className="stats-grid">
                {data.municipalities.map((m, i) => (
                  <Link
                    key={m.id}
                    href={`/municipality/${m.id}`}
                    className="stat-card"
                    style={{ borderTopColor: colors[i] }}
                  >
                    <div className="stat-card__header">
                      <h3 className="stat-card__name">{m.name}</h3>
                      <span className="stat-card__region">{m.regionCode} · {m.provinceCode}</span>
                    </div>
                    <div className="stat-card__metrics">
                      <div className="stat-card__metric">
                        <span className="stat-card__value">{formatCurrency(m.forecast?.valueMidEurSqm)}</span>
                        <span className="stat-card__label">Price/m²</span>
                      </div>
                      <div className="stat-card__metric">
                        <span className="stat-card__value" style={{ color: (m.forecast?.appreciationPct ?? 0) >= 0 ? "#4ade80" : "#f87171" }}>
                          {formatPercent(m.forecast?.appreciationPct)}
                        </span>
                        <span className="stat-card__label">12M Forecast</span>
                      </div>
                      <div className="stat-card__metric">
                        <span className="stat-card__value">{formatPercent(m.forecast?.grossYieldPct, false)}</span>
                        <span className="stat-card__label">Yield</span>
                      </div>
                    </div>
                    <div className="stat-card__score">
                      <span className="stat-card__score-value" style={{ color: colors[i] }}>
                        {m.forecast?.opportunityScore ?? "—"}
                      </span>
                      <span className="stat-card__score-label">Opportunity</span>
                    </div>
                  </Link>
                ))}
              </div>
            </section>

            {/* Charts Grid */}
            <section className="section section--charts">
              <div className="charts-grid">
                <BarCompare
                  values={data.municipalities.map((m) => m.forecast?.valueMidEurSqm ?? null)}
                  labels={data.municipalities.map((m) => m.name)}
                  colors={colors.slice(0, data.municipalities.length)}
                  title="Property Value (€/m²)"
                  formatFn={formatCurrency}
                  highlightBest={false}
                />

                <BarCompare
                  values={data.municipalities.map((m) => m.forecast?.appreciationPct ?? null)}
                  labels={data.municipalities.map((m) => m.name)}
                  colors={colors.slice(0, data.municipalities.length)}
                  title="12-Month Appreciation Forecast"
                  formatFn={(v) => formatPercent(v, true)}
                />

                <BarCompare
                  values={data.municipalities.map((m) => m.forecast?.grossYieldPct ?? null)}
                  labels={data.municipalities.map((m) => m.name)}
                  colors={colors.slice(0, data.municipalities.length)}
                  title="Gross Rental Yield"
                  formatFn={(v) => formatPercent(v, false)}
                />

                <BarCompare
                  values={data.municipalities.map((m) => m.forecast?.opportunityScore ?? null)}
                  labels={data.municipalities.map((m) => m.name)}
                  colors={colors.slice(0, data.municipalities.length)}
                  title="Opportunity Score"
                  formatFn={(v) => v?.toString() ?? "—"}
                />
              </div>
            </section>

            {/* Historical Chart */}
            <section className="section section--history">
              <HistoryChart
                series={data.municipalities.map((m) =>
                  m.historicalValues.map((v) => ({
                    periodId: v.periodId,
                    value: v.valueMidEurSqm,
                  }))
                )}
                labels={data.municipalities.map((m) => m.name)}
                colors={colors.slice(0, data.municipalities.length)}
              />
            </section>

            {/* Demographics */}
            <section className="section section--demographics">
              <DemographicsTable
                municipalities={data.municipalities}
                colors={colors.slice(0, data.municipalities.length)}
              />
            </section>

            {/* Meta Info */}
            <div className="meta">
              <span>Segment: {data.meta.segment}</span>
              <span>Horizon: {data.meta.horizonMonths} months</span>
              <span>Compared: {new Date(data.meta.comparedAt).toLocaleString("it-IT")}</span>
            </div>
          </div>
        )}

        {/* Empty State */}
        {!data && !loading && selected.length < 2 && (
          <div className="empty">
            <div className="empty__icon">⬡⬡</div>
            <h3 className="empty__title">Select municipalities to compare</h3>
            <p className="empty__text">
              Search and select at least 2 municipalities above to see a side-by-side comparison of
              property values, forecasts, and demographics.
            </p>
          </div>
        )}
      </main>

      <style jsx>{`
        .compare-page {
          min-height: 100vh;
          background: #0d0f12;
          color: #f0f2f5;
        }

        /* Navigation */
        .nav {
          position: sticky;
          top: 0;
          z-index: 100;
          display: flex;
          align-items: center;
          justify-content: space-between;
          padding: 0 32px;
          height: 64px;
          background: linear-gradient(180deg, rgba(13, 15, 18, 0.98) 0%, rgba(13, 15, 18, 0.95) 100%);
          backdrop-filter: blur(12px);
          border-bottom: 1px solid rgba(255, 255, 255, 0.06);
        }

        .nav__logo {
          display: flex;
          align-items: center;
          gap: 10px;
          text-decoration: none;
        }

        .nav__logo-icon {
          font-size: 1.25rem;
          color: #c4785c;
        }

        .nav__logo-text {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 1.1rem;
          font-weight: 600;
          color: #f0f2f5;
        }

        .nav__breadcrumb {
          display: flex;
          align-items: center;
          gap: 8px;
          font-size: 0.85rem;
        }

        .nav__breadcrumb a {
          color: #6b7a90;
          text-decoration: none;
          transition: color 0.2s;
        }

        .nav__breadcrumb a:hover {
          color: #c4785c;
        }

        .nav__separator {
          color: #3a4556;
        }

        .nav__breadcrumb span:last-child {
          color: #a8b3c7;
        }

        .nav__actions {
          display: flex;
          gap: 12px;
        }

        .nav__btn {
          padding: 8px 16px;
          font-size: 0.8rem;
          font-weight: 500;
          color: #c4785c;
          background: rgba(196, 120, 92, 0.1);
          border: 1px solid rgba(196, 120, 92, 0.2);
          border-radius: 8px;
          cursor: pointer;
          transition: all 0.2s;
        }

        .nav__btn:hover {
          background: rgba(196, 120, 92, 0.2);
          border-color: rgba(196, 120, 92, 0.4);
        }

        /* Header */
        .header {
          position: relative;
          padding: 64px 48px 48px;
          background: linear-gradient(180deg, #161920 0%, #0d0f12 100%);
          border-bottom: 1px solid rgba(255, 255, 255, 0.04);
          overflow: hidden;
        }

        .header__pattern {
          position: absolute;
          inset: 0;
          background-image:
            radial-gradient(circle at 30% 40%, rgba(196, 120, 92, 0.08) 0%, transparent 50%),
            radial-gradient(circle at 70% 60%, rgba(82, 139, 153, 0.06) 0%, transparent 50%);
          pointer-events: none;
        }

        .header__content {
          position: relative;
          max-width: 800px;
          margin: 0 auto;
          text-align: center;
        }

        .header__eyebrow {
          display: inline-block;
          padding: 6px 14px;
          margin-bottom: 20px;
          font-size: 0.7rem;
          font-weight: 600;
          text-transform: uppercase;
          letter-spacing: 0.15em;
          color: #c4785c;
          background: rgba(196, 120, 92, 0.1);
          border: 1px solid rgba(196, 120, 92, 0.2);
          border-radius: 20px;
        }

        .header__title {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 3rem;
          font-weight: 600;
          margin: 0 0 12px;
          letter-spacing: -0.01em;
        }

        .header__subtitle {
          font-size: 1rem;
          color: #8b9bb4;
          margin: 0;
          line-height: 1.6;
        }

        /* Main */
        .main {
          max-width: 1200px;
          margin: 0 auto;
          padding: 32px;
        }

        .section {
          margin-bottom: 32px;
        }

        /* Stats Grid */
        .stats-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
          gap: 20px;
        }

        .stat-card {
          display: flex;
          flex-direction: column;
          gap: 16px;
          padding: 24px;
          background: linear-gradient(165deg, rgba(26, 29, 35, 0.8) 0%, rgba(22, 25, 32, 0.9) 100%);
          border: 1px solid rgba(255, 255, 255, 0.06);
          border-top: 3px solid;
          border-radius: 16px;
          text-decoration: none;
          transition: all 0.2s;
        }

        .stat-card:hover {
          transform: translateY(-4px);
          border-color: rgba(255, 255, 255, 0.12);
          box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        }

        .stat-card__header {
          display: flex;
          flex-direction: column;
          gap: 4px;
        }

        .stat-card__name {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 1.5rem;
          font-weight: 600;
          color: #f0f2f5;
          margin: 0;
        }

        .stat-card__region {
          font-size: 0.75rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.08em;
        }

        .stat-card__metrics {
          display: grid;
          grid-template-columns: repeat(3, 1fr);
          gap: 12px;
        }

        .stat-card__metric {
          display: flex;
          flex-direction: column;
          gap: 2px;
        }

        .stat-card__value {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 1.25rem;
          font-weight: 600;
          color: #f0f2f5;
        }

        .stat-card__label {
          font-size: 0.65rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.06em;
        }

        .stat-card__score {
          display: flex;
          align-items: baseline;
          gap: 8px;
          padding-top: 12px;
          border-top: 1px solid rgba(255, 255, 255, 0.04);
        }

        .stat-card__score-value {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 2rem;
          font-weight: 600;
        }

        .stat-card__score-label {
          font-size: 0.7rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.1em;
        }

        /* Charts Grid */
        .charts-grid {
          display: grid;
          grid-template-columns: repeat(2, 1fr);
          gap: 20px;
        }

        /* Loading */
        .loading {
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          gap: 20px;
          padding: 80px;
          color: #6b7a90;
        }

        .loading__spinner {
          width: 48px;
          height: 48px;
          border: 3px solid rgba(196, 120, 92, 0.2);
          border-top-color: #c4785c;
          border-radius: 50%;
          animation: spin 1s linear infinite;
        }

        @keyframes spin {
          to { transform: rotate(360deg); }
        }

        /* Error */
        .error {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: 12px;
          padding: 24px;
          background: rgba(248, 113, 113, 0.1);
          border: 1px solid rgba(248, 113, 113, 0.2);
          border-radius: 12px;
          color: #f87171;
        }

        .error__icon {
          font-size: 1.25rem;
        }

        /* Empty State */
        .empty {
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          padding: 80px 32px;
          text-align: center;
        }

        .empty__icon {
          font-size: 4rem;
          color: #3a4556;
          margin-bottom: 24px;
          letter-spacing: -0.2em;
        }

        .empty__title {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 1.75rem;
          font-weight: 600;
          color: #a8b3c7;
          margin: 0 0 12px;
        }

        .empty__text {
          font-size: 0.95rem;
          color: #6b7a90;
          max-width: 500px;
          margin: 0;
          line-height: 1.6;
        }

        /* Meta */
        .meta {
          display: flex;
          justify-content: center;
          gap: 24px;
          padding: 24px;
          font-size: 0.75rem;
          color: #5a6677;
        }

        /* Responsive */
        @media (max-width: 1024px) {
          .charts-grid {
            grid-template-columns: 1fr;
          }
        }

        @media (max-width: 768px) {
          .header {
            padding: 48px 24px;
          }

          .header__title {
            font-size: 2.25rem;
          }

          .main {
            padding: 24px 16px;
          }

          .nav {
            padding: 0 16px;
          }

          .nav__breadcrumb {
            display: none;
          }

          .stat-card__metrics {
            grid-template-columns: 1fr 1fr;
          }

          .meta {
            flex-direction: column;
            align-items: center;
            gap: 8px;
          }
        }
      `}</style>
    </div>
  );
}

export default function ComparePage() {
  return (
    <Suspense fallback={
      <div style={{
        minHeight: "100vh",
        background: "#0d0f12",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        color: "#6b7a90"
      }}>
        Loading...
      </div>
    }>
      <ComparePageContent />
    </Suspense>
  );
}
