"use client";

import { useEffect, useState, use } from "react";
import Link from "next/link";
import { ZoneDrilldown } from "@/components/ZoneDrilldown";

interface MunicipalityDetail {
  municipality: {
    id: string;
    name: string;
    regionCode: string;
    regionName: string | null;
    provinceCode: string;
    provinceName: string | null;
    isCoastal: boolean;
    isMountain: boolean;
    areaSqKm: number | null;
  };
  forecast: {
    date: string;
    horizonMonths: number;
    valueMidEurSqm: number | null;
    appreciationPct: number | null;
    projectedValueEurSqm: number | null;
    grossYieldPct: number | null;
    opportunityScore: number | null;
    confidenceScore: number | null;
    drivers: Array<{ factor: string; direction: string; strength: number }> | null;
    risks: Array<{ factor: string; severity: string }> | null;
    modelVersion: string;
  } | null;
  historicalValues: Array<{
    periodId: string;
    valueMidEurSqm: number | null;
    valueMinEurSqm: number | null;
    valueMaxEurSqm: number | null;
    rentMidEurSqmMonth: number | null;
    pctChange1s: number | null;
    zonesWithData: number | null;
  }>;
  historicalTransactions: Array<{
    periodId: string;
    ntnTotal: number | null;
    ntnPer1000Pop: number | null;
    absorptionRate: number | null;
  }>;
  demographics: {
    year: number;
    totalPopulation: number | null;
    populationDensity: number | null;
    youngRatio: number | null;
    workingRatio: number | null;
    elderlyRatio: number | null;
    foreignRatio: number | null;
    populationGrowthRate: number | null;
    dependencyRatio: number | null;
  } | null;
  neighbors: Array<{
    municipalityId: string;
    name: string;
    sharedBorderKm: number;
    valueMidEurSqm: number | null;
  }>;
  strMetrics: {
    latest: {
      periodId: string;
      adrEur: number | null;
      occupancyRate: number | null;
      revParEur: number | null;
      monthlyRevenueEur: number | null;
      annualRevenueEur: number | null;
      activeListings: number | null;
      seasonalityFactor: number | null;
      isPeakSeason: boolean | null;
      grossYieldPct: number | null;
      netYieldPct: number | null;
    };
    historical: Array<{
      periodId: string;
      adrEur: number | null;
      occupancyRate: number | null;
      revParEur: number | null;
    }>;
  } | null;
  strSeasonality: {
    year: number;
    annualAvgAdr: number | null;
    annualAvgOccupancy: number | null;
    annualAvgRevPar: number | null;
    totalAnnualRevenue: number | null;
    seasonalityScore: number | null;
    peakMonths: string[] | null;
    shoulderMonths: string[] | null;
    offPeakMonths: string[] | null;
    peakToOffpeakRatio: number | null;
    monthlyAdrProfile: Record<string, number> | null;
    monthlyOccupancyProfile: Record<string, number> | null;
  } | null;
  regulations: {
    riskScore: number | null;
    riskLevel: string | null;
    strRegulationScore: number | null;
    heritageScore: number | null;
    strLicenseRequired: boolean;
    strMaxDaysPerYear: number | null;
    strNewPermitsAllowed: boolean;
    strZonesRestricted: boolean;
    hasHeritageZones: boolean;
    hasRentControl: boolean;
    activeRegulationsCount: number;
    riskFactors: Array<{ factor: string; severity: string; description?: string }> | null;
    investorWarningLevel: string | null;
    investorNotes: string | null;
  } | null;
  derived: {
    priceTrend: "up" | "down" | "stable" | null;
    currentValue: number | null;
    projectedValue: number | null;
  };
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

// Score Ring Component with enhanced styling
function ScoreRing({
  value,
  label,
  color,
  size = 120,
  delay = 0,
}: {
  value: number | null;
  label: string;
  color: string;
  size?: number;
  delay?: number;
}) {
  const strokeWidth = 8;
  const radius = (size - strokeWidth) / 2;
  const circumference = radius * 2 * Math.PI;
  const normalizedValue = value != null ? Math.min(value, 100) : 0;
  const progress = (normalizedValue / 100) * circumference;

  return (
    <div className="score-ring-container" style={{ animationDelay: `${delay}ms` }}>
      <div className="score-ring" style={{ width: size, height: size }}>
        <svg width={size} height={size}>
          <defs>
            <linearGradient id={`gradient-${label}`} x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stopColor={color} stopOpacity="1" />
              <stop offset="100%" stopColor={color} stopOpacity="0.6" />
            </linearGradient>
          </defs>
          <circle
            cx={size / 2}
            cy={size / 2}
            r={radius}
            fill="none"
            stroke="rgba(255,255,255,0.04)"
            strokeWidth={strokeWidth}
          />
          <circle
            cx={size / 2}
            cy={size / 2}
            r={radius}
            fill="none"
            stroke={`url(#gradient-${label})`}
            strokeWidth={strokeWidth}
            strokeDasharray={`${progress} ${circumference}`}
            strokeLinecap="round"
            transform={`rotate(-90 ${size / 2} ${size / 2})`}
            className="score-ring__progress"
          />
        </svg>
        <div className="score-ring__content">
          <span className="score-ring__value">{value != null ? Math.round(value) : "—"}</span>
          <span className="score-ring__unit">pts</span>
        </div>
      </div>
      <span className="score-ring__label">{label}</span>
      <style jsx>{`
        .score-ring-container {
          display: flex;
          flex-direction: column;
          align-items: center;
          gap: 12px;
          opacity: 0;
          animation: fadeInUp 0.6s ease forwards;
        }
        @keyframes fadeInUp {
          from { opacity: 0; transform: translateY(20px); }
          to { opacity: 1; transform: translateY(0); }
        }
        .score-ring {
          position: relative;
          display: flex;
          align-items: center;
          justify-content: center;
        }
        .score-ring__progress {
          transition: stroke-dasharray 1.2s cubic-bezier(0.4, 0, 0.2, 1);
        }
        .score-ring__content {
          position: absolute;
          display: flex;
          flex-direction: column;
          align-items: center;
        }
        .score-ring__value {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 2.25rem;
          font-weight: 600;
          color: #f0f2f5;
          line-height: 1;
        }
        .score-ring__unit {
          font-size: 0.65rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.1em;
          margin-top: 2px;
        }
        .score-ring__label {
          font-size: 0.75rem;
          font-weight: 500;
          color: #8b9bb4;
          text-transform: uppercase;
          letter-spacing: 0.12em;
        }
      `}</style>
    </div>
  );
}

// Mini Chart for historical data
function MiniChart({
  data,
  color = "#c4785c",
  height = 80,
}: {
  data: number[];
  color?: string;
  height?: number;
}) {
  if (data.length === 0) return null;

  const max = Math.max(...data);
  const min = Math.min(...data);
  const range = max - min || 1;
  const width = 100;
  const padding = 10;

  const points = data.map((value, i) => {
    const x = padding + (i / (data.length - 1)) * (width - 2 * padding);
    const y = height - padding - ((value - min) / range) * (height - 2 * padding);
    return `${x},${y}`;
  });

  const pathD = `M ${points.join(" L ")}`;
  const areaD = `${pathD} L ${width - padding},${height - padding} L ${padding},${height - padding} Z`;

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="mini-chart">
      <defs>
        <linearGradient id="chartGradient" x1="0%" y1="0%" x2="0%" y2="100%">
          <stop offset="0%" stopColor={color} stopOpacity="0.3" />
          <stop offset="100%" stopColor={color} stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={areaD} fill="url(#chartGradient)" />
      <path d={pathD} fill="none" stroke={color} strokeWidth="2" strokeLinecap="round" />
      {data.map((value, i) => {
        const x = padding + (i / (data.length - 1)) * (width - 2 * padding);
        const y = height - padding - ((value - min) / range) * (height - 2 * padding);
        return <circle key={i} cx={x} cy={y} r="3" fill={color} />;
      })}
      <style jsx>{`
        .mini-chart {
          width: 100%;
          height: ${height}px;
        }
      `}</style>
    </svg>
  );
}

// Monthly Profile Chart for STR seasonality
function MonthlyProfileChart({
  data,
  color = "#c4785c",
  height = 100,
  label,
  formatValue,
}: {
  data: Record<string, number> | null;
  color?: string;
  height?: number;
  label: string;
  formatValue?: (v: number) => string;
}) {
  if (!data) return null;

  const months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"];
  const monthNames = ["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"];
  const values = months.map((m) => data[m] ?? 0);
  const max = Math.max(...values);
  const barWidth = 100 / 12;

  return (
    <div className="monthly-profile">
      <span className="monthly-profile__label">{label}</span>
      <div className="monthly-profile__chart">
        {values.map((value, i) => (
          <div key={i} className="monthly-profile__bar-container">
            <div
              className="monthly-profile__bar"
              style={{
                height: `${max > 0 ? (value / max) * 100 : 0}%`,
                background: color,
              }}
              title={formatValue ? formatValue(value) : value.toString()}
            />
            <span className="monthly-profile__month">{monthNames[i]}</span>
          </div>
        ))}
      </div>
      <style jsx>{`
        .monthly-profile {
          padding: 16px 0;
        }
        .monthly-profile__label {
          display: block;
          font-size: 0.7rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.08em;
          margin-bottom: 12px;
        }
        .monthly-profile__chart {
          display: flex;
          align-items: flex-end;
          height: ${height}px;
          gap: 4px;
        }
        .monthly-profile__bar-container {
          flex: 1;
          display: flex;
          flex-direction: column;
          align-items: center;
          height: 100%;
        }
        .monthly-profile__bar {
          width: 100%;
          border-radius: 3px 3px 0 0;
          transition: height 0.4s ease;
          margin-top: auto;
        }
        .monthly-profile__month {
          font-size: 0.6rem;
          color: #5a6677;
          margin-top: 6px;
        }
      `}</style>
    </div>
  );
}

// Regulation Warning Badge
function RegulationBadge({
  level,
  label,
}: {
  level: "high" | "medium" | "low" | "none";
  label: string;
}) {
  const colors = {
    high: { bg: "rgba(248, 113, 113, 0.15)", border: "rgba(248, 113, 113, 0.3)", text: "#f87171", icon: "⚠" },
    medium: { bg: "rgba(251, 191, 36, 0.15)", border: "rgba(251, 191, 36, 0.3)", text: "#fbbf24", icon: "⚡" },
    low: { bg: "rgba(74, 222, 128, 0.15)", border: "rgba(74, 222, 128, 0.3)", text: "#4ade80", icon: "✓" },
    none: { bg: "rgba(139, 155, 180, 0.1)", border: "rgba(139, 155, 180, 0.2)", text: "#8b9bb4", icon: "○" },
  };
  const c = colors[level];

  return (
    <span
      className="regulation-badge"
      style={{ background: c.bg, borderColor: c.border, color: c.text }}
    >
      {c.icon} {label}
      <style jsx>{`
        .regulation-badge {
          display: inline-flex;
          align-items: center;
          gap: 6px;
          padding: 8px 14px;
          font-size: 0.75rem;
          font-weight: 500;
          border: 1px solid;
          border-radius: 8px;
          white-space: nowrap;
        }
      `}</style>
    </span>
  );
}

// Driver/Risk Badge
function FactorBadge({
  type,
  label,
  severity,
}: {
  type: "driver" | "risk";
  label: string;
  severity?: "high" | "medium" | "low";
}) {
  const severityColors = {
    high: { bg: "rgba(248, 113, 113, 0.15)", border: "rgba(248, 113, 113, 0.3)", text: "#f87171" },
    medium: { bg: "rgba(251, 191, 36, 0.15)", border: "rgba(251, 191, 36, 0.3)", text: "#fbbf24" },
    low: { bg: "rgba(74, 222, 128, 0.15)", border: "rgba(74, 222, 128, 0.3)", text: "#4ade80" },
  };

  const driverColor = { bg: "rgba(74, 222, 128, 0.12)", border: "rgba(74, 222, 128, 0.25)", text: "#4ade80" };
  const colors = type === "risk" && severity ? severityColors[severity] : driverColor;

  return (
    <span
      className="factor-badge"
      style={{
        background: colors.bg,
        borderColor: colors.border,
        color: colors.text,
      }}
    >
      {type === "driver" ? "↗" : "⚠"} {label.replace(/_/g, " ")}
      <style jsx>{`
        .factor-badge {
          display: inline-flex;
          align-items: center;
          gap: 6px;
          padding: 6px 12px;
          font-size: 0.7rem;
          font-weight: 500;
          text-transform: capitalize;
          border: 1px solid;
          border-radius: 20px;
          white-space: nowrap;
        }
      `}</style>
    </span>
  );
}

export default function MunicipalityDetailPage({
  params,
}: {
  params: Promise<{ istatCode: string }>;
}) {
  const resolvedParams = use(params);
  const [data, setData] = useState<MunicipalityDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchData() {
      try {
        const response = await fetch(`/api/municipality/${resolvedParams.istatCode}`);
        if (!response.ok) {
          if (response.status === 404) {
            setError("Municipality not found");
          } else {
            setError("Failed to load municipality data");
          }
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
    fetchData();
  }, [resolvedParams.istatCode]);

  if (loading) {
    return (
      <div className="loading-page">
        <div className="loading-spinner" />
        <span>Loading municipality data...</span>
        <style jsx>{`
          .loading-page {
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            gap: 20px;
            background: #0d0f12;
            color: #6b7a90;
          }
          .loading-spinner {
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
        `}</style>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="error-page">
        <span className="error-icon">⚠</span>
        <h1>{error || "Something went wrong"}</h1>
        <Link href="/map" className="error-link">
          Back to Map
        </Link>
        <style jsx>{`
          .error-page {
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            gap: 16px;
            background: #0d0f12;
            color: #f0f2f5;
            text-align: center;
          }
          .error-icon {
            font-size: 4rem;
            opacity: 0.4;
          }
          h1 {
            font-family: 'Cormorant Garamond', Georgia, serif;
            font-size: 1.75rem;
            font-weight: 500;
            margin: 0;
          }
          .error-link {
            margin-top: 8px;
            padding: 12px 24px;
            background: rgba(196, 120, 92, 0.15);
            border: 1px solid rgba(196, 120, 92, 0.3);
            border-radius: 8px;
            color: #e8c4a0;
            text-decoration: none;
            font-size: 0.85rem;
            transition: all 0.2s ease;
          }
          .error-link:hover {
            background: rgba(196, 120, 92, 0.25);
          }
        `}</style>
      </div>
    );
  }

  const { municipality: m, forecast: f, historicalValues, historicalTransactions, demographics: d, neighbors } = data;

  // Prepare chart data (reverse to show oldest first)
  const priceHistory = [...historicalValues]
    .reverse()
    .map((v) => v.valueMidEurSqm)
    .filter((v): v is number => v != null);

  const transactionHistory = [...historicalTransactions]
    .reverse()
    .map((t) => t.ntnTotal)
    .filter((t): t is number => t != null);

  return (
    <div className="municipality-page">
      {/* Navigation */}
      <nav className="nav">
        <Link href="/" className="nav__logo">
          <span className="nav__logo-icon">◆</span>
          <span className="nav__logo-text">Italia Immobiliare</span>
        </Link>
        <div className="nav__breadcrumb">
          <Link href="/map">Map</Link>
          <span className="nav__separator">/</span>
          <span>{m.name}</span>
        </div>
        <div className="nav__actions">
          <Link href={`/map?focus=${m.id}`} className="nav__btn">
            View on Map
          </Link>
        </div>
      </nav>

      {/* Hero Section */}
      <header className="hero">
        <div className="hero__pattern" />
        <div className="hero__content">
          <div className="hero__badges">
            {m.isCoastal && <span className="hero__badge hero__badge--coastal">Coastal</span>}
            {m.isMountain && <span className="hero__badge hero__badge--mountain">Mountain</span>}
          </div>
          <h1 className="hero__title">{m.name}</h1>
          <p className="hero__subtitle">
            {[m.provinceName, m.regionName].filter(Boolean).join(" · ")}
          </p>
          {m.areaSqKm && (
            <span className="hero__area">{formatNumber(m.areaSqKm, 1)} km²</span>
          )}
        </div>

        {/* Score Cards */}
        <div className="hero__scores">
          <ScoreRing value={f?.opportunityScore ?? null} label="Opportunity" color="#c4785c" delay={0} />
          <ScoreRing value={f?.confidenceScore ?? null} label="Confidence" color="#528b99" delay={100} />
          <div className="hero__metric" style={{ animationDelay: "200ms" }}>
            <span className="hero__metric-value">
              {formatPercent(f?.appreciationPct)}
            </span>
            <span className="hero__metric-label">12M Appreciation</span>
          </div>
          <div className="hero__metric" style={{ animationDelay: "300ms" }}>
            <span className="hero__metric-value">
              {formatPercent(f?.grossYieldPct, false)}
            </span>
            <span className="hero__metric-label">Gross Yield</span>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="main">
        {/* Property Values Section */}
        <section className="section section--values">
          <div className="section__header">
            <h2 className="section__title">Property Values</h2>
            <span className="section__subtitle">Residential market overview</span>
          </div>
          <div className="values-grid">
            <div className="value-card value-card--primary">
              <span className="value-card__label">Current Value</span>
              <span className="value-card__value">
                {formatCurrency(f?.valueMidEurSqm)}<span className="value-card__unit">/m²</span>
              </span>
              {data.derived.priceTrend && (
                <span className={`value-card__trend value-card__trend--${data.derived.priceTrend}`}>
                  {data.derived.priceTrend === "up" ? "↑ Increasing" :
                   data.derived.priceTrend === "down" ? "↓ Decreasing" : "→ Stable"}
                </span>
              )}
            </div>
            <div className="value-card">
              <span className="value-card__label">Projected (12M)</span>
              <span className="value-card__value">
                {formatCurrency(data.derived.projectedValue)}<span className="value-card__unit">/m²</span>
              </span>
            </div>
            <div className="chart-card">
              <span className="chart-card__title">Price History</span>
              {priceHistory.length > 1 ? (
                <MiniChart data={priceHistory} color="#c4785c" height={100} />
              ) : (
                <div className="chart-card__empty">Insufficient data</div>
              )}
              <div className="chart-card__labels">
                {historicalValues.slice(-1)[0]?.periodId && (
                  <>
                    <span>{historicalValues[historicalValues.length - 1]?.periodId}</span>
                    <span>{historicalValues[0]?.periodId}</span>
                  </>
                )}
              </div>
            </div>
          </div>
        </section>

        {/* Market Activity Section */}
        <section className="section section--market">
          <div className="section__header">
            <h2 className="section__title">Market Activity</h2>
            <span className="section__subtitle">Transaction volume & intensity</span>
          </div>
          <div className="market-grid">
            <div className="stat-card">
              <span className="stat-card__icon">⇄</span>
              <div className="stat-card__content">
                <span className="stat-card__value">{formatNumber(historicalTransactions[0]?.ntnTotal, 1)}</span>
                <span className="stat-card__label">NTN Transactions</span>
              </div>
            </div>
            <div className="stat-card">
              <span className="stat-card__icon">⚡</span>
              <div className="stat-card__content">
                <span className="stat-card__value">{formatNumber(historicalTransactions[0]?.ntnPer1000Pop, 2)}</span>
                <span className="stat-card__label">NTN per 1000 pop</span>
              </div>
            </div>
            <div className="stat-card">
              <span className="stat-card__icon">⏱</span>
              <div className="stat-card__content">
                <span className="stat-card__value">
                  {historicalTransactions[0]?.absorptionRate != null
                    ? `${(historicalTransactions[0].absorptionRate * 100).toFixed(0)}%`
                    : "—"}
                </span>
                <span className="stat-card__label">Absorption Rate</span>
              </div>
            </div>
            <div className="chart-card chart-card--small">
              <span className="chart-card__title">Transaction Volume</span>
              {transactionHistory.length > 1 ? (
                <MiniChart data={transactionHistory} color="#528b99" height={80} />
              ) : (
                <div className="chart-card__empty">Insufficient data</div>
              )}
            </div>
          </div>
        </section>

        {/* Demographics Section */}
        {d && (
          <section className="section section--demographics">
            <div className="section__header">
              <h2 className="section__title">Demographics</h2>
              <span className="section__subtitle">Population data ({d.year})</span>
            </div>
            <div className="demo-grid">
              <div className="demo-card demo-card--main">
                <div className="demo-card__header">
                  <span className="demo-card__value">{formatNumber(d.totalPopulation)}</span>
                  <span className="demo-card__label">Total Population</span>
                </div>
                <div className="demo-card__sub">
                  <span>{formatNumber(d.populationDensity, 1)} inhabitants/km²</span>
                  {d.populationGrowthRate != null && (
                    <span className={d.populationGrowthRate >= 0 ? "positive" : "negative"}>
                      {formatPercent(d.populationGrowthRate)} annual growth
                    </span>
                  )}
                </div>
              </div>

              <div className="demo-breakdown">
                <h4 className="demo-breakdown__title">Age Distribution</h4>
                <div className="demo-bar">
                  {d.youngRatio != null && (
                    <div
                      className="demo-bar__segment demo-bar__segment--young"
                      style={{ width: `${d.youngRatio * 100}%` }}
                      title={`Young (0-14): ${(d.youngRatio * 100).toFixed(1)}%`}
                    />
                  )}
                  {d.workingRatio != null && (
                    <div
                      className="demo-bar__segment demo-bar__segment--working"
                      style={{ width: `${d.workingRatio * 100}%` }}
                      title={`Working (15-64): ${(d.workingRatio * 100).toFixed(1)}%`}
                    />
                  )}
                  {d.elderlyRatio != null && (
                    <div
                      className="demo-bar__segment demo-bar__segment--elderly"
                      style={{ width: `${d.elderlyRatio * 100}%` }}
                      title={`Elderly (65+): ${(d.elderlyRatio * 100).toFixed(1)}%`}
                    />
                  )}
                </div>
                <div className="demo-bar__legend">
                  <span><i className="dot dot--young" /> Young {d.youngRatio ? `${(d.youngRatio * 100).toFixed(1)}%` : ""}</span>
                  <span><i className="dot dot--working" /> Working {d.workingRatio ? `${(d.workingRatio * 100).toFixed(1)}%` : ""}</span>
                  <span><i className="dot dot--elderly" /> Elderly {d.elderlyRatio ? `${(d.elderlyRatio * 100).toFixed(1)}%` : ""}</span>
                </div>
              </div>

              <div className="demo-stats">
                <div className="demo-stat">
                  <span className="demo-stat__value">
                    {d.foreignRatio != null ? `${(d.foreignRatio * 100).toFixed(1)}%` : "—"}
                  </span>
                  <span className="demo-stat__label">Foreign Residents</span>
                </div>
                <div className="demo-stat">
                  <span className="demo-stat__value">
                    {d.dependencyRatio != null ? `${(d.dependencyRatio * 100).toFixed(0)}%` : "—"}
                  </span>
                  <span className="demo-stat__label">Dependency Ratio</span>
                </div>
              </div>
            </div>
          </section>
        )}

        {/* Short-Term Rental Metrics Section */}
        {data.strMetrics && (
          <section className="section section--str">
            <div className="section__header">
              <h2 className="section__title">Short-Term Rental Analytics</h2>
              <span className="section__subtitle">Airbnb & vacation rental performance</span>
            </div>
            <div className="str-grid">
              <div className="str-card str-card--adr">
                <span className="str-card__label">Avg Daily Rate</span>
                <span className="str-card__value">
                  {formatCurrency(data.strMetrics.latest.adrEur)}
                  <span className="str-card__unit">/night</span>
                </span>
                {data.strMetrics.latest.isPeakSeason && (
                  <span className="str-card__badge str-card__badge--peak">Peak Season</span>
                )}
              </div>
              <div className="str-card">
                <span className="str-card__label">Occupancy Rate</span>
                <span className="str-card__value">
                  {data.strMetrics.latest.occupancyRate != null
                    ? `${(data.strMetrics.latest.occupancyRate * 100).toFixed(0)}%`
                    : "—"}
                </span>
              </div>
              <div className="str-card">
                <span className="str-card__label">RevPAR</span>
                <span className="str-card__value">
                  {formatCurrency(data.strMetrics.latest.revParEur)}
                  <span className="str-card__unit">/night</span>
                </span>
              </div>
              <div className="str-card str-card--revenue">
                <span className="str-card__label">Est. Annual Revenue</span>
                <span className="str-card__value">
                  {formatCurrency(data.strMetrics.latest.annualRevenueEur)}
                </span>
                <span className="str-card__sub">
                  {formatCurrency(data.strMetrics.latest.monthlyRevenueEur)}/month
                </span>
              </div>
              <div className="str-card str-card--yield">
                <span className="str-card__label">STR Gross Yield</span>
                <span className="str-card__value str-card__value--highlight">
                  {formatPercent(data.strMetrics.latest.grossYieldPct, false)}
                </span>
                {data.strMetrics.latest.netYieldPct != null && (
                  <span className="str-card__sub">
                    Net: {formatPercent(data.strMetrics.latest.netYieldPct, false)}
                  </span>
                )}
              </div>
              <div className="str-card">
                <span className="str-card__label">Active Listings</span>
                <span className="str-card__value">
                  {formatNumber(data.strMetrics.latest.activeListings)}
                </span>
              </div>
            </div>

            {/* Yield Comparison */}
            {(f?.grossYieldPct != null || data.strMetrics.latest.grossYieldPct != null) && (
              <div className="yield-comparison">
                <h4 className="yield-comparison__title">Yield Comparison</h4>
                <div className="yield-comparison__bars">
                  <div className="yield-bar">
                    <span className="yield-bar__label">Long-Term Rental</span>
                    <div className="yield-bar__track">
                      <div
                        className="yield-bar__fill yield-bar__fill--ltr"
                        style={{ width: `${Math.min((f?.grossYieldPct ?? 0) * 10, 100)}%` }}
                      />
                    </div>
                    <span className="yield-bar__value">{formatPercent(f?.grossYieldPct, false)}</span>
                  </div>
                  <div className="yield-bar">
                    <span className="yield-bar__label">Short-Term Rental</span>
                    <div className="yield-bar__track">
                      <div
                        className="yield-bar__fill yield-bar__fill--str"
                        style={{ width: `${Math.min((data.strMetrics.latest.grossYieldPct ?? 0) * 10, 100)}%` }}
                      />
                    </div>
                    <span className="yield-bar__value">{formatPercent(data.strMetrics.latest.grossYieldPct, false)}</span>
                  </div>
                </div>
              </div>
            )}

            {/* Seasonality Profile */}
            {data.strSeasonality && (
              <div className="seasonality-section">
                <div className="seasonality-header">
                  <h4 className="seasonality-header__title">Seasonality Profile</h4>
                  {data.strSeasonality.seasonalityScore != null && (
                    <span className="seasonality-header__score">
                      Seasonality Score: {data.strSeasonality.seasonalityScore.toFixed(1)}
                    </span>
                  )}
                </div>
                <div className="seasonality-charts">
                  <MonthlyProfileChart
                    data={data.strSeasonality.monthlyAdrProfile}
                    color="#c4785c"
                    label="Monthly ADR Profile"
                    formatValue={(v) => `€${v.toFixed(0)}`}
                  />
                  <MonthlyProfileChart
                    data={data.strSeasonality.monthlyOccupancyProfile}
                    color="#528b99"
                    label="Monthly Occupancy Profile"
                    formatValue={(v) => `${(v * 100).toFixed(0)}%`}
                  />
                </div>
                <div className="season-months">
                  {data.strSeasonality.peakMonths && data.strSeasonality.peakMonths.length > 0 && (
                    <div className="season-months__group">
                      <span className="season-months__label season-months__label--peak">Peak:</span>
                      <span className="season-months__list">{data.strSeasonality.peakMonths.join(", ")}</span>
                    </div>
                  )}
                  {data.strSeasonality.shoulderMonths && data.strSeasonality.shoulderMonths.length > 0 && (
                    <div className="season-months__group">
                      <span className="season-months__label season-months__label--shoulder">Shoulder:</span>
                      <span className="season-months__list">{data.strSeasonality.shoulderMonths.join(", ")}</span>
                    </div>
                  )}
                  {data.strSeasonality.offPeakMonths && data.strSeasonality.offPeakMonths.length > 0 && (
                    <div className="season-months__group">
                      <span className="season-months__label season-months__label--offpeak">Off-Peak:</span>
                      <span className="season-months__list">{data.strSeasonality.offPeakMonths.join(", ")}</span>
                    </div>
                  )}
                </div>
              </div>
            )}
          </section>
        )}

        {/* Regulations Section */}
        {data.regulations && (
          <section className="section section--regulations">
            <div className="section__header">
              <h2 className="section__title">Regulatory Environment</h2>
              <span className="section__subtitle">STR regulations & investor considerations</span>
            </div>
            <div className="regulations-content">
              {/* Risk Overview */}
              <div className="regulation-overview">
                <div className="regulation-score">
                  <span className="regulation-score__label">Regulatory Risk</span>
                  <div className={`regulation-score__level regulation-score__level--${data.regulations.riskLevel?.toLowerCase() ?? 'none'}`}>
                    {data.regulations.riskLevel ?? "Unknown"}
                  </div>
                  {data.regulations.riskScore != null && (
                    <span className="regulation-score__value">Score: {data.regulations.riskScore}/100</span>
                  )}
                </div>
                {data.regulations.investorWarningLevel && data.regulations.investorWarningLevel !== "none" && (
                  <div className={`investor-warning investor-warning--${data.regulations.investorWarningLevel}`}>
                    <span className="investor-warning__icon">⚠</span>
                    <span className="investor-warning__text">
                      {data.regulations.investorWarningLevel === "high"
                        ? "High regulatory risk - Proceed with caution"
                        : data.regulations.investorWarningLevel === "medium"
                        ? "Moderate regulatory considerations"
                        : "Low regulatory barriers"}
                    </span>
                  </div>
                )}
              </div>

              {/* STR Regulations */}
              <div className="regulation-details">
                <h4 className="regulation-details__title">STR Regulations</h4>
                <div className="regulation-badges">
                  <RegulationBadge
                    level={data.regulations.strLicenseRequired ? "medium" : "low"}
                    label={data.regulations.strLicenseRequired ? "License Required" : "No License Needed"}
                  />
                  {data.regulations.strMaxDaysPerYear != null && (
                    <RegulationBadge
                      level={data.regulations.strMaxDaysPerYear < 90 ? "high" : data.regulations.strMaxDaysPerYear < 180 ? "medium" : "low"}
                      label={`Max ${data.regulations.strMaxDaysPerYear} days/year`}
                    />
                  )}
                  <RegulationBadge
                    level={data.regulations.strNewPermitsAllowed ? "low" : "high"}
                    label={data.regulations.strNewPermitsAllowed ? "New Permits Allowed" : "New Permits Restricted"}
                  />
                  {data.regulations.strZonesRestricted && (
                    <RegulationBadge level="medium" label="Zone Restrictions" />
                  )}
                </div>
              </div>

              {/* Other Regulations */}
              <div className="regulation-details">
                <h4 className="regulation-details__title">Other Considerations</h4>
                <div className="regulation-badges">
                  {data.regulations.hasHeritageZones && (
                    <RegulationBadge level="medium" label="Heritage Zones" />
                  )}
                  {data.regulations.hasRentControl && (
                    <RegulationBadge level="high" label="Rent Control" />
                  )}
                  {data.regulations.activeRegulationsCount > 0 && (
                    <RegulationBadge
                      level={data.regulations.activeRegulationsCount > 3 ? "high" : "medium"}
                      label={`${data.regulations.activeRegulationsCount} Active Regulations`}
                    />
                  )}
                </div>
              </div>

              {/* Risk Factors */}
              {data.regulations.riskFactors && data.regulations.riskFactors.length > 0 && (
                <div className="regulation-factors">
                  <h4 className="regulation-details__title">Risk Factors</h4>
                  <div className="factors-list">
                    {data.regulations.riskFactors.map((rf, i) => (
                      <FactorBadge
                        key={i}
                        type="risk"
                        label={rf.factor}
                        severity={rf.severity as "high" | "medium" | "low"}
                      />
                    ))}
                  </div>
                </div>
              )}

              {/* Investor Notes */}
              {data.regulations.investorNotes && (
                <div className="investor-notes">
                  <h4 className="investor-notes__title">Investor Notes</h4>
                  <p className="investor-notes__text">{data.regulations.investorNotes}</p>
                </div>
              )}
            </div>
          </section>
        )}

        {/* OMI Zone Drilldown Section */}
        <section className="section section--zones">
          <div className="section__header">
            <h2 className="section__title">OMI Zone Analysis</h2>
            <span className="section__subtitle">Sub-municipal area breakdown by official OMI zones</span>
          </div>
          <ZoneDrilldown municipalityId={m.id} segment="residential" />
        </section>

        {/* Drivers & Risks Section */}
        {f && ((f.drivers?.length ?? 0) > 0 || (f.risks?.length ?? 0) > 0) && (
          <section className="section section--factors">
            <div className="section__header">
              <h2 className="section__title">Investment Factors</h2>
              <span className="section__subtitle">Key drivers and risk indicators</span>
            </div>
            <div className="factors-grid">
              {(f.drivers?.length ?? 0) > 0 && (
                <div className="factors-group">
                  <h4 className="factors-group__title">
                    <span className="factors-group__icon">↗</span> Key Drivers
                  </h4>
                  <div className="factors-list">
                    {f.drivers!.map((driver, i) => (
                      <FactorBadge key={i} type="driver" label={driver.factor} />
                    ))}
                  </div>
                </div>
              )}
              {(f.risks?.length ?? 0) > 0 && (
                <div className="factors-group">
                  <h4 className="factors-group__title">
                    <span className="factors-group__icon">⚠</span> Risk Factors
                  </h4>
                  <div className="factors-list">
                    {f.risks!.map((risk, i) => (
                      <FactorBadge
                        key={i}
                        type="risk"
                        label={risk.factor}
                        severity={risk.severity as "high" | "medium" | "low"}
                      />
                    ))}
                  </div>
                </div>
              )}
            </div>
          </section>
        )}

        {/* Neighboring Municipalities */}
        {neighbors.length > 0 && (
          <section className="section section--neighbors">
            <div className="section__header">
              <h2 className="section__title">Neighboring Comuni</h2>
              <span className="section__subtitle">Compare with adjacent municipalities</span>
            </div>
            <div className="neighbors-grid">
              {neighbors.map((n) => (
                <Link
                  key={n.municipalityId}
                  href={`/municipality/${n.municipalityId}`}
                  className="neighbor-card"
                >
                  <span className="neighbor-card__name">{n.name}</span>
                  <span className="neighbor-card__value">
                    {n.valueMidEurSqm ? formatCurrency(n.valueMidEurSqm) : "—"}
                    <span className="neighbor-card__unit">/m²</span>
                  </span>
                  <span className="neighbor-card__border">
                    {formatNumber(n.sharedBorderKm, 1)} km border
                  </span>
                </Link>
              ))}
            </div>
          </section>
        )}
      </main>

      {/* Footer */}
      <footer className="footer">
        <div className="footer__content">
          <p className="footer__disclaimer">
            Data sourced from Agenzia delle Entrate (OMI) and ISTAT.
            Forecasts are model-generated estimates and should not be considered financial advice.
          </p>
          {f?.modelVersion && (
            <span className="footer__version">Model: {f.modelVersion}</span>
          )}
        </div>
      </footer>

      <style jsx>{`
        .municipality-page {
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
          text-decoration: none;
          background: rgba(196, 120, 92, 0.1);
          border: 1px solid rgba(196, 120, 92, 0.2);
          border-radius: 8px;
          transition: all 0.2s;
        }

        .nav__btn:hover {
          background: rgba(196, 120, 92, 0.2);
          border-color: rgba(196, 120, 92, 0.4);
        }

        /* Hero */
        .hero {
          position: relative;
          padding: 80px 48px 60px;
          background: linear-gradient(180deg, #161920 0%, #0d0f12 100%);
          border-bottom: 1px solid rgba(255, 255, 255, 0.04);
          overflow: hidden;
        }

        .hero__pattern {
          position: absolute;
          inset: 0;
          background-image:
            radial-gradient(circle at 20% 30%, rgba(196, 120, 92, 0.08) 0%, transparent 50%),
            radial-gradient(circle at 80% 70%, rgba(82, 139, 153, 0.06) 0%, transparent 50%);
          pointer-events: none;
        }

        .hero__content {
          position: relative;
          max-width: 800px;
          margin-bottom: 48px;
        }

        .hero__badges {
          display: flex;
          gap: 8px;
          margin-bottom: 16px;
        }

        .hero__badge {
          padding: 6px 14px;
          font-size: 0.7rem;
          font-weight: 600;
          text-transform: uppercase;
          letter-spacing: 0.1em;
          border-radius: 20px;
        }

        .hero__badge--coastal {
          background: rgba(82, 139, 153, 0.2);
          color: #7cc4d4;
          border: 1px solid rgba(82, 139, 153, 0.3);
        }

        .hero__badge--mountain {
          background: rgba(139, 155, 180, 0.15);
          color: #a8b3c7;
          border: 1px solid rgba(139, 155, 180, 0.25);
        }

        .hero__title {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 3.5rem;
          font-weight: 600;
          margin: 0 0 8px;
          letter-spacing: -0.01em;
          line-height: 1.1;
          animation: fadeInUp 0.6s ease forwards;
        }

        .hero__subtitle {
          font-size: 1.1rem;
          color: #8b9bb4;
          margin: 0 0 8px;
          animation: fadeInUp 0.6s ease 0.1s forwards;
          opacity: 0;
        }

        .hero__area {
          font-size: 0.85rem;
          color: #5a6677;
          animation: fadeInUp 0.6s ease 0.2s forwards;
          opacity: 0;
        }

        .hero__scores {
          display: flex;
          gap: 48px;
          align-items: flex-end;
        }

        .hero__metric {
          display: flex;
          flex-direction: column;
          gap: 4px;
          opacity: 0;
          animation: fadeInUp 0.6s ease forwards;
        }

        .hero__metric-value {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 2rem;
          font-weight: 600;
          color: #f0f2f5;
        }

        .hero__metric-label {
          font-size: 0.7rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.1em;
        }

        /* Main Content */
        .main {
          max-width: 1200px;
          margin: 0 auto;
          padding: 48px 32px;
        }

        .section {
          margin-bottom: 56px;
        }

        .section__header {
          margin-bottom: 24px;
        }

        .section__title {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 1.75rem;
          font-weight: 600;
          margin: 0 0 4px;
          color: #f0f2f5;
        }

        .section__subtitle {
          font-size: 0.85rem;
          color: #6b7a90;
        }

        /* Values Grid */
        .values-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
          gap: 20px;
        }

        .value-card {
          padding: 24px;
          background: linear-gradient(165deg, rgba(26, 29, 35, 0.8) 0%, rgba(22, 25, 32, 0.9) 100%);
          border: 1px solid rgba(255, 255, 255, 0.06);
          border-radius: 16px;
          display: flex;
          flex-direction: column;
          gap: 8px;
        }

        .value-card--primary {
          background: linear-gradient(165deg, rgba(196, 120, 92, 0.15) 0%, rgba(22, 25, 32, 0.9) 100%);
          border-color: rgba(196, 120, 92, 0.2);
        }

        .value-card__label {
          font-size: 0.75rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.08em;
        }

        .value-card__value {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 2.25rem;
          font-weight: 600;
          color: #f0f2f5;
        }

        .value-card__unit {
          font-size: 1rem;
          color: #8b9bb4;
          margin-left: 4px;
        }

        .value-card__trend {
          font-size: 0.8rem;
          font-weight: 500;
        }

        .value-card__trend--up { color: #4ade80; }
        .value-card__trend--down { color: #f87171; }
        .value-card__trend--stable { color: #8b9bb4; }

        .chart-card {
          padding: 24px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 16px;
          grid-column: span 2;
        }

        .chart-card--small {
          grid-column: span 1;
        }

        .chart-card__title {
          display: block;
          font-size: 0.75rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.08em;
          margin-bottom: 16px;
        }

        .chart-card__empty {
          height: 80px;
          display: flex;
          align-items: center;
          justify-content: center;
          color: #5a6677;
          font-size: 0.85rem;
        }

        .chart-card__labels {
          display: flex;
          justify-content: space-between;
          margin-top: 8px;
          font-size: 0.7rem;
          color: #5a6677;
        }

        /* Market Grid */
        .market-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
          gap: 16px;
        }

        .stat-card {
          display: flex;
          align-items: center;
          gap: 16px;
          padding: 20px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 12px;
          transition: all 0.2s;
        }

        .stat-card:hover {
          background: rgba(26, 29, 35, 0.8);
          border-color: rgba(255, 255, 255, 0.08);
        }

        .stat-card__icon {
          font-size: 1.5rem;
          opacity: 0.5;
        }

        .stat-card__content {
          display: flex;
          flex-direction: column;
          gap: 2px;
        }

        .stat-card__value {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 1.5rem;
          font-weight: 600;
          color: #f0f2f5;
        }

        .stat-card__label {
          font-size: 0.7rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.06em;
        }

        /* Demographics */
        .demo-grid {
          display: grid;
          grid-template-columns: 1fr 1.5fr 1fr;
          gap: 24px;
        }

        .demo-card--main {
          padding: 28px;
          background: linear-gradient(165deg, rgba(82, 139, 153, 0.12) 0%, rgba(22, 25, 32, 0.8) 100%);
          border: 1px solid rgba(82, 139, 153, 0.2);
          border-radius: 16px;
        }

        .demo-card__header {
          margin-bottom: 16px;
        }

        .demo-card__value {
          display: block;
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 2.5rem;
          font-weight: 600;
          color: #f0f2f5;
          line-height: 1;
        }

        .demo-card__label {
          font-size: 0.75rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.08em;
          margin-top: 6px;
          display: block;
        }

        .demo-card__sub {
          display: flex;
          flex-direction: column;
          gap: 4px;
          font-size: 0.8rem;
          color: #8b9bb4;
        }

        .demo-card__sub .positive { color: #4ade80; }
        .demo-card__sub .negative { color: #f87171; }

        .demo-breakdown {
          padding: 24px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 16px;
        }

        .demo-breakdown__title {
          font-size: 0.75rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.08em;
          margin: 0 0 16px;
        }

        .demo-bar {
          display: flex;
          height: 24px;
          border-radius: 12px;
          overflow: hidden;
          background: rgba(255, 255, 255, 0.03);
        }

        .demo-bar__segment {
          height: 100%;
          transition: width 0.6s ease;
        }

        .demo-bar__segment--young { background: #7cc4d4; }
        .demo-bar__segment--working { background: #528b99; }
        .demo-bar__segment--elderly { background: #3a5a66; }

        .demo-bar__legend {
          display: flex;
          justify-content: space-between;
          margin-top: 12px;
          font-size: 0.7rem;
          color: #6b7a90;
        }

        .demo-bar__legend span {
          display: flex;
          align-items: center;
          gap: 6px;
        }

        .dot {
          display: inline-block;
          width: 8px;
          height: 8px;
          border-radius: 50%;
        }

        .dot--young { background: #7cc4d4; }
        .dot--working { background: #528b99; }
        .dot--elderly { background: #3a5a66; }

        .demo-stats {
          display: flex;
          flex-direction: column;
          gap: 16px;
        }

        .demo-stat {
          padding: 20px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 12px;
          text-align: center;
        }

        .demo-stat__value {
          display: block;
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 1.75rem;
          font-weight: 600;
          color: #f0f2f5;
        }

        .demo-stat__label {
          font-size: 0.7rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.06em;
        }

        /* Factors */
        .factors-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
          gap: 24px;
        }

        .factors-group {
          padding: 24px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 16px;
        }

        .factors-group__title {
          display: flex;
          align-items: center;
          gap: 8px;
          font-size: 0.8rem;
          font-weight: 600;
          color: #a8b3c7;
          margin: 0 0 16px;
        }

        .factors-group__icon {
          font-size: 1rem;
        }

        .factors-list {
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
        }

        /* STR Section */
        .str-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
          gap: 16px;
        }

        .str-card {
          padding: 20px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 12px;
          display: flex;
          flex-direction: column;
          gap: 6px;
        }

        .str-card--adr {
          background: linear-gradient(165deg, rgba(196, 120, 92, 0.12) 0%, rgba(22, 25, 32, 0.8) 100%);
          border-color: rgba(196, 120, 92, 0.2);
        }

        .str-card--yield {
          background: linear-gradient(165deg, rgba(74, 222, 128, 0.08) 0%, rgba(22, 25, 32, 0.8) 100%);
          border-color: rgba(74, 222, 128, 0.15);
        }

        .str-card__label {
          font-size: 0.7rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.08em;
        }

        .str-card__value {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 1.75rem;
          font-weight: 600;
          color: #f0f2f5;
        }

        .str-card__value--highlight {
          color: #4ade80;
        }

        .str-card__unit {
          font-size: 0.85rem;
          color: #8b9bb4;
          margin-left: 2px;
        }

        .str-card__sub {
          font-size: 0.8rem;
          color: #6b7a90;
        }

        .str-card__badge {
          display: inline-flex;
          align-self: flex-start;
          padding: 4px 10px;
          font-size: 0.65rem;
          font-weight: 600;
          text-transform: uppercase;
          letter-spacing: 0.08em;
          border-radius: 12px;
          margin-top: 4px;
        }

        .str-card__badge--peak {
          background: rgba(251, 191, 36, 0.15);
          color: #fbbf24;
          border: 1px solid rgba(251, 191, 36, 0.3);
        }

        /* Yield Comparison */
        .yield-comparison {
          margin-top: 32px;
          padding: 24px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 16px;
        }

        .yield-comparison__title {
          font-size: 0.8rem;
          font-weight: 600;
          color: #a8b3c7;
          margin: 0 0 20px;
        }

        .yield-comparison__bars {
          display: flex;
          flex-direction: column;
          gap: 16px;
        }

        .yield-bar {
          display: grid;
          grid-template-columns: 140px 1fr 80px;
          align-items: center;
          gap: 16px;
        }

        .yield-bar__label {
          font-size: 0.8rem;
          color: #8b9bb4;
        }

        .yield-bar__track {
          height: 24px;
          background: rgba(255, 255, 255, 0.04);
          border-radius: 12px;
          overflow: hidden;
        }

        .yield-bar__fill {
          height: 100%;
          border-radius: 12px;
          transition: width 0.6s ease;
        }

        .yield-bar__fill--ltr {
          background: linear-gradient(90deg, #528b99 0%, #7cc4d4 100%);
        }

        .yield-bar__fill--str {
          background: linear-gradient(90deg, #c4785c 0%, #e8a07a 100%);
        }

        .yield-bar__value {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 1.25rem;
          font-weight: 600;
          color: #f0f2f5;
          text-align: right;
        }

        /* Seasonality */
        .seasonality-section {
          margin-top: 32px;
          padding: 24px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 16px;
        }

        .seasonality-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 16px;
        }

        .seasonality-header__title {
          font-size: 0.8rem;
          font-weight: 600;
          color: #a8b3c7;
          margin: 0;
        }

        .seasonality-header__score {
          font-size: 0.75rem;
          color: #6b7a90;
        }

        .seasonality-charts {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
          gap: 24px;
        }

        .season-months {
          display: flex;
          flex-wrap: wrap;
          gap: 16px;
          margin-top: 20px;
          padding-top: 16px;
          border-top: 1px solid rgba(255, 255, 255, 0.04);
        }

        .season-months__group {
          display: flex;
          align-items: center;
          gap: 8px;
        }

        .season-months__label {
          font-size: 0.7rem;
          font-weight: 600;
          text-transform: uppercase;
          letter-spacing: 0.06em;
          padding: 4px 8px;
          border-radius: 4px;
        }

        .season-months__label--peak {
          background: rgba(248, 113, 113, 0.15);
          color: #f87171;
        }

        .season-months__label--shoulder {
          background: rgba(251, 191, 36, 0.15);
          color: #fbbf24;
        }

        .season-months__label--offpeak {
          background: rgba(82, 139, 153, 0.15);
          color: #7cc4d4;
        }

        .season-months__list {
          font-size: 0.8rem;
          color: #8b9bb4;
        }

        /* Regulations Section */
        .regulations-content {
          display: flex;
          flex-direction: column;
          gap: 24px;
        }

        .regulation-overview {
          display: flex;
          gap: 24px;
          align-items: flex-start;
          flex-wrap: wrap;
        }

        .regulation-score {
          padding: 24px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 16px;
          display: flex;
          flex-direction: column;
          gap: 8px;
          min-width: 180px;
        }

        .regulation-score__label {
          font-size: 0.7rem;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.08em;
        }

        .regulation-score__level {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 1.5rem;
          font-weight: 600;
          text-transform: capitalize;
        }

        .regulation-score__level--low { color: #4ade80; }
        .regulation-score__level--medium { color: #fbbf24; }
        .regulation-score__level--high { color: #f87171; }
        .regulation-score__level--none { color: #8b9bb4; }

        .regulation-score__value {
          font-size: 0.75rem;
          color: #6b7a90;
        }

        .investor-warning {
          flex: 1;
          display: flex;
          align-items: center;
          gap: 12px;
          padding: 20px 24px;
          border-radius: 12px;
          min-width: 280px;
        }

        .investor-warning--high {
          background: rgba(248, 113, 113, 0.1);
          border: 1px solid rgba(248, 113, 113, 0.2);
        }

        .investor-warning--medium {
          background: rgba(251, 191, 36, 0.1);
          border: 1px solid rgba(251, 191, 36, 0.2);
        }

        .investor-warning--low {
          background: rgba(74, 222, 128, 0.1);
          border: 1px solid rgba(74, 222, 128, 0.2);
        }

        .investor-warning__icon {
          font-size: 1.5rem;
        }

        .investor-warning--high .investor-warning__icon { color: #f87171; }
        .investor-warning--medium .investor-warning__icon { color: #fbbf24; }
        .investor-warning--low .investor-warning__icon { color: #4ade80; }

        .investor-warning__text {
          font-size: 0.9rem;
          color: #a8b3c7;
        }

        .regulation-details {
          padding: 24px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 16px;
        }

        .regulation-details__title {
          font-size: 0.75rem;
          font-weight: 600;
          color: #8b9bb4;
          text-transform: uppercase;
          letter-spacing: 0.06em;
          margin: 0 0 16px;
        }

        .regulation-badges {
          display: flex;
          flex-wrap: wrap;
          gap: 10px;
        }

        .regulation-factors {
          padding: 24px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 16px;
        }

        .investor-notes {
          padding: 24px;
          background: rgba(82, 139, 153, 0.08);
          border: 1px solid rgba(82, 139, 153, 0.15);
          border-radius: 16px;
        }

        .investor-notes__title {
          font-size: 0.75rem;
          font-weight: 600;
          color: #7cc4d4;
          text-transform: uppercase;
          letter-spacing: 0.06em;
          margin: 0 0 12px;
        }

        .investor-notes__text {
          font-size: 0.9rem;
          color: #a8b3c7;
          line-height: 1.6;
          margin: 0;
        }

        /* Neighbors */
        .neighbors-grid {
          display: grid;
          grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
          gap: 16px;
        }

        .neighbor-card {
          display: flex;
          flex-direction: column;
          gap: 6px;
          padding: 20px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.04);
          border-radius: 12px;
          text-decoration: none;
          transition: all 0.2s;
        }

        .neighbor-card:hover {
          background: rgba(26, 29, 35, 0.9);
          border-color: rgba(196, 120, 92, 0.3);
          transform: translateY(-2px);
        }

        .neighbor-card__name {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 1.1rem;
          font-weight: 600;
          color: #f0f2f5;
        }

        .neighbor-card__value {
          font-size: 1rem;
          color: #c4785c;
        }

        .neighbor-card__unit {
          font-size: 0.75rem;
          color: #6b7a90;
        }

        .neighbor-card__border {
          font-size: 0.7rem;
          color: #5a6677;
        }

        /* Footer */
        .footer {
          margin-top: 48px;
          padding: 32px;
          border-top: 1px solid rgba(255, 255, 255, 0.04);
          background: rgba(13, 15, 18, 0.5);
        }

        .footer__content {
          max-width: 1200px;
          margin: 0 auto;
          display: flex;
          justify-content: space-between;
          align-items: center;
        }

        .footer__disclaimer {
          font-size: 0.75rem;
          color: #5a6677;
          max-width: 600px;
          margin: 0;
        }

        .footer__version {
          font-size: 0.7rem;
          color: #3a4556;
          font-family: monospace;
        }

        /* Responsive */
        @media (max-width: 1024px) {
          .demo-grid {
            grid-template-columns: 1fr 1fr;
          }

          .demo-breakdown {
            grid-column: span 2;
          }
        }

        @media (max-width: 768px) {
          .hero {
            padding: 48px 24px;
          }

          .hero__title {
            font-size: 2.5rem;
          }

          .hero__scores {
            flex-wrap: wrap;
            gap: 32px;
          }

          .main {
            padding: 32px 20px;
          }

          .demo-grid {
            grid-template-columns: 1fr;
          }

          .demo-breakdown {
            grid-column: span 1;
          }

          .chart-card {
            grid-column: span 1;
          }

          .nav {
            padding: 0 16px;
          }

          .nav__breadcrumb {
            display: none;
          }

          .footer__content {
            flex-direction: column;
            gap: 16px;
            text-align: center;
          }

          .yield-bar {
            grid-template-columns: 100px 1fr 60px;
            gap: 8px;
          }

          .yield-bar__label {
            font-size: 0.7rem;
          }

          .regulation-overview {
            flex-direction: column;
          }

          .regulation-score {
            width: 100%;
          }

          .investor-warning {
            min-width: unset;
          }
        }
      `}</style>
    </div>
  );
}
