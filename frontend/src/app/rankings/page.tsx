"use client";

import { useEffect, useState, useCallback } from "react";
import Link from "next/link";

interface RankingEntry {
  rank: number;
  municipalityId: string;
  name: string;
  regionCode: string | null;
  regionName: string | null;
  provinceCode: string | null;
  provinceName: string | null;
  isCoastal: boolean;
  isMountain: boolean;
  valueMidEurSqm: number | null;
  appreciationPct: number | null;
  grossYieldPct: number | null;
  opportunityScore: number | null;
  confidenceScore: number | null;
}

interface RankingsResponse {
  rankings: RankingEntry[];
  pagination: {
    total: number;
    limit: number;
    offset: number;
    hasMore: boolean;
  };
  meta: {
    sortBy: string;
    sortOrder: string;
    latestDate: string | null;
  };
}

type SortField = "opportunity_score" | "forecast_appreciation_pct" | "forecast_gross_yield_pct" | "value_mid_eur_sqm" | "confidence_score";

const SORT_OPTIONS: { value: SortField; label: string }[] = [
  { value: "opportunity_score", label: "Opportunity Score" },
  { value: "forecast_appreciation_pct", label: "Appreciation" },
  { value: "forecast_gross_yield_pct", label: "Gross Yield" },
  { value: "value_mid_eur_sqm", label: "Property Value" },
  { value: "confidence_score", label: "Confidence" },
];

function formatCurrency(value: number | null): string {
  if (value == null) return "—";
  return `€${Math.round(value).toLocaleString("it-IT")}`;
}

function formatPercent(value: number | null, showSign = true): string {
  if (value == null) return "—";
  const sign = showSign && value > 0 ? "+" : "";
  return `${sign}${value.toFixed(1)}%`;
}

function ScoreBadge({ value, type }: { value: number | null; type: "opportunity" | "confidence" }) {
  if (value == null) return <span className="score-badge score-badge--empty">—</span>;

  const getColor = () => {
    if (type === "opportunity") {
      if (value >= 75) return "high";
      if (value >= 50) return "medium";
      return "low";
    }
    if (value >= 70) return "high";
    if (value >= 40) return "medium";
    return "low";
  };

  return (
    <span className={`score-badge score-badge--${getColor()}`}>
      {Math.round(value)}
      <style jsx>{`
        .score-badge {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-width: 42px;
          padding: 4px 10px;
          font-size: 0.8rem;
          font-weight: 600;
          font-variant-numeric: tabular-nums;
          border-radius: 6px;
        }
        .score-badge--empty {
          color: #5a6677;
        }
        .score-badge--high {
          background: rgba(74, 222, 128, 0.15);
          color: #4ade80;
        }
        .score-badge--medium {
          background: rgba(251, 191, 36, 0.15);
          color: #fbbf24;
        }
        .score-badge--low {
          background: rgba(248, 113, 113, 0.12);
          color: #f87171;
        }
      `}</style>
    </span>
  );
}

export default function RankingsPage() {
  const [rankings, setRankings] = useState<RankingEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sortBy, setSortBy] = useState<SortField>("opportunity_score");
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc");
  const [minConfidence, setMinConfidence] = useState(0);
  const [page, setPage] = useState(0);
  const [totalCount, setTotalCount] = useState(0);
  const [latestDate, setLatestDate] = useState<string | null>(null);
  const limit = 25;

  const fetchRankings = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        sortBy,
        sortOrder,
        limit: String(limit),
        offset: String(page * limit),
        minConfidence: String(minConfidence),
      });

      const response = await fetch(`/api/rankings?${params}`);
      if (!response.ok) throw new Error("Failed to fetch rankings");

      const data: RankingsResponse = await response.json();
      setRankings(data.rankings);
      setTotalCount(data.pagination.total);
      setLatestDate(data.meta.latestDate);
    } catch {
      setError("Failed to load rankings. Please try again.");
    } finally {
      setLoading(false);
    }
  }, [sortBy, sortOrder, page, minConfidence]);

  useEffect(() => {
    fetchRankings();
  }, [fetchRankings]);

  const handleSort = (field: SortField) => {
    if (sortBy === field) {
      setSortOrder(sortOrder === "desc" ? "asc" : "desc");
    } else {
      setSortBy(field);
      setSortOrder("desc");
    }
    setPage(0);
  };

  const totalPages = Math.ceil(totalCount / limit);

  const exportCSV = () => {
    const headers = ["Rank", "Name", "Province", "Region", "Value (€/m²)", "Appreciation (%)", "Yield (%)", "Opportunity", "Confidence"];
    const rows = rankings.map((r) => [
      r.rank,
      r.name,
      r.provinceName || "",
      r.regionName || "",
      r.valueMidEurSqm || "",
      r.appreciationPct || "",
      r.grossYieldPct || "",
      r.opportunityScore || "",
      r.confidenceScore || "",
    ]);

    const csv = [headers, ...rows].map((row) => row.join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `italian-property-rankings-${new Date().toISOString().split("T")[0]}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="rankings-page">
      {/* Navigation */}
      <nav className="nav">
        <Link href="/" className="nav__logo">
          <span className="nav__logo-icon">◆</span>
          <span className="nav__logo-text">Italia Immobiliare</span>
        </Link>
        <div className="nav__links">
          <Link href="/map" className="nav__link">Map</Link>
          <Link href="/rankings" className="nav__link nav__link--active">Rankings</Link>
          <Link href="/methodology" className="nav__link">Methodology</Link>
        </div>
      </nav>

      {/* Header */}
      <header className="header">
        <div className="header__content">
          <h1 className="header__title">Municipality Rankings</h1>
          <p className="header__subtitle">
            Investment opportunity scores for Italian comuni
            {latestDate && <span className="header__date"> · Data as of {latestDate}</span>}
          </p>
        </div>
        <div className="header__actions">
          <button onClick={exportCSV} className="header__btn header__btn--secondary">
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
              <path d="M14 10V12.6667C14 13.0203 13.8595 13.3594 13.6095 13.6095C13.3594 13.8595 13.0203 14 12.6667 14H3.33333C2.97971 14 2.64057 13.8595 2.39052 13.6095C2.14048 13.3594 2 13.0203 2 12.6667V10" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
              <polyline points="4.67 6.67 8 10 11.33 6.67" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
              <line x1="8" y1="10" x2="8" y2="2" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
            Export CSV
          </button>
        </div>
      </header>

      {/* Filters */}
      <div className="filters">
        <div className="filters__group">
          <label className="filters__label">Sort By</label>
          <div className="filters__pills">
            {SORT_OPTIONS.map((option) => (
              <button
                key={option.value}
                onClick={() => handleSort(option.value)}
                className={`filters__pill ${sortBy === option.value ? "filters__pill--active" : ""}`}
              >
                {option.label}
                {sortBy === option.value && (
                  <span className="filters__pill-arrow">
                    {sortOrder === "desc" ? "↓" : "↑"}
                  </span>
                )}
              </button>
            ))}
          </div>
        </div>

        <div className="filters__group">
          <label className="filters__label">
            Min Confidence: {minConfidence}%
          </label>
          <input
            type="range"
            min="0"
            max="80"
            step="10"
            value={minConfidence}
            onChange={(e) => {
              setMinConfidence(Number(e.target.value));
              setPage(0);
            }}
            className="filters__slider"
          />
        </div>
      </div>

      {/* Table */}
      <main className="main">
        {error ? (
          <div className="error-state">
            <span className="error-icon">⚠</span>
            <p>{error}</p>
            <button onClick={fetchRankings} className="retry-btn">
              Try Again
            </button>
          </div>
        ) : (
          <>
            <div className="table-container">
              <table className="table">
                <thead>
                  <tr>
                    <th className="table__th table__th--rank">#</th>
                    <th className="table__th table__th--name">Municipality</th>
                    <th className="table__th table__th--region">Region</th>
                    <th
                      className={`table__th table__th--sortable ${sortBy === "value_mid_eur_sqm" ? "table__th--sorted" : ""}`}
                      onClick={() => handleSort("value_mid_eur_sqm")}
                    >
                      Value
                      {sortBy === "value_mid_eur_sqm" && <span className="sort-arrow">{sortOrder === "desc" ? "↓" : "↑"}</span>}
                    </th>
                    <th
                      className={`table__th table__th--sortable ${sortBy === "forecast_appreciation_pct" ? "table__th--sorted" : ""}`}
                      onClick={() => handleSort("forecast_appreciation_pct")}
                    >
                      Appreciation
                      {sortBy === "forecast_appreciation_pct" && <span className="sort-arrow">{sortOrder === "desc" ? "↓" : "↑"}</span>}
                    </th>
                    <th
                      className={`table__th table__th--sortable ${sortBy === "forecast_gross_yield_pct" ? "table__th--sorted" : ""}`}
                      onClick={() => handleSort("forecast_gross_yield_pct")}
                    >
                      Yield
                      {sortBy === "forecast_gross_yield_pct" && <span className="sort-arrow">{sortOrder === "desc" ? "↓" : "↑"}</span>}
                    </th>
                    <th
                      className={`table__th table__th--sortable ${sortBy === "opportunity_score" ? "table__th--sorted" : ""}`}
                      onClick={() => handleSort("opportunity_score")}
                    >
                      Opportunity
                      {sortBy === "opportunity_score" && <span className="sort-arrow">{sortOrder === "desc" ? "↓" : "↑"}</span>}
                    </th>
                    <th
                      className={`table__th table__th--sortable ${sortBy === "confidence_score" ? "table__th--sorted" : ""}`}
                      onClick={() => handleSort("confidence_score")}
                    >
                      Confidence
                      {sortBy === "confidence_score" && <span className="sort-arrow">{sortOrder === "desc" ? "↓" : "↑"}</span>}
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {loading ? (
                    Array.from({ length: 10 }).map((_, i) => (
                      <tr key={i} className="table__row table__row--loading">
                        <td colSpan={8}>
                          <div className="skeleton" />
                        </td>
                      </tr>
                    ))
                  ) : rankings.length === 0 ? (
                    <tr>
                      <td colSpan={8} className="table__empty">
                        No municipalities found matching your criteria
                      </td>
                    </tr>
                  ) : (
                    rankings.map((r, index) => (
                      <tr key={r.municipalityId} className="table__row" style={{ animationDelay: `${index * 20}ms` }}>
                        <td className="table__td table__td--rank">
                          <span className={`rank-badge ${r.rank <= 3 ? "rank-badge--top" : ""}`}>
                            {r.rank}
                          </span>
                        </td>
                        <td className="table__td table__td--name">
                          <Link href={`/municipality/${r.municipalityId}`} className="table__link">
                            {r.name}
                            <div className="table__badges">
                              {r.isCoastal && <span className="mini-badge mini-badge--coastal" title="Coastal">🌊</span>}
                              {r.isMountain && <span className="mini-badge mini-badge--mountain" title="Mountain">⛰️</span>}
                            </div>
                          </Link>
                          <span className="table__province">{r.provinceName || r.provinceCode}</span>
                        </td>
                        <td className="table__td table__td--region">
                          {r.regionName || r.regionCode || "—"}
                        </td>
                        <td className="table__td table__td--value">
                          {formatCurrency(r.valueMidEurSqm)}
                          <span className="table__unit">/m²</span>
                        </td>
                        <td className="table__td table__td--percent">
                          <span className={`percent-value ${(r.appreciationPct ?? 0) >= 0 ? "positive" : "negative"}`}>
                            {formatPercent(r.appreciationPct)}
                          </span>
                        </td>
                        <td className="table__td table__td--percent">
                          {formatPercent(r.grossYieldPct, false)}
                        </td>
                        <td className="table__td table__td--score">
                          <ScoreBadge value={r.opportunityScore} type="opportunity" />
                        </td>
                        <td className="table__td table__td--score">
                          <ScoreBadge value={r.confidenceScore} type="confidence" />
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>

            {/* Pagination */}
            {totalPages > 1 && (
              <div className="pagination">
                <button
                  onClick={() => setPage(Math.max(0, page - 1))}
                  disabled={page === 0}
                  className="pagination__btn"
                >
                  ← Previous
                </button>
                <span className="pagination__info">
                  Page {page + 1} of {totalPages}
                  <span className="pagination__count"> · {totalCount} municipalities</span>
                </span>
                <button
                  onClick={() => setPage(Math.min(totalPages - 1, page + 1))}
                  disabled={page >= totalPages - 1}
                  className="pagination__btn"
                >
                  Next →
                </button>
              </div>
            )}
          </>
        )}
      </main>

      <style jsx>{`
        .rankings-page {
          min-height: 100vh;
          background: #0d0f12;
          color: #f0f2f5;
        }

        /* Navigation */
        .nav {
          display: flex;
          align-items: center;
          justify-content: space-between;
          padding: 0 32px;
          height: 64px;
          background: linear-gradient(180deg, rgba(22, 25, 32, 0.98) 0%, rgba(13, 15, 18, 0.95) 100%);
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

        .nav__links {
          display: flex;
          gap: 8px;
        }

        .nav__link {
          padding: 8px 16px;
          font-size: 0.8rem;
          font-weight: 500;
          color: #8b9bb4;
          text-decoration: none;
          border-radius: 8px;
          transition: all 0.2s;
        }

        .nav__link:hover {
          color: #d0d7e2;
          background: rgba(255, 255, 255, 0.04);
        }

        .nav__link--active {
          color: #f0f2f5;
          background: rgba(196, 120, 92, 0.15);
        }

        /* Header */
        .header {
          display: flex;
          align-items: flex-end;
          justify-content: space-between;
          padding: 48px 32px 32px;
          background: linear-gradient(180deg, #161920 0%, #0d0f12 100%);
          border-bottom: 1px solid rgba(255, 255, 255, 0.04);
        }

        .header__title {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 2.5rem;
          font-weight: 600;
          margin: 0 0 8px;
          color: #f0f2f5;
        }

        .header__subtitle {
          font-size: 0.95rem;
          color: #6b7a90;
          margin: 0;
        }

        .header__date {
          color: #5a6677;
        }

        .header__btn {
          display: inline-flex;
          align-items: center;
          gap: 8px;
          padding: 10px 18px;
          font-size: 0.85rem;
          font-weight: 500;
          border-radius: 8px;
          cursor: pointer;
          transition: all 0.2s;
        }

        .header__btn--secondary {
          background: rgba(255, 255, 255, 0.04);
          border: 1px solid rgba(255, 255, 255, 0.1);
          color: #a8b3c7;
        }

        .header__btn--secondary:hover {
          background: rgba(255, 255, 255, 0.08);
          color: #f0f2f5;
        }

        /* Filters */
        .filters {
          display: flex;
          align-items: flex-end;
          gap: 32px;
          padding: 24px 32px;
          background: rgba(22, 25, 32, 0.5);
          border-bottom: 1px solid rgba(255, 255, 255, 0.04);
        }

        .filters__group {
          display: flex;
          flex-direction: column;
          gap: 10px;
        }

        .filters__label {
          font-size: 0.7rem;
          font-weight: 500;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.08em;
        }

        .filters__pills {
          display: flex;
          gap: 8px;
          flex-wrap: wrap;
        }

        .filters__pill {
          display: inline-flex;
          align-items: center;
          gap: 6px;
          padding: 8px 14px;
          font-size: 0.8rem;
          font-weight: 500;
          color: #8b9bb4;
          background: rgba(255, 255, 255, 0.04);
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 20px;
          cursor: pointer;
          transition: all 0.2s;
        }

        .filters__pill:hover {
          background: rgba(255, 255, 255, 0.08);
          color: #d0d7e2;
        }

        .filters__pill--active {
          background: rgba(196, 120, 92, 0.2);
          border-color: rgba(196, 120, 92, 0.4);
          color: #e8c4a0;
        }

        .filters__pill-arrow {
          font-size: 0.7rem;
        }

        .filters__slider {
          width: 200px;
          height: 6px;
          -webkit-appearance: none;
          appearance: none;
          background: rgba(255, 255, 255, 0.1);
          border-radius: 3px;
          cursor: pointer;
        }

        .filters__slider::-webkit-slider-thumb {
          -webkit-appearance: none;
          appearance: none;
          width: 16px;
          height: 16px;
          background: #c4785c;
          border-radius: 50%;
          cursor: pointer;
        }

        /* Main */
        .main {
          padding: 24px 32px 48px;
        }

        .table-container {
          overflow-x: auto;
          border-radius: 12px;
          border: 1px solid rgba(255, 255, 255, 0.06);
          background: rgba(22, 25, 32, 0.5);
        }

        .table {
          width: 100%;
          border-collapse: collapse;
          font-size: 0.85rem;
        }

        .table__th {
          padding: 16px 20px;
          text-align: left;
          font-size: 0.7rem;
          font-weight: 600;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.08em;
          background: rgba(22, 25, 32, 0.8);
          border-bottom: 1px solid rgba(255, 255, 255, 0.06);
          white-space: nowrap;
        }

        .table__th--sortable {
          cursor: pointer;
          transition: color 0.2s;
        }

        .table__th--sortable:hover {
          color: #a8b3c7;
        }

        .table__th--sorted {
          color: #c4785c;
        }

        .sort-arrow {
          margin-left: 4px;
        }

        .table__th--rank {
          width: 60px;
          text-align: center;
        }

        .table__th--name {
          min-width: 200px;
        }

        .table__row {
          opacity: 0;
          animation: fadeIn 0.3s ease forwards;
        }

        @keyframes fadeIn {
          from { opacity: 0; }
          to { opacity: 1; }
        }

        .table__row:hover {
          background: rgba(255, 255, 255, 0.02);
        }

        .table__row--loading td {
          padding: 16px 20px;
        }

        .skeleton {
          height: 20px;
          background: linear-gradient(90deg, rgba(255,255,255,0.04) 0%, rgba(255,255,255,0.08) 50%, rgba(255,255,255,0.04) 100%);
          background-size: 200% 100%;
          animation: shimmer 1.5s infinite;
          border-radius: 4px;
        }

        @keyframes shimmer {
          0% { background-position: 200% 0; }
          100% { background-position: -200% 0; }
        }

        .table__td {
          padding: 14px 20px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.03);
          vertical-align: middle;
        }

        .table__td--rank {
          text-align: center;
        }

        .rank-badge {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          width: 32px;
          height: 32px;
          font-size: 0.85rem;
          font-weight: 600;
          color: #8b9bb4;
          background: rgba(255, 255, 255, 0.04);
          border-radius: 8px;
        }

        .rank-badge--top {
          background: linear-gradient(135deg, rgba(196, 120, 92, 0.3) 0%, rgba(196, 120, 92, 0.1) 100%);
          color: #e8c4a0;
        }

        .table__link {
          display: flex;
          align-items: center;
          gap: 8px;
          font-weight: 500;
          color: #f0f2f5;
          text-decoration: none;
          transition: color 0.2s;
        }

        .table__link:hover {
          color: #c4785c;
        }

        .table__badges {
          display: flex;
          gap: 4px;
        }

        .mini-badge {
          font-size: 0.75rem;
        }

        .table__province {
          display: block;
          font-size: 0.75rem;
          color: #5a6677;
          margin-top: 2px;
        }

        .table__td--value {
          font-variant-numeric: tabular-nums;
        }

        .table__unit {
          color: #5a6677;
          font-size: 0.75rem;
          margin-left: 2px;
        }

        .table__td--percent {
          font-variant-numeric: tabular-nums;
        }

        .percent-value {
          font-weight: 500;
        }

        .percent-value.positive {
          color: #4ade80;
        }

        .percent-value.negative {
          color: #f87171;
        }

        .table__empty {
          text-align: center;
          padding: 48px 20px;
          color: #6b7a90;
        }

        /* Pagination */
        .pagination {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: 24px;
          margin-top: 24px;
        }

        .pagination__btn {
          padding: 10px 18px;
          font-size: 0.85rem;
          font-weight: 500;
          color: #a8b3c7;
          background: rgba(255, 255, 255, 0.04);
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 8px;
          cursor: pointer;
          transition: all 0.2s;
        }

        .pagination__btn:hover:not(:disabled) {
          background: rgba(255, 255, 255, 0.08);
          color: #f0f2f5;
        }

        .pagination__btn:disabled {
          opacity: 0.4;
          cursor: not-allowed;
        }

        .pagination__info {
          font-size: 0.85rem;
          color: #8b9bb4;
        }

        .pagination__count {
          color: #5a6677;
        }

        /* Error State */
        .error-state {
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          padding: 80px 20px;
          text-align: center;
        }

        .error-icon {
          font-size: 3rem;
          margin-bottom: 16px;
          opacity: 0.5;
        }

        .error-state p {
          color: #8b9bb4;
          margin: 0 0 20px;
        }

        .retry-btn {
          padding: 10px 20px;
          font-size: 0.85rem;
          font-weight: 500;
          color: #c4785c;
          background: rgba(196, 120, 92, 0.1);
          border: 1px solid rgba(196, 120, 92, 0.3);
          border-radius: 8px;
          cursor: pointer;
          transition: all 0.2s;
        }

        .retry-btn:hover {
          background: rgba(196, 120, 92, 0.2);
        }

        /* Responsive */
        @media (max-width: 768px) {
          .header {
            flex-direction: column;
            align-items: flex-start;
            gap: 20px;
            padding: 32px 20px;
          }

          .header__title {
            font-size: 1.75rem;
          }

          .filters {
            flex-direction: column;
            align-items: flex-start;
            gap: 20px;
            padding: 20px;
          }

          .main {
            padding: 20px 16px 48px;
          }

          .nav {
            padding: 0 16px;
          }

          .nav__links {
            display: none;
          }
        }
      `}</style>
    </div>
  );
}
