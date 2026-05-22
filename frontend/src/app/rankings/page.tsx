"use client";

import { useEffect, useState, useCallback, useRef } from "react";
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
  grossYieldPct: number | null;
  annualizedPriceChangePct: number | null;
  salesPer1000Pop: number | null;
  dataQualityScore: number | null;
  zonesWithData: number | null;
}

interface SearchResult {
  municipalityId: string;
  name: string;
  rank: number;
  page: number;
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
    latestPeriod: string | null;
    earliestPeriod: string | null;
    periodsIncluded: string[];
    segment: string;
    filters: {
      region: string | null;
      province: string | null;
      minConfidence: number;
    };
  };
  searchResult: SearchResult | null;
}

interface RegionOption {
  code: string;
  name: string;
}

interface ProvinceOption {
  code: string;
  name: string;
  regionCode: string;
}

type SortField =
  | "value_mid_eur_sqm"
  | "gross_yield_pct"
  | "annualized_price_change_pct"
  | "ntn_per_1000_pop"
  | "data_quality_score";

const SORT_OPTIONS: { value: SortField; label: string }[] = [
  { value: "value_mid_eur_sqm", label: "Property Value" },
  { value: "gross_yield_pct", label: "Gross Yield" },
  { value: "annualized_price_change_pct", label: "Price Change" },
  { value: "ntn_per_1000_pop", label: "Sales Activity" },
  { value: "data_quality_score", label: "Data Quality" },
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

function formatNumber(value: number | null, decimals = 1): string {
  if (value == null) return "—";
  return value.toFixed(decimals);
}

function formatPeriod(period: string): string {
  // "2025S1" -> "H1 2025"
  const match = period.match(/(\d{4})S(\d)/);
  if (!match) return period;
  return `H${match[2]} ${match[1]}`;
}

function ScoreBadge({ value, type }: { value: number | null; type: "quality" }) {
  if (value == null) return <span className="score-badge score-badge--empty">—</span>;

  const getColor = () => {
    if (type === "quality") {
      if (value >= 70) return "high";
      if (value >= 40) return "medium";
      return "low";
    }
    return "medium";
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
  const [sortBy, setSortBy] = useState<SortField>("value_mid_eur_sqm");
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc");
  const [minConfidence, setMinConfidence] = useState(0);
  const [page, setPage] = useState(0);
  const [totalCount, setTotalCount] = useState(0);
  const [latestPeriod, setLatestPeriod] = useState<string | null>(null);
  const [earliestPeriod, setEarliestPeriod] = useState<string | null>(null);
  const [periodsIncluded, setPeriodsIncluded] = useState<string[]>([]);

  // New filter state
  const [regions, setRegions] = useState<RegionOption[]>([]);
  const [provinces, setProvinces] = useState<ProvinceOption[]>([]);
  const [selectedRegion, setSelectedRegion] = useState<string | null>(null);
  const [selectedProvince, setSelectedProvince] = useState<string | null>(null);
  const [semestersToAverage, setSemestersToAverage] = useState(2);

  // Search state
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResult, setSearchResult] = useState<SearchResult | null>(null);
  const [isSearching, setIsSearching] = useState(false);
  const [suggestions, setSuggestions] = useState<Array<{ id: string; name: string; provinceCode: string }>>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedSuggestionIndex, setSelectedSuggestionIndex] = useState(-1);
  const [searchedMunicipality, setSearchedMunicipality] = useState<{ id: string; name: string } | null>(null);
  const searchContainerRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<NodeJS.Timeout | null>(null);
  const highlightedRowRef = useRef<HTMLTableRowElement>(null);

  const limit = 25;

  // Fetch regions and provinces on mount
  useEffect(() => {
    async function fetchFilters() {
      try {
        const response = await fetch("/api/filters");
        if (response.ok) {
          const data = await response.json();
          setRegions(data.regions ?? []);
          setProvinces(data.provinces ?? []);
        }
      } catch {
        // Silently fail - filters are optional
      }
    }
    fetchFilters();
  }, []);

  const fetchRankings = useCallback(async (searchTerm?: string) => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        sortBy,
        sortOrder,
        limit: String(limit),
        offset: String(page * limit),
        minConfidence: String(minConfidence),
        semestersToAverage: String(semestersToAverage),
      });

      if (selectedRegion) params.set("region", selectedRegion);
      if (selectedProvince) params.set("province", selectedProvince);
      if (searchTerm) params.set("search", searchTerm);

      const response = await fetch(`/api/rankings?${params}`);
      if (!response.ok) throw new Error("Failed to fetch rankings");

      const data: RankingsResponse = await response.json();
      setRankings(data.rankings);
      setTotalCount(data.pagination.total);
      setLatestPeriod(data.meta.latestPeriod);
      setEarliestPeriod(data.meta.earliestPeriod);
      setPeriodsIncluded(data.meta.periodsIncluded);

      if (searchTerm) {
        setSearchResult(data.searchResult);
      }
    } catch {
      setError("Failed to load rankings. Please try again.");
    } finally {
      setLoading(false);
      setIsSearching(false);
    }
  }, [sortBy, sortOrder, page, minConfidence, selectedRegion, selectedProvince, semestersToAverage]);

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

  const handleRegionChange = (region: string | null) => {
    setSelectedRegion(region);
    setSelectedProvince(null);
    setPage(0);
  };

  const handleProvinceChange = (province: string | null) => {
    setSelectedProvince(province);
    setPage(0);
  };

  const handleSemestersChange = (semesters: number) => {
    setSemestersToAverage(semesters);
    setPage(0);
  };

  // Fetch autocomplete suggestions
  const fetchSuggestions = useCallback(async (query: string) => {
    if (query.length < 2) {
      setSuggestions([]);
      setShowSuggestions(false);
      return;
    }

    try {
      const response = await fetch(`/api/municipality/search?q=${encodeURIComponent(query)}&limit=8`);
      if (response.ok) {
        const data = await response.json();
        setSuggestions(data.results || []);
        setShowSuggestions(data.results?.length > 0);
        setSelectedSuggestionIndex(-1);
      }
    } catch {
      setSuggestions([]);
    }
  }, []);

  // Handle search input change with debounce
  const handleSearchInputChange = (value: string) => {
    setSearchQuery(value);
    setSearchResult(null);

    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }

    debounceRef.current = setTimeout(() => {
      fetchSuggestions(value);
    }, 200);
  };

  // Handle selecting a suggestion
  const handleSelectSuggestion = (suggestion: { id: string; name: string }) => {
    setSearchQuery(suggestion.name);
    setShowSuggestions(false);
    setSuggestions([]);
    setIsSearching(true);
    setSearchResult(null);
    setSearchedMunicipality({ id: suggestion.id, name: suggestion.name });
    // Search for this municipality's rank
    fetchRankings(suggestion.name);
  };

  // Handle keyboard navigation in suggestions
  const handleSearchKeyDown = (e: React.KeyboardEvent) => {
    if (!showSuggestions || suggestions.length === 0) return;

    if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelectedSuggestionIndex((prev) =>
        prev < suggestions.length - 1 ? prev + 1 : prev
      );
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelectedSuggestionIndex((prev) => (prev > 0 ? prev - 1 : -1));
    } else if (e.key === "Enter" && selectedSuggestionIndex >= 0) {
      e.preventDefault();
      handleSelectSuggestion(suggestions[selectedSuggestionIndex]);
    } else if (e.key === "Escape") {
      setShowSuggestions(false);
    }
  };

  // Click outside to close suggestions
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (searchContainerRef.current && !searchContainerRef.current.contains(e.target as Node)) {
        setShowSuggestions(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  // Scroll to highlighted row when it appears
  useEffect(() => {
    if (highlightedRowRef.current && searchedMunicipality) {
      // Small delay to allow render to complete
      setTimeout(() => {
        highlightedRowRef.current?.scrollIntoView({
          behavior: "smooth",
          block: "center",
        });
      }, 100);
    }
  }, [rankings, searchedMunicipality]);

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (!searchQuery.trim()) return;
    setShowSuggestions(false);
    setIsSearching(true);
    setSearchResult(null);
    fetchRankings(searchQuery.trim());
  };

  const jumpToSearchResult = () => {
    if (searchResult) {
      setPage(searchResult.page);
    }
  };

  const clearSearch = () => {
    setSearchQuery("");
    setSearchResult(null);
    setSearchedMunicipality(null);
    setSuggestions([]);
    setShowSuggestions(false);
  };

  const filteredProvinces = selectedRegion
    ? provinces.filter((p) => p.regionCode === selectedRegion)
    : provinces;

  const totalPages = Math.ceil(totalCount / limit);

  const exportCSV = () => {
    const headers = [
      "Rank",
      "Name",
      "Province",
      "Region",
      "Value (EUR/m2)",
      "Gross Yield (%)",
      "Price Change (% ann.)",
      "Sales per 1k Pop",
      "Data Quality",
    ];
    const rows = rankings.map((r) => [
      r.rank,
      r.name,
      r.provinceName || "",
      r.regionName || "",
      r.valueMidEurSqm?.toFixed(0) || "",
      r.grossYieldPct?.toFixed(2) || "",
      r.annualizedPriceChangePct?.toFixed(2) || "",
      r.salesPer1000Pop?.toFixed(1) || "",
      r.dataQualityScore?.toFixed(0) || "",
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

  const periodDisplay =
    latestPeriod && earliestPeriod
      ? latestPeriod === earliestPeriod
        ? formatPeriod(latestPeriod)
        : `${formatPeriod(earliestPeriod)} – ${formatPeriod(latestPeriod)}`
      : null;

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
            Property market data for Italian municipalities
            {periodDisplay && <span className="header__date"> · Data: {periodDisplay}</span>}
            {periodsIncluded.length > 1 && (
              <span className="header__periods"> ({periodsIncluded.length} semester avg)</span>
            )}
          </p>
        </div>
        <div className="header__actions">
          <div className="search-container" ref={searchContainerRef}>
            <form onSubmit={handleSearch} className="search-form">
              <div className="search-input-wrapper">
                <svg className="search-icon" width="16" height="16" viewBox="0 0 16 16" fill="none">
                  <circle cx="7" cy="7" r="5" stroke="currentColor" strokeWidth="1.5"/>
                  <line x1="11" y1="11" x2="14" y2="14" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
                </svg>
                <input
                  type="text"
                  value={searchQuery}
                  onChange={(e) => handleSearchInputChange(e.target.value)}
                  onKeyDown={handleSearchKeyDown}
                  onFocus={() => suggestions.length > 0 && setShowSuggestions(true)}
                  placeholder="Find municipality..."
                  className="search-input"
                  autoComplete="off"
                />
                {searchQuery && (
                  <button type="button" onClick={clearSearch} className="search-clear">×</button>
                )}
              </div>
            </form>
            {showSuggestions && suggestions.length > 0 && (
              <ul className="search-suggestions">
                {suggestions.map((s, index) => (
                  <li
                    key={s.id}
                    className={`search-suggestion ${index === selectedSuggestionIndex ? "search-suggestion--selected" : ""}`}
                    onClick={() => handleSelectSuggestion(s)}
                    onMouseEnter={() => setSelectedSuggestionIndex(index)}
                  >
                    <span className="search-suggestion__name">{s.name}</span>
                    <span className="search-suggestion__province">({s.provinceCode})</span>
                  </li>
                ))}
              </ul>
            )}
          </div>
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

      {/* Search Result Banner */}
      {searchResult && (
        <div className="search-result-banner">
          <div className="search-result-info">
            <span className="search-result-icon">📍</span>
            <span>
              <strong>{searchResult.name}</strong> ranks <strong>#{searchResult.rank}</strong> out of {totalCount.toLocaleString()} municipalities
            </span>
          </div>
          <div className="search-result-actions">
            {searchResult.page !== page && (
              <button onClick={jumpToSearchResult} className="search-result-btn">
                Jump to Page {searchResult.page + 1}
              </button>
            )}
            <button onClick={clearSearch} className="search-result-dismiss">Dismiss</button>
          </div>
        </div>
      )}

      {searchedMunicipality && !searchResult && !isSearching && !loading && (
        <div className="search-result-banner search-result-banner--not-found">
          <span className="search-result-icon">📍</span>
          <div className="search-result-info">
            <span><strong>{searchedMunicipality.name}</strong> is not in the rankings (limited data coverage)</span>
          </div>
          <div className="search-result-actions">
            <Link href={`/municipality/${searchedMunicipality.id}`} className="search-result-btn search-result-btn--link">
              View Municipality Details
            </Link>
            <button onClick={clearSearch} className="search-result-dismiss">Clear</button>
          </div>
        </div>
      )}

      {/* Filters */}
      <div className="filters">
        <div className="filters__row">
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
              Min Data Quality: {minConfidence}%
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

        <div className="filters__row">
          <div className="filters__group filters__group--select">
            <label className="filters__label">Region</label>
            <select
              value={selectedRegion || ""}
              onChange={(e) => handleRegionChange(e.target.value || null)}
              className="filters__select"
            >
              <option value="">All Regions</option>
              {regions.map((r) => (
                <option key={r.code} value={r.code}>{r.name}</option>
              ))}
            </select>
          </div>

          <div className="filters__group filters__group--select">
            <label className="filters__label">Province</label>
            <select
              value={selectedProvince || ""}
              onChange={(e) => handleProvinceChange(e.target.value || null)}
              className="filters__select"
              disabled={!selectedRegion && provinces.length > 50}
            >
              <option value="">All Provinces</option>
              {filteredProvinces.map((p) => (
                <option key={p.code} value={p.code}>{p.name}</option>
              ))}
            </select>
          </div>

          <div className="filters__group">
            <label className="filters__label">Data Period</label>
            <div className="filters__buttons">
              {[
                { value: 1, label: "Latest" },
                { value: 2, label: "1 year" },
                { value: 3, label: "18mo" },
                { value: 4, label: "2 years" },
              ].map(({ value, label }) => (
                <button
                  key={value}
                  onClick={() => handleSemestersChange(value)}
                  className={`filters__btn ${semestersToAverage === value ? "filters__btn--active" : ""}`}
                >
                  {label}
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Table */}
      <main className="main">
        {error ? (
          <div className="error-state">
            <span className="error-icon">⚠</span>
            <p>{error}</p>
            <button onClick={() => fetchRankings()} className="retry-btn">
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
                      className={`table__th table__th--sortable ${sortBy === "annualized_price_change_pct" ? "table__th--sorted" : ""}`}
                      onClick={() => handleSort("annualized_price_change_pct")}
                    >
                      Change
                      {sortBy === "annualized_price_change_pct" && <span className="sort-arrow">{sortOrder === "desc" ? "↓" : "↑"}</span>}
                    </th>
                    <th
                      className={`table__th table__th--sortable ${sortBy === "gross_yield_pct" ? "table__th--sorted" : ""}`}
                      onClick={() => handleSort("gross_yield_pct")}
                    >
                      Yield
                      {sortBy === "gross_yield_pct" && <span className="sort-arrow">{sortOrder === "desc" ? "↓" : "↑"}</span>}
                    </th>
                    <th
                      className={`table__th table__th--sortable ${sortBy === "ntn_per_1000_pop" ? "table__th--sorted" : ""}`}
                      onClick={() => handleSort("ntn_per_1000_pop")}
                    >
                      Sales
                      {sortBy === "ntn_per_1000_pop" && <span className="sort-arrow">{sortOrder === "desc" ? "↓" : "↑"}</span>}
                    </th>
                    <th
                      className={`table__th table__th--sortable ${sortBy === "data_quality_score" ? "table__th--sorted" : ""}`}
                      onClick={() => handleSort("data_quality_score")}
                    >
                      Quality
                      {sortBy === "data_quality_score" && <span className="sort-arrow">{sortOrder === "desc" ? "↓" : "↑"}</span>}
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
                    rankings.map((r, index) => {
                      const isHighlighted = searchedMunicipality?.id === r.municipalityId;
                      return (
                      <tr
                        key={r.municipalityId}
                        ref={isHighlighted ? highlightedRowRef : null}
                        className={`table__row ${isHighlighted ? "table__row--highlighted" : ""}`}
                        style={{ animationDelay: `${index * 20}ms` }}
                      >
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
                          <span className={`percent-value ${(r.annualizedPriceChangePct ?? 0) >= 0 ? "positive" : "negative"}`}>
                            {formatPercent(r.annualizedPriceChangePct)}
                          </span>
                        </td>
                        <td className="table__td table__td--percent">
                          {formatPercent(r.grossYieldPct, false)}
                        </td>
                        <td className="table__td table__td--value">
                          {formatNumber(r.salesPer1000Pop)}
                        </td>
                        <td className="table__td table__td--score">
                          <ScoreBadge value={r.dataQualityScore} type="quality" />
                        </td>
                      </tr>
                      );
                    })
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
                  <span className="pagination__count"> · {totalCount.toLocaleString()} municipalities</span>
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
          color: #8b9bb4;
        }

        .header__periods {
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

        /* Search Form */
        .search-container {
          position: relative;
        }

        .search-form {
          display: flex;
          gap: 8px;
        }

        .search-input-wrapper {
          position: relative;
          display: flex;
          align-items: center;
        }

        .search-icon {
          position: absolute;
          left: 12px;
          color: #6b7a90;
          pointer-events: none;
        }

        .search-input {
          width: 200px;
          padding: 10px 32px 10px 36px;
          font-size: 0.85rem;
          color: #f0f2f5;
          background: rgba(0, 0, 0, 0.3);
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 8px;
          outline: none;
          transition: all 0.2s;
        }

        .search-input::placeholder {
          color: #5a6677;
        }

        .search-input:focus {
          border-color: rgba(196, 120, 92, 0.5);
          background: rgba(0, 0, 0, 0.4);
        }

        .search-clear {
          position: absolute;
          right: 8px;
          width: 20px;
          height: 20px;
          display: flex;
          align-items: center;
          justify-content: center;
          font-size: 1rem;
          color: #6b7a90;
          background: transparent;
          border: none;
          cursor: pointer;
          border-radius: 50%;
        }

        .search-clear:hover {
          color: #a8b3c7;
          background: rgba(255, 255, 255, 0.1);
        }

        .search-btn {
          padding: 10px 16px;
          font-size: 0.85rem;
          font-weight: 500;
          color: #e8c4a0;
          background: rgba(196, 120, 92, 0.2);
          border: 1px solid rgba(196, 120, 92, 0.4);
          border-radius: 8px;
          cursor: pointer;
          transition: all 0.2s;
        }

        .search-btn:hover:not(:disabled) {
          background: rgba(196, 120, 92, 0.3);
        }

        .search-btn:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }

        /* Autocomplete Suggestions */
        .search-suggestions {
          position: absolute;
          top: 100%;
          left: 0;
          right: 0;
          margin-top: 4px;
          padding: 6px 0;
          background: rgba(22, 25, 32, 0.98);
          border: 1px solid rgba(255, 255, 255, 0.1);
          border-radius: 8px;
          box-shadow: 0 8px 24px rgba(0, 0, 0, 0.4);
          list-style: none;
          z-index: 100;
          max-height: 300px;
          overflow-y: auto;
        }

        .search-suggestion {
          display: flex;
          align-items: center;
          justify-content: space-between;
          padding: 10px 14px;
          cursor: pointer;
          transition: background 0.15s;
        }

        .search-suggestion:hover,
        .search-suggestion--selected {
          background: rgba(196, 120, 92, 0.15);
        }

        .search-suggestion__name {
          font-size: 0.85rem;
          color: #f0f2f5;
        }

        .search-suggestion__province {
          font-size: 0.75rem;
          color: #6b7a90;
          margin-left: 8px;
        }

        /* Search Result Banner */
        .search-result-banner {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 16px;
          padding: 14px 32px;
          background: linear-gradient(90deg, rgba(74, 222, 128, 0.1) 0%, rgba(74, 222, 128, 0.05) 100%);
          border-bottom: 1px solid rgba(74, 222, 128, 0.2);
        }

        .search-result-banner--not-found {
          background: linear-gradient(90deg, rgba(251, 191, 36, 0.1) 0%, rgba(251, 191, 36, 0.05) 100%);
          border-bottom: 1px solid rgba(251, 191, 36, 0.2);
        }

        .search-result-info {
          display: flex;
          align-items: center;
          gap: 10px;
          font-size: 0.9rem;
          color: #d0d7e2;
        }

        .search-result-icon {
          font-size: 1.1rem;
        }

        .search-result-info strong {
          color: #f0f2f5;
        }

        .search-result-actions {
          display: flex;
          gap: 8px;
        }

        .search-result-btn {
          padding: 8px 14px;
          font-size: 0.8rem;
          font-weight: 500;
          color: #4ade80;
          background: rgba(74, 222, 128, 0.15);
          border: 1px solid rgba(74, 222, 128, 0.3);
          border-radius: 6px;
          cursor: pointer;
          transition: all 0.2s;
        }

        .search-result-btn:hover {
          background: rgba(74, 222, 128, 0.25);
        }

        .search-result-btn--link {
          text-decoration: none;
          background: rgba(196, 120, 92, 0.15);
          border-color: rgba(196, 120, 92, 0.3);
          color: #e8c4a0;
        }

        .search-result-btn--link:hover {
          background: rgba(196, 120, 92, 0.25);
        }

        .search-result-dismiss {
          padding: 8px 14px;
          font-size: 0.8rem;
          font-weight: 500;
          color: #8b9bb4;
          background: transparent;
          border: 1px solid rgba(255, 255, 255, 0.1);
          border-radius: 6px;
          cursor: pointer;
          transition: all 0.2s;
        }

        .search-result-dismiss:hover {
          background: rgba(255, 255, 255, 0.05);
          color: #d0d7e2;
        }

        /* Filters */
        .filters {
          display: flex;
          flex-direction: column;
          gap: 20px;
          padding: 24px 32px;
          background: rgba(22, 25, 32, 0.5);
          border-bottom: 1px solid rgba(255, 255, 255, 0.04);
        }

        .filters__row {
          display: flex;
          align-items: flex-end;
          gap: 32px;
          flex-wrap: wrap;
        }

        .filters__group {
          display: flex;
          flex-direction: column;
          gap: 10px;
        }

        .filters__group--select {
          min-width: 180px;
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

        .filters__select {
          padding: 10px 32px 10px 12px;
          font-size: 0.8rem;
          color: #d0d7e2;
          background: rgba(0, 0, 0, 0.2);
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 8px;
          cursor: pointer;
          appearance: none;
          background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'%3E%3Cpath d='M3 4.5L6 7.5L9 4.5' stroke='%236b7a90' stroke-width='1.5' stroke-linecap='round' fill='none'/%3E%3C/svg%3E");
          background-repeat: no-repeat;
          background-position: right 12px center;
        }

        .filters__select:focus {
          outline: none;
          border-color: rgba(196, 120, 92, 0.5);
        }

        .filters__select:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }

        .filters__select option {
          background: #1a1d23;
          color: #d0d7e2;
        }

        .filters__buttons {
          display: flex;
          gap: 6px;
        }

        .filters__btn {
          padding: 8px 12px;
          font-size: 0.75rem;
          font-weight: 500;
          color: #8b9bb4;
          background: rgba(255, 255, 255, 0.04);
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 6px;
          cursor: pointer;
          transition: all 0.2s;
        }

        .filters__btn:hover {
          background: rgba(255, 255, 255, 0.08);
          color: #d0d7e2;
        }

        .filters__btn--active {
          background: rgba(196, 120, 92, 0.2);
          border-color: rgba(196, 120, 92, 0.4);
          color: #e8c4a0;
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

        .table__row--highlighted {
          opacity: 1;
          background: rgba(74, 222, 128, 0.12);
          animation: highlightPulse 2s ease-out;
        }

        .table__row--highlighted:hover {
          background: rgba(74, 222, 128, 0.18);
        }

        .table__row--highlighted .rank-badge {
          background: rgba(74, 222, 128, 0.25);
          color: #4ade80;
        }

        @keyframes highlightPulse {
          0% { opacity: 1; background: rgba(74, 222, 128, 0.3); }
          100% { opacity: 1; background: rgba(74, 222, 128, 0.12); }
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
            padding: 20px;
          }

          .filters__row {
            flex-direction: column;
            align-items: flex-start;
            gap: 20px;
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
