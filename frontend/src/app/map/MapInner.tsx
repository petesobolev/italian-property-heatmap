"use client";

import "leaflet/dist/leaflet.css";
import { useEffect, useMemo, useState, useCallback } from "react";
import { useSearchParams } from "next/navigation";
import type { GeoJsonObject, FeatureCollection, Feature } from "geojson";
import { GeoJSON, MapContainer, TileLayer, useMap, ZoomControl } from "react-leaflet";
import type { Layer, LeafletMouseEvent } from "leaflet";
import {
  FiltersSidebar,
  MunicipalityDrawer,
  MapLegend,
  CompareBar,
  ZoneLayer,
  type FiltersState,
  type MetricType,
  type MunicipalityData,
} from "@/components/map";

// Southern Italian regions eligible for 7% flat tax regime
// Regions: Sicilia (19), Calabria (18), Sardegna (20), Puglia (16),
// Campania (15), Basilicata (17), Molise (14), Abruzzo (13)
const FLAT_TAX_ELIGIBLE_REGIONS = new Set([
  "13", // Abruzzo
  "14", // Molise
  "15", // Campania
  "16", // Puglia
  "17", // Basilicata
  "18", // Calabria
  "19", // Sicilia
  "20", // Sardegna
]);

// Color scales for different metrics
const COLOR_SCALES: Record<MetricType, { stops: number[][]; noData: string }> = {
  value_mid_eur_sqm: {
    stops: [
      [30, 58, 95],    // Deep Mediterranean blue
      [45, 90, 135],
      [74, 144, 181],
      [124, 196, 212],
      [184, 224, 236],
    ],
    noData: "#2a2d35",
  },
  forecast_appreciation_pct: {
    stops: [
      [127, 29, 29],   // Deep red (negative)
      [180, 83, 9],
      [229, 231, 235], // Neutral
      [22, 163, 74],
      [22, 101, 52],   // Deep green (positive)
    ],
    noData: "#2a2d35",
  },
  forecast_gross_yield_pct: {
    stops: [
      [254, 243, 199],
      [252, 211, 77],
      [245, 158, 11],
      [217, 119, 6],
      [146, 64, 14],
    ],
    noData: "#2a2d35",
  },
  opportunity_score: {
    stops: [
      [26, 26, 46],
      [74, 63, 107],
      [196, 120, 92],  // Terracotta
      [232, 196, 160],
      [245, 235, 224],
    ],
    noData: "#2a2d35",
  },
  confidence_score: {
    stops: [
      [55, 65, 81],
      [75, 85, 99],
      [107, 114, 128],
      [156, 163, 175],
      [209, 213, 219],
    ],
    noData: "#2a2d35",
  },
  vehicle_arson_rate: {
    stops: [
      [254, 243, 199],  // Light yellow (low risk)
      [253, 224, 139],
      [248, 146, 79],   // Orange (medium risk)
      [215, 48, 31],    // Red-orange (high risk)
      [127, 29, 29],    // Deep red (very high risk)
    ],
    noData: "#2a2d35",
  },
};

// Dark map tiles for premium feel
const DARK_TILES = "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png";
const DARK_ATTRIBUTION =
  '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>';

interface MapControllerProps {
  center?: [number, number];
  zoom?: number;
  onZoomChange?: (zoom: number) => void;
}

function MapController({ center, zoom, onZoomChange }: MapControllerProps) {
  const map = useMap();

  useEffect(() => {
    if (center && zoom) {
      map.flyTo(center, zoom, { duration: 0.8 });
    }
  }, [map, center, zoom]);

  useEffect(() => {
    const handleZoom = () => {
      onZoomChange?.(map.getZoom());
    };

    map.on("zoomend", handleZoom);
    // Call once on mount to sync initial zoom
    handleZoom();

    return () => {
      map.off("zoomend", handleZoom);
    };
  }, [map, onZoomChange]);

  return null;
}

export function MapInner() {
  // URL parameters for hidden features
  const searchParams = useSearchParams();
  const showHiddenMetrics = searchParams.get("arson") === "true";

  // State
  const [geojson, setGeojson] = useState<GeoJsonObject | null>(null);
  const [valuesByMunicipality, setValuesByMunicipality] = useState<
    Record<string, number | null | undefined>
  >({});
  const [loading, setLoading] = useState(true);
  const [dataSource, setDataSource] = useState<"real" | "demo" | null>(null);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [selectedMunicipality, setSelectedMunicipality] = useState<MunicipalityData | null>(null);
  const [drawerOpen, setDrawerOpen] = useState(false);

  // Zone layer state
  const [currentZoom, setCurrentZoom] = useState(6);
  const [focusedMunicipalityId, setFocusedMunicipalityId] = useState<string | null>(null);

  // Compare state
  const [compareList, setCompareList] = useState<MunicipalityData[]>([]);

  const handleAddToCompare = useCallback((municipality: MunicipalityData) => {
    setCompareList((prev) => {
      // Don't add if already in list or at max capacity
      if (prev.some((m) => m.municipalityId === municipality.municipalityId)) {
        return prev;
      }
      if (prev.length >= 5) {
        return prev;
      }
      return [...prev, municipality];
    });
  }, []);

  const handleRemoveFromCompare = useCallback((id: string) => {
    setCompareList((prev) => prev.filter((m) => m.municipalityId !== id));
  }, []);

  const handleClearCompare = useCallback(() => {
    setCompareList([]);
  }, []);

  const isInCompareList = useMemo(
    () => (id: string) => compareList.some((m) => m.municipalityId === id),
    [compareList]
  );

  // Filters state
  const [filters, setFilters] = useState<FiltersState>({
    metric: "value_mid_eur_sqm",
    region: null,
    province: null,
    confidenceThreshold: 0,
    propertySegment: "residential",
    showFlatTaxEligible: false,
  });

  // Mock regions/provinces for UI (will be populated from API later)
  const [regions] = useState<{ code: string; name: string }[]>([
    { code: "01", name: "Piemonte" },
    { code: "02", name: "Valle d'Aosta" },
    { code: "03", name: "Lombardia" },
    { code: "04", name: "Trentino-Alto Adige" },
    { code: "05", name: "Veneto" },
    { code: "06", name: "Friuli-Venezia Giulia" },
    { code: "07", name: "Liguria" },
    { code: "08", name: "Emilia-Romagna" },
    { code: "09", name: "Toscana" },
    { code: "10", name: "Umbria" },
    { code: "11", name: "Marche" },
    { code: "12", name: "Lazio" },
    { code: "13", name: "Abruzzo" },
    { code: "14", name: "Molise" },
    { code: "15", name: "Campania" },
    { code: "16", name: "Puglia" },
    { code: "17", name: "Basilicata" },
    { code: "18", name: "Calabria" },
    { code: "19", name: "Sicilia" },
    { code: "20", name: "Sardegna" },
  ]);

  const [provinces] = useState<{ code: string; name: string; regionCode: string }[]>([
    { code: "015", name: "Milano", regionCode: "03" },
    { code: "058", name: "Roma", regionCode: "12" },
    { code: "048", name: "Firenze", regionCode: "09" },
    { code: "027", name: "Venezia", regionCode: "05" },
    { code: "063", name: "Napoli", regionCode: "15" },
  ]);

  // Load data
  useEffect(() => {
    let cancelled = false;

    async function loadGeoJSON(): Promise<GeoJsonObject> {
      try {
        const params = new URLSearchParams({ simplified: "true" });
        if (filters.region) params.set("region", filters.region);
        if (filters.province) params.set("province", filters.province);

        const realRes = await fetch(`/api/map/geojson?${params}`, {
          cache: "no-store",
        });

        if (realRes.ok) {
          const data = (await realRes.json()) as FeatureCollection;
          const hasRealGeometries =
            data.features &&
            data.features.length > 0 &&
            data.features.some((f) => f.geometry !== null);

          if (hasRealGeometries) {
            if (!cancelled) setDataSource("real");
            return data;
          }
        }
      } catch (e) {
        console.warn("Failed to load real geometries, falling back to demo:", e);
      }

      const demoRes = await fetch("/demo/municipalities.geojson", {
        cache: "no-store",
      });
      if (!demoRes.ok) throw new Error("Failed to load demo geojson");
      if (!cancelled) setDataSource("demo");
      return (await demoRes.json()) as GeoJsonObject;
    }

    async function load() {
      setLoading(true);

      const [geo, valuesRes] = await Promise.all([
        loadGeoJSON(),
        fetch(
          `/api/map/layer?metric=${filters.metric}&horizonMonths=12&segment=${filters.propertySegment}`,
          { cache: "no-store" }
        ),
      ]);

      if (!valuesRes.ok) throw new Error("Failed to load layer values");

      const layer = (await valuesRes.json()) as {
        features?: { municipalityId: string; value: number | null }[];
      };

      if (cancelled) return;

      setGeojson(geo);
      setValuesByMunicipality(
        Object.fromEntries(
          (layer.features ?? []).map((f) => [f.municipalityId, f.value])
        )
      );
      setLoading(false);
    }

    load().catch((e) => {
      console.error(e);
      if (!cancelled) {
        setGeojson(null);
        setValuesByMunicipality({});
        setLoading(false);
      }
    });

    return () => {
      cancelled = true;
    };
  }, [filters.metric, filters.region, filters.province, filters.propertySegment]);

  // Calculate value domain
  const valueDomain = useMemo(() => {
    const vals = Object.values(valuesByMunicipality)
      .filter((v): v is number => typeof v === "number" && Number.isFinite(v))
      .sort((a, b) => a - b);
    if (vals.length === 0) return { min: 0, max: 0 };
    return { min: vals[0], max: vals[vals.length - 1] };
  }, [valuesByMunicipality]);

  // Color function
  const colorFor = useCallback(
    (v: number | null | undefined) => {
      const scale = COLOR_SCALES[filters.metric];
      if (typeof v !== "number" || !Number.isFinite(v)) return scale.noData;

      const { min, max } = valueDomain;
      if (max === min) return scale.noData;

      const t = Math.max(0, Math.min(1, (v - min) / (max - min)));
      const { stops } = scale;

      // Interpolate between stops
      const scaledT = t * (stops.length - 1);
      const lowerIdx = Math.floor(scaledT);
      const upperIdx = Math.min(lowerIdx + 1, stops.length - 1);
      const localT = scaledT - lowerIdx;

      const rgb = stops[lowerIdx].map((c, i) =>
        Math.round(c + (stops[upperIdx][i] - c) * localT)
      );

      return `rgb(${rgb[0]}, ${rgb[1]}, ${rgb[2]})`;
    },
    [filters.metric, valueDomain]
  );

  // Check if municipality is eligible for 7% flat tax
  const isFlatTaxEligible = useCallback((feature: Feature | undefined): boolean => {
    if (!feature?.properties) return false;
    const regionCode = feature.properties.region_code as string | undefined;
    // Eligible if in Southern Italy region
    // Note: Full eligibility also requires population < 20,000, but we don't have that data yet
    return regionCode ? FLAT_TAX_ELIGIBLE_REGIONS.has(regionCode.padStart(2, "0")) : false;
  }, []);

  // Style function for GeoJSON
  const style = useCallback(
    (feature: Feature | undefined) => {
      const id = feature?.properties?.municipality_id as string | undefined;
      const v = id ? valuesByMunicipality[id] : null;

      const isEligible = filters.showFlatTaxEligible && isFlatTaxEligible(feature);

      if (filters.showFlatTaxEligible) {
        // When flat tax overlay is active, highlight eligible municipalities
        return {
          color: isEligible ? "#22c55e" : "rgba(255, 255, 255, 0.1)",
          weight: isEligible ? 2 : 0.5,
          fillColor: isEligible ? "rgba(34, 197, 94, 0.4)" : "rgba(100, 100, 100, 0.3)",
          fillOpacity: isEligible ? 0.7 : 0.4,
        };
      }

      return {
        color: "rgba(255, 255, 255, 0.2)",
        weight: 0.5,
        fillColor: colorFor(v),
        fillOpacity: 0.85,
      };
    },
    [valuesByMunicipality, colorFor, filters.showFlatTaxEligible, isFlatTaxEligible]
  );

  // Zoom change handler
  const handleZoomChange = useCallback((zoom: number) => {
    setCurrentZoom(zoom);
    // Clear focused municipality if zoomed out too far
    if (zoom < 10) {
      setFocusedMunicipalityId(null);
    }
  }, []);

  // Click handler
  const handleFeatureClick = useCallback((feature: Feature) => {
    const props = feature.properties || {};
    const municipalityId = props.municipality_id || "";

    // Set focused municipality for zone display
    setFocusedMunicipalityId(municipalityId);

    setSelectedMunicipality({
      municipalityId,
      name: props.name || props.municipality_id || "Unknown",
      provinceCode: props.province_code,
      regionCode: props.region_code,
      coastalFlag: props.coastal_flag,
      mountainFlag: props.mountain_flag,
      // Mock some data - in real app this would come from API
      valueMidEurSqm: Math.random() * 5000 + 1500,
      valueMinEurSqm: Math.random() * 1000 + 1000,
      valueMaxEurSqm: Math.random() * 5000 + 5000,
      forecastAppreciationPct: (Math.random() - 0.3) * 10,
      forecastGrossYieldPct: Math.random() * 6 + 2,
      opportunityScore: Math.random() * 100,
      confidenceScore: Math.random() * 100,
      population: Math.floor(Math.random() * 100000) + 1000,
      populationDensity: Math.random() * 500 + 50,
      youngRatio: Math.random() * 0.2,
      elderlyRatio: Math.random() * 0.3,
      foreignRatio: Math.random() * 0.15,
      ntnTotal: Math.random() * 500,
      ntnPer1000Pop: Math.random() * 20,
    });
    setDrawerOpen(true);
  }, []);

  // Event handlers for each feature
  const onEachFeature = useCallback(
    (feature: Feature, layer: Layer) => {
      const name =
        feature?.properties?.name ??
        feature?.properties?.municipality_id ??
        "Unknown";
      const id = feature?.properties?.municipality_id as string | undefined;
      const v = id ? valuesByMunicipality[id] : null;

      let label: string;
      if (filters.showFlatTaxEligible) {
        const isEligible = isFlatTaxEligible(feature);
        label = `${name}${isEligible ? " ✓ 7% Flat Tax Eligible" : ""}`;
      } else {
        label =
          typeof v === "number"
            ? `${name}: €${Math.round(v).toLocaleString()}/m²`
            : `${name}: no data`;
      }

      layer.bindTooltip(label, {
        sticky: true,
        className: "map-tooltip",
      });

      layer.on({
        click: () => handleFeatureClick(feature),
        mouseover: (e: LeafletMouseEvent) => {
          const target = e.target;
          target.setStyle({
            weight: 2,
            color: "rgba(232, 196, 160, 0.8)",
            fillOpacity: 1,
          });
        },
        mouseout: (e: LeafletMouseEvent) => {
          const target = e.target;
          target.setStyle(style(feature));
        },
      });
    },
    [valuesByMunicipality, style, handleFeatureClick, filters.showFlatTaxEligible, isFlatTaxEligible]
  );

  const featureCount = useMemo(() => {
    if (!geojson || geojson.type !== "FeatureCollection") return 0;
    return (geojson as FeatureCollection).features?.length ?? 0;
  }, [geojson]);

  return (
    <div className="map-container">
      {/* Filters Sidebar */}
      <FiltersSidebar
        filters={filters}
        onFiltersChange={setFilters}
        regions={regions}
        provinces={provinces}
        isCollapsed={sidebarCollapsed}
        onToggleCollapse={() => setSidebarCollapsed(!sidebarCollapsed)}
        showHiddenMetrics={showHiddenMetrics}
      />

      {/* Map */}
      <MapContainer
        center={[41.8719, 12.5674]}
        zoom={6}
        scrollWheelZoom
        className="map-leaflet"
        zoomControl={false}
      >
        <ZoomControl position="bottomright" />
        <TileLayer attribution={DARK_ATTRIBUTION} url={DARK_TILES} />
        {geojson && (
          <GeoJSON
            key={`${filters.metric}-${filters.propertySegment}-${filters.showFlatTaxEligible}-${filters.region || 'all'}-${filters.province || 'all'}`}
            data={geojson}
            style={style}
            onEachFeature={onEachFeature}
          />
        )}
        {filters.metric !== "vehicle_arson_rate" && (
          <ZoneLayer
            municipalityId={focusedMunicipalityId}
            visible={currentZoom >= 11}
            metric={filters.metric}
          />
        )}
        <MapController onZoomChange={handleZoomChange} />
      </MapContainer>

      {/* Legend */}
      <MapLegend
        metric={filters.metric}
        min={valueDomain.min}
        max={valueDomain.max}
        isLoading={loading}
      />

      {/* Data source indicator */}
      <div className="data-badge">
        {loading ? (
          <span className="data-badge__loading">Loading...</span>
        ) : (
          <>
            <span
              className={`data-badge__dot ${dataSource === "real" ? "data-badge__dot--real" : "data-badge__dot--demo"}`}
            />
            <span className="data-badge__text">
              {dataSource === "real" ? "PostGIS" : "Demo"} · {featureCount.toLocaleString()}
            </span>
          </>
        )}
      </div>

      {/* Zone indicator */}
      {focusedMunicipalityId && currentZoom >= 11 && filters.metric !== "vehicle_arson_rate" && (
        <div className="zone-indicator">
          <span className="zone-indicator__icon">◎</span>
          <span className="zone-indicator__text">
            Zones: {selectedMunicipality?.name || focusedMunicipalityId}
          </span>
          <button
            className="zone-indicator__close"
            onClick={() => setFocusedMunicipalityId(null)}
            aria-label="Clear zone focus"
          >
            ×
          </button>
        </div>
      )}

      {/* Municipality Drawer */}
      <MunicipalityDrawer
        municipality={selectedMunicipality}
        isOpen={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        onAddToCompare={handleAddToCompare}
        isInCompareList={selectedMunicipality ? isInCompareList(selectedMunicipality.municipalityId) : false}
      />

      {/* Compare Bar */}
      <CompareBar
        municipalities={compareList}
        onRemove={handleRemoveFromCompare}
        onClear={handleClearCompare}
      />

      <style jsx global>{`
        .map-container {
          position: absolute;
          inset: 0;
          width: 100%;
          height: 100%;
          background: #0d0f12;
        }

        .map-leaflet {
          width: 100%;
          height: 100%;
          background: #0d0f12;
        }

        .map-tooltip {
          background: linear-gradient(165deg,
            rgba(22, 25, 32, 0.95) 0%,
            rgba(13, 15, 18, 0.97) 100%
          );
          border: 1px solid rgba(255, 255, 255, 0.1);
          border-radius: 8px;
          padding: 8px 12px;
          font-family: 'DM Sans', -apple-system, sans-serif;
          font-size: 0.8rem;
          color: #f0f2f5;
          box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
        }

        .map-tooltip::before {
          display: none;
        }

        .leaflet-control-zoom {
          border: none !important;
          box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3) !important;
        }

        .leaflet-control-zoom a {
          background: linear-gradient(165deg,
            rgba(22, 25, 32, 0.95) 0%,
            rgba(13, 15, 18, 0.97) 100%
          ) !important;
          border: 1px solid rgba(255, 255, 255, 0.08) !important;
          color: #a8b3c7 !important;
          width: 36px !important;
          height: 36px !important;
          line-height: 36px !important;
          font-size: 18px !important;
        }

        .leaflet-control-zoom a:hover {
          background: rgba(255, 255, 255, 0.08) !important;
          color: #f0f2f5 !important;
        }

        .leaflet-control-zoom-in {
          border-radius: 8px 8px 0 0 !important;
        }

        .leaflet-control-zoom-out {
          border-radius: 0 0 8px 8px !important;
        }

        .data-badge {
          position: absolute;
          top: 16px;
          right: 16px;
          z-index: 1000;
          display: flex;
          align-items: center;
          gap: 8px;
          padding: 8px 14px;
          background: linear-gradient(165deg,
            rgba(22, 25, 32, 0.95) 0%,
            rgba(13, 15, 18, 0.97) 100%
          );
          backdrop-filter: blur(12px);
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 20px;
          font-size: 0.7rem;
          color: #a8b3c7;
          box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
        }

        .data-badge__loading {
          color: #6b7a90;
        }

        .data-badge__dot {
          width: 8px;
          height: 8px;
          border-radius: 50%;
        }

        .data-badge__dot--real {
          background: #4ade80;
          box-shadow: 0 0 8px rgba(74, 222, 128, 0.4);
        }

        .data-badge__dot--demo {
          background: #fbbf24;
          box-shadow: 0 0 8px rgba(251, 191, 36, 0.4);
        }

        .data-badge__text {
          font-weight: 500;
          letter-spacing: 0.02em;
        }

        .zone-indicator {
          position: absolute;
          top: 56px;
          right: 16px;
          z-index: 1000;
          display: flex;
          align-items: center;
          gap: 8px;
          padding: 8px 12px;
          background: linear-gradient(165deg,
            rgba(196, 120, 92, 0.15) 0%,
            rgba(196, 120, 92, 0.08) 100%
          );
          backdrop-filter: blur(12px);
          border: 1px solid rgba(196, 120, 92, 0.3);
          border-radius: 20px;
          font-size: 0.7rem;
          color: #f0f2f5;
          box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
          animation: slideIn 0.3s ease;
        }

        @keyframes slideIn {
          from {
            opacity: 0;
            transform: translateX(10px);
          }
          to {
            opacity: 1;
            transform: translateX(0);
          }
        }

        .zone-indicator__icon {
          color: #c4785c;
          font-size: 0.9rem;
        }

        .zone-indicator__text {
          font-weight: 500;
          letter-spacing: 0.02em;
          max-width: 150px;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }

        .zone-indicator__close {
          background: none;
          border: none;
          color: #6b7a90;
          font-size: 1rem;
          cursor: pointer;
          padding: 0 0 0 4px;
          line-height: 1;
          transition: color 0.2s;
        }

        .zone-indicator__close:hover {
          color: #f0f2f5;
        }
      `}</style>
    </div>
  );
}
