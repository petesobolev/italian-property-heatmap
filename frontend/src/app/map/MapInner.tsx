"use client";

import "leaflet/dist/leaflet.css";
import { useEffect, useMemo, useState, useCallback, useRef } from "react";
import { useSearchParams } from "next/navigation";
import type { GeoJsonObject, FeatureCollection, Feature } from "geojson";
import { GeoJSON, MapContainer, TileLayer, useMap, ZoomControl } from "react-leaflet";
import L from "leaflet";
import type { Layer, LeafletMouseEvent, LatLngBounds } from "leaflet";
import {
  FiltersSidebar,
  MunicipalityDrawer,
  MapLegend,
  CompareBar,
  ZoneLayer,
  RegionBoundaries,
  type FiltersState,
  type MetricType,
  type MunicipalityData,
} from "@/components/map";
import { CommandPalette } from "@/components/map/CommandPalette";

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
// fixedRange: Use fixed min/max instead of data-driven range (matches MapLegend config)
const COLOR_SCALES: Record<MetricType, { stops: number[][]; noData: string; fixedRange?: { min: number; max: number } }> = {
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
  rent_mid_eur_sqm_month: {
    stops: [
      [240, 249, 232],  // Light green (low rent)
      [186, 228, 188],
      [123, 204, 196],
      [67, 162, 202],
      [8, 104, 172],    // Deep blue (high rent)
    ],
    noData: "#2a2d35",
  },
  gross_yield_pct: {
    stops: [
      [254, 240, 217],  // Cream (low yield)
      [253, 204, 138],
      [252, 141, 89],
      [227, 74, 51],
      [179, 0, 0],      // Deep red (high yield)
    ],
    noData: "#2a2d35",
    fixedRange: { min: 0, max: 15 },
  },
  price_variance_pct: {
    stops: [
      [26, 152, 80],    // Green (low variance - stable pricing)
      [145, 207, 96],
      [255, 255, 191],  // Yellow (moderate)
      [252, 141, 89],
      [215, 48, 39],    // Red (high variance - uncertain)
    ],
    noData: "#2a2d35",
    fixedRange: { min: 0, max: 100 },
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
    fixedRange: { min: -10, max: 10 },
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
    fixedRange: { min: 0, max: 10 },
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
    fixedRange: { min: 0, max: 100 },
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
    fixedRange: { min: 0, max: 100 },
  },
  foreign_ratio: {
    stops: [
      [240, 249, 255],  // Very light blue (low %)
      [186, 230, 253],
      [56, 189, 248],   // Sky blue (medium %)
      [2, 132, 199],    // Blue (higher %)
      [7, 89, 133],     // Deep blue (highest %)
    ],
    noData: "#2a2d35",
    fixedRange: { min: 0, max: 25 },
  },
  population_growth_rate: {
    stops: [
      [180, 50, 50],    // Dark red (declining fast)
      [220, 120, 100],  // Light red (declining)
      [180, 180, 180],  // Neutral gray (0% change)
      [100, 180, 100],  // Light green (growing)
      [50, 140, 50],    // Dark green (growing fast)
    ],
    noData: "#2a2d35",
    // No fixedRange - use dynamic range based on viewport data
  },
};

// Dark map tiles for premium feel
const DARK_TILES = "https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png";
const DARK_LABELS = "https://{s}.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}{r}.png";
const DARK_ATTRIBUTION =
  '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>';

interface MapControllerProps {
  center?: [number, number];
  zoom?: number;
  onZoomChange?: (zoom: number) => void;
  onCenterChange?: (center: [number, number]) => void;
  onBoundsChange?: (bounds: LatLngBounds) => void;
}

function MapController({ center, zoom, onZoomChange, onCenterChange, onBoundsChange }: MapControllerProps) {
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

    const handleMove = () => {
      const c = map.getCenter();
      onCenterChange?.([c.lat, c.lng]);
      onBoundsChange?.(map.getBounds());
    };

    map.on("zoomend", handleZoom);
    map.on("moveend", handleMove);
    // Call once on mount to sync initial state
    handleZoom();
    handleMove();

    return () => {
      map.off("zoomend", handleZoom);
      map.off("moveend", handleMove);
    };
  }, [map, onZoomChange, onCenterChange, onBoundsChange]);

  return null;
}

// Component to capture map ref and handle drag tooltip cleanup
function MapRefCapture({
  mapRef,
  activeTooltipLayerRef,
}: {
  mapRef: React.MutableRefObject<L.Map | null>;
  activeTooltipLayerRef: React.MutableRefObject<Layer | null>;
}) {
  const map = useMap();

  useEffect(() => {
    mapRef.current = map;
  }, [map, mapRef]);

  // Close active tooltip when map drag/zoom starts to prevent stuck tooltips
  useEffect(() => {
    const closeAllTooltips = () => {
      // Close tracked tooltip
      if (activeTooltipLayerRef.current) {
        activeTooltipLayerRef.current.closeTooltip();
        activeTooltipLayerRef.current = null;
      }
      // Also close any tooltip on the map (catches orphaned ones)
      map.closeTooltip();
    };

    map.on("dragstart", closeAllTooltips);
    map.on("zoomstart", closeAllTooltips);
    // Also close on regular clicks (user clicking elsewhere)
    map.on("click", closeAllTooltips);

    return () => {
      map.off("dragstart", closeAllTooltips);
      map.off("zoomstart", closeAllTooltips);
      map.off("click", closeAllTooltips);
    };
  }, [map, activeTooltipLayerRef]);

  return null;
}

// Component to handle initial focus on a specific municipality from URL
interface FocusHandlerProps {
  municipalityId: string | null;
  geojson: FeatureCollection | null;
  onFocused: (municipalityId: string) => void;
  shouldFocus: boolean;
  onFocusComplete: (municipalityId: string) => void;
}

function FocusHandler({ municipalityId, geojson, onFocused, shouldFocus, onFocusComplete }: FocusHandlerProps) {
  const map = useMap();

  useEffect(() => {
    if (!municipalityId || !geojson || !shouldFocus) return;

    // Find the feature for this municipality
    const feature = geojson.features.find(
      (f) => f.properties?.municipality_id === municipalityId
    );

    if (!feature || !feature.geometry) {
      onFocusComplete(municipalityId); // Mark as complete even if not found to prevent retrying
      return;
    }

    // Calculate bounds from geometry
    const coords: number[][] = [];
    const geometry = feature.geometry;

    if (geometry.type === "Polygon") {
      coords.push(...(geometry.coordinates[0] as number[][]));
    } else if (geometry.type === "MultiPolygon") {
      for (const polygon of geometry.coordinates) {
        coords.push(...(polygon[0] as number[][]));
      }
    }

    if (coords.length === 0) {
      onFocusComplete(municipalityId);
      return;
    }

    // Calculate bounds (coords are [lng, lat])
    let minLng = Infinity, maxLng = -Infinity, minLat = Infinity, maxLat = -Infinity;
    for (const [lng, lat] of coords) {
      if (lng < minLng) minLng = lng;
      if (lng > maxLng) maxLng = lng;
      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
    }

    // Create Leaflet bounds (uses [lat, lng] order)
    const bounds = L.latLngBounds(
      [minLat, minLng],
      [maxLat, maxLng]
    );

    // Fly to the bounds with padding
    map.flyToBounds(bounds, {
      padding: [50, 50],
      duration: 0.8,
      maxZoom: 12,
    });

    // Set the focused municipality
    onFocused(municipalityId);
    onFocusComplete(municipalityId);
  }, [municipalityId, geojson, map, onFocused, shouldFocus, onFocusComplete]);

  return null;
}

// Check if a point is inside a polygon using ray casting algorithm
// Point-in-polygon using ray casting algorithm
// Uses x/y coordinates (GeoJSON order: [lng, lat] = [x, y])
function pointInPolygon(x: number, y: number, polygon: number[][]): boolean {
  let inside = false;

  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const xi = polygon[i][0], yi = polygon[i][1];
    const xj = polygon[j][0], yj = polygon[j][1];

    if (((yi > y) !== (yj > y)) && (x < (xj - xi) * (y - yi) / (yj - yi) + xi)) {
      inside = !inside;
    }
  }

  return inside;
}

// Find municipality at a given point
// Input point is [lat, lng] (Leaflet order), GeoJSON uses [lng, lat]
function findMunicipalityAtPoint(
  point: [number, number],
  geojson: FeatureCollection | null
): string | null {
  if (!geojson?.features) return null;

  // Convert from Leaflet [lat, lng] to GeoJSON [lng, lat] order
  const [lat, lng] = point;
  const x = lng;
  const y = lat;

  for (const feature of geojson.features) {
    const geometry = feature.geometry;
    if (!geometry) continue;

    const municipalityId = feature.properties?.municipality_id as string | undefined;
    if (!municipalityId) continue;

    if (geometry.type === "Polygon") {
      const coords = geometry.coordinates[0] as number[][];
      if (pointInPolygon(x, y, coords)) {
        return municipalityId;
      }
    } else if (geometry.type === "MultiPolygon") {
      for (const polygon of geometry.coordinates) {
        const coords = polygon[0] as number[][];
        if (pointInPolygon(x, y, coords)) {
          return municipalityId;
        }
      }
    }
  }

  return null;
}

export function MapInner() {
  // URL parameters for hidden features and focus
  const searchParams = useSearchParams();
  const showHiddenMetrics = searchParams.get("arson") === "true";
  const focusParam = searchParams.get("focus");

  // State
  const [geojson, setGeojson] = useState<GeoJsonObject | null>(null);
  const [valuesByMunicipality, setValuesByMunicipality] = useState<
    Record<string, number | null | undefined>
  >({});
  const [loading, setLoading] = useState(true);
  const [loadingProgress, setLoadingProgress] = useState({ percent: 0, stage: "Initializing..." });
  const [availablePeriodsCount, setAvailablePeriodsCount] = useState(4); // Default to 4, will be updated from API
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [selectedMunicipality, setSelectedMunicipality] = useState<MunicipalityData | null>(null);
  const [drawerOpen, setDrawerOpen] = useState(false);

  // Zone layer state
  const [currentZoom, setCurrentZoom] = useState(6);
  const [mapCenter, setMapCenter] = useState<[number, number]>([41.8719, 12.5674]);
  const [mapBounds, setMapBounds] = useState<LatLngBounds | null>(null);
  const [focusedMunicipalityId, setFocusedMunicipalityId] = useState<string | null>(null);
  const [autoDetectedMunicipalityId, setAutoDetectedMunicipalityId] = useState<string | null>(null);
  const [lastFocusedParam, setLastFocusedParam] = useState<string | null>(null);

  // Track if we need to focus (when focusParam exists and differs from last focused)
  const shouldFocus = focusParam !== null && focusParam !== lastFocusedParam;

  // Compare state
  const [compareList, setCompareList] = useState<MunicipalityData[]>([]);

  // Command palette state
  const [commandPaletteOpen, setCommandPaletteOpen] = useState(false);
  const mapRef = useRef<L.Map | null>(null);

  // Track active tooltip layer to prevent ghost tooltips
  const activeTooltipLayerRef = useRef<Layer | null>(null);

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
    semestersToAverage: 1,
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

  // Command palette keyboard shortcut (⌘K / Ctrl+K)
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        setCommandPaletteOpen(true);
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  // Handle zoom to location bounds from command palette
  const handleZoomToBounds = useCallback((bounds: [[number, number], [number, number]]) => {
    if (mapRef.current) {
      const leafletBounds = L.latLngBounds(bounds[0], bounds[1]);
      mapRef.current.flyToBounds(leafletBounds, {
        padding: [50, 50],
        duration: 0.8,
        maxZoom: 12,
      });
    }
  }, []);

  // Load data
  useEffect(() => {
    let cancelled = false;

    async function loadGeoJSON(): Promise<GeoJsonObject> {
      try {
        const params = new URLSearchParams({ simplified: "true" });
        if (filters.region) params.set("region", filters.region);
        if (filters.province) params.set("province", filters.province);

        const realRes = await fetch(`/api/map/geojson?${params}`);

        if (realRes.ok) {
          const data = (await realRes.json()) as FeatureCollection;
          const hasRealGeometries =
            data.features &&
            data.features.length > 0 &&
            data.features.some((f) => f.geometry !== null);

          if (hasRealGeometries) {
            return data;
          }
        }
      } catch (e) {
        console.warn("Failed to load real geometries, falling back to demo:", e);
      }

      const demoRes = await fetch("/demo/municipalities.geojson");
      if (!demoRes.ok) throw new Error("Failed to load demo geojson");
      return (await demoRes.json()) as GeoJsonObject;
    }

    async function load() {
      setLoading(true);
      setLoadingProgress({ percent: 5, stage: "Loading map boundaries..." });

      // Track completion of parallel fetches
      let geoComplete = false;
      let valuesComplete = false;

      const updateProgress = () => {
        if (geoComplete && valuesComplete) {
          setLoadingProgress({ percent: 95, stage: "Rendering map..." });
        } else if (geoComplete) {
          setLoadingProgress({ percent: 70, stage: "Loading property data..." });
        } else if (valuesComplete) {
          setLoadingProgress({ percent: 50, stage: "Processing boundaries..." });
        }
      };

      const [geo, valuesRes] = await Promise.all([
        loadGeoJSON().then((result) => {
          geoComplete = true;
          updateProgress();
          return result;
        }),
        fetch(
          `/api/map/layer?metric=${filters.metric}&horizonMonths=12&segment=${filters.propertySegment}&semesters=${filters.semestersToAverage}`
        ).then((res) => {
          valuesComplete = true;
          updateProgress();
          return res;
        }),
      ]);

      if (!valuesRes.ok) throw new Error("Failed to load layer values");

      setLoadingProgress({ percent: 85, stage: "Processing data..." });

      const layer = (await valuesRes.json()) as {
        features?: { municipalityId: string; value: number | null }[];
        availablePeriodsCount?: number;
      };

      if (cancelled) return;

      setGeojson(geo);
      setValuesByMunicipality(
        Object.fromEntries(
          (layer.features ?? []).map((f) => [f.municipalityId, f.value])
        )
      );
      // Update available periods count if provided
      if (layer.availablePeriodsCount !== undefined) {
        setAvailablePeriodsCount(layer.availablePeriodsCount);
      }
      setLoadingProgress({ percent: 100, stage: "Complete" });
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
  }, [filters.metric, filters.region, filters.province, filters.propertySegment, filters.semestersToAverage]);

  // Get visible municipality IDs based on current map bounds
  const visibleMunicipalityIds = useMemo(() => {
    if (!mapBounds || !geojson || geojson.type !== "FeatureCollection") {
      return null; // Return null to indicate we should use all values
    }

    const fc = geojson as FeatureCollection;
    const visibleIds = new Set<string>();

    for (const feature of fc.features) {
      const id = feature.properties?.municipality_id as string | undefined;
      if (!id || !feature.geometry) continue;

      // Check if the feature's bounding box intersects with the map bounds
      // For simplicity, we check if the feature's centroid is within bounds
      // or if any point of the geometry is within bounds
      const geometry = feature.geometry;
      let isVisible = false;

      if (geometry.type === "Polygon") {
        const coords = geometry.coordinates[0] as number[][];
        for (const [lng, lat] of coords) {
          if (mapBounds.contains([lat, lng])) {
            isVisible = true;
            break;
          }
        }
      } else if (geometry.type === "MultiPolygon") {
        outer: for (const polygon of geometry.coordinates) {
          const coords = polygon[0] as number[][];
          for (const [lng, lat] of coords) {
            if (mapBounds.contains([lat, lng])) {
              isVisible = true;
              break outer;
            }
          }
        }
      }

      if (isVisible) {
        visibleIds.add(id);
      }
    }

    return visibleIds.size > 0 ? visibleIds : null;
  }, [geojson, mapBounds]);

  // Calculate value domain from visible municipalities only
  const valueDomain = useMemo(() => {
    let vals: number[];

    if (visibleMunicipalityIds) {
      // Filter to only visible municipalities
      vals = Array.from(visibleMunicipalityIds)
        .map((id) => valuesByMunicipality[id])
        .filter((v): v is number => typeof v === "number" && Number.isFinite(v))
        .sort((a, b) => a - b);
    } else {
      // Fall back to all values
      vals = Object.values(valuesByMunicipality)
        .filter((v): v is number => typeof v === "number" && Number.isFinite(v))
        .sort((a, b) => a - b);
    }

    if (vals.length === 0) return { min: 0, max: 0 };
    return { min: vals[0], max: vals[vals.length - 1] };
  }, [valuesByMunicipality, visibleMunicipalityIds]);

  // Color function
  const colorFor = useCallback(
    (v: number | null | undefined) => {
      const scale = COLOR_SCALES[filters.metric];
      if (typeof v !== "number" || !Number.isFinite(v)) return scale.noData;

      // Use fixed range if defined, otherwise fall back to data-driven range
      const min = scale.fixedRange?.min ?? valueDomain.min;
      const max = scale.fixedRange?.max ?? valueDomain.max;
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
  // Requirements: Southern Italy region + population under 30,000
  // (threshold increased from 20,000 to 30,000 per 2025 budget law)
  const isFlatTaxEligible = useCallback((feature: Feature | undefined): boolean => {
    if (!feature?.properties) return false;
    const regionCode = feature.properties.region_code as string | undefined;
    const population = feature.properties.population as number | null | undefined;

    // Must be in Southern Italy region
    const inEligibleRegion = regionCode ? FLAT_TAX_ELIGIBLE_REGIONS.has(regionCode.padStart(2, "0")) : false;

    // Must have population under 30,000 (if population data available)
    const populationEligible = population === null || population === undefined || population < 30000;

    return inEligibleRegion && populationEligible;
  }, []);

  // Check if municipality is in a flat tax eligible region (Southern Italy)
  const isInEligibleRegion = useCallback((feature: Feature | undefined): boolean => {
    if (!feature?.properties) return false;
    const regionCode = feature.properties.region_code as string | undefined;
    return regionCode ? FLAT_TAX_ELIGIBLE_REGIONS.has(regionCode.padStart(2, "0")) : false;
  }, []);

  // Style function for GeoJSON
  const style = useCallback(
    (feature: Feature | undefined) => {
      const id = feature?.properties?.municipality_id as string | undefined;
      const v = id ? valuesByMunicipality[id] : null;

      if (filters.showFlatTaxEligible) {
        const isEligible = isFlatTaxEligible(feature);
        const inRegion = isInEligibleRegion(feature);

        if (isEligible) {
          // Eligible: Southern Italy with population under 30k - Green
          return {
            color: "#22c55e",
            weight: 2,
            fillColor: "rgba(34, 197, 94, 0.4)",
            fillOpacity: 0.7,
          };
        } else if (inRegion) {
          // In Southern Italy but population too high (30k+) - Red/Orange
          return {
            color: "#ef4444",
            weight: 2,
            fillColor: "rgba(239, 68, 68, 0.4)",
            fillOpacity: 0.7,
          };
        } else {
          // Not in eligible region (Northern Italy) - Gray
          return {
            color: "rgba(255, 255, 255, 0.1)",
            weight: 0.5,
            fillColor: "rgba(100, 100, 100, 0.3)",
            fillOpacity: 0.4,
          };
        }
      }

      return {
        color: "rgba(255, 255, 255, 0.2)",
        weight: 0.5,
        fillColor: colorFor(v),
        fillOpacity: 0.65,
      };
    },
    [valuesByMunicipality, colorFor, filters.showFlatTaxEligible, isFlatTaxEligible, isInEligibleRegion]
  );

  // Ref to always access the current style function (avoids stale closures)
  const styleRef = useRef(style);
  useEffect(() => {
    styleRef.current = style;
  }, [style]);

  // Clean up tooltips when GeoJSON data or metric changes to prevent ghost tooltips
  useEffect(() => {
    if (activeTooltipLayerRef.current) {
      activeTooltipLayerRef.current.closeTooltip();
      activeTooltipLayerRef.current = null;
    }
    // Also close any orphaned tooltips via the map ref
    if (mapRef.current) {
      mapRef.current.closeTooltip();
    }
  }, [geojson, valuesByMunicipality, filters.metric]);

  // Zoom change handler
  const handleZoomChange = useCallback((zoom: number) => {
    setCurrentZoom(zoom);
    // Clear focused municipality if zoomed out too far
    if (zoom < 10) {
      setFocusedMunicipalityId(null);
      setAutoDetectedMunicipalityId(null);
    }
  }, []);

  // Center change handler
  const handleCenterChange = useCallback((center: [number, number]) => {
    setMapCenter(center);
  }, []);

  // Bounds change handler
  const handleBoundsChange = useCallback((bounds: LatLngBounds) => {
    setMapBounds(bounds);
  }, []);

  // Auto-detect municipality at map center when zoomed in
  useEffect(() => {
    if (currentZoom < 11 || !geojson || geojson.type !== "FeatureCollection") {
      setAutoDetectedMunicipalityId(null);
      return;
    }

    const detected = findMunicipalityAtPoint(mapCenter, geojson as FeatureCollection);
    setAutoDetectedMunicipalityId(detected);
  }, [currentZoom, mapCenter, geojson]);

  // Effective municipality ID for zones: manual focus takes precedence over auto-detect
  const effectiveMunicipalityId = focusedMunicipalityId || autoDetectedMunicipalityId;

  // Click handler - fetches real data from API
  const handleFeatureClick = useCallback(
    async (feature: Feature) => {
      const props = feature.properties || {};
      const municipalityId = props.municipality_id || "";

      // Set focused municipality for zone display
      setFocusedMunicipalityId(municipalityId);

      // Use the already-loaded value from the map layer (same as tooltip)
      const mapValue = valuesByMunicipality[municipalityId];

      // Set initial data from feature properties and map layer value
      setSelectedMunicipality({
        municipalityId,
        name: props.name || props.municipality_id || "Unknown",
        provinceCode: props.province_code,
        regionCode: props.region_code,
        coastalFlag: props.coastal_flag,
        mountainFlag: props.mountain_flag,
        valueMidEurSqm: typeof mapValue === "number" ? mapValue : undefined,
      });
      setDrawerOpen(true);

      // Fetch full details from API
      try {
        const res = await fetch(
          `/api/municipality/${municipalityId}?segment=${filters.propertySegment}`
        );
        if (!res.ok) return;

        const data = await res.json();

        // Update with full data from API
        setSelectedMunicipality({
          municipalityId,
          name: data.municipality?.name || props.name || municipalityId,
          provinceCode: data.municipality?.provinceCode,
          provinceName: data.municipality?.provinceName,
          regionCode: data.municipality?.regionCode,
          regionName: data.municipality?.regionName,
          coastalFlag: data.municipality?.isCoastal,
          mountainFlag: data.municipality?.isMountain,
          // Values from historical data (latest semester)
          valueMidEurSqm: data.historicalValues?.[0]?.valueMidEurSqm ?? mapValue,
          valueMinEurSqm: data.historicalValues?.[0]?.valueMinEurSqm,
          valueMaxEurSqm: data.historicalValues?.[0]?.valueMaxEurSqm,
          // Forecasts
          forecastAppreciationPct: data.forecast?.appreciationPct,
          forecastGrossYieldPct: data.forecast?.grossYieldPct,
          opportunityScore: data.forecast?.opportunityScore,
          confidenceScore: data.forecast?.confidenceScore,
          // Demographics
          demographicsYear: data.demographics?.year,
          population: data.demographics?.totalPopulation,
          populationDensity: data.demographics?.populationDensity,
          youngRatio: data.demographics?.youngRatio,
          elderlyRatio: data.demographics?.elderlyRatio,
          foreignRatio: data.demographics?.foreignRatio,
          // Transactions (latest semester) - use municipal or fallback to provincial
          ntnTotal: data.historicalTransactions?.[0]?.ntnTotal
            ?? data.provincialTransactions?.[0]?.ntnTotal,
          ntnPer1000Pop: data.historicalTransactions?.[0]?.ntnPer1000Pop
            // Calculate from provincial data: use capoluogo NTN / population
            ?? (data.provincialTransactions?.[0]?.ntnCapoluogo && data.demographics?.totalPopulation
              ? (data.provincialTransactions[0].ntnCapoluogo / (data.demographics.totalPopulation / 1000))
              : undefined),
          // Provincial-level flag
          isProvincialTransaction: !data.historicalTransactions?.[0] && !!data.provincialTransactions?.[0],
          // Transaction period
          transactionPeriod: data.historicalTransactions?.[0]?.periodId
            ?? data.provincialTransactions?.[0]?.periodId,
        });
      } catch (e) {
        console.error("Failed to fetch municipality details:", e);
      }
    },
    [valuesByMunicipality, filters.propertySegment]
  );

  // Zone click handler - opens drawer for the zone's municipality
  const handleZoneClick = useCallback(
    async (municipalityId: string) => {
      // Set focused municipality for zone display
      setFocusedMunicipalityId(municipalityId);

      // Use the already-loaded value from the map layer
      const mapValue = valuesByMunicipality[municipalityId];

      // Set initial data with what we have
      setSelectedMunicipality({
        municipalityId,
        name: "Loading...",
        valueMidEurSqm: typeof mapValue === "number" ? mapValue : undefined,
      });
      setDrawerOpen(true);

      // Fetch full details from API
      try {
        const res = await fetch(
          `/api/municipality/${municipalityId}?segment=${filters.propertySegment}`
        );
        if (!res.ok) return;

        const data = await res.json();

        // Update with full data from API
        setSelectedMunicipality({
          municipalityId,
          name: data.municipality?.name || municipalityId,
          provinceCode: data.municipality?.provinceCode,
          provinceName: data.municipality?.provinceName,
          regionCode: data.municipality?.regionCode,
          regionName: data.municipality?.regionName,
          coastalFlag: data.municipality?.isCoastal,
          mountainFlag: data.municipality?.isMountain,
          valueMidEurSqm: data.historicalValues?.[0]?.valueMidEurSqm ?? mapValue,
          valueMinEurSqm: data.historicalValues?.[0]?.valueMinEurSqm,
          valueMaxEurSqm: data.historicalValues?.[0]?.valueMaxEurSqm,
          forecastAppreciationPct: data.forecast?.appreciationPct,
          forecastGrossYieldPct: data.forecast?.grossYieldPct,
          opportunityScore: data.forecast?.opportunityScore,
          confidenceScore: data.forecast?.confidenceScore,
          demographicsYear: data.demographics?.year,
          population: data.demographics?.totalPopulation,
          populationDensity: data.demographics?.populationDensity,
          youngRatio: data.demographics?.youngRatio,
          elderlyRatio: data.demographics?.elderlyRatio,
          foreignRatio: data.demographics?.foreignRatio,
          ntnTotal: data.historicalTransactions?.[0]?.ntnTotal
            ?? data.provincialTransactions?.[0]?.ntnTotal,
          ntnPer1000Pop: data.historicalTransactions?.[0]?.ntnPer1000Pop
            // Calculate from provincial data: use capoluogo NTN / population
            ?? (data.provincialTransactions?.[0]?.ntnCapoluogo && data.demographics?.totalPopulation
              ? (data.provincialTransactions[0].ntnCapoluogo / (data.demographics.totalPopulation / 1000))
              : undefined),
          isProvincialTransaction: !data.historicalTransactions?.[0] && !!data.provincialTransactions?.[0],
          transactionPeriod: data.historicalTransactions?.[0]?.periodId
            ?? data.provincialTransactions?.[0]?.periodId,
        });
      } catch (e) {
        console.error("Failed to fetch municipality details:", e);
      }
    },
    [valuesByMunicipality, filters.propertySegment]
  );

  // Metric-specific tooltip formatting
  const formatTooltipValue = useCallback((value: number, metric: MetricType): string => {
    switch (metric) {
      case "value_mid_eur_sqm":
        return `€${Math.round(value).toLocaleString()}/m²`;
      case "rent_mid_eur_sqm_month":
        return `€${value.toFixed(1)}/m²/mo`;
      case "gross_yield_pct":
        return `${value.toFixed(1)}% yield`;
      case "price_variance_pct":
        return `${value.toFixed(0)}% variance`;
      case "forecast_appreciation_pct":
        return `${value > 0 ? "+" : ""}${value.toFixed(1)}%`;
      case "forecast_gross_yield_pct":
        return `${value.toFixed(1)}% yield`;
      case "opportunity_score":
        return `${Math.round(value)} pts`;
      case "confidence_score":
        return `${Math.round(value)}% confidence`;
      case "foreign_ratio":
        return `${value.toFixed(1)}% foreign`;
      case "population_growth_rate":
        return `${value >= 0 ? "+" : ""}${value.toFixed(1)}% YoY`;
      default:
        return value.toLocaleString();
    }
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
            ? `${name}: ${formatTooltipValue(v, filters.metric)}`
            : `${name}: no data`;
      }

      layer.bindTooltip(label, {
        sticky: false,
        permanent: false,
        direction: "top",
        offset: [0, -10],
        className: "map-tooltip",
      });

      layer.on({
        click: () => handleFeatureClick(feature),
        mouseover: (e: LeafletMouseEvent) => {
          const target = e.target as Layer;
          // Close any previously active tooltip to prevent ghosts
          if (activeTooltipLayerRef.current && activeTooltipLayerRef.current !== target) {
            activeTooltipLayerRef.current.closeTooltip();
          }
          activeTooltipLayerRef.current = target;

          (target as L.Path).setStyle({
            weight: 2,
            color: "rgba(232, 196, 160, 0.8)",
            fillOpacity: 0.8,
          });
          (target as L.Path).bringToFront();
          target.openTooltip();
        },
        mouseout: (e: LeafletMouseEvent) => {
          const target = e.target as Layer;
          // Use styleRef to get current style (valueDomain may have changed since mount)
          const currentStyle = styleRef.current(feature);
          (target as L.Path).setStyle({
            weight: currentStyle.weight,
            color: currentStyle.color,
            fillColor: currentStyle.fillColor,
            fillOpacity: currentStyle.fillOpacity,
          });
          target.closeTooltip();
          // Clear active ref if this was the active layer
          if (activeTooltipLayerRef.current === target) {
            activeTooltipLayerRef.current = null;
          }
        },
      });
    },
    [valuesByMunicipality, handleFeatureClick, filters.showFlatTaxEligible, filters.metric, isFlatTaxEligible, formatTooltipValue]
  );

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
        availablePeriodsCount={availablePeriodsCount}
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
            key={`${filters.metric}-${filters.propertySegment}-${filters.showFlatTaxEligible}-${filters.region || 'all'}-${filters.province || 'all'}-${filters.semestersToAverage}-${Object.keys(valuesByMunicipality).length}`}
            data={geojson}
            style={style}
            onEachFeature={onEachFeature}
          />
        )}
        {/* Region boundaries for visual clarity */}
        <RegionBoundaries />
        {/* ZoneLayer handles metric filtering internally for municipality-only metrics */}
        <ZoneLayer
          municipalityId={effectiveMunicipalityId}
          visible={currentZoom >= 11}
          metric={filters.metric}
          onZoneClick={handleZoneClick}
        />
        {/* Labels layer on top of polygons for readability */}
        <TileLayer url={DARK_LABELS} pane="shadowPane" />
        <MapController onZoomChange={handleZoomChange} onCenterChange={handleCenterChange} onBoundsChange={handleBoundsChange} />
        <MapRefCapture mapRef={mapRef} activeTooltipLayerRef={activeTooltipLayerRef} />
        <FocusHandler
          municipalityId={focusParam}
          geojson={geojson as FeatureCollection | null}
          onFocused={setFocusedMunicipalityId}
          shouldFocus={shouldFocus}
          onFocusComplete={setLastFocusedParam}
        />
      </MapContainer>

      {/* Legend */}
      <MapLegend
        metric={filters.metric}
        min={valueDomain.min}
        max={valueDomain.max}
        isLoading={loading}
      />

      {/* Loading overlay */}
      {loading && (
        <div className="loading-overlay">
          <div className="loading-overlay__content">
            <div className="loading-overlay__spinner" />
            <div className="loading-overlay__progress">
              <div className="loading-overlay__progress-bar">
                <div
                  className="loading-overlay__progress-fill"
                  style={{ width: `${loadingProgress.percent}%` }}
                />
              </div>
              <span className="loading-overlay__percent">{loadingProgress.percent}%</span>
            </div>
            <span className="loading-overlay__text">{loadingProgress.stage}</span>
          </div>
        </div>
      )}

      {/* Search button */}
      <button
        className="search-button"
        onClick={() => setCommandPaletteOpen(true)}
        aria-label="Search locations"
      >
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="11" cy="11" r="8" />
          <path d="m21 21-4.35-4.35" />
        </svg>
        <span className="search-button__text">Search</span>
        <kbd className="search-button__kbd">⌘K</kbd>
      </button>

      {/* Zone indicator - hide for municipality-only metrics that don't have zone data */}
      {effectiveMunicipalityId && currentZoom >= 11 && ![
        "foreign_ratio",
        "population_growth_rate",
        "vehicle_arson_rate",
        "forecast_appreciation_pct",
        "forecast_gross_yield_pct",
        "opportunity_score",
        "confidence_score",
      ].includes(filters.metric) && (
        <div className="zone-indicator">
          <span className="zone-indicator__icon">◎</span>
          <span className="zone-indicator__text">
            Zones: {selectedMunicipality?.name || effectiveMunicipalityId}
          </span>
          {focusedMunicipalityId && (
            <button
              className="zone-indicator__close"
              onClick={() => setFocusedMunicipalityId(null)}
              aria-label="Clear zone focus"
            >
              ×
            </button>
          )}
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

      {/* Command Palette */}
      <CommandPalette
        isOpen={commandPaletteOpen}
        onClose={() => setCommandPaletteOpen(false)}
        onSelectLocation={handleZoomToBounds}
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

        .loading-overlay {
          position: absolute;
          inset: 0;
          z-index: 1000;
          display: flex;
          align-items: center;
          justify-content: center;
          background: rgba(13, 15, 18, 0.7);
          backdrop-filter: blur(4px);
        }

        .loading-overlay__content {
          display: flex;
          flex-direction: column;
          align-items: center;
          gap: 16px;
          padding: 32px 48px;
          background: linear-gradient(165deg,
            rgba(22, 25, 32, 0.98) 0%,
            rgba(13, 15, 18, 0.99) 100%
          );
          border: 1px solid rgba(255, 255, 255, 0.1);
          border-radius: 16px;
          box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
        }

        .loading-overlay__spinner {
          width: 40px;
          height: 40px;
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

        .loading-overlay__progress {
          display: flex;
          align-items: center;
          gap: 12px;
          width: 200px;
        }

        .loading-overlay__progress-bar {
          flex: 1;
          height: 6px;
          background: rgba(255, 255, 255, 0.1);
          border-radius: 3px;
          overflow: hidden;
        }

        .loading-overlay__progress-fill {
          height: 100%;
          background: linear-gradient(90deg, #c4785c, #e8a87c);
          border-radius: 3px;
          transition: width 0.3s ease-out;
        }

        .loading-overlay__percent {
          font-family: 'DM Sans', -apple-system, sans-serif;
          font-size: 0.8rem;
          font-weight: 600;
          color: #c4785c;
          min-width: 36px;
          text-align: right;
          font-variant-numeric: tabular-nums;
        }

        .loading-overlay__text {
          font-family: 'DM Sans', -apple-system, sans-serif;
          font-size: 0.85rem;
          font-weight: 500;
          color: #6b7a90;
          letter-spacing: 0.02em;
        }

        .search-button {
          position: absolute;
          top: 16px;
          left: 50%;
          transform: translateX(-50%);
          z-index: 1000;
          display: flex;
          align-items: center;
          gap: 8px;
          padding: 10px 16px;
          background: linear-gradient(165deg,
            rgba(22, 25, 32, 0.95) 0%,
            rgba(13, 15, 18, 0.97) 100%
          );
          backdrop-filter: blur(12px);
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 24px;
          font-family: 'DM Sans', -apple-system, sans-serif;
          font-size: 0.85rem;
          color: #6b7a90;
          cursor: pointer;
          transition: all 0.15s ease;
          box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
        }

        .search-button:hover {
          border-color: rgba(196, 120, 92, 0.3);
          color: #a8b3c7;
          box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
        }

        .search-button__text {
          font-weight: 500;
        }

        .search-button__kbd {
          padding: 3px 6px;
          background: rgba(255, 255, 255, 0.06);
          border: 1px solid rgba(255, 255, 255, 0.1);
          border-radius: 4px;
          font-size: 0.7rem;
          font-family: -apple-system, BlinkMacSystemFont, sans-serif;
          color: #4a5568;
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
