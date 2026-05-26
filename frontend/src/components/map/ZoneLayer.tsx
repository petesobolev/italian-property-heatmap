"use client";

import { useEffect, useState, useCallback, useMemo, useRef } from "react";
import { GeoJSON, useMap, Marker } from "react-leaflet";
import type { Feature, FeatureCollection } from "geojson";
import type { Layer, LatLngExpression, PathOptions, GeoJSON as LeafletGeoJSON } from "leaflet";
import L from "leaflet";
import type { MetricType } from "./FiltersSidebar";

interface ZoneProperties {
  omi_zone_id: string;
  zone_code: string;
  zone_description: string | null;
  zone_type: string | null;
  metric_value: number | null;
  value_mid_eur_sqm: number | null;
}

interface ZoneLayerProps {
  municipalityId: string | null;
  visible: boolean;
  metric?: MetricType;
  onZoneClick?: (municipalityId: string) => void;
}

// Minimum zoom level to show zones
const ZONE_VISIBLE_ZOOM = 11;
// Zoom level to show permanent zone labels (same as visible zoom)
const ZONE_LABEL_ZOOM = 11;

// Color scales for different metrics (matching MapInner.tsx)
const COLOR_SCALES: Record<string, number[][]> = {
  value_mid_eur_sqm: [
    [30, 58, 95],    // Deep Mediterranean blue
    [45, 90, 135],
    [74, 144, 181],
    [124, 196, 212],
    [184, 224, 236],
  ],
  rent_mid_eur_sqm_month: [
    [240, 249, 232],  // Light green (low rent)
    [186, 228, 188],
    [123, 204, 196],
    [67, 162, 202],
    [8, 104, 172],    // Deep blue (high rent)
  ],
  gross_yield_pct: [
    [254, 240, 217],  // Cream (low yield)
    [253, 204, 138],
    [252, 141, 89],
    [227, 74, 51],
    [179, 0, 0],      // Deep red (high yield)
  ],
  price_variance_pct: [
    [26, 152, 80],    // Green (low variance)
    [145, 207, 96],
    [255, 255, 191],  // Yellow (moderate)
    [252, 141, 89],
    [215, 48, 39],    // Red (high variance)
  ],
};

// Fixed ranges for certain metrics
const FIXED_RANGES: Record<string, { min: number; max: number }> = {
  gross_yield_pct: { min: 0, max: 15 },
  price_variance_pct: { min: 0, max: 100 },
};

const NO_DATA_COLOR = "rgba(42, 45, 53, 0.6)"; // Neutral gray for missing data

// Format metric value for tooltip
function formatMetricValue(value: number | null, metric: string): string {
  if (value == null || !Number.isFinite(value)) return "N/A";

  switch (metric) {
    case "value_mid_eur_sqm":
      return `€${Math.round(value).toLocaleString()}/m²`;
    case "rent_mid_eur_sqm_month":
      return `€${value.toFixed(1)}/m²/mo`;
    case "gross_yield_pct":
      return `${value.toFixed(1)}% yield`;
    case "price_variance_pct":
      return `${value.toFixed(0)}% variance`;
    default:
      return `€${Math.round(value).toLocaleString()}/m²`;
  }
}

// Get zone center for label placement
function getFeatureCenter(feature: Feature): LatLngExpression | null {
  if (!feature.geometry) return null;

  try {
    const geojsonLayer = L.geoJSON(feature);
    const bounds = geojsonLayer.getBounds();
    if (bounds.isValid()) {
      const center = bounds.getCenter();
      return [center.lat, center.lng];
    }
  } catch {
    return null;
  }
  return null;
}

// Create a div icon for zone labels
function createLabelIcon(zoneCode: string, zoneName: string | null): L.DivIcon {
  const displayText = zoneName || zoneCode;
  const shortText = displayText.length > 20 ? displayText.slice(0, 18) + "..." : displayText;

  return L.divIcon({
    className: "zone-label",
    html: `<div class="zone-label__inner">
      <span class="zone-label__code">${zoneCode}</span>
      ${zoneName ? `<span class="zone-label__name">${shortText}</span>` : ""}
    </div>`,
    iconSize: [0, 0],
    iconAnchor: [0, 0],
  });
}

export function ZoneLayer({ municipalityId, visible, metric = "value_mid_eur_sqm", onZoneClick }: ZoneLayerProps) {
  const map = useMap();
  const [zones, setZones] = useState<FeatureCollection | null>(null);
  const [loading, setLoading] = useState(false);
  const [currentZoom, setCurrentZoom] = useState(map.getZoom());
  const [valueDomain, setValueDomain] = useState<{ min: number; max: number }>({ min: 0, max: 0 });

  // Create a custom pane for zones to ensure they render above municipalities
  useEffect(() => {
    if (!map.getPane("zonesPane")) {
      map.createPane("zonesPane");
      const pane = map.getPane("zonesPane");
      if (pane) {
        pane.style.zIndex = "450"; // Above overlayPane (400) but below tooltips (600)
      }
    }
  }, [map]);

  // Track zoom level
  useEffect(() => {
    const handleZoom = () => {
      setCurrentZoom(map.getZoom());
    };

    map.on("zoomend", handleZoom);
    return () => {
      map.off("zoomend", handleZoom);
    };
  }, [map]);

  // Fetch zones when municipality or metric changes
  useEffect(() => {
    if (!municipalityId) {
      setZones(null);
      return;
    }

    // Skip fetching for metrics that don't have zone-level data
    // These are municipality-level only: demographics, forecasts, crime stats
    const municipalityOnlyMetrics = [
      "foreign_ratio",
      "population_growth_rate",
      "vehicle_arson_rate",
      "forecast_appreciation_pct",
      "forecast_gross_yield_pct",
      "opportunity_score",
      "confidence_score",
    ];
    if (municipalityOnlyMetrics.includes(metric)) {
      setZones(null);
      return;
    }

    let cancelled = false;

    async function fetchZones() {
      setLoading(true);
      try {
        const response = await fetch(
          `/api/zones/geojson?municipality_id=${municipalityId}&segment=residential&metric=${metric}`
        );
        if (!response.ok) {
          console.warn("Failed to fetch zones");
          return;
        }
        const data = await response.json();
        if (cancelled) return;

        setZones(data);

        // Calculate value domain for coloring (use fixed range if defined)
        const fixedRange = FIXED_RANGES[metric];
        if (fixedRange) {
          setValueDomain(fixedRange);
        } else {
          const values = data.features
            .map((f: Feature) => (f.properties as ZoneProperties)?.metric_value ?? (f.properties as ZoneProperties)?.value_mid_eur_sqm)
            .filter((v: unknown): v is number => typeof v === "number" && Number.isFinite(v));

          if (values.length > 0) {
            setValueDomain({
              min: Math.min(...values),
              max: Math.max(...values),
            });
          }
        }
      } catch (error) {
        console.warn("Error fetching zones:", error);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    fetchZones();

    return () => {
      cancelled = true;
    };
  }, [municipalityId, metric]);

  // Color function for zones - uses metric-specific color scale
  const colorFor = useCallback(
    (feature: Feature | undefined) => {
      const props = feature?.properties as ZoneProperties | undefined;
      const value = props?.metric_value ?? props?.value_mid_eur_sqm;

      // No value data - show neutral gray
      if (value == null || !Number.isFinite(value)) {
        return NO_DATA_COLOR;
      }

      // Interpolate using the metric-specific color scale
      const { min, max } = valueDomain;
      if (max <= min) {
        return NO_DATA_COLOR;
      }

      const t = Math.max(0, Math.min(1, (value - min) / (max - min)));
      const stops = COLOR_SCALES[metric] || COLOR_SCALES.value_mid_eur_sqm;

      // Interpolate between stops
      const scaledT = t * (stops.length - 1);
      const lowerIdx = Math.floor(scaledT);
      const upperIdx = Math.min(lowerIdx + 1, stops.length - 1);
      const localT = scaledT - lowerIdx;

      const rgb = stops[lowerIdx].map((c, i) =>
        Math.round(c + (stops[upperIdx][i] - c) * localT)
      );

      return `rgba(${rgb[0]}, ${rgb[1]}, ${rgb[2]}, 0.75)`;
    },
    [valueDomain, metric]
  );

  // Style function for zones
  const style = useCallback(
    (feature: Feature | undefined): PathOptions => ({
      color: "rgba(255, 255, 255, 0.8)",
      weight: 2,
      fillColor: colorFor(feature),
      fillOpacity: 0.8,
      pane: "zonesPane", // Render above municipality layer
    }),
    [colorFor]
  );

  // Use a ref to always access the current style function in event handlers
  const styleRef = useRef(style);
  useEffect(() => {
    styleRef.current = style;
  }, [style]);

  // Ref to GeoJSON layer for managing interactivity
  const geoJsonRef = useRef<LeafletGeoJSON | null>(null);

  // Determine if a zone is the "background" zone (largest, usually R1/R type)
  // that should be non-interactive when overlapping with smaller zones
  // Uses zone_type as a fast heuristic: R (rural) zones are typically background
  const backgroundZoneCode = useMemo(() => {
    if (!zones?.features || zones.features.length <= 1) return null;

    // Fast path: if there's exactly one R-type zone, it's likely the background
    const rZones = zones.features.filter(
      (f) => (f.properties as ZoneProperties).zone_type === "R"
    );

    if (rZones.length === 1) {
      return (rZones[0].properties as ZoneProperties).zone_code;
    }

    // If no R zones or multiple R zones, don't mark any as background
    return null;
  }, [zones]);

  // After render, disable pointer events on the background zone so smaller zones can capture events
  useEffect(() => {
    if (!geoJsonRef.current || !backgroundZoneCode) return;

    const disableBackgroundPointerEvents = () => {
      if (!geoJsonRef.current) return;

      // Find the background zone's layer and disable its pointer events via CSS class
      geoJsonRef.current.eachLayer((layer) => {
        const pathLayer = layer as L.Path & { feature?: Feature; getElement?: () => SVGElement | null };
        const props = pathLayer.feature?.properties as ZoneProperties | undefined;
        if (props?.zone_code === backgroundZoneCode) {
          // Add class to disable pointer events
          const element = pathLayer.getElement?.();
          if (element) {
            element.classList.add("zone-background-noninteractive");
          }
        }
      });
    };

    // Defer to ensure SVG elements are rendered - use setTimeout for more reliable timing
    const timeoutId = setTimeout(disableBackgroundPointerEvents, 100);
    return () => clearTimeout(timeoutId);
  }, [zones, backgroundZoneCode, valueDomain]);

  // Event handlers for each zone feature - needs metric for tooltip formatting
  const onEachFeature = useCallback(
    (feature: Feature, layer: Layer) => {
      const props = feature.properties as ZoneProperties;
      const value = props.metric_value ?? props.value_mid_eur_sqm;

      // Build tooltip content
      let tooltipContent = `<strong>${props.zone_code}</strong>`;
      if (props.zone_description) {
        tooltipContent += `<br/>${props.zone_description}`;
      }
      // Show metric value with proper formatting
      const formattedValue = formatMetricValue(value, metric);
      const valueColor = value != null ? "#4a90b5" : "#6b7a90";
      tooltipContent += `<br/><span style="color: ${valueColor}; font-weight: 600">${formattedValue}</span>`;

      if (props.zone_type) {
        const typeLabel = getZoneTypeLabel(props.zone_type);
        tooltipContent += `<br/><span style="color: #6b7a90; font-size: 0.75rem">${typeLabel}</span>`;
      }

      layer.bindTooltip(tooltipContent, {
        sticky: true,
        className: "zone-tooltip",
        direction: "top",
        offset: [0, -10],
      });

      layer.on({
        mouseover: (e) => {
          const target = e.target;
          target.setStyle({
            weight: 3,
            color: "rgba(255, 255, 255, 0.9)",
            fillOpacity: 0.85,
          });
          target.bringToFront();
        },
        mouseout: (e) => {
          const target = e.target;
          // Use styleRef to get the current style function (avoids stale closure)
          target.setStyle(styleRef.current(feature));
        },
        click: () => {
          // Open municipality drawer when clicking on a zone
          if (municipalityId && onZoneClick) {
            onZoneClick(municipalityId);
          }
        },
      });
    },
    [metric, municipalityId, onZoneClick] // Depend on metric for proper tooltip formatting
  );

  // Calculate zone centers for labels
  const zoneCenters = useMemo(() => {
    if (!zones?.features) return [];

    return zones.features
      .map((feature) => {
        const center = getFeatureCenter(feature);
        if (!center) return null;

        const props = feature.properties as ZoneProperties;
        return {
          center,
          zoneCode: props.zone_code,
          zoneName: props.zone_description,
        };
      })
      .filter((item): item is NonNullable<typeof item> => item !== null);
  }, [zones]);

  // Don't render if not visible or zoom is too low
  if (!visible || currentZoom < ZONE_VISIBLE_ZOOM) {
    return null;
  }

  // Show loading state or no zones message
  if (loading || !zones || zones.features.length === 0) {
    return null;
  }

  const showLabels = currentZoom >= ZONE_LABEL_ZOOM;

  return (
    <>
      <GeoJSON
        ref={geoJsonRef}
        key={`zones-${municipalityId}-${metric}-${valueDomain.min}-${valueDomain.max}`}
        data={zones}
        style={style}
        onEachFeature={onEachFeature}
      />

      {/* Show permanent labels when zoomed in enough */}
      {showLabels &&
        zoneCenters.map(({ center, zoneCode, zoneName }) => (
          <Marker
            key={zoneCode}
            position={center}
            icon={createLabelIcon(zoneCode, zoneName)}
            interactive={false}
          />
        ))}

      <style jsx global>{`
        .zone-tooltip {
          background: linear-gradient(
            165deg,
            rgba(22, 25, 32, 0.95) 0%,
            rgba(13, 15, 18, 0.97) 100%
          );
          border: 1px solid rgba(196, 120, 92, 0.3);
          border-radius: 8px;
          padding: 10px 14px;
          font-family: "DM Sans", -apple-system, sans-serif;
          font-size: 0.85rem;
          color: #f0f2f5;
          box-shadow: 0 4px 16px rgba(0, 0, 0, 0.4);
          line-height: 1.5;
        }

        .zone-tooltip::before {
          display: none;
        }

        /* Make background zones (R-type) non-interactive so smaller zones can capture events */
        .zone-background-noninteractive {
          pointer-events: none !important;
        }

        .zone-label {
          background: transparent !important;
          border: none !important;
        }

        .zone-label__inner {
          display: flex;
          flex-direction: column;
          align-items: center;
          gap: 2px;
          transform: translate(-50%, -50%);
          text-shadow: 0 1px 3px rgba(0, 0, 0, 0.8), 0 0 6px rgba(0, 0, 0, 0.6);
          pointer-events: none;
        }

        .zone-label__code {
          font-family: monospace;
          font-size: 0.75rem;
          font-weight: 700;
          color: #fff;
          background: rgba(196, 120, 92, 0.85);
          padding: 2px 6px;
          border-radius: 4px;
          white-space: nowrap;
        }

        .zone-label__name {
          font-family: "DM Sans", -apple-system, sans-serif;
          font-size: 0.7rem;
          font-weight: 500;
          color: #f0f2f5;
          white-space: nowrap;
          max-width: 120px;
          overflow: hidden;
          text-overflow: ellipsis;
        }
      `}</style>
    </>
  );
}

function getZoneTypeLabel(type: string): string {
  const labels: Record<string, string> = {
    B: "Central",
    C: "Semi-central",
    D: "Peripheral",
    E: "Suburban",
    R: "Rural",
    centrale: "Central",
    semicentrale: "Semi-central",
    periferica: "Peripheral",
    suburbana: "Suburban",
    rurale: "Rural",
  };
  return labels[type] || labels[type?.charAt(0)] || type;
}
