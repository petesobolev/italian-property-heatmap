"use client";

import { useEffect, useState, useCallback, useMemo, useRef } from "react";
import { GeoJSON, useMap, Marker } from "react-leaflet";
import type { Feature, FeatureCollection } from "geojson";
import type { Layer, LatLngExpression, PathOptions } from "leaflet";
import L from "leaflet";

interface ZoneProperties {
  omi_zone_id: string;
  zone_code: string;
  zone_description: string | null;
  zone_type: string | null;
  value_mid_eur_sqm: number | null;
}

interface ZoneLayerProps {
  municipalityId: string | null;
  visible: boolean;
  metric?: string;
}

// Minimum zoom level to show zones
const ZONE_VISIBLE_ZOOM = 11;
// Zoom level to show permanent zone labels (same as visible zoom)
const ZONE_LABEL_ZOOM = 11;

// Color palette for zones based on zone type
const ZONE_TYPE_COLORS: Record<string, string> = {
  B: "rgba(196, 120, 92, 0.6)", // Central - terracotta
  C: "rgba(74, 144, 181, 0.6)", // Semi-central - blue
  D: "rgba(124, 196, 212, 0.6)", // Peripheral - light blue
  E: "rgba(184, 224, 236, 0.6)", // Suburban - very light blue
  R: "rgba(100, 140, 100, 0.6)", // Rural - green
};

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

export function ZoneLayer({ municipalityId, visible, metric }: ZoneLayerProps) {
  const map = useMap();
  const [zones, setZones] = useState<FeatureCollection | null>(null);
  const [loading, setLoading] = useState(false);
  const [currentZoom, setCurrentZoom] = useState(map.getZoom());
  const [valueDomain, setValueDomain] = useState<{ min: number; max: number }>({ min: 0, max: 0 });

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

  // Fetch zones when municipality changes (fetch regardless of visibility)
  useEffect(() => {
    if (!municipalityId) {
      setZones(null);
      return;
    }

    let cancelled = false;

    async function fetchZones() {
      setLoading(true);
      try {
        const response = await fetch(
          `/api/zones/geojson?municipality_id=${municipalityId}&segment=residential`
        );
        if (!response.ok) {
          console.warn("Failed to fetch zones");
          return;
        }
        const data = await response.json();
        if (cancelled) return;

        setZones(data);

        // Calculate value domain for coloring
        const values = data.features
          .map((f: Feature) => (f.properties as ZoneProperties)?.value_mid_eur_sqm)
          .filter((v: unknown): v is number => typeof v === "number" && Number.isFinite(v));

        if (values.length > 0) {
          setValueDomain({
            min: Math.min(...values),
            max: Math.max(...values),
          });
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
  }, [municipalityId]);

  // Color function for zones
  const colorFor = useCallback(
    (feature: Feature | undefined) => {
      const props = feature?.properties as ZoneProperties | undefined;
      const zoneType = props?.zone_type?.charAt(0) ?? "";

      // If we have value data and are showing value metric, use value-based coloring
      if (metric === "value_mid_eur_sqm" && props?.value_mid_eur_sqm != null) {
        const { min, max } = valueDomain;
        if (max > min) {
          const t = Math.max(0, Math.min(1, (props.value_mid_eur_sqm - min) / (max - min)));
          // Use a blue-to-terracotta scale
          const r = Math.round(45 + t * 151); // 45 -> 196
          const g = Math.round(90 + t * 30);  // 90 -> 120
          const b = Math.round(135 - t * 43); // 135 -> 92
          return `rgba(${r}, ${g}, ${b}, 0.65)`;
        }
      }

      // Default to zone type coloring
      return ZONE_TYPE_COLORS[zoneType] || "rgba(150, 150, 150, 0.5)";
    },
    [metric, valueDomain]
  );

  // Style function for zones
  const style = useCallback(
    (feature: Feature | undefined): PathOptions => ({
      color: "rgba(255, 255, 255, 0.6)",
      weight: 2,
      fillColor: colorFor(feature),
      fillOpacity: 0.7,
    }),
    [colorFor]
  );

  // Use a ref to always access the current style function in event handlers
  const styleRef = useRef(style);
  useEffect(() => {
    styleRef.current = style;
  }, [style]);

  // Event handlers for each zone feature
  const onEachFeature = useCallback(
    (feature: Feature, layer: Layer) => {
      const props = feature.properties as ZoneProperties;
      const zoneName = props.zone_description || props.zone_code;
      const value = props.value_mid_eur_sqm;

      // Build tooltip content
      let tooltipContent = `<strong>${props.zone_code}</strong>`;
      if (props.zone_description) {
        tooltipContent += `<br/>${props.zone_description}`;
      }
      if (value != null) {
        tooltipContent += `<br/><span style="color: #c4785c">\u20AC${Math.round(value).toLocaleString()}/m\u00B2</span>`;
      }
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
      });
    },
    [] // No dependencies - we use styleRef to always get current style
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
  if (!visible || currentZoom < ZONE_VISIBLE_ZOOM || !zones) {
    return null;
  }

  const showLabels = currentZoom >= ZONE_LABEL_ZOOM;

  return (
    <>
      <GeoJSON
        key={`zones-${municipalityId}-${metric}`}
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
