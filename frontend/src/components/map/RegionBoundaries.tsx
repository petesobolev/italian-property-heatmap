"use client";

import { useEffect, useState } from "react";
import { GeoJSON, Marker, useMap } from "react-leaflet";
import type { FeatureCollection, Feature, Polygon, MultiPolygon } from "geojson";
import type { PathOptions } from "leaflet";
import L from "leaflet";

interface RegionBoundariesProps {
  visible?: boolean;
}

const REGION_STYLE: PathOptions = {
  color: "rgba(255, 200, 150, 0.8)", // Warm orange/tan color
  weight: 1.5,
  fill: false,
  dashArray: "8, 4", // Dashed line for distinction
};

// Calculate centroid of a polygon (simple average of coordinates)
function calculateCentroid(coordinates: number[][][]): [number, number] {
  let totalX = 0;
  let totalY = 0;
  let totalPoints = 0;

  // Use outer ring only (first array)
  const ring = coordinates[0];
  for (const point of ring) {
    totalX += point[0]; // longitude
    totalY += point[1]; // latitude
    totalPoints++;
  }

  return [totalY / totalPoints, totalX / totalPoints]; // [lat, lng] for Leaflet
}

// Get centroid from a feature's geometry
function getFeatureCentroid(feature: Feature): [number, number] | null {
  const geometry = feature.geometry;

  if (geometry.type === "Polygon") {
    return calculateCentroid((geometry as Polygon).coordinates);
  } else if (geometry.type === "MultiPolygon") {
    // For MultiPolygon, use the largest polygon (most points)
    const polygons = (geometry as MultiPolygon).coordinates;
    let largestPolygon = polygons[0];
    let maxPoints = polygons[0][0].length;

    for (const polygon of polygons) {
      if (polygon[0].length > maxPoints) {
        maxPoints = polygon[0].length;
        largestPolygon = polygon;
      }
    }

    return calculateCentroid(largestPolygon);
  }

  return null;
}

// Create a text label icon
function createLabelIcon(name: string) {
  return L.divIcon({
    className: "region-label",
    html: `<span>${name}</span>`,
    iconSize: [0, 0],
    iconAnchor: [0, 0],
  });
}

// Component to render region labels that respond to zoom
function RegionLabels({ regions }: { regions: FeatureCollection }) {
  const map = useMap();
  const [zoom, setZoom] = useState(map.getZoom());

  useEffect(() => {
    const handleZoom = () => setZoom(map.getZoom());
    map.on("zoomend", handleZoom);
    return () => {
      map.off("zoomend", handleZoom);
    };
  }, [map]);

  // Only show labels at certain zoom levels (not too zoomed in)
  if (zoom > 9) return null;

  return (
    <>
      {regions.features.map((feature) => {
        const name = feature.properties?.region_name;
        const regionCode = feature.properties?.region_code;
        if (!name) return null;

        const centroid = getFeatureCentroid(feature);
        if (!centroid) return null;

        return (
          <Marker
            key={`label-${regionCode}`}
            position={centroid}
            icon={createLabelIcon(name)}
            interactive={false}
          />
        );
      })}
    </>
  );
}

export function RegionBoundaries({ visible = true }: RegionBoundariesProps) {
  const [regions, setRegions] = useState<FeatureCollection | null>(null);

  useEffect(() => {
    async function fetchRegions() {
      try {
        const response = await fetch("/api/regions/geojson");
        if (!response.ok) return;
        const data = await response.json();
        setRegions(data);
      } catch (error) {
        console.warn("Failed to fetch region boundaries:", error);
      }
    }

    fetchRegions();
  }, []);

  if (!visible || !regions || regions.features.length === 0) {
    return null;
  }

  return (
    <>
      <GeoJSON
        key="region-boundaries"
        data={regions}
        style={() => REGION_STYLE}
        onEachFeature={(feature: Feature, layer) => {
          const name = feature.properties?.region_name;
          if (name) {
            layer.bindTooltip(name, {
              sticky: true,
              className: "region-tooltip",
              direction: "center",
            });
          }
        }}
        pane="shadowPane"
      />
      <RegionLabels regions={regions} />
      <style jsx global>{`
        .region-label {
          background: transparent !important;
          border: none !important;
          box-shadow: none !important;
        }
        .region-label span {
          display: block;
          font-size: 11px;
          font-weight: 500;
          color: rgba(255, 220, 180, 0.85);
          text-shadow:
            -1px -1px 2px rgba(0, 0, 0, 0.8),
            1px -1px 2px rgba(0, 0, 0, 0.8),
            -1px 1px 2px rgba(0, 0, 0, 0.8),
            1px 1px 2px rgba(0, 0, 0, 0.8),
            0 0 4px rgba(0, 0, 0, 0.6);
          white-space: nowrap;
          pointer-events: none;
          transform: translate(-50%, -50%);
          letter-spacing: 0.5px;
          text-transform: uppercase;
        }
      `}</style>
    </>
  );
}
