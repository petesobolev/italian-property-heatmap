"use client";

import { useEffect, useState } from "react";
import { GeoJSON } from "react-leaflet";
import type { FeatureCollection, Feature } from "geojson";
import type { PathOptions } from "leaflet";

interface RegionBoundariesProps {
  visible?: boolean;
}

const REGION_STYLE: PathOptions = {
  color: "rgba(255, 200, 150, 0.8)", // Warm orange/tan color
  weight: 1.5,
  fill: false,
  dashArray: "8, 4", // Dashed line for distinction
};

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
  );
}
