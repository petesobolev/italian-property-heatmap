import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";
import { promises as fs } from "fs";
import path from "path";

interface ZoneRow {
  omi_zone_id: string;
  zone_code: string;
  zone_description: string | null;
  zone_type: string | null;
}

interface ZoneValueRow {
  omi_zone_id: string;
  period_id: string;
  value_mid_eur_sqm: number | null;
  value_min_eur_sqm: number | null;
  value_max_eur_sqm: number | null;
  rent_mid_eur_sqm_month: number | null;
  value_pct_change_1s: number | null;
}

interface MunicipalityGeom {
  municipality_id: string;
  geom: GeoJSON.Geometry | null;
  geom_simplified: GeoJSON.Geometry | null;
}

interface DemoMunicipalityFeature {
  type: "Feature";
  properties: {
    municipality_id: string;
    name: string;
  };
  geometry: GeoJSON.Geometry;
}

// Demo zone definitions for major cities
const DEMO_ZONES: Record<string, Array<{ code: string; type: string; name: string; value: number }>> = {
  "015146": [ // Milano
    { code: "B1", type: "B", name: "Centro Storico", value: 8500 },
    { code: "B2", type: "B", name: "Duomo - San Babila", value: 9200 },
    { code: "C1", type: "C", name: "Porta Venezia", value: 6200 },
    { code: "C2", type: "C", name: "Navigli", value: 6800 },
    { code: "C3", type: "C", name: "Isola", value: 5900 },
    { code: "D1", type: "D", name: "Città Studi", value: 4200 },
    { code: "D2", type: "D", name: "Lambrate", value: 4500 },
    { code: "D3", type: "D", name: "Lorenteggio", value: 3500 },
    { code: "E1", type: "E", name: "Quarto Oggiaro", value: 2800 },
    { code: "E2", type: "E", name: "Baggio", value: 2600 },
    { code: "R1", type: "R", name: "Chiaravalle", value: 2200 },
  ],
  "058091": [ // Roma
    { code: "B1", type: "B", name: "Centro Storico", value: 7200 },
    { code: "B2", type: "B", name: "Trastevere", value: 6800 },
    { code: "B3", type: "B", name: "Prati", value: 6500 },
    { code: "C1", type: "C", name: "Testaccio", value: 5200 },
    { code: "C2", type: "C", name: "San Giovanni", value: 4800 },
    { code: "C3", type: "C", name: "Trieste", value: 5500 },
    { code: "C4", type: "C", name: "Parioli", value: 6200 },
    { code: "D1", type: "D", name: "Tuscolano", value: 3500 },
    { code: "D2", type: "D", name: "Montesacro", value: 3200 },
    { code: "D3", type: "D", name: "Ostiense", value: 3400 },
    { code: "D4", type: "D", name: "Magliana", value: 2900 },
    { code: "E1", type: "E", name: "Tor Bella Monaca", value: 2200 },
    { code: "E2", type: "E", name: "Casal Palocco", value: 2800 },
    { code: "R1", type: "R", name: "Campagna Romana", value: 1800 },
  ],
  "048017": [ // Firenze
    { code: "B1", type: "B", name: "Centro Storico", value: 6500 },
    { code: "B2", type: "B", name: "Santa Croce", value: 5800 },
    { code: "C1", type: "C", name: "San Frediano", value: 4500 },
    { code: "C2", type: "C", name: "Campo di Marte", value: 4200 },
    { code: "D1", type: "D", name: "Rifredi", value: 3200 },
    { code: "D2", type: "D", name: "Isolotto", value: 2900 },
    { code: "E1", type: "E", name: "Peretola", value: 2400 },
  ],
  "027042": [ // Venezia
    { code: "B1", type: "B", name: "San Marco", value: 7500 },
    { code: "B2", type: "B", name: "Dorsoduro", value: 6200 },
    { code: "C1", type: "C", name: "Cannaregio", value: 4800 },
    { code: "C2", type: "C", name: "Giudecca", value: 4200 },
    { code: "D1", type: "D", name: "Mestre Centro", value: 2800 },
    { code: "D2", type: "D", name: "Marghera", value: 2200 },
    { code: "E1", type: "E", name: "Favaro Veneto", value: 1900 },
  ],
  "063049": [ // Napoli
    { code: "B1", type: "B", name: "Chiaia", value: 5500 },
    { code: "B2", type: "B", name: "Centro Storico", value: 4200 },
    { code: "C1", type: "C", name: "Vomero", value: 4800 },
    { code: "C2", type: "C", name: "Posillipo", value: 5200 },
    { code: "D1", type: "D", name: "Fuorigrotta", value: 2800 },
    { code: "D2", type: "D", name: "Bagnoli", value: 2400 },
    { code: "E1", type: "E", name: "Scampia", value: 1400 },
    { code: "E2", type: "E", name: "Ponticelli", value: 1600 },
  ],
};

// Generate demo zone geometries by creating a grid within the municipality bounds
function generateDemoZoneGeometries(
  zones: Array<{ code: string; type: string; name: string; value: number }>,
  municipalityGeom: GeoJSON.Geometry | null
): Array<{ zone: typeof zones[0]; geometry: GeoJSON.Polygon }> {
  const results: Array<{ zone: typeof zones[0]; geometry: GeoJSON.Polygon }> = [];

  if (!municipalityGeom) {
    return results;
  }

  // Get bounding box of the municipality
  const bbox = getBoundingBox(municipalityGeom);
  if (!bbox) return results;

  const [minLng, minLat, maxLng, maxLat] = bbox;
  const width = maxLng - minLng;
  const height = maxLat - minLat;

  // Calculate center for concentric zone layout
  const centerLng = (minLng + maxLng) / 2;
  const centerLat = (minLat + maxLat) / 2;

  // Sort zones by type (B=center, C=semi-center, D=peripheral, E=suburban, R=rural)
  const zoneOrder: Record<string, number> = { B: 0, C: 1, D: 2, E: 3, R: 4 };
  const sortedZones = [...zones].sort((a, b) => {
    return (zoneOrder[a.type] ?? 5) - (zoneOrder[b.type] ?? 5);
  });

  // Group zones by type
  const zonesByType = new Map<string, typeof zones>();
  for (const zone of sortedZones) {
    if (!zonesByType.has(zone.type)) {
      zonesByType.set(zone.type, []);
    }
    zonesByType.get(zone.type)!.push(zone);
  }

  // Generate geometries based on zone type
  const typeRadii: Record<string, [number, number]> = {
    B: [0, 0.2],
    C: [0.2, 0.45],
    D: [0.45, 0.7],
    E: [0.7, 0.9],
    R: [0.9, 1.1],
  };

  for (const [type, typeZones] of zonesByType) {
    const [innerR, outerR] = typeRadii[type] || [0.8, 1.0];
    const count = typeZones.length;

    for (let i = 0; i < count; i++) {
      const zone = typeZones[i];

      // Calculate angle segment for this zone
      const startAngle = (i / count) * 2 * Math.PI;
      const endAngle = ((i + 1) / count) * 2 * Math.PI;

      // Create a wedge-shaped polygon
      const innerRadius = Math.max(width, height) * 0.5 * innerR;
      const outerRadius = Math.max(width, height) * 0.5 * outerR;

      const points: [number, number][] = [];
      const steps = 12;

      // Inner arc
      for (let s = 0; s <= steps; s++) {
        const angle = startAngle + (s / steps) * (endAngle - startAngle);
        points.push([
          centerLng + innerRadius * Math.cos(angle) * (width / Math.max(height, 0.001)),
          centerLat + innerRadius * Math.sin(angle),
        ]);
      }

      // Outer arc (reverse direction)
      for (let s = steps; s >= 0; s--) {
        const angle = startAngle + (s / steps) * (endAngle - startAngle);
        points.push([
          centerLng + outerRadius * Math.cos(angle) * (width / Math.max(height, 0.001)),
          centerLat + outerRadius * Math.sin(angle),
        ]);
      }

      // Close the polygon
      points.push(points[0]);

      results.push({
        zone,
        geometry: {
          type: "Polygon",
          coordinates: [points],
        },
      });
    }
  }

  return results;
}

function getBoundingBox(geom: GeoJSON.Geometry): [number, number, number, number] | null {
  const coords: number[][] = [];

  function extractCoords(g: GeoJSON.Geometry) {
    if (g.type === "Point") {
      coords.push(g.coordinates as number[]);
    } else if (g.type === "MultiPoint" || g.type === "LineString") {
      coords.push(...(g.coordinates as number[][]));
    } else if (g.type === "MultiLineString" || g.type === "Polygon") {
      for (const ring of g.coordinates as number[][][]) {
        coords.push(...ring);
      }
    } else if (g.type === "MultiPolygon") {
      for (const polygon of g.coordinates as number[][][][]) {
        for (const ring of polygon) {
          coords.push(...ring);
        }
      }
    } else if (g.type === "GeometryCollection") {
      for (const geom of g.geometries) {
        extractCoords(geom);
      }
    }
  }

  extractCoords(geom);

  if (coords.length === 0) return null;

  let minLng = Infinity, minLat = Infinity;
  let maxLng = -Infinity, maxLat = -Infinity;

  for (const [lng, lat] of coords) {
    minLng = Math.min(minLng, lng);
    minLat = Math.min(minLat, lat);
    maxLng = Math.max(maxLng, lng);
    maxLat = Math.max(maxLat, lat);
  }

  return [minLng, minLat, maxLng, maxLat];
}

async function loadDemoMunicipality(municipalityId: string): Promise<GeoJSON.Geometry | null> {
  try {
    const filePath = path.join(process.cwd(), "public", "demo", "municipalities.geojson");
    const fileContent = await fs.readFile(filePath, "utf-8");
    const data = JSON.parse(fileContent) as { features: DemoMunicipalityFeature[] };

    const feature = data.features.find(
      (f) => f.properties.municipality_id === municipalityId
    );

    return feature?.geometry || null;
  } catch {
    return null;
  }
}

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const municipalityId = searchParams.get("municipality_id");
  const segment = searchParams.get("segment") ?? "residential";

  if (!municipalityId) {
    return NextResponse.json(
      { error: "municipality_id is required" },
      { status: 400 }
    );
  }

  const supabase = createSupabaseServerClient();

  // First, try to get zones with real geometry from database
  const { data: geojsonData } = await supabase.rpc(
    "get_omi_zones_geojson",
    { p_municipality_id: municipalityId }
  );

  if (geojsonData?.features?.length > 0 &&
      geojsonData.features.some((f: { geometry: unknown }) => f.geometry !== null)) {
    // Real geometry exists in database
    const zoneIds = geojsonData.features.map(
      (f: { properties: { omi_zone_id: string } }) => f.properties.omi_zone_id
    );

    const { data: values } = await supabase
      .schema("mart")
      .from("omi_zone_values_semester")
      .select("omi_zone_id, value_mid_eur_sqm")
      .in("omi_zone_id", zoneIds)
      .eq("property_segment", segment)
      .order("period_id", { ascending: false });

    const latestValues = new Map<string, number | null>();
    for (const v of (values as ZoneValueRow[]) ?? []) {
      if (!latestValues.has(v.omi_zone_id)) {
        latestValues.set(v.omi_zone_id, v.value_mid_eur_sqm);
      }
    }

    for (const feature of geojsonData.features) {
      feature.properties.value_mid_eur_sqm =
        latestValues.get(feature.properties.omi_zone_id) ?? null;
    }

    return NextResponse.json({
      ...geojsonData,
      municipality_id: municipalityId,
      segment,
      source: "database",
    });
  }

  // No real geometry, try to get zones from database without geometry
  const { data: zonesData } = await supabase
    .schema("core")
    .from("omi_zones")
    .select("omi_zone_id, zone_code, zone_description, zone_type")
    .eq("municipality_id", municipalityId);

  let zones = (zonesData as ZoneRow[]) ?? [];

  // If no zones in database, check if we have demo zones
  const demoZones = DEMO_ZONES[municipalityId];

  if (zones.length === 0 && !demoZones) {
    return NextResponse.json({
      type: "FeatureCollection",
      features: [],
      municipality_id: municipalityId,
      segment,
      source: "no_zones",
    });
  }

  // Get municipality geometry - first try database, then demo file
  let municipalityGeom: GeoJSON.Geometry | null = null;

  const { data: munGeom } = await supabase
    .schema("core")
    .from("municipalities")
    .select("municipality_id, geom, geom_simplified")
    .eq("municipality_id", municipalityId)
    .single();

  municipalityGeom = (munGeom as MunicipalityGeom | null)?.geom_simplified ||
                     (munGeom as MunicipalityGeom | null)?.geom ||
                     null;

  // If no geometry in database, try demo file
  if (!municipalityGeom) {
    municipalityGeom = await loadDemoMunicipality(municipalityId);
  }

  if (!municipalityGeom) {
    return NextResponse.json({
      type: "FeatureCollection",
      features: [],
      municipality_id: municipalityId,
      segment,
      source: "no_geometry",
    });
  }

  // Use demo zones if database zones are empty
  if (demoZones && zones.length === 0) {
    // Generate features from demo zones
    const zoneGeometries = generateDemoZoneGeometries(demoZones, municipalityGeom);

    const features = zoneGeometries.map(({ zone, geometry }) => ({
      type: "Feature" as const,
      properties: {
        omi_zone_id: `${municipalityId}_${zone.code}`,
        zone_code: zone.code,
        zone_description: zone.name,
        zone_type: zone.type,
        value_mid_eur_sqm: zone.value,
      },
      geometry,
    }));

    return NextResponse.json({
      type: "FeatureCollection",
      features,
      municipality_id: municipalityId,
      segment,
      source: "demo",
    });
  }

  // Database zones exist but no geometry - generate demo geometries
  const zoneIds = zones.map((z) => z.omi_zone_id);
  const { data: values } = await supabase
    .schema("mart")
    .from("omi_zone_values_semester")
    .select("omi_zone_id, value_mid_eur_sqm")
    .in("omi_zone_id", zoneIds)
    .eq("property_segment", segment)
    .order("period_id", { ascending: false });

  const latestValues = new Map<string, number | null>();
  for (const v of (values as ZoneValueRow[]) ?? []) {
    if (!latestValues.has(v.omi_zone_id)) {
      latestValues.set(v.omi_zone_id, v.value_mid_eur_sqm);
    }
  }

  // Convert database zones to the format needed for geometry generation
  const zoneData = zones.map((z) => ({
    code: z.zone_code,
    type: z.zone_type?.charAt(0) || "D",
    name: z.zone_description || z.zone_code,
    value: latestValues.get(z.omi_zone_id) ?? 3000,
  }));

  const zoneGeometries = generateDemoZoneGeometries(zoneData, municipalityGeom);

  const features = zones.map((zone, i) => ({
    type: "Feature" as const,
    properties: {
      omi_zone_id: zone.omi_zone_id,
      zone_code: zone.zone_code,
      zone_description: zone.zone_description,
      zone_type: zone.zone_type,
      value_mid_eur_sqm: latestValues.get(zone.omi_zone_id) ?? null,
    },
    geometry: zoneGeometries[i]?.geometry ?? null,
  }));

  const validFeatures = features.filter((f) => f.geometry !== null);

  return NextResponse.json({
    type: "FeatureCollection",
    features: validFeatures,
    municipality_id: municipalityId,
    segment,
    source: "demo_geometry",
  });
}
