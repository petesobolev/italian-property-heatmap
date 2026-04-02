import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";
import { readFileSync, existsSync } from "fs";
import { join } from "path";

interface GeoJSONFeature {
  type: "Feature";
  properties: {
    municipality_id: string;
    name: string;
    province_code: string;
    region_code: string;
  };
  geometry: unknown;
}

interface GeoJSONCollection {
  type: "FeatureCollection";
  features: GeoJSONFeature[];
}

// Cache for GeoJSON data
let geoJSONCache: GeoJSONCollection | null = null;

function getGeoJSONData(): GeoJSONCollection | null {
  if (geoJSONCache) return geoJSONCache;

  const filePath = join(process.cwd(), "public", "demo", "municipalities.geojson");
  if (!existsSync(filePath)) return null;

  try {
    const data = readFileSync(filePath, "utf-8");
    geoJSONCache = JSON.parse(data) as GeoJSONCollection;
    return geoJSONCache;
  } catch {
    return null;
  }
}

function searchGeoJSON(query: string, limit: number): Array<{
  id: string;
  name: string;
  provinceCode: string;
  regionCode: string;
}> {
  const data = getGeoJSONData();
  if (!data) return [];

  const normalizedQuery = query.toLowerCase().trim();
  const results: Array<{
    id: string;
    name: string;
    provinceCode: string;
    regionCode: string;
    score: number;
  }> = [];

  for (const feature of data.features) {
    const name = feature.properties.name.toLowerCase();
    let score = 0;

    // Exact match gets highest score
    if (name === normalizedQuery) {
      score = 100;
    }
    // Starts with query
    else if (name.startsWith(normalizedQuery)) {
      score = 80;
    }
    // Contains query
    else if (name.includes(normalizedQuery)) {
      score = 50;
    }
    // Province code match
    else if (feature.properties.province_code.toLowerCase() === normalizedQuery) {
      score = 30;
    }

    if (score > 0) {
      results.push({
        id: feature.properties.municipality_id,
        name: feature.properties.name,
        provinceCode: feature.properties.province_code,
        regionCode: feature.properties.region_code,
        score,
      });
    }
  }

  // Sort by score descending, then by name
  results.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    return a.name.localeCompare(b.name);
  });

  return results.slice(0, limit).map(({ score, ...rest }) => rest);
}

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);

  const query = searchParams.get("q") ?? "";
  const limit = Math.min(Number(searchParams.get("limit") ?? "10"), 50);

  if (query.length < 2) {
    return NextResponse.json({
      results: [],
      message: "Query must be at least 2 characters",
    });
  }

  const supabase = createSupabaseServerClient();

  // Try database first
  const { data: dbResults, error } = await supabase
    .schema("core")
    .from("municipalities")
    .select("municipality_id, municipality_name, province_code, region_code")
    .ilike("municipality_name", `%${query}%`)
    .order("municipality_name")
    .limit(limit);

  if (!error && dbResults && dbResults.length > 0) {
    return NextResponse.json({
      results: dbResults.map((m) => ({
        id: m.municipality_id,
        name: m.municipality_name,
        provinceCode: m.province_code,
        regionCode: m.region_code,
      })),
      source: "database",
    });
  }

  // Fallback to GeoJSON search
  const geoResults = searchGeoJSON(query, limit);

  return NextResponse.json({
    results: geoResults,
    source: "geojson",
  });
}
