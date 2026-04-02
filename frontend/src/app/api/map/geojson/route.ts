import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

interface MunicipalityRow {
  municipality_id: string;
  municipality_name: string;
  province_code: string | null;
  region_code: string | null;
  coastal_flag: boolean;
  mountain_flag: boolean;
  geom_geojson: string | null;
}

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);

  // Optional bbox filter: minLon,minLat,maxLon,maxLat
  const bbox = searchParams.get("bbox");
  const regionCode = searchParams.get("region");
  const provinceCode = searchParams.get("province");
  const simplified = searchParams.get("simplified") !== "false"; // Default to simplified

  const supabase = createSupabaseServerClient();

  // Build the query using raw SQL through RPC since we need PostGIS functions
  // First, let's check if we have real data by counting municipalities with geometries
  const { count, error: countError } = await supabase
    .schema("core")
    .from("municipalities")
    .select("*", { count: "exact", head: true })
    .not(simplified ? "geom_simplified" : "geom", "is", null);

  if (countError) {
    return NextResponse.json(
      { error: countError.message, type: "FeatureCollection", features: [] },
      { status: 500 }
    );
  }

  // If no real geometries exist, return empty with a note
  if (!count || count === 0) {
    return NextResponse.json({
      type: "FeatureCollection",
      features: [],
      note: "No municipality geometries loaded yet. Run the ISTAT boundaries ingestion script.",
    });
  }

  // Use RPC to call a PostGIS-enabled function
  // For now, we'll use a simpler approach: fetch the data and have PostGIS convert to GeoJSON
  // We need to use the raw query capability

  let query = supabase
    .schema("core")
    .from("municipalities")
    .select(
      `municipality_id,
       municipality_name,
       province_code,
       region_code,
       coastal_flag,
       mountain_flag`
    );

  // Apply filters
  if (regionCode) {
    query = query.eq("region_code", regionCode);
  }
  if (provinceCode) {
    query = query.eq("province_code", provinceCode);
  }

  // Limit for performance - we'll handle bbox on client side for now
  // In production, this should be done server-side with PostGIS
  const { data: municipalities, error: dataError } = await query.limit(10000);

  if (dataError) {
    return NextResponse.json(
      { error: dataError.message, type: "FeatureCollection", features: [] },
      { status: 500 }
    );
  }

  // Now we need to get geometries - this requires a raw SQL query
  // Since Supabase JS doesn't support PostGIS functions directly in select,
  // we'll use a database function or RPC

  // For MVP, let's create a simpler approach using the SQL editor endpoint
  // We'll call our custom RPC function

  const geomColumn = simplified ? "geom_simplified" : "geom";

  // Build WHERE clause
  const conditions: string[] = [`${geomColumn} IS NOT NULL`];
  const params: any[] = [];
  let paramIndex = 1;

  if (regionCode) {
    conditions.push(`region_code = $${paramIndex}`);
    params.push(regionCode);
    paramIndex++;
  }
  if (provinceCode) {
    conditions.push(`province_code = $${paramIndex}`);
    params.push(provinceCode);
    paramIndex++;
  }
  if (bbox) {
    const [minLon, minLat, maxLon, maxLat] = bbox.split(",").map(Number);
    if ([minLon, minLat, maxLon, maxLat].every((n) => !isNaN(n))) {
      conditions.push(
        `ST_Intersects(${geomColumn}, ST_MakeEnvelope($${paramIndex}, $${paramIndex + 1}, $${paramIndex + 2}, $${paramIndex + 3}, 4326))`
      );
      params.push(minLon, minLat, maxLon, maxLat);
      paramIndex += 4;
    }
  }

  const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

  // Use RPC to execute raw SQL
  const { data: geoData, error: geoError } = await supabase.rpc("get_municipalities_geojson", {
    geom_column: geomColumn,
    where_clause: whereClause,
    region_filter: regionCode || null,
    province_filter: provinceCode || null,
  });

  if (geoError) {
    // If RPC doesn't exist yet, fall back to simple approach
    // Return municipality IDs only - frontend can use demo fallback
    console.warn("RPC not available:", geoError.message);

    return NextResponse.json({
      type: "FeatureCollection",
      features: (municipalities || []).map((m) => ({
        type: "Feature",
        properties: {
          municipality_id: m.municipality_id,
          name: m.municipality_name,
          province_code: m.province_code,
          region_code: m.region_code,
          coastal_flag: m.coastal_flag,
          mountain_flag: m.mountain_flag,
        },
        geometry: null, // Geometry not available without RPC
      })),
      note: "RPC function not yet created. Run migration to add get_municipalities_geojson function.",
    });
  }

  return NextResponse.json(geoData);
}
