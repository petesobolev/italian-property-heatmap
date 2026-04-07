import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);

  const regionCode = searchParams.get("region");
  const provinceCode = searchParams.get("province");
  const simplified = searchParams.get("simplified") !== "false"; // Default to simplified

  const supabase = createSupabaseServerClient();
  const geomColumn = simplified ? "geom_simplified" : "geom";

  // Call our PostGIS RPC function directly
  const { data: geoData, error: geoError } = await supabase.rpc("get_municipalities_geojson", {
    geom_column: geomColumn,
    region_filter: regionCode || null,
    province_filter: provinceCode || null,
    bbox_filter: null,
  });

  if (geoError) {
    console.error("RPC error:", geoError.message);
    return NextResponse.json({
      type: "FeatureCollection",
      features: [],
      error: geoError.message,
      note: "Database query failed. Check that get_municipalities_geojson function exists.",
    });
  }

  // The RPC returns the GeoJSON directly
  return NextResponse.json(geoData);
}
