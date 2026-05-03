import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

export async function GET() {
  const supabase = createSupabaseServerClient();

  // Get regions as GeoJSON via RPC
  const { data, error } = await supabase.rpc("get_regions_geojson");

  if (error) {
    console.error("Error fetching regions:", error);
    return NextResponse.json({
      type: "FeatureCollection",
      features: [],
    });
  }

  return NextResponse.json(data);
}
