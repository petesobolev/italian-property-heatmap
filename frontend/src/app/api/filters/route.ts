import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

export async function GET() {
  const supabase = createSupabaseServerClient();

  // Fetch regions and provinces in parallel
  const [regionsResult, provincesResult] = await Promise.all([
    supabase
      .schema("core")
      .from("regions")
      .select("region_code, region_name")
      .order("region_name"),
    supabase
      .schema("core")
      .from("provinces")
      .select("province_code, province_name, region_code")
      .neq("province_name", "-")
      .order("province_name"),
  ]);

  const regions = (regionsResult.data ?? []).map((r) => ({
    code: r.region_code,
    name: r.region_name,
  }));

  const provinces = (provincesResult.data ?? []).map((p) => ({
    code: p.province_code,
    name: p.province_name,
    regionCode: p.region_code,
  }));

  return NextResponse.json({
    regions,
    provinces,
    strategies: [
      { id: "balanced", label: "Balanced" },
      { id: "growth", label: "Growth" },
      { id: "yield", label: "Yield" },
    ],
    propertySegments: [
      { id: "residential", label: "Residential" },
      { id: "residential_economy", label: "Residential (Economy)" },
      { id: "residential_civil", label: "Residential (Civil)" },
    ],
  });
}
