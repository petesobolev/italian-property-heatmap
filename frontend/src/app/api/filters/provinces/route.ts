import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

export async function GET() {
  const supabase = createSupabaseServerClient();

  const { data, error } = await supabase
    .schema("core")
    .from("provinces")
    .select("province_code, province_name, region_code, province_abbreviation")
    .order("province_name");

  if (error) {
    console.error("Error fetching provinces:", error);
    return NextResponse.json({ provinces: [] }, { status: 500 });
  }

  // Map to expected format, fixing any "-" names using abbreviation
  const ABBREVIATION_TO_NAME: Record<string, string> = {
    FI: "Firenze",
    // Add others if needed
  };

  const provinces = (data || []).map((p) => ({
    code: p.province_code,
    name: p.province_name === "-"
      ? (ABBREVIATION_TO_NAME[p.province_abbreviation] || p.province_abbreviation)
      : p.province_name,
    regionCode: p.region_code,
  }));

  return NextResponse.json(
    { provinces },
    {
      headers: {
        "Cache-Control": "public, max-age=86400, stale-while-revalidate=604800",
      },
    }
  );
}
