import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

interface RouteParams {
  params: Promise<{ istatCode: string }>;
}

interface ZoneRow {
  omi_zone_id: string;
  zone_code: string;
  zone_description: string | null;
  zone_type: string | null;
  microzone_code: string | null;
  zone_classification: string | null;
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

export async function GET(request: Request, { params }: RouteParams) {
  const { istatCode } = await params;
  const { searchParams } = new URL(request.url);
  const segment = searchParams.get("segment") ?? "residential";

  const supabase = createSupabaseServerClient();

  // Use RPC function to get zones (bypasses schema permission issues)
  const { data: zonesData, error: zonesError } = await supabase
    .rpc("get_municipality_zones", { p_municipality_id: istatCode });

  if (zonesError) {
    // If RPC doesn't exist, fall back to direct query
    if (zonesError.message.includes("function") || zonesError.message.includes("does not exist")) {
      // Try direct query as fallback
      const { data: zones, error: directError } = await supabase
        .schema("core")
        .from("omi_zones")
        .select("omi_zone_id, zone_code, zone_description, zone_type, microzone_code, zone_classification")
        .eq("municipality_id", istatCode);

      if (directError) {
        return NextResponse.json(
          { error: "Failed to fetch zones", details: directError.message },
          { status: 500 }
        );
      }

      return processZones(supabase, zones as ZoneRow[] | null, istatCode, segment);
    }

    return NextResponse.json(
      { error: "Failed to fetch zones", details: zonesError.message },
      { status: 500 }
    );
  }

  return processZones(supabase, zonesData as ZoneRow[] | null, istatCode, segment);
}

async function processZones(
  supabase: ReturnType<typeof createSupabaseServerClient>,
  zones: ZoneRow[] | null,
  istatCode: string,
  segment: string
) {
  if (!zones || zones.length === 0) {
    return NextResponse.json({
      type: "FeatureCollection",
      features: [],
      stats: null,
      municipality_id: istatCode,
      segment,
      message: "No OMI zones found for this municipality",
    });
  }

  // Get latest values for each zone
  const zoneIds = zones.map((z) => z.omi_zone_id);

  let zoneValues: ZoneValueRow[] | null = null;
  try {
    const { data } = await supabase.rpc("get_zone_values", { p_zone_ids: zoneIds, p_segment: segment });
    zoneValues = data as ZoneValueRow[] | null;
  } catch {
    // RPC doesn't exist, will fall back to direct query
  }

  // If RPC doesn't exist, try direct query
  let values: ZoneValueRow[] = zoneValues ?? [];
  if (!zoneValues) {
    const { data: directValues } = await supabase
      .schema("mart")
      .from("omi_zone_values_semester")
      .select("omi_zone_id, period_id, value_mid_eur_sqm, value_min_eur_sqm, value_max_eur_sqm, rent_mid_eur_sqm_month, value_pct_change_1s")
      .in("omi_zone_id", zoneIds)
      .eq("property_segment", segment)
      .order("period_id", { ascending: false });
    values = (directValues as ZoneValueRow[]) ?? [];
  }

  // Create a map of omi_zone_id to latest values
  const latestValues = new Map<string, {
    periodId: string;
    valueMidEurSqm: number | null;
    valueMinEurSqm: number | null;
    valueMaxEurSqm: number | null;
    rentMidEurSqmMonth: number | null;
    pctChange1s: number | null;
  }>();

  for (const v of values) {
    if (!latestValues.has(v.omi_zone_id)) {
      latestValues.set(v.omi_zone_id, {
        periodId: v.period_id,
        valueMidEurSqm: v.value_mid_eur_sqm,
        valueMinEurSqm: v.value_min_eur_sqm,
        valueMaxEurSqm: v.value_max_eur_sqm,
        rentMidEurSqmMonth: v.rent_mid_eur_sqm_month,
        pctChange1s: v.value_pct_change_1s,
      });
    }
  }

  // Build response with features
  const features = zones.map((zone) => {
    const zoneValues = latestValues.get(zone.omi_zone_id);

    return {
      type: "Feature" as const,
      properties: {
        zoneId: zone.omi_zone_id,
        zoneCode: zone.zone_code,
        zoneName: zone.zone_description,
        zoneType: zone.zone_type,
        microzoneCode: zone.microzone_code,
        zoneClassification: zone.zone_classification,
        values: zoneValues ?? null,
        forecast: null,
      },
      geometry: null,
    };
  });

  // Calculate zone statistics
  const valuesArray = Array.from(latestValues.values())
    .map((v) => v.valueMidEurSqm)
    .filter((v): v is number => v != null);

  const stats = valuesArray.length > 0
    ? {
        minValue: Math.min(...valuesArray),
        maxValue: Math.max(...valuesArray),
        avgValue: valuesArray.reduce((a, b) => a + b, 0) / valuesArray.length,
        zonesWithData: valuesArray.length,
        totalZones: zones.length,
      }
    : null;

  return NextResponse.json({
    type: "FeatureCollection",
    features,
    stats,
    municipality_id: istatCode,
    segment,
  });
}
