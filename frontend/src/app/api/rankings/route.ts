import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

type SortField =
  | "opportunity_score"
  | "forecast_appreciation_pct"
  | "forecast_gross_yield_pct"
  | "value_mid_eur_sqm"
  | "confidence_score";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);

  // Parse query parameters
  const sortBy = (searchParams.get("sortBy") ?? "opportunity_score") as SortField;
  const sortOrder = searchParams.get("sortOrder") === "asc" ? true : false;
  const limit = Math.min(100, Number(searchParams.get("limit") ?? "50"));
  const offset = Number(searchParams.get("offset") ?? "0");
  const regionCode = searchParams.get("region");
  const provinceCode = searchParams.get("province");
  const minConfidence = Number(searchParams.get("minConfidence") ?? "0");
  const segment = searchParams.get("segment") ?? "residential";
  const horizonMonths = Number(searchParams.get("horizonMonths") ?? "12");

  const supabase = createSupabaseServerClient();

  // Get latest forecast date
  const { data: latestDateData } = await supabase
    .schema("model")
    .from("forecasts_municipality")
    .select("forecast_date")
    .eq("horizon_months", horizonMonths)
    .eq("property_segment", segment)
    .eq("publishable_flag", true)
    .order("forecast_date", { ascending: false })
    .limit(1);

  const latestDate = latestDateData?.[0]?.forecast_date;
  if (!latestDate) {
    return NextResponse.json({
      rankings: [],
      pagination: { total: 0, limit, offset },
      meta: { sortBy, sortOrder: sortOrder ? "asc" : "desc", latestDate: null },
    });
  }

  // Build forecasts query
  let forecastsQuery = supabase
    .schema("model")
    .from("forecasts_municipality")
    .select(
      `
      municipality_id,
      value_mid_eur_sqm,
      forecast_appreciation_pct,
      forecast_gross_yield_pct,
      opportunity_score,
      confidence_score,
      drivers,
      risks
    `,
      { count: "exact" }
    )
    .eq("forecast_date", latestDate)
    .eq("horizon_months", horizonMonths)
    .eq("property_segment", segment)
    .eq("publishable_flag", true)
    .gte("confidence_score", minConfidence);

  // We need to join with municipalities for filtering
  // First get the forecasts, then filter
  const { data: forecasts, error: forecastsError, count: totalCount } = await forecastsQuery
    .order(sortBy, { ascending: sortOrder, nullsFirst: false })
    .range(offset, offset + limit - 1);

  if (forecastsError) {
    return NextResponse.json(
      { error: forecastsError.message },
      { status: 500 }
    );
  }

  // Get municipality details for the forecasts
  const municipalityIds = (forecasts ?? []).map((f) => f.municipality_id);

  const { data: municipalities } = await supabase
    .schema("core")
    .from("municipalities")
    .select(
      `
      municipality_id,
      municipality_name,
      region_code,
      province_code,
      coastal_flag,
      mountain_flag
    `
    )
    .in("municipality_id", municipalityIds);

  // Get region and province names (exclude null/empty — .in() must not receive null)
  const regionCodes = [
    ...new Set(
      (municipalities ?? [])
        .map((m) => m.region_code)
        .filter((c): c is string => typeof c === "string" && c.length > 0)
    ),
  ];
  const provinceCodes = [
    ...new Set(
      (municipalities ?? [])
        .map((m) => m.province_code)
        .filter((c): c is string => typeof c === "string" && c.length > 0)
    ),
  ];

  const [{ data: regions }, { data: provinces }] = await Promise.all([
    regionCodes.length > 0
      ? supabase
          .schema("core")
          .from("regions")
          .select("region_code, region_name")
          .in("region_code", regionCodes)
      : Promise.resolve({ data: [] as { region_code: string; region_name: string }[] | null }),
    provinceCodes.length > 0
      ? supabase
          .schema("core")
          .from("provinces")
          .select("province_code, province_name")
          .in("province_code", provinceCodes)
      : Promise.resolve({
          data: [] as { province_code: string; province_name: string }[] | null,
        }),
  ]);

  // Create lookup maps
  const muniMap = new Map(
    (municipalities ?? []).map((m) => [m.municipality_id, m])
  );
  const regionMap = new Map(
    (regions ?? []).map((r) => [r.region_code, r.region_name])
  );
  const provinceMap = new Map(
    (provinces ?? []).map((p) => [p.province_code, p.province_name])
  );

  // Build rankings with all data
  let rankings = (forecasts ?? []).map((f, index) => {
    const muni = muniMap.get(f.municipality_id);
    return {
      rank: offset + index + 1,
      municipalityId: f.municipality_id,
      name: muni?.municipality_name ?? f.municipality_id,
      regionCode: muni?.region_code ?? null,
      regionName: muni?.region_code ? regionMap.get(muni.region_code) : null,
      provinceCode: muni?.province_code ?? null,
      provinceName: muni?.province_code ? provinceMap.get(muni.province_code) : null,
      isCoastal: muni?.coastal_flag ?? false,
      isMountain: muni?.mountain_flag ?? false,
      valueMidEurSqm: f.value_mid_eur_sqm,
      appreciationPct: f.forecast_appreciation_pct,
      grossYieldPct: f.forecast_gross_yield_pct,
      opportunityScore: f.opportunity_score,
      confidenceScore: f.confidence_score,
      drivers: f.drivers,
      risks: f.risks,
    };
  });

  // Apply region/province filters client-side (could be optimized with a join)
  if (regionCode) {
    rankings = rankings.filter((r) => r.regionCode === regionCode);
  }
  if (provinceCode) {
    rankings = rankings.filter((r) => r.provinceCode === provinceCode);
  }

  return NextResponse.json({
    rankings,
    pagination: {
      total: totalCount ?? 0,
      limit,
      offset,
      hasMore: offset + rankings.length < (totalCount ?? 0),
    },
    meta: {
      sortBy,
      sortOrder: sortOrder ? "asc" : "desc",
      latestDate,
      segment,
      horizonMonths,
      filters: {
        region: regionCode,
        province: provinceCode,
        minConfidence,
      },
    },
  });
}
