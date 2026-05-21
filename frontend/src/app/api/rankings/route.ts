import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

type SortField =
  | "value_mid_eur_sqm"
  | "gross_yield_pct"
  | "annualized_price_change_pct"
  | "ntn_per_1000_pop"
  | "data_quality_score";

// Map frontend sort fields to database columns
const SORT_FIELD_MAP: Record<SortField, string> = {
  value_mid_eur_sqm: "value_mid_eur_sqm",
  gross_yield_pct: "gross_yield_pct",
  annualized_price_change_pct: "annualized_price_change_pct",
  ntn_per_1000_pop: "ntn_per_1000_pop",
  data_quality_score: "data_quality_score",
};

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);

  // Parse query parameters
  const sortBy = (searchParams.get("sortBy") ?? "value_mid_eur_sqm") as SortField;
  const sortOrder = searchParams.get("sortOrder") === "asc" ? true : false;
  const limit = Math.min(100, Number(searchParams.get("limit") ?? "50"));
  const offset = Number(searchParams.get("offset") ?? "0");
  const regionCode = searchParams.get("region");
  const provinceCode = searchParams.get("province");
  const minConfidence = Number(searchParams.get("minConfidence") ?? "0");
  const segment = searchParams.get("segment") ?? "residential";
  const semestersToAverage = Math.min(4, Math.max(1, Number(searchParams.get("semestersToAverage") ?? "2")));
  const searchQuery = searchParams.get("search")?.toLowerCase().trim();

  const supabase = createSupabaseServerClient();

  // Step 1: Get municipality info with region/province for filtering
  let municipalitiesQuery = supabase
    .schema("core")
    .from("municipalities")
    .select("municipality_id, municipality_name, region_code, province_code, coastal_flag, mountain_flag");

  // Apply region/province filters at the database level
  if (regionCode) {
    municipalitiesQuery = municipalitiesQuery.eq("region_code", regionCode);
  }
  if (provinceCode) {
    municipalitiesQuery = municipalitiesQuery.eq("province_code", provinceCode);
  }

  const { data: municipalities, error: muniError } = await municipalitiesQuery;

  if (muniError) {
    return NextResponse.json({ error: muniError.message }, { status: 500 });
  }

  // Create municipality lookup and get valid IDs for filtering
  const muniMap = new Map(
    (municipalities ?? []).map((m) => [m.municipality_id, m])
  );
  const validMunicipalityIds = new Set((municipalities ?? []).map((m) => m.municipality_id));

  // Step 2: Get the N most recent periods
  const { data: periodsData, error: periodsError } = await supabase
    .schema("mart")
    .from("municipality_values_semester")
    .select("period_id")
    .eq("property_segment", segment)
    .order("period_id", { ascending: false });

  if (periodsError) {
    return NextResponse.json({ error: periodsError.message }, { status: 500 });
  }

  // Get unique periods and take the most recent N
  const uniquePeriods = [...new Set(periodsData?.map((p) => p.period_id) ?? [])];
  const selectedPeriods = uniquePeriods.slice(0, semestersToAverage);

  if (selectedPeriods.length === 0) {
    return NextResponse.json({
      rankings: [],
      pagination: { total: 0, limit, offset, hasMore: false },
      meta: {
        sortBy,
        sortOrder: sortOrder ? "asc" : "desc",
        latestPeriod: null,
        earliestPeriod: null,
        periodsIncluded: [],
        segment,
        filters: { region: regionCode, province: provinceCode, minConfidence },
      },
    });
  }

  const latestPeriod = selectedPeriods[0];
  const earliestPeriod = selectedPeriods[selectedPeriods.length - 1];

  // Step 3: Get values data for selected periods
  const { data: valuesData, error: valuesError } = await supabase
    .schema("mart")
    .from("municipality_values_semester")
    .select("municipality_id, period_id, value_mid_eur_sqm, rent_mid_eur_sqm_month, data_quality_score, zones_with_data")
    .eq("property_segment", segment)
    .in("period_id", selectedPeriods)
    .not("value_mid_eur_sqm", "is", null);

  if (valuesError) {
    return NextResponse.json({ error: valuesError.message }, { status: 500 });
  }

  // Step 4: Get latest transactions data
  const { data: transactionsData, error: transactionsError } = await supabase
    .schema("mart")
    .from("municipality_transactions_semester")
    .select("municipality_id, ntn_per_1000_pop")
    .eq("property_segment", segment)
    .eq("period_id", latestPeriod);

  if (transactionsError) {
    return NextResponse.json({ error: transactionsError.message }, { status: 500 });
  }

  // Create transaction lookup
  const transactionMap = new Map(
    (transactionsData ?? []).map((t) => [t.municipality_id, t.ntn_per_1000_pop])
  );

  // Step 5: Aggregate values by municipality (only for valid municipality IDs based on region/province filter)
  const municipalityData = new Map<
    string,
    {
      values: { period: string; value: number; rent: number | null }[];
      dataQuality: number[];
      zonesWithData: number[];
    }
  >();

  for (const row of valuesData ?? []) {
    // Skip municipalities not in our filtered set
    if (!validMunicipalityIds.has(row.municipality_id)) continue;

    const existing = municipalityData.get(row.municipality_id) ?? {
      values: [],
      dataQuality: [],
      zonesWithData: [],
    };

    existing.values.push({
      period: row.period_id,
      value: row.value_mid_eur_sqm,
      rent: row.rent_mid_eur_sqm_month,
    });
    if (row.data_quality_score != null) {
      existing.dataQuality.push(row.data_quality_score);
    }
    if (row.zones_with_data != null) {
      existing.zonesWithData.push(row.zones_with_data);
    }

    municipalityData.set(row.municipality_id, existing);
  }

  // Calculate aggregated metrics for each municipality
  interface AggregatedMunicipality {
    municipalityId: string;
    valueMidEurSqm: number;
    rentMidEurSqmMonth: number | null;
    grossYieldPct: number | null;
    annualizedPriceChangePct: number | null;
    dataQualityScore: number | null;
    zonesWithData: number | null;
    ntnPer1000Pop: number | null;
  }

  const aggregated: AggregatedMunicipality[] = [];

  for (const [municipalityId, data] of municipalityData) {
    // Need at least the latest period with a valid (non-zero) value
    const latestValue = data.values.find((v) => v.period === latestPeriod);
    if (!latestValue) continue;

    // Skip municipalities with zero or missing values (likely damaged/uninhabited areas)
    if (latestValue.value <= 0) continue;

    // Sort values by period (newest first)
    const sortedValues = [...data.values].sort((a, b) => b.period.localeCompare(a.period));

    // Calculate average value and rent
    const avgValue = sortedValues.reduce((sum, v) => sum + v.value, 0) / sortedValues.length;
    const rentsWithData = sortedValues.filter((v) => v.rent != null);
    const avgRent = rentsWithData.length > 0
      ? rentsWithData.reduce((sum, v) => sum + (v.rent ?? 0), 0) / rentsWithData.length
      : null;

    // Calculate gross yield (annual rent / value * 100)
    const grossYieldPct = avgRent != null && avgValue > 0
      ? (avgRent * 12 / avgValue) * 100
      : null;

    // Calculate annualized price change (CAGR)
    let annualizedPriceChangePct: number | null = null;
    if (sortedValues.length >= 2) {
      const latestVal = sortedValues[0].value;
      const earliestVal = sortedValues[sortedValues.length - 1].value;
      const numSemesters = sortedValues.length;

      if (earliestVal > 0 && latestVal > 0) {
        // CAGR formula: ((latest/earliest)^(2/numSemesters) - 1) * 100
        // 2/numSemesters converts to annual rate (2 semesters = 1 year)
        annualizedPriceChangePct = (Math.pow(latestVal / earliestVal, 2 / numSemesters) - 1) * 100;
      }
    }

    // Average data quality score
    const dataQualityScore = data.dataQuality.length > 0
      ? data.dataQuality.reduce((sum, s) => sum + s, 0) / data.dataQuality.length
      : null;

    // Max zones with data
    const zonesWithData = data.zonesWithData.length > 0
      ? Math.max(...data.zonesWithData)
      : null;

    aggregated.push({
      municipalityId,
      valueMidEurSqm: avgValue,
      rentMidEurSqmMonth: avgRent,
      grossYieldPct,
      annualizedPriceChangePct,
      dataQualityScore,
      zonesWithData,
      ntnPer1000Pop: transactionMap.get(municipalityId) ?? null,
    });
  }

  // Apply confidence filter
  let filtered = minConfidence > 0
    ? aggregated.filter((m) => (m.dataQualityScore ?? 0) >= minConfidence)
    : aggregated;

  if (filtered.length === 0) {
    return NextResponse.json({
      rankings: [],
      pagination: { total: 0, limit, offset, hasMore: false },
      meta: {
        sortBy,
        sortOrder: sortOrder ? "asc" : "desc",
        latestPeriod,
        earliestPeriod,
        periodsIncluded: selectedPeriods,
        segment,
        filters: { region: regionCode, province: provinceCode, minConfidence },
      },
    });
  }

  // Get region and province names for display
  const regionCodes = [...new Set(
    (municipalities ?? [])
      .map((m) => m.region_code)
      .filter((c): c is string => typeof c === "string" && c.length > 0)
  )];
  const provinceCodes = [...new Set(
    (municipalities ?? [])
      .map((m) => m.province_code)
      .filter((c): c is string => typeof c === "string" && c.length > 0)
  )];

  const [{ data: regions }, { data: provinces }] = await Promise.all([
    regionCodes.length > 0
      ? supabase.schema("core").from("regions").select("region_code, region_name").in("region_code", regionCodes)
      : Promise.resolve({ data: [] as { region_code: string; region_name: string }[] | null }),
    provinceCodes.length > 0
      ? supabase.schema("core").from("provinces").select("province_code, province_name").in("province_code", provinceCodes)
      : Promise.resolve({ data: [] as { province_code: string; province_name: string }[] | null }),
  ]);

  const regionMap = new Map((regions ?? []).map((r) => [r.region_code, r.region_name]));
  const provinceMap = new Map((provinces ?? []).map((p) => [p.province_code, p.province_name]));

  // Sort the results
  const sortField = SORT_FIELD_MAP[sortBy] || "value_mid_eur_sqm";
  const sortedData = [...filtered].sort((a, b) => {
    let aVal: number | null;
    let bVal: number | null;

    switch (sortField) {
      case "value_mid_eur_sqm":
        aVal = a.valueMidEurSqm;
        bVal = b.valueMidEurSqm;
        break;
      case "gross_yield_pct":
        aVal = a.grossYieldPct;
        bVal = b.grossYieldPct;
        break;
      case "annualized_price_change_pct":
        aVal = a.annualizedPriceChangePct;
        bVal = b.annualizedPriceChangePct;
        break;
      case "ntn_per_1000_pop":
        aVal = a.ntnPer1000Pop;
        bVal = b.ntnPer1000Pop;
        break;
      case "data_quality_score":
        aVal = a.dataQualityScore;
        bVal = b.dataQualityScore;
        break;
      default:
        aVal = a.valueMidEurSqm;
        bVal = b.valueMidEurSqm;
    }

    // Handle nulls - push to end
    if (aVal == null && bVal == null) return 0;
    if (aVal == null) return 1;
    if (bVal == null) return -1;

    return sortOrder ? aVal - bVal : bVal - aVal;
  });

  const totalCount = sortedData.length;

  // Handle municipality search - find rank of searched municipality
  let searchResult: {
    municipalityId: string;
    name: string;
    rank: number;
    page: number;
  } | null = null;

  if (searchQuery) {
    // Find municipality by name (partial match)
    const searchIndex = sortedData.findIndex((m) => {
      const muni = muniMap.get(m.municipalityId);
      return muni?.municipality_name.toLowerCase().includes(searchQuery);
    });

    if (searchIndex !== -1) {
      const found = sortedData[searchIndex];
      const muni = muniMap.get(found.municipalityId);
      searchResult = {
        municipalityId: found.municipalityId,
        name: muni?.municipality_name ?? found.municipalityId,
        rank: searchIndex + 1,
        page: Math.floor(searchIndex / limit),
      };
    }
  }

  const paginatedData = sortedData.slice(offset, offset + limit);

  // Build final response
  const rankings = paginatedData.map((m, index) => {
    const muni = muniMap.get(m.municipalityId);
    return {
      rank: offset + index + 1,
      municipalityId: m.municipalityId,
      name: muni?.municipality_name ?? m.municipalityId,
      regionCode: muni?.region_code ?? null,
      regionName: muni?.region_code ? regionMap.get(muni.region_code) ?? null : null,
      provinceCode: muni?.province_code ?? null,
      provinceName: muni?.province_code ? provinceMap.get(muni.province_code) ?? null : null,
      isCoastal: muni?.coastal_flag ?? false,
      isMountain: muni?.mountain_flag ?? false,
      valueMidEurSqm: m.valueMidEurSqm,
      grossYieldPct: m.grossYieldPct,
      annualizedPriceChangePct: m.annualizedPriceChangePct,
      salesPer1000Pop: m.ntnPer1000Pop,
      dataQualityScore: m.dataQualityScore,
      zonesWithData: m.zonesWithData,
    };
  });

  return NextResponse.json({
    rankings,
    pagination: {
      total: totalCount,
      limit,
      offset,
      hasMore: offset + rankings.length < totalCount,
    },
    meta: {
      sortBy,
      sortOrder: sortOrder ? "asc" : "desc",
      latestPeriod,
      earliestPeriod,
      periodsIncluded: selectedPeriods,
      segment,
      filters: {
        region: regionCode,
        province: provinceCode,
        minConfidence,
      },
    },
    searchResult,
  });
}
