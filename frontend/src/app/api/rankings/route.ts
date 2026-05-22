import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

type SortField =
  | "value_mid_eur_sqm"
  | "gross_yield_pct"
  | "annualized_price_change_pct"
  | "ntn_per_1000_pop"
  | "data_quality_score";

interface RankingRow {
  municipality_id: string;
  municipality_name: string;
  region_code: string;
  region_name: string;
  province_code: string;
  province_name: string;
  coastal_flag: boolean;
  mountain_flag: boolean;
  value_mid_eur_sqm: number;
  rent_mid_eur_sqm_month: number | null;
  gross_yield_pct: number | null;
  annualized_price_change_pct: number | null;
  ntn_per_1000_pop: number | null;
  data_quality_score: number | null;
  zones_with_data: number | null;
  latest_period: string;
  earliest_period: string;
}

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);

  // Parse query parameters
  const sortBy = (searchParams.get("sortBy") ?? "value_mid_eur_sqm") as SortField;
  const sortOrder = searchParams.get("sortOrder") === "asc" ? true : false;
  const limit = Math.min(100, Number(searchParams.get("limit") ?? "50"));
  const offset = Number(searchParams.get("offset") ?? "0");
  const regionCode = searchParams.get("region") || null;
  const provinceCode = searchParams.get("province") || null;
  const minConfidence = Number(searchParams.get("minConfidence") ?? "0");
  const segment = searchParams.get("segment") ?? "residential";
  const semestersToAverage = Math.min(4, Math.max(1, Number(searchParams.get("semestersToAverage") ?? "2")));
  const searchQuery = searchParams.get("search")?.toLowerCase().trim();

  const supabase = createSupabaseServerClient();

  // Use the comprehensive RPC function that handles all aggregation server-side
  // Need to paginate to work around PostgREST's max rows limit (1000)
  const PAGE_SIZE = 1000;
  let allData: RankingRow[] = [];
  let pageOffset = 0;
  let hasMore = true;

  while (hasMore) {
    const { data: pageData, error: pageError } = await supabase
      .schema("mart")
      .rpc("get_municipality_rankings", {
        p_segment: segment,
        p_num_semesters: semestersToAverage,
        p_region_code: regionCode,
        p_province_code: provinceCode,
        p_min_confidence: minConfidence,
      })
      .range(pageOffset, pageOffset + PAGE_SIZE - 1);

    if (pageError) {
      console.error("Rankings RPC error:", pageError);
      return NextResponse.json({ error: pageError.message }, { status: 500 });
    }

    const rows = (pageData ?? []) as RankingRow[];
    allData = allData.concat(rows);

    if (rows.length < PAGE_SIZE) {
      hasMore = false;
    } else {
      pageOffset += PAGE_SIZE;
    }
  }

  if (allData.length === 0) {
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
      searchResult: null,
    });
  }

  // Get period info from first row (all rows have same period info)
  const latestPeriod = allData[0].latest_period;
  const earliestPeriod = allData[0].earliest_period;

  // Sort the results based on requested sort field and order
  const sortedData = [...allData].sort((a, b) => {
    let aVal: number | null;
    let bVal: number | null;

    switch (sortBy) {
      case "value_mid_eur_sqm":
        aVal = a.value_mid_eur_sqm;
        bVal = b.value_mid_eur_sqm;
        break;
      case "gross_yield_pct":
        aVal = a.gross_yield_pct;
        bVal = b.gross_yield_pct;
        break;
      case "annualized_price_change_pct":
        aVal = a.annualized_price_change_pct;
        bVal = b.annualized_price_change_pct;
        break;
      case "ntn_per_1000_pop":
        aVal = a.ntn_per_1000_pop;
        bVal = b.ntn_per_1000_pop;
        break;
      case "data_quality_score":
        aVal = a.data_quality_score;
        bVal = b.data_quality_score;
        break;
      default:
        aVal = a.value_mid_eur_sqm;
        bVal = b.value_mid_eur_sqm;
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
    const searchIndex = sortedData.findIndex((m) =>
      m.municipality_name.toLowerCase().includes(searchQuery)
    );

    if (searchIndex !== -1) {
      const found = sortedData[searchIndex];
      searchResult = {
        municipalityId: found.municipality_id,
        name: found.municipality_name,
        rank: searchIndex + 1,
        page: Math.floor(searchIndex / limit),
      };
    }
  }

  const paginatedData = sortedData.slice(offset, offset + limit);

  // Build final response
  const rankings = paginatedData.map((m, index) => ({
    rank: offset + index + 1,
    municipalityId: m.municipality_id,
    name: m.municipality_name,
    regionCode: m.region_code,
    regionName: m.region_name,
    provinceCode: m.province_code,
    provinceName: m.province_name,
    isCoastal: m.coastal_flag ?? false,
    isMountain: m.mountain_flag ?? false,
    valueMidEurSqm: m.value_mid_eur_sqm,
    grossYieldPct: m.gross_yield_pct,
    annualizedPriceChangePct: m.annualized_price_change_pct,
    salesPer1000Pop: m.ntn_per_1000_pop,
    dataQualityScore: m.data_quality_score,
    zonesWithData: m.zones_with_data,
  }));

  // Generate periods array from latest to earliest
  const periodsIncluded: string[] = [];
  if (latestPeriod && earliestPeriod) {
    // Parse periods like "2025H2" to generate the range
    const parseYearSemester = (p: string) => {
      const year = parseInt(p.substring(0, 4));
      const semester = parseInt(p.substring(5));
      return { year, semester };
    };

    const latest = parseYearSemester(latestPeriod);
    const earliest = parseYearSemester(earliestPeriod);

    let current = { ...latest };
    while (current.year > earliest.year ||
           (current.year === earliest.year && current.semester >= earliest.semester)) {
      periodsIncluded.push(`${current.year}H${current.semester}`);
      if (current.semester === 1) {
        current = { year: current.year - 1, semester: 2 };
      } else {
        current = { year: current.year, semester: 1 };
      }
    }
  }

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
      periodsIncluded,
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
