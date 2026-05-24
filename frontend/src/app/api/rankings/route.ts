import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

type SortField =
  | "value_mid_eur_sqm"
  | "gross_yield_pct"
  | "annualized_price_change_pct"
  | "ntn_per_1000_pop";

interface CacheRow {
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
  zones_with_data: number | null;
  latest_period: string;
  earliest_period: string;
  periods_count: number;
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
  const searchQuery = searchParams.get("search")?.toLowerCase().trim();

  const supabase = createSupabaseServerClient();

  // Build base query on the materialized view
  let query = supabase
    .schema("mart")
    .from("municipality_rankings_cache")
    .select("*", { count: "exact" });

  // Apply filters
  if (regionCode) {
    query = query.eq("region_code", regionCode);
  }
  if (provinceCode) {
    query = query.eq("province_code", provinceCode);
  }

  // Apply sorting
  const sortColumn = sortBy;
  query = query.order(sortColumn, {
    ascending: sortOrder,
    nullsFirst: false
  });

  // Apply pagination
  query = query.range(offset, offset + limit - 1);

  const { data, error, count } = await query;

  if (error) {
    console.error("Rankings cache query error:", error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }

  const rows = (data ?? []) as CacheRow[];
  const totalCount = count ?? 0;

  if (rows.length === 0) {
    return NextResponse.json({
      rankings: [],
      pagination: { total: 0, limit, offset, hasMore: false },
      meta: {
        sortBy,
        sortOrder: sortOrder ? "asc" : "desc",
        latestPeriod: null,
        earliestPeriod: null,
        periodsIncluded: [],
        segment: "residential",
        filters: { region: regionCode, province: provinceCode },
      },
      searchResult: null,
    });
  }

  // Get period info from first row
  const latestPeriod = rows[0].latest_period;
  const earliestPeriod = rows[0].earliest_period;

  // Handle municipality search - find rank of searched municipality
  let searchResult: {
    municipalityId: string;
    name: string;
    rank: number;
    page: number;
    regionCode: string | null;
    regionName: string | null;
    provinceCode: string | null;
    provinceName: string | null;
  } | null = null;

  if (searchQuery) {
    // Use a separate query to find the search result with its rank
    // This query finds municipalities matching the search and returns the highest-ranked one
    let searchRankQuery = supabase
      .schema("mart")
      .from("municipality_rankings_cache")
      .select("municipality_id, municipality_name, region_code, region_name, province_code, province_name, value_mid_eur_sqm, gross_yield_pct, annualized_price_change_pct, ntn_per_1000_pop")
      .ilike("municipality_name", `%${searchQuery}%`);

    // Apply same filters as main query
    if (regionCode) {
      searchRankQuery = searchRankQuery.eq("region_code", regionCode);
    }
    if (provinceCode) {
      searchRankQuery = searchRankQuery.eq("province_code", provinceCode);
    }

    // Order by the same sort column to get the highest-ranked match
    searchRankQuery = searchRankQuery.order(sortBy, {
      ascending: sortOrder,
      nullsFirst: false
    });

    const { data: searchData } = await searchRankQuery.limit(1);

    if (searchData && searchData.length > 0) {
      const found = searchData[0];

      // Count how many municipalities rank higher than this one
      let rankQuery = supabase
        .schema("mart")
        .from("municipality_rankings_cache")
        .select("municipality_id", { count: "exact", head: true });

      // Apply same filters
      if (regionCode) {
        rankQuery = rankQuery.eq("region_code", regionCode);
      }
      if (provinceCode) {
        rankQuery = rankQuery.eq("province_code", provinceCode);
      }

      // Count items with better rank based on sort
      const sortValue = found[sortBy as keyof typeof found] as number | null;
      if (sortValue !== null) {
        if (sortOrder) {
          // Ascending: count items with lower value
          rankQuery = rankQuery.lt(sortBy, sortValue);
        } else {
          // Descending: count items with higher value
          rankQuery = rankQuery.gt(sortBy, sortValue);
        }
      }

      const { count: higherRanked } = await rankQuery;
      const rank = (higherRanked ?? 0) + 1;

      searchResult = {
        municipalityId: found.municipality_id,
        name: found.municipality_name,
        rank,
        page: Math.floor((rank - 1) / limit),
        regionCode: found.region_code,
        regionName: found.region_name,
        provinceCode: found.province_code,
        provinceName: found.province_name,
      };
    }
  }

  // Build final response
  const rankings = rows.map((m, index) => ({
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
      segment: "residential",
      filters: {
        region: regionCode,
        province: provinceCode,
      },
    },
    searchResult,
  });
}
