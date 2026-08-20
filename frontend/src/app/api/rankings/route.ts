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

interface MunicipalityInfo {
  municipality_id: string;
  municipality_name: string;
  region_code: string;
  region_name: string;
  province_code: string;
  province_name: string;
  coastal_flag: boolean;
  mountain_flag: boolean;
}

interface SemesterValue {
  municipality_id: string;
  value_mid_eur_sqm: number;
  rent_mid_eur_sqm_month: number | null;
  period_id: string;
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
  // Semesters to average: 1, 2, 4 (default/cache), or 8
  const semestersToAverage = Math.min(Math.max(Number(searchParams.get("semesters") ?? "4"), 1), 8);

  const supabase = createSupabaseServerClient();

  // For 4 semesters (default), use the pre-computed cache for performance
  // For other values, compute dynamically
  if (semestersToAverage !== 4) {
    return getDynamicRankings(
      supabase,
      semestersToAverage,
      sortBy,
      sortOrder,
      limit,
      offset,
      regionCode,
      provinceCode,
      searchQuery
    );
  }

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

  // Cache for 2 minutes with stale-while-revalidate for fast perceived performance
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
  }, {
    headers: {
      "Cache-Control": "public, max-age=120, stale-while-revalidate=600",
    },
  });
}

// Dynamic rankings computation for non-default semester counts
async function getDynamicRankings(
  supabase: ReturnType<typeof createSupabaseServerClient>,
  semestersToAverage: number,
  sortBy: SortField,
  sortOrder: boolean,
  limit: number,
  offset: number,
  regionCode: string | null,
  provinceCode: string | null,
  searchQuery: string | undefined
) {
  const segment = "residential";

  // Get available periods
  const { data: availablePeriods, error: periodError } = await supabase
    .schema("mart")
    .rpc("get_available_periods", { p_segment: segment });

  if (periodError) {
    return NextResponse.json(
      { error: periodError.message, rankings: [], pagination: { total: 0, limit, offset, hasMore: false } },
      { status: 500 }
    );
  }

  const allPeriods = availablePeriods?.map((p: { period_id: string }) => p.period_id) ?? [];
  if (allPeriods.length === 0) {
    return NextResponse.json({
      rankings: [],
      pagination: { total: 0, limit, offset, hasMore: false },
      meta: {
        sortBy,
        sortOrder: sortOrder ? "asc" : "desc",
        latestPeriod: null,
        earliestPeriod: null,
        periodsIncluded: [],
        semestersToAverage,
        segment,
        filters: { region: regionCode, province: provinceCode },
      },
      searchResult: null,
    });
  }

  const periodsToUse = allPeriods.slice(0, semestersToAverage);
  const latestPeriod = periodsToUse[0];
  const earliestPeriod = periodsToUse[periodsToUse.length - 1];

  // Fetch municipality info (for names, region, province, etc.)
  const municipalityInfo = new Map<string, MunicipalityInfo>();
  {
    const batchSize = 1000;
    let batchOffset = 0;
    let hasMore = true;

    while (hasMore) {
      let infoQuery = supabase
        .schema("mart")
        .from("municipality_rankings_cache")
        .select("municipality_id, municipality_name, region_code, region_name, province_code, province_name, coastal_flag, mountain_flag")
        .range(batchOffset, batchOffset + batchSize - 1);

      if (regionCode) infoQuery = infoQuery.eq("region_code", regionCode);
      if (provinceCode) infoQuery = infoQuery.eq("province_code", provinceCode);

      const { data: batch } = await infoQuery;

      if (batch && batch.length > 0) {
        for (const row of batch) {
          municipalityInfo.set(row.municipality_id, row as MunicipalityInfo);
        }
        batchOffset += batchSize;
        hasMore = batch.length === batchSize;
      } else {
        hasMore = false;
      }
    }
  }

  // Fetch semester values for selected periods
  const allValues: SemesterValue[] = [];
  {
    const batchSize = 1000;
    let batchOffset = 0;
    let hasMore = true;

    while (hasMore) {
      const { data: batch, error: batchError } = await supabase
        .schema("mart")
        .from("municipality_values_semester")
        .select("municipality_id, value_mid_eur_sqm, rent_mid_eur_sqm_month, period_id")
        .in("period_id", periodsToUse)
        .eq("property_segment", segment)
        .not("value_mid_eur_sqm", "is", null)
        .range(batchOffset, batchOffset + batchSize - 1);

      if (batchError) {
        return NextResponse.json(
          { error: batchError.message, rankings: [], pagination: { total: 0, limit, offset, hasMore: false } },
          { status: 500 }
        );
      }

      if (batch && batch.length > 0) {
        allValues.push(...(batch as SemesterValue[]));
        batchOffset += batchSize;
        hasMore = batch.length === batchSize;
      } else {
        hasMore = false;
      }
    }
  }

  // Aggregate values by municipality
  const aggregated = new Map<string, {
    valueSum: number;
    rentSum: number;
    rentCount: number;
    valueCount: number;
    latestValue: number | null;
    earliestValue: number | null;
  }>();

  for (const row of allValues) {
    const existing = aggregated.get(row.municipality_id);
    if (existing) {
      existing.valueSum += row.value_mid_eur_sqm;
      existing.valueCount += 1;
      if (row.rent_mid_eur_sqm_month) {
        existing.rentSum += row.rent_mid_eur_sqm_month;
        existing.rentCount += 1;
      }
      if (row.period_id === latestPeriod) existing.latestValue = row.value_mid_eur_sqm;
      if (row.period_id === earliestPeriod) existing.earliestValue = row.value_mid_eur_sqm;
    } else {
      aggregated.set(row.municipality_id, {
        valueSum: row.value_mid_eur_sqm,
        rentSum: row.rent_mid_eur_sqm_month ?? 0,
        rentCount: row.rent_mid_eur_sqm_month ? 1 : 0,
        valueCount: 1,
        latestValue: row.period_id === latestPeriod ? row.value_mid_eur_sqm : null,
        earliestValue: row.period_id === earliestPeriod ? row.value_mid_eur_sqm : null,
      });
    }
  }

  // Build ranking entries
  interface RankingData {
    municipalityId: string;
    name: string;
    regionCode: string | null;
    regionName: string | null;
    provinceCode: string | null;
    provinceName: string | null;
    isCoastal: boolean;
    isMountain: boolean;
    valueMidEurSqm: number;
    grossYieldPct: number | null;
    annualizedPriceChangePct: number | null;
    salesPer1000Pop: number | null;
    zonesWithData: number;
  }

  const rankingEntries: RankingData[] = [];

  for (const [municipalityId, data] of aggregated) {
    const info = municipalityInfo.get(municipalityId);
    if (!info) continue; // Skip if filtered out by region/province

    const avgValue = data.valueSum / data.valueCount;
    const avgRent = data.rentCount > 0 ? data.rentSum / data.rentCount : null;
    const grossYield = avgRent && avgValue > 0 ? (avgRent * 12 / avgValue) * 100 : null;

    // Calculate annualized price change
    let annualizedChange: number | null = null;
    if (data.latestValue && data.earliestValue && data.earliestValue > 0 && semestersToAverage > 1) {
      const totalChange = (data.latestValue - data.earliestValue) / data.earliestValue;
      const years = semestersToAverage / 2;
      // Annualize: ((1 + total) ^ (1/years) - 1) * 100
      annualizedChange = (Math.pow(1 + totalChange, 1 / years) - 1) * 100;
    }

    rankingEntries.push({
      municipalityId,
      name: info.municipality_name,
      regionCode: info.region_code,
      regionName: info.region_name,
      provinceCode: info.province_code,
      provinceName: info.province_name,
      isCoastal: info.coastal_flag ?? false,
      isMountain: info.mountain_flag ?? false,
      valueMidEurSqm: avgValue,
      grossYieldPct: grossYield,
      annualizedPriceChangePct: annualizedChange,
      salesPer1000Pop: null, // Not available in dynamic query
      zonesWithData: data.valueCount,
    });
  }

  // Sort entries
  const sortKey = sortBy === "ntn_per_1000_pop" ? "salesPer1000Pop" :
                  sortBy === "gross_yield_pct" ? "grossYieldPct" :
                  sortBy === "annualized_price_change_pct" ? "annualizedPriceChangePct" :
                  "valueMidEurSqm";

  rankingEntries.sort((a, b) => {
    const aVal = a[sortKey as keyof RankingData] as number | null;
    const bVal = b[sortKey as keyof RankingData] as number | null;
    if (aVal === null && bVal === null) return 0;
    if (aVal === null) return 1;
    if (bVal === null) return -1;
    return sortOrder ? aVal - bVal : bVal - aVal;
  });

  const totalCount = rankingEntries.length;

  // Handle search
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
    const foundIndex = rankingEntries.findIndex(e =>
      e.name.toLowerCase().includes(searchQuery)
    );
    if (foundIndex >= 0) {
      const found = rankingEntries[foundIndex];
      searchResult = {
        municipalityId: found.municipalityId,
        name: found.name,
        rank: foundIndex + 1,
        page: Math.floor(foundIndex / limit),
        regionCode: found.regionCode,
        regionName: found.regionName,
        provinceCode: found.provinceCode,
        provinceName: found.provinceName,
      };
    }
  }

  // Apply pagination
  const paginatedEntries = rankingEntries.slice(offset, offset + limit);

  const rankings = paginatedEntries.map((e, index) => ({
    rank: offset + index + 1,
    ...e,
  }));

  // Cache for 2 minutes with stale-while-revalidate
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
      periodsIncluded: periodsToUse,
      semestersToAverage,
      segment,
      filters: {
        region: regionCode,
        province: provinceCode,
      },
    },
    searchResult,
  }, {
    headers: {
      "Cache-Control": "public, max-age=120, stale-while-revalidate=600",
    },
  });
}
