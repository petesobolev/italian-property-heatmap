import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";
import { readFileSync, existsSync } from "fs";
import { join } from "path";

interface RouteParams {
  params: Promise<{ istatCode: string }>;
}

interface GeoJSONFeature {
  type: "Feature";
  properties: {
    municipality_id: string;
    name: string;
    province_code: string;
    region_code: string;
  };
  geometry: unknown;
}

interface GeoJSONCollection {
  type: "FeatureCollection";
  features: GeoJSONFeature[];
}

// Cache for GeoJSON data
let geoJSONCache: GeoJSONCollection | null = null;

function getGeoJSONData(): GeoJSONCollection | null {
  if (geoJSONCache) return geoJSONCache;

  const filePath = join(process.cwd(), "public", "demo", "municipalities.geojson");
  if (!existsSync(filePath)) return null;

  try {
    const data = readFileSync(filePath, "utf-8");
    geoJSONCache = JSON.parse(data) as GeoJSONCollection;
    return geoJSONCache;
  } catch {
    return null;
  }
}

function findMunicipalityInGeoJSON(istatCode: string): GeoJSONFeature | null {
  const data = getGeoJSONData();
  if (!data) return null;
  return data.features.find((f) => f.properties.municipality_id === istatCode) ?? null;
}

export async function GET(request: Request, { params }: RouteParams) {
  const { istatCode } = await params;
  const { searchParams } = new URL(request.url);
  const segment = searchParams.get("segment") ?? "residential";
  const horizonMonths = Number(searchParams.get("horizonMonths") ?? "12");

  const supabase = createSupabaseServerClient();

  // 1. Fetch basic municipality info from database
  const { data: municipality, error: muniError } = await supabase
    .schema("core")
    .from("municipalities")
    .select(
      `
      municipality_id,
      municipality_name,
      region_code,
      province_code,
      coastal_flag,
      mountain_flag,
      area_sqkm
    `
    )
    .eq("municipality_id", istatCode)
    .single();

  // Fallback to GeoJSON if database is empty
  let municipalityData: {
    municipality_id: string;
    municipality_name: string;
    region_code: string;
    province_code: string;
    coastal_flag: boolean;
    mountain_flag: boolean;
    area_sqkm: number | null;
  } | null = null;

  if (muniError || !municipality) {
    // Try to find in local GeoJSON
    const geoFeature = findMunicipalityInGeoJSON(istatCode);
    if (geoFeature) {
      municipalityData = {
        municipality_id: geoFeature.properties.municipality_id,
        municipality_name: geoFeature.properties.name,
        region_code: geoFeature.properties.region_code,
        province_code: geoFeature.properties.province_code,
        coastal_flag: false,
        mountain_flag: false,
        area_sqkm: null,
      };
    }
  } else {
    municipalityData = municipality;
  }

  if (!municipalityData) {
    return NextResponse.json(
      { error: "Municipality not found", code: istatCode },
      { status: 404 }
    );
  }

  // 2. Fetch region and province names
  const [{ data: region }, { data: province }] = await Promise.all([
    supabase
      .schema("core")
      .from("regions")
      .select("region_name")
      .eq("region_code", municipalityData.region_code)
      .single(),
    supabase
      .schema("core")
      .from("provinces")
      .select("province_name")
      .eq("province_code", municipalityData.province_code)
      .single(),
  ]);

  // 3. Fetch latest forecast
  const { data: latestForecast } = await supabase
    .schema("model")
    .from("forecasts_municipality")
    .select(
      `
      forecast_date,
      horizon_months,
      value_mid_eur_sqm,
      forecast_appreciation_pct,
      forecast_gross_yield_pct,
      opportunity_score,
      confidence_score,
      drivers,
      risks,
      model_version
    `
    )
    .eq("municipality_id", istatCode)
    .eq("property_segment", segment)
    .eq("horizon_months", horizonMonths)
    .eq("publishable_flag", true)
    .order("forecast_date", { ascending: false })
    .limit(1)
    .single();

  // 4. Fetch historical values (last 8 semesters = 4 years)
  const { data: historicalValues } = await supabase
    .schema("mart")
    .from("municipality_values_semester")
    .select(
      `
      period_id,
      value_mid_eur_sqm,
      value_min_eur_sqm,
      value_max_eur_sqm,
      rent_mid_eur_sqm_month,
      value_pct_change_1s,
      zones_with_data
    `
    )
    .eq("municipality_id", istatCode)
    .eq("property_segment", segment)
    .order("period_id", { ascending: false })
    .limit(8);

  // 5. Fetch historical transactions
  const { data: historicalTransactions } = await supabase
    .schema("mart")
    .from("municipality_transactions_semester")
    .select(
      `
      period_id,
      ntn_total,
      ntn_per_1000_pop,
      absorption_rate
    `
    )
    .eq("municipality_id", istatCode)
    .eq("property_segment", segment)
    .order("period_id", { ascending: false })
    .limit(8);

  // 6. Fetch latest demographics
  const { data: demographics } = await supabase
    .schema("mart")
    .from("municipality_demographics_year")
    .select(
      `
      reference_year,
      total_population,
      population_density,
      young_ratio,
      working_ratio,
      elderly_ratio,
      foreign_ratio,
      population_growth_rate,
      dependency_ratio
    `
    )
    .eq("municipality_id", istatCode)
    .order("reference_year", { ascending: false })
    .limit(1)
    .single();

  // 7. Fetch neighboring municipalities
  const { data: neighbors } = await supabase
    .schema("core")
    .from("municipality_neighbors")
    .select(
      `
      neighbor_id,
      shared_border_km
    `
    )
    .eq("municipality_id", istatCode)
    .order("shared_border_km", { ascending: false })
    .limit(10);

  // If we have neighbors, get their names and values
  let neighborDetails: Array<{
    municipalityId: string;
    name: string;
    sharedBorderKm: number;
    valueMidEurSqm: number | null;
  }> = [];

  if (neighbors && neighbors.length > 0) {
    const neighborIds = neighbors.map((n) => n.neighbor_id);

    const { data: neighborMunis } = await supabase
      .schema("core")
      .from("municipalities")
      .select("municipality_id, municipality_name")
      .in("municipality_id", neighborIds);

    // Get latest values for neighbors
    const { data: neighborValues } = await supabase
      .schema("mart")
      .from("municipality_values_semester")
      .select("municipality_id, value_mid_eur_sqm, period_id")
      .in("municipality_id", neighborIds)
      .eq("property_segment", segment)
      .order("period_id", { ascending: false });

    // Create lookup maps
    const nameMap = new Map(
      (neighborMunis ?? []).map((m) => [m.municipality_id, m.municipality_name])
    );

    // Get latest value per municipality
    const valueMap = new Map<string, number>();
    for (const v of neighborValues ?? []) {
      if (!valueMap.has(v.municipality_id)) {
        valueMap.set(v.municipality_id, v.value_mid_eur_sqm);
      }
    }

    neighborDetails = neighbors.map((n) => ({
      municipalityId: n.neighbor_id,
      name: nameMap.get(n.neighbor_id) ?? n.neighbor_id,
      sharedBorderKm: n.shared_border_km,
      valueMidEurSqm: valueMap.get(n.neighbor_id) ?? null,
    }));
  }

  // 8. Fetch latest STR data
  const { data: strData } = await supabase
    .schema("mart")
    .from("municipality_str_month")
    .select(
      `
      period_id,
      adr_eur,
      occupancy_rate,
      rev_par_eur,
      monthly_revenue_avg_eur,
      annual_revenue_estimate_eur,
      active_listings_count,
      seasonality_factor,
      is_peak_season,
      str_gross_yield_pct,
      str_net_yield_pct
    `
    )
    .eq("municipality_id", istatCode)
    .order("period_id", { ascending: false })
    .limit(12);

  // 9. Fetch STR seasonality profile
  const { data: strSeasonality } = await supabase
    .schema("mart")
    .from("municipality_str_seasonality")
    .select(
      `
      reference_year,
      annual_avg_adr_eur,
      annual_avg_occupancy,
      annual_avg_rev_par_eur,
      total_annual_revenue_estimate_eur,
      seasonality_score,
      peak_months,
      shoulder_months,
      off_peak_months,
      peak_to_offpeak_adr_ratio,
      monthly_adr_profile,
      monthly_occupancy_profile
    `
    )
    .eq("municipality_id", istatCode)
    .order("reference_year", { ascending: false })
    .limit(1)
    .single();

  // 10. Fetch regulation data
  const { data: regulations } = await supabase
    .schema("mart")
    .from("municipality_regulations")
    .select(
      `
      regulation_risk_score,
      regulation_risk_level,
      str_regulation_score,
      heritage_score,
      str_license_required,
      str_max_days_per_year,
      str_new_permits_allowed,
      str_zones_restricted,
      has_heritage_zones,
      has_rent_control,
      active_regulations_count,
      risk_factors,
      investor_warning_level,
      investor_notes
    `
    )
    .eq("municipality_id", istatCode)
    .single();

  // 11. Calculate some derived metrics
  const currentValue = latestForecast?.value_mid_eur_sqm ?? null;
  const appreciation = latestForecast?.forecast_appreciation_pct ?? null;
  const projectedValue =
    currentValue && appreciation
      ? currentValue * (1 + appreciation / 100)
      : null;

  // Calculate price trend direction from historical
  let priceTrend: "up" | "down" | "stable" | null = null;
  if (historicalValues && historicalValues.length >= 2) {
    const recent = historicalValues[0]?.value_mid_eur_sqm;
    const previous = historicalValues[1]?.value_mid_eur_sqm;
    if (recent && previous) {
      const change = ((recent - previous) / previous) * 100;
      if (change > 1) priceTrend = "up";
      else if (change < -1) priceTrend = "down";
      else priceTrend = "stable";
    }
  }

  return NextResponse.json({
    municipality: {
      id: municipalityData.municipality_id,
      name: municipalityData.municipality_name,
      regionCode: municipalityData.region_code,
      regionName: region?.region_name ?? null,
      provinceCode: municipalityData.province_code,
      provinceName: province?.province_name ?? null,
      isCoastal: municipalityData.coastal_flag ?? false,
      isMountain: municipalityData.mountain_flag ?? false,
      areaSqKm: municipalityData.area_sqkm,
    },
    forecast: latestForecast
      ? {
          date: latestForecast.forecast_date,
          horizonMonths: latestForecast.horizon_months,
          valueMidEurSqm: latestForecast.value_mid_eur_sqm,
          appreciationPct: latestForecast.forecast_appreciation_pct,
          projectedValueEurSqm: projectedValue,
          grossYieldPct: latestForecast.forecast_gross_yield_pct,
          opportunityScore: latestForecast.opportunity_score,
          confidenceScore: latestForecast.confidence_score,
          drivers: latestForecast.drivers,
          risks: latestForecast.risks,
          modelVersion: latestForecast.model_version,
        }
      : null,
    historicalValues: (historicalValues ?? []).map((v) => ({
      periodId: v.period_id,
      valueMidEurSqm: v.value_mid_eur_sqm,
      valueMinEurSqm: v.value_min_eur_sqm,
      valueMaxEurSqm: v.value_max_eur_sqm,
      rentMidEurSqmMonth: v.rent_mid_eur_sqm_month,
      pctChange1s: v.value_pct_change_1s,
      zonesWithData: v.zones_with_data,
    })),
    historicalTransactions: (historicalTransactions ?? []).map((t) => ({
      periodId: t.period_id,
      ntnTotal: t.ntn_total,
      ntnPer1000Pop: t.ntn_per_1000_pop,
      absorptionRate: t.absorption_rate,
    })),
    demographics: demographics
      ? {
          year: demographics.reference_year,
          totalPopulation: demographics.total_population,
          populationDensity: demographics.population_density,
          youngRatio: demographics.young_ratio,
          workingRatio: demographics.working_ratio,
          elderlyRatio: demographics.elderly_ratio,
          foreignRatio: demographics.foreign_ratio,
          populationGrowthRate: demographics.population_growth_rate,
          dependencyRatio: demographics.dependency_ratio,
        }
      : null,
    neighbors: neighborDetails,
    // STR (Short-Term Rental) data
    strMetrics: strData && strData.length > 0
      ? {
          latest: {
            periodId: strData[0].period_id,
            adrEur: strData[0].adr_eur,
            occupancyRate: strData[0].occupancy_rate,
            revParEur: strData[0].rev_par_eur,
            monthlyRevenueEur: strData[0].monthly_revenue_avg_eur,
            annualRevenueEur: strData[0].annual_revenue_estimate_eur,
            activeListings: strData[0].active_listings_count,
            seasonalityFactor: strData[0].seasonality_factor,
            isPeakSeason: strData[0].is_peak_season,
            grossYieldPct: strData[0].str_gross_yield_pct,
            netYieldPct: strData[0].str_net_yield_pct,
          },
          historical: strData.map((s) => ({
            periodId: s.period_id,
            adrEur: s.adr_eur,
            occupancyRate: s.occupancy_rate,
            revParEur: s.rev_par_eur,
          })),
        }
      : null,
    strSeasonality: strSeasonality
      ? {
          year: strSeasonality.reference_year,
          annualAvgAdr: strSeasonality.annual_avg_adr_eur,
          annualAvgOccupancy: strSeasonality.annual_avg_occupancy,
          annualAvgRevPar: strSeasonality.annual_avg_rev_par_eur,
          totalAnnualRevenue: strSeasonality.total_annual_revenue_estimate_eur,
          seasonalityScore: strSeasonality.seasonality_score,
          peakMonths: strSeasonality.peak_months,
          shoulderMonths: strSeasonality.shoulder_months,
          offPeakMonths: strSeasonality.off_peak_months,
          peakToOffpeakRatio: strSeasonality.peak_to_offpeak_adr_ratio,
          monthlyAdrProfile: strSeasonality.monthly_adr_profile,
          monthlyOccupancyProfile: strSeasonality.monthly_occupancy_profile,
        }
      : null,
    // Regulation data
    regulations: regulations
      ? {
          riskScore: regulations.regulation_risk_score,
          riskLevel: regulations.regulation_risk_level,
          strRegulationScore: regulations.str_regulation_score,
          heritageScore: regulations.heritage_score,
          strLicenseRequired: regulations.str_license_required,
          strMaxDaysPerYear: regulations.str_max_days_per_year,
          strNewPermitsAllowed: regulations.str_new_permits_allowed,
          strZonesRestricted: regulations.str_zones_restricted,
          hasHeritageZones: regulations.has_heritage_zones,
          hasRentControl: regulations.has_rent_control,
          activeRegulationsCount: regulations.active_regulations_count,
          riskFactors: regulations.risk_factors,
          investorWarningLevel: regulations.investor_warning_level,
          investorNotes: regulations.investor_notes,
        }
      : null,
    derived: {
      priceTrend,
      currentValue,
      projectedValue,
    },
  });
}
