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

  // 5. Fetch historical transactions (municipal level)
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

  // 5b. Fallback: Fetch provincial transactions if no municipal data
  // Provincial data shows capoluogo vs non-capoluogo transactions
  const provinceCode = municipalityData.province_code;
  let provincialTransactions: Array<{
    period_id: string;
    ntn_total: number;
    is_capoluogo: boolean;
  }> | null = null;

  if (!historicalTransactions || historicalTransactions.length === 0) {
    const { data: provTx } = await supabase
      .schema("mart")
      .from("province_transactions_semester")
      .select("period_id, ntn_total, is_capoluogo")
      .eq("province_code", provinceCode)
      .eq("property_segment", segment)
      .order("period_id", { ascending: false })
      .limit(16); // Get both capoluogo and non-capoluogo for 8 periods

    provincialTransactions = provTx;
  }

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

  // 11. Calculate derived metrics and fallback forecasts

  // Get current values from historical data if forecast doesn't have them
  const latestHistorical = historicalValues?.[0];
  const currentValue = latestForecast?.value_mid_eur_sqm ?? latestHistorical?.value_mid_eur_sqm ?? null;
  const currentRent = latestHistorical?.rent_mid_eur_sqm_month ?? null;

  // Calculate gross yield from rent/value if not in forecast
  // Formula: (monthly_rent * 12) / purchase_price * 100
  let calculatedGrossYield: number | null = null;
  if (currentRent && currentValue && currentValue > 0) {
    calculatedGrossYield = (currentRent * 12) / currentValue * 100;
  }

  // Calculate appreciation forecast from historical trend
  // Use average of semester-over-semester changes, annualized (x2 for yearly)
  let calculatedAppreciation: number | null = null;
  if (historicalValues && historicalValues.length >= 2) {
    // Collect valid semester changes
    const semesterChanges: number[] = [];
    for (let i = 0; i < Math.min(historicalValues.length - 1, 4); i++) {
      const current = historicalValues[i]?.value_mid_eur_sqm;
      const previous = historicalValues[i + 1]?.value_mid_eur_sqm;
      if (current && previous && previous > 0) {
        const change = ((current - previous) / previous) * 100;
        semesterChanges.push(change);
      }
    }

    if (semesterChanges.length > 0) {
      // Average semester change, then annualize (x2)
      const avgSemesterChange = semesterChanges.reduce((a, b) => a + b, 0) / semesterChanges.length;
      calculatedAppreciation = avgSemesterChange * 2; // Annualized

      // Clamp to reasonable range (-30% to +30%)
      calculatedAppreciation = Math.max(-30, Math.min(30, calculatedAppreciation));
    }
  }

  const appreciation = latestForecast?.forecast_appreciation_pct ?? calculatedAppreciation;
  const grossYield = latestForecast?.forecast_gross_yield_pct ?? calculatedGrossYield;

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

  // Calculate confidence score based on data availability
  let calculatedConfidence: number | null = null;
  if (historicalValues && historicalValues.length > 0) {
    // Base confidence on: data recency, number of periods, and zones with data
    const periodsAvailable = historicalValues.length;
    const zonesWithData = latestHistorical?.zones_with_data ?? 1;
    const hasRentData = currentRent !== null;

    // Simple confidence formula:
    // - 40 points base if we have any data
    // - Up to 30 points for number of historical periods (max at 6+)
    // - Up to 20 points for zones with data (max at 10+)
    // - 10 points bonus if we have rent data
    const periodScore = Math.min(periodsAvailable / 6, 1) * 30;
    const zoneScore = Math.min(zonesWithData / 10, 1) * 20;
    const rentBonus = hasRentData ? 10 : 0;

    calculatedConfidence = 40 + periodScore + zoneScore + rentBonus;
  }

  // Calculate opportunity score (0-100) based on investment attractiveness
  let calculatedOpportunityScore: number | null = null;
  if (historicalValues && historicalValues.length > 0 && currentValue) {
    const zonesWithData = latestHistorical?.zones_with_data ?? 1;
    const valueMin = latestHistorical?.value_min_eur_sqm;
    const valueMax = latestHistorical?.value_max_eur_sqm;

    // 1. Yield Component (0-35 points)
    // Italian average yield is ~4-5%, excellent is 6%+
    // Score: yield * 5, capped at 35
    const yieldScore = grossYield
      ? Math.min(35, grossYield * 5)
      : 0;

    // 2. Appreciation Trend Component (0-25 points)
    // Positive appreciation is good, but not too high (overheating)
    // Best range: 2-8% annual appreciation
    // Score: peaks at 5% appreciation = 25 points
    let appreciationScore = 0;
    if (appreciation !== null) {
      if (appreciation >= 0 && appreciation <= 10) {
        // Positive appreciation: score peaks at 5%
        appreciationScore = 25 - Math.abs(appreciation - 5) * 3;
      } else if (appreciation < 0 && appreciation >= -5) {
        // Slight decline: partial credit
        appreciationScore = Math.max(0, 10 + appreciation * 2);
      }
      // Very negative or very high appreciation gets 0
      appreciationScore = Math.max(0, appreciationScore);
    }

    // 3. Affordability Component (0-20 points)
    // Lower prices = more accessible entry point
    // Italian average is ~1500-2000 €/m²
    // Score: inversely proportional to price, capped
    let affordabilityScore = 0;
    if (currentValue < 1000) {
      affordabilityScore = 20; // Very affordable
    } else if (currentValue < 2000) {
      affordabilityScore = 20 - (currentValue - 1000) / 100; // 20 to 10
    } else if (currentValue < 4000) {
      affordabilityScore = 10 - (currentValue - 2000) / 400; // 10 to 5
    } else {
      affordabilityScore = Math.max(0, 5 - (currentValue - 4000) / 2000); // 5 to 0
    }

    // 4. Price Stability Component (0-10 points)
    // Lower variance (max-min)/mid = more predictable market
    let stabilityScore = 10;
    if (valueMin && valueMax && currentValue > 0) {
      const spreadRatio = (valueMax - valueMin) / currentValue;
      // Typical spread is 0.3-0.8, lower is better
      stabilityScore = Math.max(0, 10 - spreadRatio * 10);
    }

    // 5. Data Quality Component (0-10 points)
    // More zones with data = more reliable assessment
    const dataQualityScore = Math.min(10, zonesWithData / 3);

    // Total opportunity score
    calculatedOpportunityScore = Math.round(
      yieldScore + appreciationScore + affordabilityScore + stabilityScore + dataQualityScore
    );

    // Clamp to 0-100
    calculatedOpportunityScore = Math.max(0, Math.min(100, calculatedOpportunityScore));
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
    forecast: {
      date: latestForecast?.forecast_date ?? latestHistorical?.period_id ?? null,
      horizonMonths: latestForecast?.horizon_months ?? horizonMonths,
      valueMidEurSqm: currentValue,
      appreciationPct: appreciation,
      projectedValueEurSqm: projectedValue,
      grossYieldPct: grossYield,
      opportunityScore: latestForecast?.opportunity_score ?? calculatedOpportunityScore,
      confidenceScore: latestForecast?.confidence_score ?? calculatedConfidence,
      drivers: latestForecast?.drivers ?? null,
      risks: latestForecast?.risks ?? null,
      modelVersion: latestForecast?.model_version ?? (calculatedAppreciation !== null ? "historical-trend-v1" : null),
      // Flag to indicate if this is a calculated vs model forecast
      isCalculated: latestForecast === null && (calculatedAppreciation !== null || calculatedGrossYield !== null),
    },
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
    // Provincial-level transaction data (fallback when no municipal data)
    provincialTransactions: provincialTransactions
      ? (() => {
          // Group by period and sum capoluogo + non-capoluogo
          const byPeriod = new Map<string, { capoluogo: number; nonCapoluogo: number }>();
          for (const t of provincialTransactions) {
            const existing = byPeriod.get(t.period_id) || { capoluogo: 0, nonCapoluogo: 0 };
            if (t.is_capoluogo) {
              existing.capoluogo = t.ntn_total;
            } else {
              existing.nonCapoluogo = t.ntn_total;
            }
            byPeriod.set(t.period_id, existing);
          }
          // Convert to array sorted by period descending
          return Array.from(byPeriod.entries())
            .sort((a, b) => b[0].localeCompare(a[0]))
            .slice(0, 8)
            .map(([periodId, data]) => ({
              periodId,
              ntnCapoluogo: data.capoluogo,
              ntnNonCapoluogo: data.nonCapoluogo,
              ntnTotal: data.capoluogo + data.nonCapoluogo,
            }));
        })()
      : null,
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
