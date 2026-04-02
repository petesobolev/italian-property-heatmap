import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";
import { readFileSync, existsSync } from "fs";
import { join } from "path";

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

function findMunicipalitiesInGeoJSON(ids: string[]): Map<string, GeoJSONFeature> {
  const data = getGeoJSONData();
  if (!data) return new Map();

  const result = new Map<string, GeoJSONFeature>();
  for (const feature of data.features) {
    if (ids.includes(feature.properties.municipality_id)) {
      result.set(feature.properties.municipality_id, feature);
    }
  }
  return result;
}

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);

  // Get municipality IDs (comma-separated)
  const idsParam = searchParams.get("ids");
  if (!idsParam) {
    return NextResponse.json(
      { error: "Missing 'ids' parameter. Provide 2-5 comma-separated municipality IDs." },
      { status: 400 }
    );
  }

  const ids = idsParam.split(",").map((id) => id.trim()).filter(Boolean);

  if (ids.length < 2 || ids.length > 5) {
    return NextResponse.json(
      { error: "Please provide between 2 and 5 municipality IDs." },
      { status: 400 }
    );
  }

  const segment = searchParams.get("segment") ?? "residential";
  const horizonMonths = Number(searchParams.get("horizonMonths") ?? "12");

  const supabase = createSupabaseServerClient();

  // 1. Fetch municipality info from database or GeoJSON
  const { data: dbMunicipalities } = await supabase
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
    .in("municipality_id", ids);

  // Build map of municipality data, falling back to GeoJSON
  const municipalityMap = new Map<string, {
    id: string;
    name: string;
    regionCode: string;
    provinceCode: string;
    isCoastal: boolean;
    isMountain: boolean;
    areaSqKm: number | null;
  }>();

  // Add database results
  for (const m of dbMunicipalities ?? []) {
    municipalityMap.set(m.municipality_id, {
      id: m.municipality_id,
      name: m.municipality_name,
      regionCode: m.region_code,
      provinceCode: m.province_code,
      isCoastal: m.coastal_flag ?? false,
      isMountain: m.mountain_flag ?? false,
      areaSqKm: m.area_sqkm,
    });
  }

  // Fallback to GeoJSON for missing municipalities
  const missingIds = ids.filter((id) => !municipalityMap.has(id));
  if (missingIds.length > 0) {
    const geoFeatures = findMunicipalitiesInGeoJSON(missingIds);
    for (const [id, feature] of geoFeatures) {
      municipalityMap.set(id, {
        id: feature.properties.municipality_id,
        name: feature.properties.name,
        regionCode: feature.properties.region_code,
        provinceCode: feature.properties.province_code,
        isCoastal: false,
        isMountain: false,
        areaSqKm: null,
      });
    }
  }

  // Check if all municipalities were found
  const foundIds = [...municipalityMap.keys()];
  const notFoundIds = ids.filter((id) => !foundIds.includes(id));
  if (notFoundIds.length > 0) {
    return NextResponse.json(
      { error: `Municipalities not found: ${notFoundIds.join(", ")}` },
      { status: 404 }
    );
  }

  // 2. Fetch forecasts for all municipalities
  const { data: forecasts } = await supabase
    .schema("model")
    .from("forecasts_municipality")
    .select(
      `
      municipality_id,
      forecast_date,
      value_mid_eur_sqm,
      forecast_appreciation_pct,
      forecast_gross_yield_pct,
      opportunity_score,
      confidence_score,
      drivers,
      risks
    `
    )
    .in("municipality_id", ids)
    .eq("property_segment", segment)
    .eq("horizon_months", horizonMonths)
    .eq("publishable_flag", true)
    .order("forecast_date", { ascending: false });

  // Get latest forecast per municipality
  type ForecastRow = {
    municipality_id: string;
    forecast_date: string;
    value_mid_eur_sqm: number | null;
    forecast_appreciation_pct: number | null;
    forecast_gross_yield_pct: number | null;
    opportunity_score: number | null;
    confidence_score: number | null;
    drivers: Array<{ factor: string; direction: string; strength: number }> | null;
    risks: Array<{ factor: string; severity: string }> | null;
  };
  const forecastMap = new Map<string, ForecastRow>();
  for (const f of (forecasts ?? []) as ForecastRow[]) {
    if (!forecastMap.has(f.municipality_id)) {
      forecastMap.set(f.municipality_id, f);
    }
  }

  // 3. Fetch historical values for all municipalities (last 8 semesters)
  const { data: historicalValues } = await supabase
    .schema("mart")
    .from("municipality_values_semester")
    .select(
      `
      municipality_id,
      period_id,
      value_mid_eur_sqm,
      rent_mid_eur_sqm_month
    `
    )
    .in("municipality_id", ids)
    .eq("property_segment", segment)
    .order("period_id", { ascending: false })
    .limit(ids.length * 8);

  // Group by municipality
  const historicalMap = new Map<string, Array<{
    periodId: string;
    valueMidEurSqm: number | null;
    rentMidEurSqmMonth: number | null;
  }>>();

  for (const v of historicalValues ?? []) {
    if (!historicalMap.has(v.municipality_id)) {
      historicalMap.set(v.municipality_id, []);
    }
    historicalMap.get(v.municipality_id)!.push({
      periodId: v.period_id,
      valueMidEurSqm: v.value_mid_eur_sqm,
      rentMidEurSqmMonth: v.rent_mid_eur_sqm_month,
    });
  }

  // 4. Fetch demographics for all municipalities
  const { data: demographics } = await supabase
    .schema("mart")
    .from("municipality_demographics_year")
    .select(
      `
      municipality_id,
      reference_year,
      total_population,
      population_density,
      young_ratio,
      elderly_ratio,
      foreign_ratio,
      population_growth_rate
    `
    )
    .in("municipality_id", ids)
    .order("reference_year", { ascending: false });

  // Get latest demographics per municipality
  type DemographicsRow = {
    municipality_id: string;
    reference_year: number;
    total_population: number | null;
    population_density: number | null;
    young_ratio: number | null;
    elderly_ratio: number | null;
    foreign_ratio: number | null;
    population_growth_rate: number | null;
  };
  const demographicsMap = new Map<string, DemographicsRow>();
  for (const d of (demographics ?? []) as DemographicsRow[]) {
    if (!demographicsMap.has(d.municipality_id)) {
      demographicsMap.set(d.municipality_id, d);
    }
  }

  // 5. Build comparison response
  const municipalities = ids.map((id) => {
    const info = municipalityMap.get(id)!;
    const forecast = forecastMap.get(id);
    const history = historicalMap.get(id) ?? [];
    const demo = demographicsMap.get(id);

    return {
      id: info.id,
      name: info.name,
      regionCode: info.regionCode,
      provinceCode: info.provinceCode,
      isCoastal: info.isCoastal,
      isMountain: info.isMountain,
      areaSqKm: info.areaSqKm,
      forecast: forecast
        ? {
            date: forecast.forecast_date,
            valueMidEurSqm: forecast.value_mid_eur_sqm,
            appreciationPct: forecast.forecast_appreciation_pct,
            grossYieldPct: forecast.forecast_gross_yield_pct,
            opportunityScore: forecast.opportunity_score,
            confidenceScore: forecast.confidence_score,
            drivers: forecast.drivers,
            risks: forecast.risks,
          }
        : null,
      historicalValues: history.slice(0, 8),
      demographics: demo
        ? {
            year: demo.reference_year,
            totalPopulation: demo.total_population,
            populationDensity: demo.population_density,
            youngRatio: demo.young_ratio,
            elderlyRatio: demo.elderly_ratio,
            foreignRatio: demo.foreign_ratio,
            populationGrowthRate: demo.population_growth_rate,
          }
        : null,
    };
  });

  // 6. Calculate comparison metrics
  const metrics = {
    valueMidEurSqm: {
      values: municipalities.map((m) => m.forecast?.valueMidEurSqm ?? null),
      min: Math.min(...municipalities.filter((m) => m.forecast?.valueMidEurSqm).map((m) => m.forecast!.valueMidEurSqm!)),
      max: Math.max(...municipalities.filter((m) => m.forecast?.valueMidEurSqm).map((m) => m.forecast!.valueMidEurSqm!)),
      avg: municipalities.filter((m) => m.forecast?.valueMidEurSqm).reduce((sum, m) => sum + m.forecast!.valueMidEurSqm!, 0) /
           municipalities.filter((m) => m.forecast?.valueMidEurSqm).length || 0,
    },
    appreciationPct: {
      values: municipalities.map((m) => m.forecast?.appreciationPct ?? null),
      best: municipalities.reduce((best, m) =>
        (m.forecast?.appreciationPct ?? -Infinity) > (best.forecast?.appreciationPct ?? -Infinity) ? m : best
      ).id,
    },
    grossYieldPct: {
      values: municipalities.map((m) => m.forecast?.grossYieldPct ?? null),
      best: municipalities.reduce((best, m) =>
        (m.forecast?.grossYieldPct ?? -Infinity) > (best.forecast?.grossYieldPct ?? -Infinity) ? m : best
      ).id,
    },
    opportunityScore: {
      values: municipalities.map((m) => m.forecast?.opportunityScore ?? null),
      best: municipalities.reduce((best, m) =>
        (m.forecast?.opportunityScore ?? -Infinity) > (best.forecast?.opportunityScore ?? -Infinity) ? m : best
      ).id,
    },
  };

  return NextResponse.json({
    municipalities,
    metrics,
    meta: {
      segment,
      horizonMonths,
      comparedAt: new Date().toISOString(),
    },
  });
}
