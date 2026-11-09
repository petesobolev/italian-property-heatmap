import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const metric = searchParams.get("metric") ?? "value_mid_eur_sqm";
  const horizonMonths = Number(searchParams.get("horizonMonths") ?? "12");
  const segment = searchParams.get("segment") ?? "residential";
  const semestersToAverage = Math.min(Math.max(Number(searchParams.get("semesters") ?? "1"), 1), 8);

  const supabase = createSupabaseServerClient();

  // Handle foreign residents ratio from demographics
  if (metric === "foreign_ratio") {
    // Fetch foreign_ratio from municipality_demographics_year
    // Use pagination to get all records
    const allRows: { municipality_id: string; foreign_ratio: number }[] = [];
    const batchSize = 1000;
    let offset = 0;
    let hasMore = true;

    // Get the latest year with data
    const { data: latestYear } = await supabase
      .schema("mart")
      .from("municipality_demographics_year")
      .select("reference_year")
      .not("foreign_ratio", "is", null)
      .order("reference_year", { ascending: false })
      .limit(1);

    const year = latestYear?.[0]?.reference_year ?? 2026;

    while (hasMore) {
      const { data: batch, error: batchError } = await supabase
        .schema("mart")
        .from("municipality_demographics_year")
        .select("municipality_id, foreign_ratio")
        .eq("reference_year", year)
        .not("foreign_ratio", "is", null)
        .range(offset, offset + batchSize - 1);

      if (batchError) {
        return NextResponse.json(
          { metric, asOf: new Date().toISOString(), error: batchError.message, features: [] },
          { status: 500 }
        );
      }

      if (batch && batch.length > 0) {
        allRows.push(...batch);
        offset += batchSize;
        hasMore = batch.length === batchSize;
      } else {
        hasMore = false;
      }
    }

    return NextResponse.json({
      metric,
      asOf: String(year),
      source: "mart.municipality_demographics_year",
      features: allRows.map((r) => ({
        municipalityId: r.municipality_id,
        value: r.foreign_ratio * 100, // Convert to percentage
      })),
    }, {
      headers: {
        "Cache-Control": "public, max-age=3600, stale-while-revalidate=86400",
      },
    });
  }

  // Handle population growth rate from demographics
  if (metric === "population_growth_rate") {
    // Fetch population_growth_rate from municipality_demographics_year
    // Use pagination to get all records
    const allRows: { municipality_id: string; population_growth_rate: number }[] = [];
    const batchSize = 1000;
    let offset = 0;
    let hasMore = true;

    // Get the latest year with data
    const { data: latestYear } = await supabase
      .schema("mart")
      .from("municipality_demographics_year")
      .select("reference_year")
      .not("population_growth_rate", "is", null)
      .order("reference_year", { ascending: false })
      .limit(1);

    const year = latestYear?.[0]?.reference_year ?? 2026;

    while (hasMore) {
      const { data: batch, error: batchError } = await supabase
        .schema("mart")
        .from("municipality_demographics_year")
        .select("municipality_id, population_growth_rate")
        .eq("reference_year", year)
        .not("population_growth_rate", "is", null)
        .range(offset, offset + batchSize - 1);

      if (batchError) {
        return NextResponse.json(
          { metric, asOf: new Date().toISOString(), error: batchError.message, features: [] },
          { status: 500 }
        );
      }

      if (batch && batch.length > 0) {
        allRows.push(...batch);
        offset += batchSize;
        hasMore = batch.length === batchSize;
      } else {
        hasMore = false;
      }
    }

    return NextResponse.json({
      metric,
      asOf: String(year),
      source: "mart.municipality_demographics_year",
      features: allRows.map((r) => ({
        municipalityId: r.municipality_id,
        value: r.population_growth_rate, // Already in percentage
      })),
    }, {
      headers: {
        "Cache-Control": "public, max-age=3600, stale-while-revalidate=86400",
      },
    });
  }

  // Handle vehicle arson metric separately
  if (metric === "vehicle_arson_rate") {
    const { data: arsonData, error: arsonError } = await supabase
      .schema("mart")
      .from("vehicle_arson_municipality_year")
      .select("municipality_id, rate_per_100k_residents, confidence_grade")
      .eq("year", 2023) // Latest year
      .order("rate_per_100k_residents", { ascending: false });

    if (arsonError) {
      return NextResponse.json(
        {
          metric,
          horizonMonths,
          segment,
          asOf: new Date().toISOString(),
          error: arsonError.message,
          features: [],
        },
        { status: 500 }
      );
    }

    return NextResponse.json({
      metric,
      horizonMonths,
      segment,
      asOf: "2023",
      features: (arsonData ?? []).map((r) => ({
        municipalityId: r.municipality_id,
        value: r.rate_per_100k_residents,
        confidenceGrade: r.confidence_grade,
      })),
    }, {
      headers: {
        "Cache-Control": "public, max-age=3600, stale-while-revalidate=86400",
      },
    });
  }

  // Handle metrics that use the optimized RPC function
  // This replaces multiple batch fetches + JS calculations with a single SQL query
  const rpcMetrics = ["value_mid_eur_sqm", "rent_mid_eur_sqm_month", "gross_yield_pct", "condition_premium_pct", "price_variance_pct"];

  if (rpcMetrics.includes(metric)) {
    // Get available periods for metadata
    const { data: availablePeriods, error: periodError } = await supabase
      .schema("mart")
      .rpc("get_available_periods", { p_segment: segment });

    if (periodError) {
      return NextResponse.json(
        {
          metric,
          horizonMonths,
          segment,
          semestersToAverage,
          asOf: new Date().toISOString(),
          error: periodError.message,
          features: [],
        },
        { status: 500 }
      );
    }

    const allAvailablePeriods = availablePeriods?.map((p: { period_id: string }) => p.period_id) ?? [];
    const availablePeriodsCount = allAvailablePeriods.length;
    const periodsToUse = allAvailablePeriods.slice(0, semestersToAverage);

    if (periodsToUse.length === 0) {
      return NextResponse.json({
        metric,
        horizonMonths,
        segment,
        semestersToAverage,
        availablePeriodsCount: 0,
        asOf: new Date().toISOString(),
        features: [],
        note: "No property values found yet. Run the OMI ingestion to load data.",
      });
    }

    // Call the optimized RPC function with pagination (Supabase limits to 1000 rows)
    const allMetricData: { municipality_id: string; metric_value: number; semesters_with_data: number }[] = [];
    const batchSize = 1000;
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const { data: batch, error: batchError } = await supabase
        .schema("mart")
        .rpc("get_layer_metric_values", {
          p_metric: metric,
          p_segment: segment,
          p_semesters_to_average: semestersToAverage,
        })
        .range(offset, offset + batchSize - 1);

      if (batchError) {
        return NextResponse.json(
          {
            metric,
            horizonMonths,
            segment,
            semestersToAverage,
            asOf: new Date().toISOString(),
            error: batchError.message,
            features: [],
          },
          { status: 500 }
        );
      }

      if (batch && batch.length > 0) {
        allMetricData.push(...batch);
        offset += batchSize;
        hasMore = batch.length === batchSize;
      } else {
        hasMore = false;
      }
    }

    const features = allMetricData.map((r) => ({
      municipalityId: r.municipality_id,
      value: r.metric_value,
      ...(semestersToAverage > 1 ? { semestersWithData: r.semesters_with_data } : {}),
    }));

    return NextResponse.json({
      metric,
      horizonMonths,
      segment,
      semestersToAverage,
      availablePeriodsCount,
      latestPeriod: allAvailablePeriods[0],
      asOf: periodsToUse[0],
      periodsIncluded: periodsToUse,
      source: "mart.get_layer_metric_values (RPC)",
      features,
    }, {
      headers: {
        "Cache-Control": "public, max-age=3600, stale-while-revalidate=86400",
      },
    });
  }

  // Handle value percentage change metric (compares current to historical)
  if (metric === "value_pct_change") {
    // changeYears: 0 = max (since 2016), 1-10 = number of years to look back
    const changeYears = Number(searchParams.get("changeYears") ?? "1");

    // Get available periods
    const { data: availablePeriods, error: periodError } = await supabase
      .schema("mart")
      .rpc("get_available_periods", { p_segment: segment });

    if (periodError) {
      return NextResponse.json(
        { metric, segment, changeYears, asOf: new Date().toISOString(), error: periodError.message, features: [] },
        { status: 500 }
      );
    }

    const allPeriods = availablePeriods?.map((p: { period_id: string }) => p.period_id) ?? [];
    const availablePeriodsCount = allPeriods.length;

    if (allPeriods.length < 2) {
      return NextResponse.json({
        metric,
        segment,
        changeYears,
        availablePeriodsCount,
        asOf: new Date().toISOString(),
        features: [],
        note: "Insufficient historical data for value change calculation. Need at least 2 periods.",
      });
    }

    // Determine how many semesters to look back (2 semesters per year)
    // changeYears = 0 means "max" (go back to earliest available)
    const semestersBack = changeYears === 0
      ? allPeriods.length - 1
      : changeYears * 2;

    const currentPeriod = allPeriods[0]; // Most recent
    // Use earliest available if requested period exceeds available data
    const comparisonIndex = Math.min(semestersBack, allPeriods.length - 1);
    const comparisonPeriod = allPeriods[comparisonIndex];

    // Fetch values for both periods in parallel with batching
    // Include zones_with_data for filtering
    const fetchPeriodValues = async (periodId: string) => {
      const rows: { municipality_id: string; value_mid_eur_sqm: number; zones_with_data: number }[] = [];
      const batchSize = 1000;
      let offset = 0;
      let hasMore = true;

      while (hasMore) {
        const { data: batch, error } = await supabase
          .schema("mart")
          .from("municipality_values_semester")
          .select("municipality_id, value_mid_eur_sqm, zones_with_data")
          .eq("period_id", periodId)
          .eq("property_segment", segment)
          .not("value_mid_eur_sqm", "is", null)
          .range(offset, offset + batchSize - 1);

        if (error) throw error;

        if (batch && batch.length > 0) {
          rows.push(...batch);
          offset += batchSize;
          hasMore = batch.length === batchSize;
        } else {
          hasMore = false;
        }
      }
      return rows;
    };

    let currentValues: { municipality_id: string; value_mid_eur_sqm: number; zones_with_data: number }[];
    let comparisonValues: { municipality_id: string; value_mid_eur_sqm: number; zones_with_data: number }[];

    try {
      [currentValues, comparisonValues] = await Promise.all([
        fetchPeriodValues(currentPeriod),
        fetchPeriodValues(comparisonPeriod),
      ]);
    } catch (err) {
      return NextResponse.json(
        { metric, segment, changeYears, asOf: new Date().toISOString(), error: String(err), features: [] },
        { status: 500 }
      );
    }

    // Build maps for lookup
    const currentMap = new Map(currentValues.map(r => [r.municipality_id, r]));
    const comparisonMap = new Map(comparisonValues.map(r => [r.municipality_id, r]));

    // Calculate percentage changes
    // Filter out municipalities where zone count differs significantly between periods
    // This prevents artificial swings from incomplete zone data
    const features: { municipalityId: string; value: number }[] = [];
    for (const [municipalityId, current] of currentMap) {
      const past = comparisonMap.get(municipalityId);
      if (!past || past.value_mid_eur_sqm <= 0) continue;

      // Filter out municipalities with inconsistent zone coverage between periods
      // Only apply strict filtering for small zone counts (where missing zones have big impact)
      // For municipalities with many zones, allow more variance as samples are more representative
      const currentZones = current.zones_with_data ?? 0;
      const pastZones = past.zones_with_data ?? 0;
      const minZones = Math.min(currentZones, pastZones);
      const maxZones = Math.max(currentZones, pastZones);

      if (minZones > 0 && maxZones > 0) {
        const zoneRatio = minZones / maxZones;
        // Strict filtering for small zone counts (like Scalea with 1-4 zones)
        // More lenient for larger zone counts (like Torrita di Siena with 12-55 zones)
        if (minZones < 3 && zoneRatio < 0.5) {
          // Very few zones and big variance - skip
          continue;
        }
        if (minZones >= 3 && minZones < 10 && zoneRatio < 0.4) {
          // Medium zone count, require 40% ratio
          continue;
        }
        // For 10+ zones, don't filter based on ratio - sample is large enough
      }

      const pctChange = ((current.value_mid_eur_sqm - past.value_mid_eur_sqm) / past.value_mid_eur_sqm) * 100;
      features.push({ municipalityId, value: pctChange });
    }

    return NextResponse.json({
      metric,
      segment,
      changeYears,
      availablePeriodsCount,
      currentPeriod,
      comparisonPeriod,
      asOf: currentPeriod,
      source: "mart.municipality_values_semester (zone-filtered)",
      features,
    }, {
      headers: { "Cache-Control": "public, max-age=300, stale-while-revalidate=3600" },
    });
  }

  // Forecast metrics from model.forecasts_municipality
  // Latest published snapshot approach (MVP): pick latest forecast_date for requested horizon+segment.
  const { data: latest, error: latestError } = await supabase
    .schema("model")
    .from("forecasts_municipality")
    .select("forecast_date")
    .eq("horizon_months", horizonMonths)
    .eq("property_segment", segment)
    .eq("publishable_flag", true)
    .order("forecast_date", { ascending: false })
    .limit(1);

  if (latestError) {
    return NextResponse.json(
      {
        metric,
        horizonMonths,
        segment,
        asOf: new Date().toISOString(),
        error: latestError.message,
        features: [],
      },
      { status: 500 }
    );
  }

  const latestDate = latest?.[0]?.forecast_date ?? null;
  if (!latestDate) {
    return NextResponse.json({
      metric,
      horizonMonths,
      segment,
      asOf: new Date().toISOString(),
      features: [],
      note: "No forecasts found yet. Load municipalities + insert at least one forecast snapshot.",
    });
  }

  const { data: rows, error: rowsError } = await supabase
    .schema("model")
    .from("forecasts_municipality")
    .select(
      "municipality_id,value_mid_eur_sqm,forecast_appreciation_pct,forecast_gross_yield_pct,opportunity_score,confidence_score"
    )
    .eq("forecast_date", latestDate)
    .eq("horizon_months", horizonMonths)
    .eq("property_segment", segment)
    .eq("publishable_flag", true);

  if (rowsError) {
    return NextResponse.json(
      {
        metric,
        horizonMonths,
        segment,
        asOf: new Date().toISOString(),
        error: rowsError.message,
        features: [],
      },
      { status: 500 }
    );
  }

  const valueKey = (() => {
    switch (metric) {
      case "forecast_appreciation_pct":
        return "forecast_appreciation_pct" as const;
      case "forecast_gross_yield_pct":
        return "forecast_gross_yield_pct" as const;
      case "opportunity_score":
        return "opportunity_score" as const;
      case "confidence_score":
        return "confidence_score" as const;
      default:
        return "value_mid_eur_sqm" as const;
    }
  })();

  // Cache for 5 minutes - metric values update with new data ingestion
  return NextResponse.json({
    metric,
    horizonMonths,
    segment,
    asOf: latestDate,
    features: (rows ?? []).map((r) => ({
      municipalityId: r.municipality_id,
      value: r[valueKey],
    })),
  }, {
    headers: {
      "Cache-Control": "public, max-age=3600, stale-while-revalidate=86400",
    },
  });
}
