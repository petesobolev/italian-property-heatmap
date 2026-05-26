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
        "Cache-Control": "public, max-age=300, stale-while-revalidate=3600",
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
        "Cache-Control": "public, max-age=300, stale-while-revalidate=3600",
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
        "Cache-Control": "public, max-age=300, stale-while-revalidate=3600",
      },
    });
  }

  // Handle actual observed property values from mart table
  if (metric === "value_mid_eur_sqm") {
    // Get distinct periods with data using RPC (avoids row limit issues)
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

    // Get all available periods and take the N most recent
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

    // Get municipality values for all selected periods
    // Note: Supabase defaults to 1000 rows max, so we fetch in batches
    const allMartRows: { municipality_id: string; value_mid_eur_sqm: number; period_id: string }[] = [];
    const batchSize = 1000;
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const { data: batch, error: batchError } = await supabase
        .schema("mart")
        .from("municipality_values_semester")
        .select("municipality_id, value_mid_eur_sqm, period_id")
        .in("period_id", periodsToUse)
        .eq("property_segment", segment)
        .not("value_mid_eur_sqm", "is", null)
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
        allMartRows.push(...batch);
        offset += batchSize;
        hasMore = batch.length === batchSize;
      } else {
        hasMore = false;
      }
    }

    // If only 1 semester, return as before
    if (semestersToAverage === 1) {
      return NextResponse.json({
        metric,
        horizonMonths,
        segment,
        semestersToAverage,
        availablePeriodsCount,
        asOf: periodsToUse[0],
        periodsIncluded: periodsToUse,
        source: "mart.municipality_values_semester",
        features: allMartRows.map((r) => ({
          municipalityId: r.municipality_id,
          value: r.value_mid_eur_sqm,
        })),
      }, {
        headers: {
          "Cache-Control": "public, max-age=300, stale-while-revalidate=3600",
        },
      });
    }

    // Average values across semesters for each municipality
    const municipalityValues = new Map<string, { sum: number; count: number }>();
    for (const row of allMartRows) {
      const existing = municipalityValues.get(row.municipality_id);
      if (existing) {
        existing.sum += row.value_mid_eur_sqm;
        existing.count += 1;
      } else {
        municipalityValues.set(row.municipality_id, { sum: row.value_mid_eur_sqm, count: 1 });
      }
    }

    const features = Array.from(municipalityValues.entries()).map(([municipalityId, { sum, count }]) => ({
      municipalityId,
      value: sum / count,
      semestersWithData: count,
    }));

    return NextResponse.json({
      metric,
      horizonMonths,
      segment,
      semestersToAverage,
      availablePeriodsCount,
      asOf: periodsToUse[0],
      periodsIncluded: periodsToUse,
      source: "mart.municipality_values_semester (averaged)",
      features,
    }, {
      headers: {
        "Cache-Control": "public, max-age=300, stale-while-revalidate=3600",
      },
    });
  }

  // Handle rental price metric from mart table
  if (metric === "rent_mid_eur_sqm_month") {
    const { data: availablePeriods, error: periodError } = await supabase
      .schema("mart")
      .rpc("get_available_periods", { p_segment: segment });

    if (periodError) {
      return NextResponse.json(
        { metric, segment, asOf: new Date().toISOString(), error: periodError.message, features: [] },
        { status: 500 }
      );
    }

    const allAvailablePeriods = availablePeriods?.map((p: { period_id: string }) => p.period_id) ?? [];
    const availablePeriodsCount = allAvailablePeriods.length;
    const periodsToUse = allAvailablePeriods.slice(0, semestersToAverage);

    if (periodsToUse.length === 0) {
      return NextResponse.json({
        metric, segment, semestersToAverage, availablePeriodsCount: 0,
        asOf: new Date().toISOString(), features: [],
        note: "No rental data found yet.",
      });
    }

    // Fetch rental data in batches
    const allRentRows: { municipality_id: string; rent_mid_eur_sqm_month: number; period_id: string }[] = [];
    const batchSize = 1000;
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const { data: batch, error: batchError } = await supabase
        .schema("mart")
        .from("municipality_values_semester")
        .select("municipality_id, rent_mid_eur_sqm_month, period_id")
        .in("period_id", periodsToUse)
        .eq("property_segment", segment)
        .not("rent_mid_eur_sqm_month", "is", null)
        .range(offset, offset + batchSize - 1);

      if (batchError) {
        return NextResponse.json(
          { metric, segment, asOf: new Date().toISOString(), error: batchError.message, features: [] },
          { status: 500 }
        );
      }

      if (batch && batch.length > 0) {
        allRentRows.push(...batch);
        offset += batchSize;
        hasMore = batch.length === batchSize;
      } else {
        hasMore = false;
      }
    }

    // Average across semesters
    const municipalityRents = new Map<string, { sum: number; count: number }>();
    for (const row of allRentRows) {
      const existing = municipalityRents.get(row.municipality_id);
      if (existing) {
        existing.sum += row.rent_mid_eur_sqm_month;
        existing.count += 1;
      } else {
        municipalityRents.set(row.municipality_id, { sum: row.rent_mid_eur_sqm_month, count: 1 });
      }
    }

    const features = Array.from(municipalityRents.entries()).map(([municipalityId, { sum, count }]) => ({
      municipalityId,
      value: sum / count,
    }));

    return NextResponse.json({
      metric, segment, semestersToAverage, availablePeriodsCount,
      asOf: periodsToUse[0], periodsIncluded: periodsToUse,
      source: "mart.municipality_values_semester",
      features,
    }, {
      headers: { "Cache-Control": "public, max-age=300, stale-while-revalidate=3600" },
    });
  }

  // Handle gross yield metric (calculated from rent and value)
  if (metric === "gross_yield_pct") {
    const { data: availablePeriods, error: periodError } = await supabase
      .schema("mart")
      .rpc("get_available_periods", { p_segment: segment });

    if (periodError) {
      return NextResponse.json(
        { metric, segment, asOf: new Date().toISOString(), error: periodError.message, features: [] },
        { status: 500 }
      );
    }

    const allAvailablePeriods = availablePeriods?.map((p: { period_id: string }) => p.period_id) ?? [];
    const availablePeriodsCount = allAvailablePeriods.length;
    const periodsToUse = allAvailablePeriods.slice(0, semestersToAverage);

    if (periodsToUse.length === 0) {
      return NextResponse.json({
        metric, segment, semestersToAverage, availablePeriodsCount: 0,
        asOf: new Date().toISOString(), features: [],
        note: "No data found yet.",
      });
    }

    // Fetch both value and rent data in batches
    const allRows: { municipality_id: string; value_mid_eur_sqm: number; rent_mid_eur_sqm_month: number; period_id: string }[] = [];
    const batchSize = 1000;
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const { data: batch, error: batchError } = await supabase
        .schema("mart")
        .from("municipality_values_semester")
        .select("municipality_id, value_mid_eur_sqm, rent_mid_eur_sqm_month, period_id")
        .in("period_id", periodsToUse)
        .eq("property_segment", segment)
        .not("value_mid_eur_sqm", "is", null)
        .not("rent_mid_eur_sqm_month", "is", null)
        .range(offset, offset + batchSize - 1);

      if (batchError) {
        return NextResponse.json(
          { metric, segment, asOf: new Date().toISOString(), error: batchError.message, features: [] },
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

    // Average values and rents, then calculate yield
    const municipalityData = new Map<string, { valueSum: number; rentSum: number; count: number }>();
    for (const row of allRows) {
      const existing = municipalityData.get(row.municipality_id);
      if (existing) {
        existing.valueSum += row.value_mid_eur_sqm;
        existing.rentSum += row.rent_mid_eur_sqm_month;
        existing.count += 1;
      } else {
        municipalityData.set(row.municipality_id, {
          valueSum: row.value_mid_eur_sqm,
          rentSum: row.rent_mid_eur_sqm_month,
          count: 1,
        });
      }
    }

    const features = Array.from(municipalityData.entries()).map(([municipalityId, { valueSum, rentSum, count }]) => {
      const avgValue = valueSum / count;
      const avgRent = rentSum / count;
      // Gross yield = (annual rent / price) * 100
      const grossYield = avgValue > 0 ? (avgRent * 12 / avgValue) * 100 : null;
      return { municipalityId, value: grossYield };
    }).filter(f => f.value !== null);

    return NextResponse.json({
      metric, segment, semestersToAverage, availablePeriodsCount,
      asOf: periodsToUse[0], periodsIncluded: periodsToUse,
      source: "mart.municipality_values_semester (calculated)",
      features,
    }, {
      headers: { "Cache-Control": "public, max-age=300, stale-while-revalidate=3600" },
    });
  }

  // Handle condition premium metric (OTTIMO vs NORMALE price difference)
  // Pre-computed in mart.municipality_values_semester during ingestion
  if (metric === "condition_premium_pct") {
    const { data: availablePeriods, error: periodError } = await supabase
      .schema("mart")
      .rpc("get_available_periods", { p_segment: segment });

    if (periodError) {
      return NextResponse.json(
        { metric, segment, asOf: new Date().toISOString(), error: periodError.message, features: [] },
        { status: 500 }
      );
    }

    const allAvailablePeriods = availablePeriods?.map((p: { period_id: string }) => p.period_id) ?? [];
    const availablePeriodsCount = allAvailablePeriods.length;
    const periodsToUse = allAvailablePeriods.slice(0, semestersToAverage);

    if (periodsToUse.length === 0) {
      return NextResponse.json({
        metric, segment, semestersToAverage, availablePeriodsCount: 0,
        asOf: new Date().toISOString(), features: [],
        note: "No data found yet.",
      });
    }

    // Fetch condition premium from pre-computed mart table in batches
    const allRows: { municipality_id: string; condition_premium_pct: number; period_id: string }[] = [];
    const batchSize = 1000;
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const { data: batch, error: batchError } = await supabase
        .schema("mart")
        .from("municipality_values_semester")
        .select("municipality_id, condition_premium_pct, period_id")
        .in("period_id", periodsToUse)
        .eq("property_segment", segment)
        .not("condition_premium_pct", "is", null)
        .range(offset, offset + batchSize - 1);

      if (batchError) {
        return NextResponse.json(
          { metric, segment, asOf: new Date().toISOString(), error: batchError.message, features: [] },
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

    // Average condition premium across semesters
    const municipalityData = new Map<string, { sum: number; count: number }>();
    for (const row of allRows) {
      const existing = municipalityData.get(row.municipality_id);
      if (existing) {
        existing.sum += row.condition_premium_pct;
        existing.count += 1;
      } else {
        municipalityData.set(row.municipality_id, { sum: row.condition_premium_pct, count: 1 });
      }
    }

    const features = Array.from(municipalityData.entries()).map(([municipalityId, { sum, count }]) => ({
      municipalityId,
      value: sum / count,
    }));

    return NextResponse.json({
      metric, segment, semestersToAverage, availablePeriodsCount,
      asOf: periodsToUse[0],
      periodsIncluded: periodsToUse,
      source: "mart.municipality_values_semester (pre-computed)",
      features,
    }, {
      headers: { "Cache-Control": "public, max-age=300, stale-while-revalidate=3600" },
    });
  }

  // Handle price variance metric (coefficient of variation)
  if (metric === "price_variance_pct") {
    const { data: availablePeriods, error: periodError } = await supabase
      .schema("mart")
      .rpc("get_available_periods", { p_segment: segment });

    if (periodError) {
      return NextResponse.json(
        { metric, segment, asOf: new Date().toISOString(), error: periodError.message, features: [] },
        { status: 500 }
      );
    }

    const allAvailablePeriods = availablePeriods?.map((p: { period_id: string }) => p.period_id) ?? [];
    const availablePeriodsCount = allAvailablePeriods.length;
    const periodsToUse = allAvailablePeriods.slice(0, semestersToAverage);

    if (periodsToUse.length === 0) {
      return NextResponse.json({
        metric, segment, semestersToAverage, availablePeriodsCount: 0,
        asOf: new Date().toISOString(), features: [],
        note: "No data found yet.",
      });
    }

    // Fetch min, max, mid values in batches
    const allRows: { municipality_id: string; value_min_eur_sqm: number; value_max_eur_sqm: number; value_mid_eur_sqm: number; period_id: string }[] = [];
    const batchSize = 1000;
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const { data: batch, error: batchError } = await supabase
        .schema("mart")
        .from("municipality_values_semester")
        .select("municipality_id, value_min_eur_sqm, value_max_eur_sqm, value_mid_eur_sqm, period_id")
        .in("period_id", periodsToUse)
        .eq("property_segment", segment)
        .not("value_mid_eur_sqm", "is", null)
        .not("value_min_eur_sqm", "is", null)
        .not("value_max_eur_sqm", "is", null)
        .range(offset, offset + batchSize - 1);

      if (batchError) {
        return NextResponse.json(
          { metric, segment, asOf: new Date().toISOString(), error: batchError.message, features: [] },
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

    // Average the min/max/mid across semesters, then calculate variance
    const municipalityData = new Map<string, { minSum: number; maxSum: number; midSum: number; count: number }>();
    for (const row of allRows) {
      const existing = municipalityData.get(row.municipality_id);
      if (existing) {
        existing.minSum += row.value_min_eur_sqm;
        existing.maxSum += row.value_max_eur_sqm;
        existing.midSum += row.value_mid_eur_sqm;
        existing.count += 1;
      } else {
        municipalityData.set(row.municipality_id, {
          minSum: row.value_min_eur_sqm,
          maxSum: row.value_max_eur_sqm,
          midSum: row.value_mid_eur_sqm,
          count: 1,
        });
      }
    }

    const features = Array.from(municipalityData.entries()).map(([municipalityId, { minSum, maxSum, midSum, count }]) => {
      const avgMin = minSum / count;
      const avgMax = maxSum / count;
      const avgMid = midSum / count;
      // Price variance = (max - min) / mid * 100
      const variance = avgMid > 0 ? ((avgMax - avgMin) / avgMid) * 100 : null;
      return { municipalityId, value: variance };
    }).filter(f => f.value !== null);

    return NextResponse.json({
      metric, segment, semestersToAverage, availablePeriodsCount,
      asOf: periodsToUse[0], periodsIncluded: periodsToUse,
      source: "mart.municipality_values_semester (calculated)",
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
      "Cache-Control": "public, max-age=300, stale-while-revalidate=3600",
    },
  });
}
