import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const metric = searchParams.get("metric") ?? "value_mid_eur_sqm";
  const horizonMonths = Number(searchParams.get("horizonMonths") ?? "12");
  const segment = searchParams.get("segment") ?? "residential";

  const supabase = createSupabaseServerClient();

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
    });
  }

  // Handle actual observed property values from mart table
  if (metric === "value_mid_eur_sqm") {
    // Get latest period with data from mart.municipality_values_semester
    const { data: latestPeriod, error: periodError } = await supabase
      .schema("mart")
      .from("municipality_values_semester")
      .select("period_id")
      .eq("property_segment", segment)
      .not("value_mid_eur_sqm", "is", null)
      .order("period_id", { ascending: false })
      .limit(1);

    if (periodError) {
      return NextResponse.json(
        {
          metric,
          horizonMonths,
          segment,
          asOf: new Date().toISOString(),
          error: periodError.message,
          features: [],
        },
        { status: 500 }
      );
    }

    const latestPeriodId = latestPeriod?.[0]?.period_id ?? null;
    if (!latestPeriodId) {
      return NextResponse.json({
        metric,
        horizonMonths,
        segment,
        asOf: new Date().toISOString(),
        features: [],
        note: "No property values found yet. Run the OMI ingestion to load data.",
      });
    }

    // Get municipality values for the latest period
    // Note: Supabase defaults to 1000 rows max, so we fetch in batches
    const allMartRows: { municipality_id: string; value_mid_eur_sqm: number }[] = [];
    const batchSize = 1000;
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const { data: batch, error: batchError } = await supabase
        .schema("mart")
        .from("municipality_values_semester")
        .select("municipality_id, value_mid_eur_sqm")
        .eq("period_id", latestPeriodId)
        .eq("property_segment", segment)
        .not("value_mid_eur_sqm", "is", null)
        .range(offset, offset + batchSize - 1);

      if (batchError) {
        return NextResponse.json(
          {
            metric,
            horizonMonths,
            segment,
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

    return NextResponse.json({
      metric,
      horizonMonths,
      segment,
      asOf: latestPeriodId,
      source: "mart.municipality_values_semester",
      features: allMartRows.map((r) => ({
        municipalityId: r.municipality_id,
        value: r.value_mid_eur_sqm,
      })),
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

  return NextResponse.json({
    metric,
    horizonMonths,
    segment,
    asOf: latestDate,
    features: (rows ?? []).map((r) => ({
      municipalityId: r.municipality_id,
      value: r[valueKey],
    })),
  });
}
