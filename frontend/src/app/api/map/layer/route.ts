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

  // Standard metrics from forecasts table
  // Latest published snapshot approach (MVP): pick latest forecast_date for requested horizon+segment.
  // Later we can switch this to a dedicated publish view with approvals.
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
      case "value_mid_eur_sqm":
        return "value_mid_eur_sqm" as const;
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
