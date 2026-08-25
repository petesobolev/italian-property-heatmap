import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

/**
 * API endpoint to fetch flat tax eligibility data
 *
 * Returns municipalities eligible for the 7% flat tax regime for foreign retirees.
 * Eligibility paths:
 * - southern_italy: Southern Italy (Mezzogiorno) with population < 20,000
 * - sisma_2016: 2016 earthquake zone municipalities (Lazio, Marche, Umbria)
 * - southern_italy+sisma_2016: Qualifies via both paths (Abruzzo earthquake zone)
 */
export async function GET() {
  const supabase = createSupabaseServerClient();

  // Fetch all eligible municipalities from the database
  const allRows: { municipality_id: string; eligibility_reason: string }[] = [];
  const batchSize = 1000;
  let offset = 0;
  let hasMore = true;

  while (hasMore) {
    const { data: batch, error: batchError } = await supabase
      .schema("mart")
      .from("flat_tax_eligibility")
      .select("municipality_id, eligibility_reason")
      .eq("is_eligible", true)
      .range(offset, offset + batchSize - 1);

    if (batchError) {
      return NextResponse.json(
        { error: batchError.message, municipalities: [] },
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

  // Count by eligibility reason for debugging
  const countByReason: Record<string, number> = {};
  for (const row of allRows) {
    countByReason[row.eligibility_reason] = (countByReason[row.eligibility_reason] || 0) + 1;
  }

  return NextResponse.json(
    {
      municipalities: allRows,
      total: allRows.length,
      byReason: countByReason,
    },
    {
      headers: {
        // Cache for 1 hour since this data rarely changes
        "Cache-Control": "public, max-age=3600, stale-while-revalidate=86400",
      },
    }
  );
}
