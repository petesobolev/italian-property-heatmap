import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

export async function GET() {
  const supabase = createSupabaseServerClient();

  // We don't assume any tables exist yet. This call will typically return an auth error,
  // but it still confirms the server can reach Supabase with the configured URL/key.
  const { error } = await supabase.auth.getUser();

  return NextResponse.json({
    ok: true,
    note: error ? "Reached Supabase (no user session)" : "Reached Supabase",
    error: error?.message ?? null,
  });
}

