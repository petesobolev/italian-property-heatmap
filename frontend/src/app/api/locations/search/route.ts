import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

interface SearchResult {
  id: string;
  name: string;
  type: "region" | "province" | "municipality";
  parentName?: string;
  bounds: [[number, number], [number, number]]; // [[south, west], [north, east]]
}

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const query = searchParams.get("q")?.trim().toLowerCase();

  if (!query || query.length < 2) {
    return NextResponse.json({ results: [] });
  }

  const supabase = createSupabaseServerClient();
  const results: SearchResult[] = [];

  // Search regions
  const { data: regions } = await supabase
    .schema("core")
    .from("regions")
    .select("region_code, region_name")
    .ilike("region_name", `%${query}%`)
    .limit(5);

  if (regions) {
    // Get bounds for matching regions using RPC
    for (const region of regions) {
      const { data: bounds } = await supabase.rpc("get_region_bounds", {
        p_region_code: region.region_code,
      });

      if (bounds && bounds.length > 0) {
        const b = bounds[0];
        results.push({
          id: `region-${region.region_code}`,
          name: region.region_name,
          type: "region",
          bounds: [[b.south, b.west], [b.north, b.east]],
        });
      }
    }
  }

  // Search provinces - prioritize exact > starts-with > contains
  const { data: exactProvinces } = await supabase
    .schema("core")
    .from("provinces")
    .select("province_code, province_name, province_abbreviation, regions!inner(region_name)")
    .or(`province_name.ilike.${query},province_abbreviation.ilike.${query}`)
    .limit(2);

  const { data: startsWithProvinces } = await supabase
    .schema("core")
    .from("provinces")
    .select("province_code, province_name, province_abbreviation, regions!inner(region_name)")
    .or(`province_name.ilike.${query}%,province_abbreviation.ilike.${query}%`)
    .limit(5);

  const { data: partialProvinces } = await supabase
    .schema("core")
    .from("provinces")
    .select("province_code, province_name, province_abbreviation, regions!inner(region_name)")
    .or(`province_name.ilike.%${query}%,province_abbreviation.ilike.%${query}%`)
    .limit(10);

  const seenProvinceCodes = new Set<string>();
  const provinces = [
    ...(exactProvinces || []),
    ...(startsWithProvinces || []),
    ...(partialProvinces || []),
  ].filter((p) => {
    if (seenProvinceCodes.has(p.province_code)) return false;
    seenProvinceCodes.add(p.province_code);
    return true;
  }).slice(0, 10);

  if (provinces) {
    for (const province of provinces) {
      const { data: bounds } = await supabase.rpc("get_province_bounds", {
        p_province_code: province.province_code,
      });

      if (bounds && bounds.length > 0) {
        const b = bounds[0];
        // Handle joined region data (could be object or array depending on Supabase version)
        const regionsData = province.regions as unknown;
        const regionName = Array.isArray(regionsData)
          ? (regionsData[0] as { region_name: string })?.region_name
          : (regionsData as { region_name: string })?.region_name;
        results.push({
          id: `province-${province.province_code}`,
          name: province.province_name !== "-" ? province.province_name : `Province of ${province.province_abbreviation}`,
          type: "province",
          parentName: regionName,
          bounds: [[b.south, b.west], [b.north, b.east]],
        });
      }
    }
  }

  // Search municipalities - prioritize exact matches, then starts-with, then contains
  // First, get exact matches only
  const { data: exactMunicipalities } = await supabase
    .schema("core")
    .from("municipalities")
    .select("municipality_id, municipality_name, provinces!inner(province_name, province_abbreviation)")
    .ilike("municipality_name", query)
    .limit(1);

  // Then get starts-with matches
  const { data: startsWithMunicipalities } = await supabase
    .schema("core")
    .from("municipalities")
    .select("municipality_id, municipality_name, provinces!inner(province_name, province_abbreviation)")
    .ilike("municipality_name", `${query}%`)
    .limit(10);

  // Then get partial matches
  const { data: partialMunicipalities } = await supabase
    .schema("core")
    .from("municipalities")
    .select("municipality_id, municipality_name, provinces!inner(province_name, province_abbreviation)")
    .ilike("municipality_name", `%${query}%`)
    .limit(15);

  // Combine, prioritizing exact > starts-with > contains, and deduplicating
  const seenIds = new Set<string>();
  const municipalities = [
    ...(exactMunicipalities || []),
    ...(startsWithMunicipalities || []),
    ...(partialMunicipalities || []),
  ].filter((m) => {
    if (seenIds.has(m.municipality_id)) return false;
    seenIds.add(m.municipality_id);
    return true;
  }).slice(0, 15);

  if (municipalities) {
    for (const muni of municipalities) {
      const { data: bounds } = await supabase.rpc("get_municipality_bounds", {
        p_municipality_id: muni.municipality_id,
      });

      if (bounds && bounds.length > 0) {
        const b = bounds[0];
        // Handle joined province data (could be object or array depending on Supabase version)
        const provincesData = muni.provinces as unknown;
        const province = Array.isArray(provincesData)
          ? (provincesData[0] as { province_name: string; province_abbreviation: string })
          : (provincesData as { province_name: string; province_abbreviation: string });
        const provinceName = province?.province_name !== "-"
          ? province?.province_name
          : province?.province_abbreviation;
        results.push({
          id: `municipality-${muni.municipality_id}`,
          name: muni.municipality_name,
          type: "municipality",
          parentName: provinceName,
          bounds: [[b.south, b.west], [b.north, b.east]],
        });
      }
    }
  }

  // Sort results: exact matches first, then by type (region > province > municipality)
  results.sort((a, b) => {
    const aExact = a.name.toLowerCase() === query;
    const bExact = b.name.toLowerCase() === query;
    if (aExact && !bExact) return -1;
    if (bExact && !aExact) return 1;

    const typeOrder = { region: 0, province: 1, municipality: 2 };
    return typeOrder[a.type] - typeOrder[b.type];
  });

  return NextResponse.json(
    { results: results.slice(0, 20) },
    {
      headers: {
        // Cache for 5 minutes, revalidate up to 1 hour
        "Cache-Control": "public, max-age=300, stale-while-revalidate=3600",
      },
    }
  );
}
