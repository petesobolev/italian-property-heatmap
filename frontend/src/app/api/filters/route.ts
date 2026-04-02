import { NextResponse } from "next/server";

export async function GET() {
  return NextResponse.json({
    regions: [],
    provinces: [],
    strategies: [
      { id: "balanced", label: "Balanced" },
      { id: "growth", label: "Growth" },
      { id: "yield", label: "Yield" },
    ],
    propertySegments: [
      { id: "residential", label: "Residential" },
      { id: "residential_economy", label: "Residential (Economy)" },
      { id: "residential_civil", label: "Residential (Civil)" },
    ],
  });
}

