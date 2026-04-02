#!/usr/bin/env python3
"""
Generate demo OMI zone data for existing municipalities.

This creates realistic zone data for municipalities in the demo dataset,
including zone geometries (simplified), values, and forecasts.
"""

import os
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

# Load environment variables from .env.local
def load_env():
    """Load environment variables from frontend/.env.local"""
    env_path = Path(__file__).parent.parent.parent.parent / "frontend" / ".env.local"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ[key] = value

load_env()

from supabase import create_client, Client

# Zone types: B=Central, C=Semi-central, D=Peripheral, E=Suburban, R=Rural
ZONE_TYPES = ["B", "C", "D", "E", "R"]

# Zone type characteristics (value multiplier, transaction density)
ZONE_CHARACTERISTICS = {
    "B": {"value_mult": 1.4, "name_prefix": "Centro", "transactions": 1.2},
    "C": {"value_mult": 1.15, "name_prefix": "Semicentro", "transactions": 1.1},
    "D": {"value_mult": 0.85, "name_prefix": "Periferia", "transactions": 0.9},
    "E": {"value_mult": 0.7, "name_prefix": "Suburbano", "transactions": 0.7},
    "R": {"value_mult": 0.5, "name_prefix": "Rurale", "transactions": 0.4},
}

# Sample zone names for Italian cities
ZONE_NAMES = {
    "B": ["Centro Storico", "Centro Direzionale", "Zona Monumentale", "Quartiere Storico"],
    "C": ["Zona Residenziale Nord", "Zona Residenziale Sud", "Quartiere Commerciale", "Zona Mista"],
    "D": ["Zona Industriale", "Periferia Nord", "Periferia Est", "Periferia Ovest", "Zona Artigianale"],
    "E": ["Zona Agricola", "Area Suburbana", "Fascia Esterna", "Zona di Espansione"],
    "R": ["Campagna", "Area Rurale", "Zona Agricola Esterna"],
}


def get_supabase_client() -> Client:
    """Create Supabase client from environment variables."""
    url = os.environ.get("SUPABASE_URL") or os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    # Prefer service role key for schema access
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not key:
        key = os.environ.get("SUPABASE_ANON_KEY")
        print("WARNING: Using anon key - may not have schema permissions")

    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables")

    return create_client(url, key)


def generate_zone_geometry(base_lat: float, base_lon: float, zone_idx: int, zone_type: str) -> dict:
    """Generate a simplified polygon geometry for a zone."""
    # Create a rough polygon offset from the center based on zone type and index
    angle_offset = (zone_idx * 45) % 360
    distance_mult = {"B": 0.01, "C": 0.025, "D": 0.04, "E": 0.06, "R": 0.08}[zone_type]

    import math
    center_lat = base_lat + distance_mult * math.cos(math.radians(angle_offset))
    center_lon = base_lon + distance_mult * math.sin(math.radians(angle_offset))

    # Create a rough hexagonal polygon
    size = 0.008 + random.random() * 0.005
    points = []
    for i in range(6):
        angle = math.radians(i * 60 + random.random() * 10)
        lat = center_lat + size * math.cos(angle) * (0.9 + random.random() * 0.2)
        lon = center_lon + size * math.sin(angle) * (0.9 + random.random() * 0.2)
        points.append([lon, lat])
    points.append(points[0])  # Close the polygon

    return {
        "type": "Polygon",
        "coordinates": [points]
    }


def generate_zones_for_municipality(
    supabase: Client,
    municipality_id: str,
    municipality_name: str,
    base_value: float,
    lat: float,
    lon: float
) -> list:
    """Generate OMI zones for a single municipality."""
    zones = []

    # Determine number of zones based on municipality size (larger cities have more zones)
    if "Milano" in municipality_name or "Roma" in municipality_name:
        num_zones = random.randint(12, 18)
    elif "Torino" in municipality_name or "Napoli" in municipality_name:
        num_zones = random.randint(8, 12)
    else:
        num_zones = random.randint(3, 8)

    # Generate zones with appropriate distribution
    zone_idx = 0
    for zone_type in ZONE_TYPES:
        # Number of zones of this type
        if zone_type == "B":
            count = 1 if num_zones < 5 else random.randint(1, 3)
        elif zone_type == "C":
            count = random.randint(1, max(1, num_zones // 4))
        elif zone_type == "D":
            count = random.randint(1, max(1, num_zones // 3))
        elif zone_type == "E":
            count = random.randint(0, max(1, num_zones // 4))
        else:  # R
            count = 1 if num_zones > 5 else 0

        for i in range(count):
            if zone_idx >= num_zones:
                break

            zone_idx += 1
            chars = ZONE_CHARACTERISTICS[zone_type]

            # Generate zone code (e.g., "B1", "C2", "D1")
            zone_code = f"{zone_type}{i + 1}"

            # Pick a zone name
            zone_name = random.choice(ZONE_NAMES[zone_type])
            if i > 0:
                zone_name = f"{zone_name} {i + 1}"

            # Generate geometry
            geom = generate_zone_geometry(lat, lon, zone_idx, zone_type)

            # Calculate zone value with some randomness
            value_mult = chars["value_mult"] * (0.85 + random.random() * 0.3)
            zone_value = base_value * value_mult

            zone = {
                "zone_id": f"{municipality_id}_{zone_code}",
                "municipality_id": municipality_id,
                "zone_code": zone_code,
                "zone_name": zone_name,
                "zone_type": zone_type,
                "microzone_code": f"{zone_code}.{random.randint(1, 3)}",
                "geom_simplified": json.dumps(geom),
                "base_value": zone_value,
                "transaction_mult": chars["transactions"],
            }
            zones.append(zone)

    return zones


def insert_zone_data(supabase: Client, zones: list, period_id: str = "2024H2"):
    """Insert zone records and their values into the database."""

    # Prepare zone records for core.omi_zones
    zone_records = []
    for z in zones:
        zone_records.append({
            "zone_id": z["zone_id"],
            "municipality_id": z["municipality_id"],
            "zone_code": z["zone_code"],
            "zone_name": z["zone_name"],
            "zone_type": z["zone_type"],
            "microzone_code": z["microzone_code"],
            # Note: geom_simplified would need ST_GeomFromGeoJSON in real implementation
        })

    # Insert zones (upsert to handle re-runs)
    print(f"Inserting {len(zone_records)} zones into core.omi_zones...")
    supabase.schema("core").table("omi_zones").upsert(
        zone_records,
        on_conflict="zone_id"
    ).execute()

    # Prepare zone values for mart.omi_zone_values_semester
    value_records = []
    for z in zones:
        base_val = z["base_value"]
        # Add some variance
        val_mid = base_val * (0.95 + random.random() * 0.1)
        val_min = val_mid * 0.7
        val_max = val_mid * 1.4
        rent = val_mid * 0.004  # ~0.4% monthly rent to value

        value_records.append({
            "zone_id": z["zone_id"],
            "period_id": period_id,
            "property_segment": "residential",
            "value_mid_eur_sqm": round(val_mid, 2),
            "value_min_eur_sqm": round(val_min, 2),
            "value_max_eur_sqm": round(val_max, 2),
            "rent_mid_eur_sqm_month": round(rent, 2),
            "value_pct_change_1s": round((random.random() - 0.3) * 8, 2),  # -2.4% to +5.6%
            "transaction_count": int(random.randint(5, 50) * z["transaction_mult"]),
        })

    print(f"Inserting {len(value_records)} zone values into mart.omi_zone_values_semester...")
    supabase.schema("mart").table("omi_zone_values_semester").upsert(
        value_records,
        on_conflict="zone_id,period_id,property_segment"
    ).execute()

    # Prepare zone forecasts for model.forecasts_omi_zone
    forecast_records = []
    for z in zones:
        # Not all zones get forecasts (simulating data availability)
        if random.random() < 0.7:
            forecast_records.append({
                "zone_id": z["zone_id"],
                "property_segment": "residential",
                "forecast_date": datetime.now().strftime("%Y-%m-%d"),
                "horizon_months": 12,
                "forecast_appreciation_pct": round((random.random() - 0.2) * 10, 2),  # -2% to +8%
                "opportunity_score": round(40 + random.random() * 50, 1),  # 40-90
                "confidence_score": round(50 + random.random() * 40, 1),  # 50-90
                "publishable_flag": True,
                "model_version": "v0.1-demo",
            })

    if forecast_records:
        print(f"Inserting {len(forecast_records)} zone forecasts into model.forecasts_omi_zone...")
        supabase.schema("model").table("forecasts_omi_zone").upsert(
            forecast_records,
            on_conflict="zone_id,property_segment,forecast_date,horizon_months"
        ).execute()


def main():
    """Main entry point."""
    print("OMI Zone Demo Data Generator")
    print("=" * 50)

    supabase = get_supabase_client()

    # Get existing municipalities from the demo data
    print("\nFetching existing municipalities...")
    result = supabase.schema("core").table("municipalities").select(
        "municipality_id, municipality_name"
    ).execute()

    municipalities = result.data
    if not municipalities:
        print("No municipalities found in database. Please run boundary ingestion first.")
        return

    print(f"Found {len(municipalities)} municipalities")

    # Get some base values from existing forecasts or use defaults
    forecast_result = supabase.schema("model").table("forecasts_municipality").select(
        "municipality_id, value_mid_eur_sqm"
    ).execute()

    value_map = {f["municipality_id"]: f["value_mid_eur_sqm"] for f in forecast_result.data if f.get("value_mid_eur_sqm")}

    # Base coordinates for known cities (approximate centers)
    city_coords = {
        "015146": (45.4642, 9.1900),   # Milano
        "058091": (41.9028, 12.4964),  # Roma
        "001272": (45.0703, 7.6869),   # Torino
        "063049": (40.8518, 14.2681),  # Napoli
    }

    all_zones = []
    for muni in municipalities:
        muni_id = muni["municipality_id"]
        muni_name = muni["municipality_name"]

        # Get base value (default to 3000 €/m² if not available)
        base_value = value_map.get(muni_id, 3000)

        # Get coordinates (use defaults if not in our list)
        lat, lon = city_coords.get(muni_id, (42.5 + random.random(), 12.5 + random.random()))

        print(f"\nGenerating zones for {muni_name} ({muni_id})...")
        zones = generate_zones_for_municipality(
            supabase, muni_id, muni_name, base_value, lat, lon
        )
        all_zones.extend(zones)
        print(f"  Created {len(zones)} zones")

    # Insert all zone data
    print(f"\n{'=' * 50}")
    print(f"Total zones generated: {len(all_zones)}")
    insert_zone_data(supabase, all_zones)

    print("\nDone! OMI zone demo data has been loaded.")


if __name__ == "__main__":
    main()
