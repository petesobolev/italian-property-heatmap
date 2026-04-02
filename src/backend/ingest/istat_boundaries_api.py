#!/usr/bin/env python3
"""
ISTAT Administrative Boundaries Ingestion Script (Supabase API version)

Uses Supabase REST API instead of direct PostgreSQL connection.
This avoids needing the database password.

Usage:
    python istat_boundaries_api.py --year 2024

Environment variables (from frontend/.env.local):
    SUPABASE_URL - Supabase project URL
    SUPABASE_SERVICE_ROLE_KEY - Service role key for admin access
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Optional, List, Dict, Any

import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ISTAT data URLs
ISTAT_BOUNDARIES_URLS = {
    2024: "https://www.istat.it/storage/cartografia/confini_amministrativi/generalizzati/2024/Limiti01012024_g.zip",
    2023: "https://www.istat.it/storage/cartografia/confini_amministrativi/generalizzati/2023/Limiti01012023_g.zip",
}


class SupabaseClient:
    """Simple Supabase REST API client."""

    def __init__(self, url: str, service_key: str):
        self.url = url.rstrip("/")
        self.headers = {
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }

    def upsert(self, table: str, data: List[Dict], schema: str = "core") -> bool:
        """Upsert data into a table."""
        endpoint = f"{self.url}/rest/v1/{table}"
        headers = {**self.headers, "Prefer": "resolution=merge-duplicates"}

        # Add schema header if not public
        if schema != "public":
            headers["Accept-Profile"] = schema
            headers["Content-Profile"] = schema

        response = requests.post(endpoint, json=data, headers=headers)

        if response.status_code in (200, 201, 204):
            return True
        else:
            logger.error(f"Upsert failed: {response.status_code} - {response.text}")
            return False

    def rpc(self, function: str, params: Dict = None) -> Any:
        """Call a database function via RPC."""
        endpoint = f"{self.url}/rest/v1/rpc/{function}"
        response = requests.post(endpoint, json=params or {}, headers=self.headers)

        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"RPC failed: {response.status_code} - {response.text}")
            return None


def load_env_from_file(env_path: Path) -> Dict[str, str]:
    """Load environment variables from a .env file."""
    env_vars = {}
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    env_vars[key.strip()] = value.strip().strip('"').strip("'")
    return env_vars


def download_istat_boundaries(year: int, output_dir: Path) -> Path:
    """Download ISTAT boundaries zip file."""
    url = ISTAT_BOUNDARIES_URLS.get(year)
    if not url:
        raise ValueError(f"No URL configured for year {year}")

    output_path = output_dir / f"istat_boundaries_{year}.zip"

    if output_path.exists():
        logger.info(f"Using cached file: {output_path}")
        return output_path

    logger.info(f"Downloading ISTAT boundaries from {url}")
    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info(f"Downloaded to {output_path}")
    return output_path


def extract_and_load_geojson(zip_path: Path) -> Dict:
    """Extract zip and load GeoJSON or convert shapefile to GeoJSON."""
    import geopandas as gpd

    extract_dir = zip_path.parent / "extracted"
    extract_dir.mkdir(exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    # Find shapefile for Comuni
    for shp_path in extract_dir.rglob("*Com*.shp"):
        logger.info(f"Found shapefile: {shp_path}")
        gdf = gpd.read_file(shp_path)

        # Reproject to WGS84 if needed
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            logger.info(f"Reprojecting from {gdf.crs} to EPSG:4326")
            gdf = gdf.to_crs(epsg=4326)

        return gdf

    raise FileNotFoundError("No Comuni shapefile found")


def process_regions(gdf, client: SupabaseClient) -> int:
    """Extract and upload regions."""
    # Region names lookup (ISTAT 2024 doesn't include names in shapefile)
    REGION_NAMES = {
        "01": "Piemonte", "02": "Valle d'Aosta", "03": "Lombardia",
        "04": "Trentino-Alto Adige", "05": "Veneto", "06": "Friuli-Venezia Giulia",
        "07": "Liguria", "08": "Emilia-Romagna", "09": "Toscana",
        "10": "Umbria", "11": "Marche", "12": "Lazio",
        "13": "Abruzzo", "14": "Molise", "15": "Campania",
        "16": "Puglia", "17": "Basilicata", "18": "Calabria",
        "19": "Sicilia", "20": "Sardegna",
    }

    # Get unique regions
    region_codes = gdf["COD_REG"].unique()

    regions_data = []
    for code in region_codes:
        region_code = str(code).zfill(2)
        regions_data.append({
            "region_code": region_code,
            "region_name": REGION_NAMES.get(region_code, f"Region {region_code}"),
        })

    logger.info(f"Uploading {len(regions_data)} regions...")
    if client.upsert("regions", regions_data, schema="core"):
        logger.info(f"Uploaded {len(regions_data)} regions")
        return len(regions_data)
    return 0


def process_provinces(gdf, client: SupabaseClient) -> int:
    """Extract and upload provinces."""
    # Get unique provinces (using COD_PROV and COD_REG only, since names aren't in 2024 shapefile)
    provinces_df = gdf[["COD_PROV", "COD_REG"]].drop_duplicates(subset=["COD_PROV"])

    # Province names lookup (subset - will be incomplete)
    PROVINCE_NAMES = {
        "001": "Torino", "002": "Vercelli", "003": "Novara", "004": "Cuneo",
        "005": "Asti", "006": "Alessandria", "007": "Aosta", "008": "Imperia",
        "009": "Savona", "010": "Genova", "011": "La Spezia", "012": "Varese",
        "013": "Como", "014": "Sondrio", "015": "Milano", "016": "Bergamo",
        "017": "Brescia", "018": "Pavia", "019": "Cremona", "020": "Mantova",
        "021": "Bolzano", "022": "Trento", "023": "Verona", "024": "Vicenza",
        "025": "Belluno", "026": "Treviso", "027": "Venezia", "028": "Padova",
        "029": "Rovigo", "030": "Udine", "031": "Gorizia", "032": "Trieste",
        "033": "Piacenza", "034": "Parma", "035": "Reggio Emilia", "036": "Modena",
        "037": "Bologna", "038": "Ferrara", "039": "Ravenna", "040": "Forlì-Cesena",
        "041": "Pesaro-Urbino", "042": "Ancona", "043": "Macerata", "044": "Ascoli Piceno",
        "045": "Massa-Carrara", "046": "Lucca", "047": "Pistoia", "048": "Firenze",
        "049": "Livorno", "050": "Pisa", "051": "Arezzo", "052": "Siena",
        "053": "Grosseto", "054": "Perugia", "055": "Terni", "056": "Viterbo",
        "057": "Rieti", "058": "Roma", "059": "Latina", "060": "Frosinone",
        "061": "Caserta", "062": "Benevento", "063": "Napoli", "064": "Avellino",
        "065": "Salerno", "066": "L'Aquila", "067": "Teramo", "068": "Pescara",
        "069": "Chieti", "070": "Campobasso", "071": "Foggia", "072": "Bari",
        "073": "Taranto", "074": "Brindisi", "075": "Lecce", "076": "Potenza",
        "077": "Matera", "078": "Cosenza", "079": "Catanzaro", "080": "Reggio Calabria",
        "081": "Trapani", "082": "Palermo", "083": "Messina", "084": "Agrigento",
        "085": "Caltanissetta", "086": "Enna", "087": "Catania", "088": "Ragusa",
        "089": "Siracusa", "090": "Sassari", "091": "Nuoro", "092": "Cagliari",
        "093": "Pordenone", "094": "Isernia", "095": "Oristano", "096": "Biella",
        "097": "Lecco", "098": "Lodi", "099": "Rimini", "100": "Prato",
        "101": "Crotone", "102": "Vibo Valentia", "103": "Verbano-Cusio-Ossola",
        "108": "Barletta-Andria-Trani", "109": "Fermo", "110": "Monza e Brianza",
        "111": "Sud Sardegna",
    }

    provinces_data = []
    for _, row in provinces_df.iterrows():
        prov_code = str(row["COD_PROV"]).zfill(3)
        provinces_data.append({
            "province_code": prov_code,
            "province_name": PROVINCE_NAMES.get(prov_code, f"Province {prov_code}"),
            "region_code": str(row["COD_REG"]).zfill(2),
        })

    logger.info(f"Uploading {len(provinces_data)} provinces...")

    # Upload in batches
    batch_size = 50
    total = 0
    for i in range(0, len(provinces_data), batch_size):
        batch = provinces_data[i:i + batch_size]
        if client.upsert("provinces", batch, schema="core"):
            total += len(batch)
            logger.info(f"Uploaded {total}/{len(provinces_data)} provinces...")

    return total


def process_municipalities(gdf, client: SupabaseClient) -> int:
    """Extract and upload municipalities."""
    from shapely.validation import make_valid

    municipalities_data = []

    for idx, row in gdf.iterrows():
        municipality_id = str(row.get("PRO_COM_T", row.get("PRO_COM", ""))).zfill(6)
        municipality_name = row.get("COMUNE", row.get("DEN_COM", ""))
        province_code = str(row.get("COD_PROV", "")).zfill(3)
        region_code = str(row.get("COD_REG", "")).zfill(2)

        if not municipality_id or municipality_id == "000000":
            continue

        municipalities_data.append({
            "municipality_id": municipality_id,
            "municipality_name": municipality_name,
            "province_code": province_code,
            "region_code": region_code,
            # Geometry will be handled separately via SQL migration
        })

    logger.info(f"Uploading {len(municipalities_data)} municipalities...")

    # Upload in batches
    batch_size = 100
    total = 0
    for i in range(0, len(municipalities_data), batch_size):
        batch = municipalities_data[i:i + batch_size]
        if client.upsert("municipalities", batch, schema="core"):
            total += len(batch)
            if total % 500 == 0:
                logger.info(f"Uploaded {total}/{len(municipalities_data)} municipalities...")

    logger.info(f"Uploaded {total} municipalities total")
    return total


def create_geojson_file(gdf, output_path: Path) -> None:
    """Create a GeoJSON file for the frontend to use."""
    from shapely.validation import make_valid

    features = []
    for idx, row in gdf.iterrows():
        municipality_id = str(row.get("PRO_COM_T", row.get("PRO_COM", ""))).zfill(6)
        municipality_name = row.get("COMUNE", row.get("DEN_COM", ""))

        if not municipality_id or municipality_id == "000000":
            continue

        geom = row.geometry
        if geom is None or geom.is_empty:
            continue

        geom = make_valid(geom)
        # Simplify for frontend performance
        geom_simplified = geom.simplify(0.002, preserve_topology=True)

        features.append({
            "type": "Feature",
            "properties": {
                "municipality_id": municipality_id,
                "name": municipality_name,
                "province_code": str(row.get("COD_PROV", "")).zfill(3),
                "region_code": str(row.get("COD_REG", "")).zfill(2),
            },
            "geometry": geom_simplified.__geo_interface__,
        })

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    with open(output_path, "w") as f:
        json.dump(geojson, f)

    logger.info(f"Created GeoJSON with {len(features)} features at {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Import ISTAT administrative boundaries")
    parser.add_argument("--year", type=int, default=2024, help="Reference year for boundaries")
    parser.add_argument("--data-dir", type=str, default="./data", help="Directory for downloaded data")
    parser.add_argument("--geojson-only", action="store_true", help="Only create GeoJSON file, skip database upload")
    args = parser.parse_args()

    # Find project root and load env
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent.parent
    env_path = project_root / "frontend" / ".env.local"

    env_vars = load_env_from_file(env_path)

    supabase_url = env_vars.get("NEXT_PUBLIC_SUPABASE_URL") or os.getenv("SUPABASE_URL")
    service_key = env_vars.get("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not service_key:
        logger.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
        logger.error(f"Checked: {env_path}")
        sys.exit(1)

    logger.info(f"Using Supabase URL: {supabase_url}")

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Download and extract
        zip_path = download_istat_boundaries(args.year, data_dir)
        gdf = extract_and_load_geojson(zip_path)

        logger.info(f"Loaded {len(gdf)} features")
        logger.info(f"Columns: {list(gdf.columns)}")

        # Create GeoJSON for frontend
        geojson_path = project_root / "frontend" / "public" / "demo" / "municipalities.geojson"
        create_geojson_file(gdf, geojson_path)

        if args.geojson_only:
            logger.info("GeoJSON created. Skipping database upload.")
            return

        # Upload to Supabase
        client = SupabaseClient(supabase_url, service_key)

        regions_count = process_regions(gdf, client)
        provinces_count = process_provinces(gdf, client)
        municipalities_count = process_municipalities(gdf, client)

        logger.info(f"Ingestion complete!")
        logger.info(f"  Regions: {regions_count}")
        logger.info(f"  Provinces: {provinces_count}")
        logger.info(f"  Municipalities: {municipalities_count}")

    except Exception as e:
        logger.exception(f"Ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
