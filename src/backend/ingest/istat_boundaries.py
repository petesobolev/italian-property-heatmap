#!/usr/bin/env python3
"""
ISTAT Administrative Boundaries Ingestion Script

Downloads and imports Italian municipal boundaries from ISTAT into PostGIS.
Data source: https://www.istat.it/it/archivio/222527

Usage:
    python istat_boundaries.py [--download] [--year YEAR]

Environment variables:
    DATABASE_URL - Full PostgreSQL connection string, or use:
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import argparse
import logging
import os
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Optional

import geopandas as gpd
import requests
from shapely import wkb
from shapely.validation import make_valid

from db import (
    create_ingestion_run,
    complete_ingestion_run,
    get_db_cursor,
    get_db_connection,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ISTAT data URLs - Generalized boundaries (less detailed, faster)
# Using WGS84 projection version
ISTAT_BOUNDARIES_URLS = {
    2024: "https://www.istat.it/storage/cartografia/confini_amministrativi/generalizzati/2024/Limiti01012024_g.zip",
    2023: "https://www.istat.it/storage/cartografia/confini_amministrativi/generalizzati/2023/Limiti01012023_g.zip",
    2022: "https://www.istat.it/storage/cartografia/confini_amministrativi/generalizzati/2022/Limiti01012022_g.zip",
}

# Simplified tolerance in degrees (approx 100m at Italian latitudes)
SIMPLIFY_TOLERANCE = 0.001


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


def extract_shapefile(zip_path: Path, layer_pattern: str) -> Path:
    """Extract and find shapefile matching pattern."""
    extract_dir = zip_path.parent / "extracted"
    extract_dir.mkdir(exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    # Find shapefile matching pattern
    for shp_path in extract_dir.rglob(f"*{layer_pattern}*.shp"):
        logger.info(f"Found shapefile: {shp_path}")
        return shp_path

    raise FileNotFoundError(f"No shapefile matching '{layer_pattern}' found")


def load_regions(gdf: gpd.GeoDataFrame) -> int:
    """Load unique regions from the municipalities GeoDataFrame."""
    # Extract unique regions
    regions = gdf[["COD_REG", "DEN_REG"]].drop_duplicates()
    regions = regions.rename(columns={"COD_REG": "region_code", "DEN_REG": "region_name"})

    # Convert region code to string with leading zeros
    regions["region_code"] = regions["region_code"].astype(str).str.zfill(2)

    # Dissolve geometries by region to get region boundaries
    region_geoms = gdf.dissolve(by="COD_REG")
    region_geoms = region_geoms.reset_index()
    region_geoms["region_code"] = region_geoms["COD_REG"].astype(str).str.zfill(2)

    count = 0
    with get_db_cursor() as cursor:
        for _, row in regions.iterrows():
            # Get the dissolved geometry for this region
            geom_row = region_geoms[region_geoms["region_code"] == row["region_code"]]
            if len(geom_row) > 0:
                geom = geom_row.iloc[0].geometry
                geom = make_valid(geom)
                geom_wkb = geom.wkb_hex
                geom_simplified = geom.simplify(SIMPLIFY_TOLERANCE, preserve_topology=True)
                geom_simplified_wkb = geom_simplified.wkb_hex
            else:
                geom_wkb = None
                geom_simplified_wkb = None

            cursor.execute(
                """
                INSERT INTO core.regions (region_code, region_name, geom, geom_simplified)
                VALUES (%s, %s, ST_GeomFromWKB(%s::geometry, 4326), ST_GeomFromWKB(%s::geometry, 4326))
                ON CONFLICT (region_code) DO UPDATE SET
                    region_name = EXCLUDED.region_name,
                    geom = EXCLUDED.geom,
                    geom_simplified = EXCLUDED.geom_simplified,
                    updated_at = now()
                """,
                (row["region_code"], row["region_name"], geom_wkb, geom_simplified_wkb),
            )
            count += 1

    logger.info(f"Loaded {count} regions")
    return count


def load_provinces(gdf: gpd.GeoDataFrame) -> int:
    """Load unique provinces from the municipalities GeoDataFrame."""
    # Extract unique provinces
    provinces = gdf[["COD_PROV", "DEN_PROV", "COD_REG", "SIGLA"]].drop_duplicates(subset=["COD_PROV"])
    provinces = provinces.rename(
        columns={
            "COD_PROV": "province_code",
            "DEN_PROV": "province_name",
            "COD_REG": "region_code",
            "SIGLA": "province_abbreviation",
        }
    )

    # Convert codes to strings with leading zeros
    provinces["province_code"] = provinces["province_code"].astype(str).str.zfill(3)
    provinces["region_code"] = provinces["region_code"].astype(str).str.zfill(2)

    # Dissolve geometries by province
    province_geoms = gdf.dissolve(by="COD_PROV")
    province_geoms = province_geoms.reset_index()
    province_geoms["province_code"] = province_geoms["COD_PROV"].astype(str).str.zfill(3)

    count = 0
    with get_db_cursor() as cursor:
        for _, row in provinces.iterrows():
            # Get the dissolved geometry
            geom_row = province_geoms[province_geoms["province_code"] == row["province_code"]]
            if len(geom_row) > 0:
                geom = geom_row.iloc[0].geometry
                geom = make_valid(geom)
                geom_wkb = geom.wkb_hex
                geom_simplified = geom.simplify(SIMPLIFY_TOLERANCE, preserve_topology=True)
                geom_simplified_wkb = geom_simplified.wkb_hex
            else:
                geom_wkb = None
                geom_simplified_wkb = None

            cursor.execute(
                """
                INSERT INTO core.provinces (province_code, province_name, region_code, province_abbreviation, geom, geom_simplified)
                VALUES (%s, %s, %s, %s, ST_GeomFromWKB(%s::geometry, 4326), ST_GeomFromWKB(%s::geometry, 4326))
                ON CONFLICT (province_code) DO UPDATE SET
                    province_name = EXCLUDED.province_name,
                    region_code = EXCLUDED.region_code,
                    province_abbreviation = EXCLUDED.province_abbreviation,
                    geom = EXCLUDED.geom,
                    geom_simplified = EXCLUDED.geom_simplified,
                    updated_at = now()
                """,
                (
                    row["province_code"],
                    row["province_name"],
                    row["region_code"],
                    row.get("province_abbreviation"),
                    geom_wkb,
                    geom_simplified_wkb,
                ),
            )
            count += 1

    logger.info(f"Loaded {count} provinces")
    return count


def load_municipalities(gdf: gpd.GeoDataFrame) -> int:
    """Load municipalities into core.municipalities."""
    # ISTAT column mapping:
    # PRO_COM_T = municipality code (6 digits as text)
    # COMUNE = municipality name
    # COD_PROV = province code
    # COD_REG = region code

    count = 0
    skipped = 0

    with get_db_cursor() as cursor:
        for _, row in gdf.iterrows():
            try:
                # Get municipality code - ISTAT uses PRO_COM_T for the full code
                municipality_id = str(row.get("PRO_COM_T", row.get("PRO_COM", ""))).zfill(6)
                municipality_name = row.get("COMUNE", row.get("DEN_COM", ""))
                province_code = str(row.get("COD_PROV", "")).zfill(3)
                region_code = str(row.get("COD_REG", "")).zfill(2)

                # Get geometry
                geom = row.geometry
                if geom is None or geom.is_empty:
                    logger.warning(f"Skipping {municipality_id} - no geometry")
                    skipped += 1
                    continue

                # Validate and fix geometry if needed
                geom = make_valid(geom)

                # Convert to WKB hex for PostGIS
                geom_wkb = geom.wkb_hex

                # Create simplified version
                geom_simplified = geom.simplify(SIMPLIFY_TOLERANCE, preserve_topology=True)
                geom_simplified_wkb = geom_simplified.wkb_hex

                cursor.execute(
                    """
                    INSERT INTO core.municipalities (
                        municipality_id, municipality_name, province_code, region_code,
                        geom, geom_simplified
                    )
                    VALUES (
                        %s, %s, %s, %s,
                        ST_Multi(ST_GeomFromWKB(%s::geometry, 4326)),
                        ST_Multi(ST_GeomFromWKB(%s::geometry, 4326))
                    )
                    ON CONFLICT (municipality_id) DO UPDATE SET
                        municipality_name = EXCLUDED.municipality_name,
                        province_code = EXCLUDED.province_code,
                        region_code = EXCLUDED.region_code,
                        geom = EXCLUDED.geom,
                        geom_simplified = EXCLUDED.geom_simplified,
                        updated_at = now()
                    """,
                    (
                        municipality_id,
                        municipality_name,
                        province_code,
                        region_code,
                        geom_wkb,
                        geom_simplified_wkb,
                    ),
                )
                count += 1

                if count % 500 == 0:
                    logger.info(f"Processed {count} municipalities...")

            except Exception as e:
                logger.error(f"Error processing municipality: {e}")
                skipped += 1

    logger.info(f"Loaded {count} municipalities, skipped {skipped}")
    return count


def compute_neighbors() -> int:
    """Compute municipality neighbors using ST_Touches."""
    logger.info("Computing municipality neighbors...")

    with get_db_cursor() as cursor:
        # Clear existing neighbors
        cursor.execute("TRUNCATE core.municipality_neighbors")

        # Find all touching municipalities
        cursor.execute(
            """
            INSERT INTO core.municipality_neighbors (municipality_id, neighbor_id, shared_border_km)
            SELECT
                a.municipality_id,
                b.municipality_id,
                ST_Length(
                    ST_Transform(
                        ST_Intersection(a.geom, b.geom),
                        32632  -- UTM zone 32N for Italy
                    )
                ) / 1000.0 AS shared_border_km
            FROM core.municipalities a
            JOIN core.municipalities b ON ST_Touches(a.geom, b.geom)
            WHERE a.municipality_id < b.municipality_id
            """
        )

        # Also insert reverse relationships
        cursor.execute(
            """
            INSERT INTO core.municipality_neighbors (municipality_id, neighbor_id, shared_border_km)
            SELECT neighbor_id, municipality_id, shared_border_km
            FROM core.municipality_neighbors
            """
        )

        cursor.execute("SELECT COUNT(*) as cnt FROM core.municipality_neighbors")
        result = cursor.fetchone()
        count = result["cnt"]

    logger.info(f"Computed {count} neighbor relationships")
    return count


def main():
    parser = argparse.ArgumentParser(description="Import ISTAT administrative boundaries")
    parser.add_argument("--year", type=int, default=2024, help="Reference year for boundaries")
    parser.add_argument("--download", action="store_true", help="Download fresh data")
    parser.add_argument("--data-dir", type=str, default="./data", help="Directory for downloaded data")
    parser.add_argument("--skip-neighbors", action="store_true", help="Skip neighbor computation")
    parser.add_argument("--shapefile", type=str, help="Path to local shapefile (skip download)")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    # Start ingestion run
    ingestion_run_id = create_ingestion_run(
        source_name="istat_boundaries",
        source_version=str(args.year),
    )
    logger.info(f"Started ingestion run {ingestion_run_id}")

    try:
        # Get shapefile path
        if args.shapefile:
            shp_path = Path(args.shapefile)
        else:
            # Download and extract
            zip_path = download_istat_boundaries(args.year, data_dir)
            shp_path = extract_shapefile(zip_path, "Com")  # Comuni layer

        # Load shapefile with geopandas
        logger.info(f"Loading shapefile: {shp_path}")
        gdf = gpd.read_file(shp_path)

        # Reproject to WGS84 if needed
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            logger.info(f"Reprojecting from {gdf.crs} to EPSG:4326")
            gdf = gdf.to_crs(epsg=4326)

        logger.info(f"Loaded {len(gdf)} features")
        logger.info(f"Columns: {list(gdf.columns)}")

        # Load data in order (regions -> provinces -> municipalities)
        regions_count = load_regions(gdf)
        provinces_count = load_provinces(gdf)
        municipalities_count = load_municipalities(gdf)

        # Compute neighbors (can be slow for 8000 municipalities)
        neighbors_count = 0
        if not args.skip_neighbors:
            neighbors_count = compute_neighbors()

        total_loaded = regions_count + provinces_count + municipalities_count

        # Mark success
        complete_ingestion_run(
            ingestion_run_id,
            rows_loaded=total_loaded,
            success=True,
        )
        logger.info(f"Ingestion completed successfully. Total rows: {total_loaded}")

    except Exception as e:
        logger.exception(f"Ingestion failed: {e}")
        complete_ingestion_run(
            ingestion_run_id,
            rows_loaded=0,
            error_notes=str(e),
            success=False,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
