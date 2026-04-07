#!/usr/bin/env python3
"""
ISTAT Municipality Boundaries Ingestion Script

Downloads Italian administrative boundaries from ISTAT and loads them into
the Supabase PostGIS database.

Data Source:
    https://www.istat.it/storage/cartografia/confini_amministrativi/generalizzati/2025/Limiti01012025_g.zip

Target Tables:
    - core.regions
    - core.provinces
    - core.municipalities

Usage:
    python load_municipalities.py [--dry-run] [--skip-download]

Environment Variables (from frontend/.env.local):
    - NEXT_PUBLIC_SUPABASE_URL: Supabase project URL
    - SUPABASE_SERVICE_ROLE_KEY: Service role key for database access
"""

import os
import sys
import zipfile
import tempfile
import argparse
from pathlib import Path
from urllib.parse import urlparse

import requests
import geopandas as gpd
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from shapely import wkb
from shapely.geometry import MultiPolygon, Polygon
from shapely.validation import make_valid


# ISTAT data source
ISTAT_URL = "https://www.istat.it/storage/cartografia/confini_amministrativi/generalizzati/2025/Limiti01012025_g.zip"

# Expected shapefile names within the ZIP
COMUNI_SHAPEFILE = "Limiti01012025_g/Com01012025_g/Com01012025_g_WGS84.shp"
PROVINCE_SHAPEFILE = "Limiti01012025_g/ProvCM01012025_g/ProvCM01012025_g_WGS84.shp"
REGIONI_SHAPEFILE = "Limiti01012025_g/Reg01012025_g/Reg01012025_g_WGS84.shp"

# Simplification tolerance for web display (in degrees, ~111m at equator)
SIMPLIFY_TOLERANCE = 0.001


def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.parent.parent


def load_env_vars() -> dict:
    """Load environment variables from frontend/.env.local."""
    env_path = get_project_root() / "frontend" / ".env.local"

    if not env_path.exists():
        raise FileNotFoundError(f"Environment file not found: {env_path}")

    load_dotenv(env_path)

    supabase_url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not service_role_key:
        raise ValueError("Missing required environment variables: NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

    return {
        "supabase_url": supabase_url,
        "service_role_key": service_role_key
    }


def construct_connection_params() -> dict:
    """
    Construct PostgreSQL connection parameters from environment.

    Returns a dict with connection parameters for psycopg2.connect()
    """
    # Check for individual DB_* parameters first (most reliable)
    db_host = os.getenv("DB_HOST")
    db_password = os.getenv("DB_PASSWORD")

    if db_host and db_password:
        print(f"  Using DB_* parameters from environment")
        return {
            "host": db_host,
            "port": os.getenv("DB_PORT", "5432"),
            "dbname": os.getenv("DB_NAME", "postgres"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": db_password,
        }

    # Check for explicit DATABASE_URL
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        print(f"  Using DATABASE_URL from environment")
        return {"dsn": database_url}

    return None


def try_connect(conn_params: dict) -> psycopg2.extensions.connection:
    """Try connecting with connection parameters."""
    if conn_params is None:
        raise ValueError("No connection parameters provided")

    # If we have a DSN (connection string), use it directly
    if "dsn" in conn_params:
        dsn = conn_params["dsn"]
        safe_string = dsn.split('@')[1] if '@' in dsn else dsn
        print(f"  Trying: ...@{safe_string}")
        conn = psycopg2.connect(dsn)
        print(f"  Connected successfully!")
        return conn

    # Otherwise use individual parameters
    print(f"  Trying: {conn_params['user']}@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}")
    conn = psycopg2.connect(**conn_params)
    print(f"  Connected successfully!")
    return conn


def download_istat_data(url: str, output_dir: Path) -> Path:
    """Download and extract ISTAT shapefile ZIP."""
    print(f"Downloading ISTAT data from: {url}")

    zip_path = output_dir / "istat_boundaries.zip"

    # Download with progress indication
    response = requests.get(url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0

    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total_size > 0:
                pct = (downloaded / total_size) * 100
                print(f"\r  Downloaded: {downloaded:,} / {total_size:,} bytes ({pct:.1f}%)", end="")

    print("\n  Download complete.")

    # Extract ZIP
    print(f"Extracting ZIP to: {output_dir}")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(output_dir)

    return output_dir


def ensure_multipolygon(geom):
    """Convert Polygon to MultiPolygon if needed, and ensure validity."""
    if geom is None:
        return None

    # Make geometry valid if it isn't
    if not geom.is_valid:
        geom = make_valid(geom)

    # Convert Polygon to MultiPolygon
    if isinstance(geom, Polygon):
        return MultiPolygon([geom])
    elif isinstance(geom, MultiPolygon):
        return geom
    else:
        # Handle GeometryCollection or other types
        polygons = []
        if hasattr(geom, 'geoms'):
            for g in geom.geoms:
                if isinstance(g, Polygon):
                    polygons.append(g)
                elif isinstance(g, MultiPolygon):
                    polygons.extend(g.geoms)
        if polygons:
            return MultiPolygon(polygons)
        return None


def simplify_geometry(geom, tolerance: float = SIMPLIFY_TOLERANCE):
    """Simplify geometry while preserving topology."""
    if geom is None:
        return None
    return geom.simplify(tolerance, preserve_topology=True)


def load_regions(gdf: gpd.GeoDataFrame, conn, dry_run: bool = False) -> int:
    """
    Load regions into core.regions table.

    ISTAT fields:
        COD_REG: Region code (2 digits)
        DEN_REG: Region name
    """
    print("\n--- Loading Regions ---")

    # Map ISTAT fields to our schema
    regions_df = gdf[['COD_REG', 'DEN_REG', 'geometry']].copy()
    regions_df.columns = ['region_code', 'region_name', 'geometry']

    # Ensure region_code is zero-padded to 2 digits
    regions_df['region_code'] = regions_df['region_code'].astype(str).str.zfill(2)

    # Process geometries
    regions_df['geom'] = regions_df['geometry'].apply(ensure_multipolygon)
    regions_df['geom_simplified'] = regions_df['geom'].apply(simplify_geometry)

    # Remove rows with invalid geometries
    valid_mask = regions_df['geom'].notna()
    regions_df = regions_df[valid_mask]

    print(f"  Found {len(regions_df)} regions")

    if dry_run:
        print("  [DRY RUN] Would upsert regions")
        return len(regions_df)

    # Prepare data for upsert
    with conn.cursor() as cur:
        # Upsert each region
        for _, row in regions_df.iterrows():
            geom_wkb = row['geom'].wkb_hex if row['geom'] else None
            geom_simplified_wkb = row['geom_simplified'].wkb_hex if row['geom_simplified'] else None

            cur.execute("""
                INSERT INTO core.regions (region_code, region_name, geom, geom_simplified, updated_at)
                VALUES (%s, %s, ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)), ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)), now())
                ON CONFLICT (region_code) DO UPDATE SET
                    region_name = EXCLUDED.region_name,
                    geom = EXCLUDED.geom,
                    geom_simplified = EXCLUDED.geom_simplified,
                    updated_at = now()
            """, (row['region_code'], row['region_name'], geom_wkb, geom_simplified_wkb))

        conn.commit()

    print(f"  Upserted {len(regions_df)} regions")
    return len(regions_df)


def load_provinces(gdf: gpd.GeoDataFrame, conn, dry_run: bool = False) -> int:
    """
    Load provinces into core.provinces table.

    ISTAT fields:
        COD_PROV: Province code (3 digits)
        DEN_PROV or DEN_UTS: Province name
        COD_REG: Region code
        SIGLA: Province abbreviation (e.g., "MI")
    """
    print("\n--- Loading Provinces ---")

    # Determine correct name column (varies by ISTAT version)
    name_col = 'DEN_PROV' if 'DEN_PROV' in gdf.columns else 'DEN_UTS'
    sigla_col = 'SIGLA' if 'SIGLA' in gdf.columns else None

    cols = ['COD_PROV', name_col, 'COD_REG', 'geometry']
    if sigla_col:
        cols.insert(3, sigla_col)

    provinces_df = gdf[cols].copy()

    # Rename columns
    col_mapping = {
        'COD_PROV': 'province_code',
        name_col: 'province_name',
        'COD_REG': 'region_code',
        'geometry': 'geometry'
    }
    if sigla_col:
        col_mapping[sigla_col] = 'province_abbreviation'

    provinces_df = provinces_df.rename(columns=col_mapping)

    # Ensure codes are zero-padded
    provinces_df['province_code'] = provinces_df['province_code'].astype(str).str.zfill(3)
    provinces_df['region_code'] = provinces_df['region_code'].astype(str).str.zfill(2)

    # Process geometries
    provinces_df['geom'] = provinces_df['geometry'].apply(ensure_multipolygon)
    provinces_df['geom_simplified'] = provinces_df['geom'].apply(simplify_geometry)

    # Remove rows with invalid geometries
    valid_mask = provinces_df['geom'].notna()
    provinces_df = provinces_df[valid_mask]

    print(f"  Found {len(provinces_df)} provinces")

    if dry_run:
        print("  [DRY RUN] Would upsert provinces")
        return len(provinces_df)

    # Upsert provinces
    with conn.cursor() as cur:
        for _, row in provinces_df.iterrows():
            geom_wkb = row['geom'].wkb_hex if row['geom'] else None
            geom_simplified_wkb = row['geom_simplified'].wkb_hex if row['geom_simplified'] else None
            abbrev = row.get('province_abbreviation')

            cur.execute("""
                INSERT INTO core.provinces (province_code, province_name, region_code, province_abbreviation, geom, geom_simplified, updated_at)
                VALUES (%s, %s, %s, %s, ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)), ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)), now())
                ON CONFLICT (province_code) DO UPDATE SET
                    province_name = EXCLUDED.province_name,
                    region_code = EXCLUDED.region_code,
                    province_abbreviation = EXCLUDED.province_abbreviation,
                    geom = EXCLUDED.geom,
                    geom_simplified = EXCLUDED.geom_simplified,
                    updated_at = now()
            """, (row['province_code'], row['province_name'], row['region_code'], abbrev, geom_wkb, geom_simplified_wkb))

        conn.commit()

    print(f"  Upserted {len(provinces_df)} provinces")
    return len(provinces_df)


def load_municipalities(gdf: gpd.GeoDataFrame, conn, dry_run: bool = False) -> int:
    """
    Load municipalities into core.municipalities table.

    ISTAT fields:
        PRO_COM_T: Municipality code (6 digits, text to preserve leading zeros)
        COMUNE: Municipality name
        COD_PROV: Province code
        COD_REG: Region code
    """
    print("\n--- Loading Municipalities ---")

    # Map ISTAT fields to our schema
    municipalities_df = gdf[['PRO_COM_T', 'COMUNE', 'COD_PROV', 'COD_REG', 'geometry']].copy()
    municipalities_df.columns = ['municipality_id', 'municipality_name', 'province_code', 'region_code', 'geometry']

    # Ensure codes are properly formatted
    municipalities_df['municipality_id'] = municipalities_df['municipality_id'].astype(str).str.zfill(6)
    municipalities_df['province_code'] = municipalities_df['province_code'].astype(str).str.zfill(3)
    municipalities_df['region_code'] = municipalities_df['region_code'].astype(str).str.zfill(2)

    # Process geometries
    print("  Processing geometries...")
    municipalities_df['geom'] = municipalities_df['geometry'].apply(ensure_multipolygon)
    municipalities_df['geom_simplified'] = municipalities_df['geom'].apply(simplify_geometry)

    # Remove rows with invalid geometries
    valid_mask = municipalities_df['geom'].notna()
    invalid_count = (~valid_mask).sum()
    if invalid_count > 0:
        print(f"  Warning: {invalid_count} municipalities have invalid geometries and will be skipped")
    municipalities_df = municipalities_df[valid_mask]

    print(f"  Found {len(municipalities_df)} municipalities")

    # Show sample
    print("\n  Sample municipalities:")
    sample = municipalities_df.head(5)[['municipality_id', 'municipality_name', 'province_code', 'region_code']]
    print(sample.to_string(index=False))

    if dry_run:
        print("\n  [DRY RUN] Would upsert municipalities")
        return len(municipalities_df)

    # Batch upsert municipalities
    print("\n  Upserting municipalities in batches...")
    batch_size = 100
    total = len(municipalities_df)
    upserted = 0

    with conn.cursor() as cur:
        for i in range(0, total, batch_size):
            batch = municipalities_df.iloc[i:i+batch_size]

            for _, row in batch.iterrows():
                geom_wkb = row['geom'].wkb_hex if row['geom'] else None
                geom_simplified_wkb = row['geom_simplified'].wkb_hex if row['geom_simplified'] else None

                cur.execute("""
                    INSERT INTO core.municipalities (municipality_id, municipality_name, province_code, region_code, geom, geom_simplified, updated_at)
                    VALUES (%s, %s, %s, %s, ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)), ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)), now())
                    ON CONFLICT (municipality_id) DO UPDATE SET
                        municipality_name = EXCLUDED.municipality_name,
                        province_code = EXCLUDED.province_code,
                        region_code = EXCLUDED.region_code,
                        geom = EXCLUDED.geom,
                        geom_simplified = EXCLUDED.geom_simplified,
                        updated_at = now()
                """, (row['municipality_id'], row['municipality_name'], row['province_code'], row['region_code'], geom_wkb, geom_simplified_wkb))

            conn.commit()
            upserted += len(batch)
            print(f"\r  Progress: {upserted:,} / {total:,} ({(upserted/total)*100:.1f}%)", end="")

    print(f"\n  Upserted {upserted} municipalities")
    return upserted


def print_summary(conn):
    """Print summary statistics from the database."""
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    with conn.cursor() as cur:
        # Count regions
        cur.execute("SELECT COUNT(*) FROM core.regions WHERE geom IS NOT NULL")
        regions_count = cur.fetchone()[0]
        print(f"  Regions with geometry:       {regions_count:,}")

        # Count provinces
        cur.execute("SELECT COUNT(*) FROM core.provinces WHERE geom IS NOT NULL")
        provinces_count = cur.fetchone()[0]
        print(f"  Provinces with geometry:     {provinces_count:,}")

        # Count municipalities
        cur.execute("SELECT COUNT(*) FROM core.municipalities WHERE geom IS NOT NULL")
        municipalities_count = cur.fetchone()[0]
        print(f"  Municipalities with geometry: {municipalities_count:,}")

        # Municipalities by region
        print("\n  Municipalities per region:")
        cur.execute("""
            SELECT r.region_name, COUNT(m.municipality_id) as cnt
            FROM core.regions r
            LEFT JOIN core.municipalities m ON r.region_code = m.region_code
            GROUP BY r.region_code, r.region_name
            ORDER BY cnt DESC
        """)
        for row in cur.fetchall():
            print(f"    {row[0]}: {row[1]:,}")


def main():
    parser = argparse.ArgumentParser(
        description="Load ISTAT municipality boundaries into Supabase PostGIS"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse data but don't write to database"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Use existing downloaded data (for development)"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Directory containing already extracted ISTAT shapefiles"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("ISTAT Municipality Boundaries Ingestion")
    print("=" * 60)

    # Load environment variables
    print("\nLoading environment variables...")
    env_vars = load_env_vars()
    print(f"  Supabase URL: {env_vars['supabase_url']}")

    # Construct connection parameters
    conn_params = construct_connection_params()

    # Connect to database (for validation)
    if not args.dry_run:
        print("\nConnecting to database...")
        try:
            conn = try_connect(conn_params)
        except Exception as e:
            print(f"  ERROR: Failed to connect to database: {e}")
            sys.exit(1)
    else:
        conn = None
        print("\n[DRY RUN MODE - No database connection]")

    # Download or use existing data
    if args.data_dir:
        data_dir = Path(args.data_dir)
        print(f"\nUsing existing data directory: {data_dir}")
    elif args.skip_download:
        # Use project data directory
        data_dir = get_project_root() / "data" / "istat_boundaries"
        print(f"\nUsing existing data directory: {data_dir}")
    else:
        # Download to temporary directory
        data_dir = Path(tempfile.mkdtemp(prefix="istat_"))
        print(f"\nDownloading data to: {data_dir}")
        download_istat_data(ISTAT_URL, data_dir)

    # Verify shapefiles exist
    comuni_path = data_dir / COMUNI_SHAPEFILE
    province_path = data_dir / PROVINCE_SHAPEFILE
    regioni_path = data_dir / REGIONI_SHAPEFILE

    # Check if files exist, try alternative paths if not
    if not comuni_path.exists():
        # Try finding the shapefile
        shp_files = list(data_dir.rglob("*Com*WGS84.shp"))
        if shp_files:
            comuni_path = shp_files[0]
        else:
            print(f"ERROR: Comuni shapefile not found at {comuni_path}")
            sys.exit(1)

    if not province_path.exists():
        shp_files = list(data_dir.rglob("*Prov*WGS84.shp"))
        if shp_files:
            province_path = shp_files[0]
        else:
            print(f"ERROR: Province shapefile not found at {province_path}")
            sys.exit(1)

    if not regioni_path.exists():
        shp_files = list(data_dir.rglob("*Reg*WGS84.shp"))
        if shp_files:
            regioni_path = shp_files[0]
        else:
            print(f"ERROR: Regioni shapefile not found at {regioni_path}")
            sys.exit(1)

    print(f"\nShapefiles found:")
    print(f"  Comuni:   {comuni_path}")
    print(f"  Province: {province_path}")
    print(f"  Regioni:  {regioni_path}")

    # Load shapefiles with geopandas
    print("\nReading shapefiles...")

    print("  Loading Regioni...")
    gdf_regioni = gpd.read_file(regioni_path)
    print(f"    Columns: {list(gdf_regioni.columns)}")
    print(f"    CRS: {gdf_regioni.crs}")
    print(f"    Records: {len(gdf_regioni)}")

    print("  Loading Province...")
    gdf_province = gpd.read_file(province_path)
    print(f"    Columns: {list(gdf_province.columns)}")
    print(f"    CRS: {gdf_province.crs}")
    print(f"    Records: {len(gdf_province)}")

    print("  Loading Comuni...")
    gdf_comuni = gpd.read_file(comuni_path)
    print(f"    Columns: {list(gdf_comuni.columns)}")
    print(f"    CRS: {gdf_comuni.crs}")
    print(f"    Records: {len(gdf_comuni)}")

    # Ensure CRS is WGS84
    if gdf_regioni.crs and gdf_regioni.crs.to_epsg() != 4326:
        print("  Reprojecting Regioni to EPSG:4326...")
        gdf_regioni = gdf_regioni.to_crs(epsg=4326)

    if gdf_province.crs and gdf_province.crs.to_epsg() != 4326:
        print("  Reprojecting Province to EPSG:4326...")
        gdf_province = gdf_province.to_crs(epsg=4326)

    if gdf_comuni.crs and gdf_comuni.crs.to_epsg() != 4326:
        print("  Reprojecting Comuni to EPSG:4326...")
        gdf_comuni = gdf_comuni.to_crs(epsg=4326)

    # Load data in order (regions -> provinces -> municipalities due to FK constraints)
    try:
        regions_count = load_regions(gdf_regioni, conn, dry_run=args.dry_run)
        provinces_count = load_provinces(gdf_province, conn, dry_run=args.dry_run)
        municipalities_count = load_municipalities(gdf_comuni, conn, dry_run=args.dry_run)

        # Print summary
        if not args.dry_run and conn:
            print_summary(conn)
        else:
            print("\n" + "=" * 60)
            print("DRY RUN SUMMARY")
            print("=" * 60)
            print(f"  Regions parsed:       {regions_count:,}")
            print(f"  Provinces parsed:     {provinces_count:,}")
            print(f"  Municipalities parsed: {municipalities_count:,}")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        if conn:
            conn.close()

    print("\n" + "=" * 60)
    print("Ingestion complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
