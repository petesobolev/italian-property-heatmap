-- Italian Property Heatmap - Complete Schema Migration
-- Adds all tables for raw and mart layers, plus core reference tables

begin;

-- ============================================================================
-- CORE SCHEMA - Reference dimensions
-- ============================================================================

-- Core: Regions (20 Italian regions)
create table if not exists core.regions (
  region_code text primary key,           -- ISTAT region code (01-20)
  region_name text not null,
  geom geometry(MultiPolygon, 4326) null,
  geom_simplified geometry(MultiPolygon, 4326) null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists regions_geom_gix on core.regions using gist (geom);
create index if not exists regions_geom_simplified_gix on core.regions using gist (geom_simplified);

-- Core: Provinces (107 Italian provinces)
create table if not exists core.provinces (
  province_code text primary key,         -- ISTAT province code (3 digits)
  province_name text not null,
  region_code text not null references core.regions(region_code),
  province_abbreviation text null,        -- e.g. "MI" for Milano
  geom geometry(MultiPolygon, 4326) null,
  geom_simplified geometry(MultiPolygon, 4326) null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists provinces_region_idx on core.provinces (region_code);
create index if not exists provinces_geom_gix on core.provinces using gist (geom);
create index if not exists provinces_geom_simplified_gix on core.provinces using gist (geom_simplified);

-- Add foreign keys to municipalities now that regions/provinces exist
-- First, drop existing indexes if needed to modify the table safely
alter table core.municipalities
  add constraint municipalities_region_fk
  foreign key (region_code) references core.regions(region_code)
  on delete set null
  not valid;  -- Don't validate existing rows yet

alter table core.municipalities
  add constraint municipalities_province_fk
  foreign key (province_code) references core.provinces(province_code)
  on delete set null
  not valid;

-- Core: Municipality neighbors (for spatial analysis)
create table if not exists core.municipality_neighbors (
  municipality_id text not null references core.municipalities(municipality_id),
  neighbor_id text not null references core.municipalities(municipality_id),
  shared_border_km numeric null,          -- Length of shared border
  created_at timestamptz not null default now(),
  primary key (municipality_id, neighbor_id)
);

create index if not exists municipality_neighbors_neighbor_idx
  on core.municipality_neighbors (neighbor_id);

-- Core: OMI Zones (sub-municipal zones from Agenzia delle Entrate)
create table if not exists core.omi_zones (
  omi_zone_id text primary key,           -- Format: municipality_id + zone_code
  municipality_id text not null references core.municipalities(municipality_id),
  zone_code text not null,                -- B1, C1, D1, etc.
  zone_type text null,                    -- Centrale, Semicentrale, Periferica, etc.
  zone_description text null,
  microzone_code text null,
  geom geometry(MultiPolygon, 4326) null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists omi_zones_municipality_idx on core.omi_zones (municipality_id);
create index if not exists omi_zones_geom_gix on core.omi_zones using gist (geom);

-- ============================================================================
-- RAW SCHEMA - Raw ingested datasets
-- ============================================================================

-- Raw: OMI Property Values (from Agenzia delle Entrate)
create table if not exists raw.omi_property_values (
  id bigserial primary key,
  ingestion_run_id bigint references admin.ingestion_runs(ingestion_run_id),
  omi_zone_id text null,                  -- References core.omi_zones if available
  municipality_id text not null,          -- ISTAT municipality code
  period_id text not null,                -- Semester: e.g. "2024S1"
  property_type text not null,            -- residenziale, commerciale, terziario, produttivo
  property_subtype text null,             -- ville, appartamenti, box, negozi, etc.
  state text null,                        -- OTTIMO, NORMALE, SCADENTE
  value_min_eur_sqm numeric null,
  value_max_eur_sqm numeric null,
  rent_min_eur_sqm_month numeric null,
  rent_max_eur_sqm_month numeric null,
  surface_min_sqm numeric null,
  surface_max_sqm numeric null,
  source_file text null,
  raw_data jsonb null,
  created_at timestamptz not null default now()
);

create index if not exists omi_property_values_municipality_idx
  on raw.omi_property_values (municipality_id);
create index if not exists omi_property_values_period_idx
  on raw.omi_property_values (period_id);
create index if not exists omi_property_values_type_idx
  on raw.omi_property_values (property_type);
create index if not exists omi_property_values_ingestion_idx
  on raw.omi_property_values (ingestion_run_id);

-- Raw: OMI Transactions (NTN - Numero Transazioni Normalizzate)
create table if not exists raw.omi_transactions (
  id bigserial primary key,
  ingestion_run_id bigint references admin.ingestion_runs(ingestion_run_id),
  municipality_id text not null,
  period_id text not null,                -- Semester: e.g. "2024S1"
  property_type text not null,            -- residenziale, commerciale, etc.
  ntn numeric null,                       -- Normalized transaction count
  imt numeric null,                       -- Market intensity index
  quotation_stock int null,               -- Number of listings
  source_file text null,
  raw_data jsonb null,
  created_at timestamptz not null default now()
);

create index if not exists omi_transactions_municipality_idx
  on raw.omi_transactions (municipality_id);
create index if not exists omi_transactions_period_idx
  on raw.omi_transactions (period_id);
create index if not exists omi_transactions_ingestion_idx
  on raw.omi_transactions (ingestion_run_id);

-- Raw: ISTAT Population demographics
create table if not exists raw.istat_population (
  id bigserial primary key,
  ingestion_run_id bigint references admin.ingestion_runs(ingestion_run_id),
  municipality_id text not null,
  reference_year int not null,
  reference_date date null,
  total_population int null,
  male_population int null,
  female_population int null,
  -- Age brackets
  population_0_14 int null,
  population_15_24 int null,
  population_25_44 int null,
  population_45_64 int null,
  population_65_plus int null,
  -- Foreign residents
  foreign_population int null,
  -- Natural movement
  births int null,
  deaths int null,
  -- Migration
  immigration int null,
  emigration int null,
  internal_immigration int null,
  internal_emigration int null,
  -- Household data
  households int null,
  avg_household_size numeric null,
  source_file text null,
  raw_data jsonb null,
  created_at timestamptz not null default now(),
  unique (municipality_id, reference_year)
);

create index if not exists istat_population_municipality_idx
  on raw.istat_population (municipality_id);
create index if not exists istat_population_year_idx
  on raw.istat_population (reference_year);
create index if not exists istat_population_ingestion_idx
  on raw.istat_population (ingestion_run_id);

-- Raw: ISTAT Tourism statistics
create table if not exists raw.istat_tourism (
  id bigserial primary key,
  ingestion_run_id bigint references admin.ingestion_runs(ingestion_run_id),
  municipality_id text not null,
  period_id text not null,                -- Month: e.g. "2024-07"
  -- Arrivals and presences
  arrivals_total int null,
  arrivals_italians int null,
  arrivals_foreigners int null,
  presences_total int null,
  presences_italians int null,
  presences_foreigners int null,
  -- Accommodation capacity
  establishments_count int null,
  beds_count int null,
  -- Type breakdown (optional)
  accommodation_type text null,           -- hotel, B&B, agriturismo, etc.
  source_file text null,
  raw_data jsonb null,
  created_at timestamptz not null default now()
);

create index if not exists istat_tourism_municipality_idx
  on raw.istat_tourism (municipality_id);
create index if not exists istat_tourism_period_idx
  on raw.istat_tourism (period_id);
create index if not exists istat_tourism_ingestion_idx
  on raw.istat_tourism (ingestion_run_id);

-- ============================================================================
-- MART SCHEMA - Curated analytics tables
-- ============================================================================

-- Mart: Municipality values by semester (aggregated from OMI zones)
create table if not exists mart.municipality_values_semester (
  municipality_id text not null references core.municipalities(municipality_id),
  period_id text not null references core.time_periods(period_id),
  property_segment text not null default 'residential',
  -- Aggregated values
  value_min_eur_sqm numeric null,
  value_max_eur_sqm numeric null,
  value_mid_eur_sqm numeric null,         -- (min + max) / 2
  value_median_eur_sqm numeric null,
  rent_min_eur_sqm_month numeric null,
  rent_max_eur_sqm_month numeric null,
  rent_mid_eur_sqm_month numeric null,
  -- Zone counts
  zones_count int null,
  zones_with_data int null,
  -- Change metrics (vs prior semester)
  value_pct_change_1s numeric null,
  value_pct_change_2s numeric null,       -- vs 2 semesters ago (YoY)
  -- Data quality
  data_quality_score numeric null,        -- 0-100
  updated_at timestamptz not null default now(),
  primary key (municipality_id, period_id, property_segment)
);

create index if not exists municipality_values_period_idx
  on mart.municipality_values_semester (period_id);
create index if not exists municipality_values_segment_idx
  on mart.municipality_values_semester (property_segment);

-- Mart: Municipality transactions by semester
create table if not exists mart.municipality_transactions_semester (
  municipality_id text not null references core.municipalities(municipality_id),
  period_id text not null references core.time_periods(period_id),
  property_segment text not null default 'residential',
  -- Transaction metrics
  ntn_total numeric null,                 -- Total normalized transactions
  ntn_per_1000_pop numeric null,          -- Per capita metric
  imt_avg numeric null,                   -- Average market intensity
  quotation_stock_total int null,
  -- Change metrics
  ntn_pct_change_1s numeric null,
  ntn_pct_change_2s numeric null,
  -- Derived metrics
  absorption_rate numeric null,           -- transactions / listings
  days_on_market_avg numeric null,
  updated_at timestamptz not null default now(),
  primary key (municipality_id, period_id, property_segment)
);

create index if not exists municipality_transactions_period_idx
  on mart.municipality_transactions_semester (period_id);

-- Mart: Municipality demographics by year
create table if not exists mart.municipality_demographics_year (
  municipality_id text not null references core.municipalities(municipality_id),
  reference_year int not null,
  -- Population metrics
  total_population int null,
  population_density numeric null,        -- per sq km
  -- Age ratios
  young_ratio numeric null,               -- 0-14 / total
  working_ratio numeric null,             -- 15-64 / total
  elderly_ratio numeric null,             -- 65+ / total
  dependency_ratio numeric null,          -- (0-14 + 65+) / 15-64
  old_age_index numeric null,             -- 65+ / 0-14
  -- Foreign population
  foreign_ratio numeric null,             -- foreign / total
  -- Growth metrics
  population_growth_rate numeric null,    -- YoY change
  natural_balance numeric null,           -- births - deaths
  migration_balance numeric null,         -- immigration - emigration
  -- Household metrics
  households int null,
  avg_household_size numeric null,
  updated_at timestamptz not null default now(),
  primary key (municipality_id, reference_year)
);

create index if not exists municipality_demographics_year_idx
  on mart.municipality_demographics_year (reference_year);

-- ============================================================================
-- MODEL SCHEMA - Feature store and model outputs
-- ============================================================================

-- Model: Features at municipality-semester level for ML
create table if not exists model.features_municipality_semester (
  municipality_id text not null references core.municipalities(municipality_id),
  period_id text not null references core.time_periods(period_id),
  property_segment text not null default 'residential',

  -- Price features
  value_mid_eur_sqm numeric null,
  value_pct_change_1s numeric null,
  value_pct_change_2s numeric null,
  value_pct_change_4s numeric null,       -- 2-year change
  value_volatility_4s numeric null,       -- Std dev over 4 semesters

  -- Transaction features
  ntn_total numeric null,
  ntn_per_1000_pop numeric null,
  ntn_pct_change_1s numeric null,
  absorption_rate numeric null,

  -- Yield features
  gross_yield_pct numeric null,           -- Annual rent / value
  rent_value_ratio numeric null,

  -- Demographics features
  population int null,
  population_growth_rate numeric null,
  young_ratio numeric null,
  elderly_ratio numeric null,
  foreign_ratio numeric null,
  dependency_ratio numeric null,

  -- Spatial features
  coastal_flag boolean null,
  mountain_flag boolean null,
  province_avg_value numeric null,        -- Province average for comparison
  region_avg_value numeric null,          -- Region average for comparison
  value_vs_province_pct numeric null,     -- Premium/discount vs province
  value_vs_region_pct numeric null,
  neighbor_avg_value numeric null,        -- Average of neighboring municipalities

  -- Tourism features
  tourism_intensity numeric null,         -- presences / population
  seasonality_index numeric null,

  -- Metadata
  feature_completeness_score numeric null, -- % of features populated
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (municipality_id, period_id, property_segment)
);

create index if not exists features_period_idx
  on model.features_municipality_semester (period_id);
create index if not exists features_segment_idx
  on model.features_municipality_semester (property_segment);

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Function to generate simplified geometry at different tolerance levels
create or replace function core.simplify_geometry(
  geom geometry,
  tolerance_degrees numeric default 0.001
) returns geometry as $$
begin
  return ST_SimplifyPreserveTopology(geom, tolerance_degrees);
end;
$$ language plpgsql immutable;

-- Function to calculate property segment from Italian OMI categories
create or replace function core.normalize_property_segment(
  omi_property_type text
) returns text as $$
begin
  return case lower(trim(omi_property_type))
    when 'residenziale' then 'residential'
    when 'abitazioni' then 'residential'
    when 'commerciale' then 'commercial'
    when 'negozi' then 'commercial'
    when 'terziario' then 'commercial'
    when 'uffici' then 'commercial'
    when 'produttivo' then 'industrial'
    when 'capannoni' then 'industrial'
    else 'other'
  end;
end;
$$ language plpgsql immutable;

-- ============================================================================
-- API FUNCTIONS - GeoJSON endpoints
-- ============================================================================

-- Function to get municipalities as GeoJSON FeatureCollection
create or replace function public.get_municipalities_geojson(
  geom_column text default 'geom_simplified',
  where_clause text default '',
  region_filter text default null,
  province_filter text default null
) returns jsonb as $$
declare
  result jsonb;
begin
  -- Build and execute dynamic query for GeoJSON
  execute format(
    $sql$
    select jsonb_build_object(
      'type', 'FeatureCollection',
      'features', coalesce(jsonb_agg(
        jsonb_build_object(
          'type', 'Feature',
          'properties', jsonb_build_object(
            'municipality_id', municipality_id,
            'name', municipality_name,
            'province_code', province_code,
            'region_code', region_code,
            'coastal_flag', coastal_flag,
            'mountain_flag', mountain_flag
          ),
          'geometry', ST_AsGeoJSON(%I)::jsonb
        )
      ), '[]'::jsonb)
    )
    from core.municipalities
    where %I is not null
      and ($1 is null or region_code = $1)
      and ($2 is null or province_code = $2)
    $sql$,
    geom_column,
    geom_column
  ) into result using region_filter, province_filter;

  return result;
end;
$$ language plpgsql stable security definer;

-- Grant execute permission to anon and authenticated users
grant execute on function public.get_municipalities_geojson(text, text, text, text) to anon;
grant execute on function public.get_municipalities_geojson(text, text, text, text) to authenticated;

commit;
