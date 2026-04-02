-- Italian Property Heatmap - Phase 3: Rental Data Schema
-- Adds tables for rental data, yield calculations, and comparison features

begin;

-- ============================================================================
-- RAW SCHEMA - Rental listing data
-- ============================================================================

-- Raw: Long-term rental listings (from portals like Immobiliare.it, Idealista)
create table if not exists raw.rental_long_term (
  id bigserial primary key,
  ingestion_run_id bigint references admin.ingestion_runs(ingestion_run_id),
  municipality_id text not null,
  period_id text not null,                  -- Month: e.g. "2024-07"
  property_type text null,                  -- apartment, house, villa, etc.
  bedrooms int null,
  bathrooms int null,
  surface_sqm numeric null,
  rent_monthly_eur numeric null,
  rent_per_sqm_month numeric null,
  -- Location details
  zone_description text null,
  latitude numeric null,
  longitude numeric null,
  -- Listing metadata
  listing_source text null,                 -- immobiliare.it, idealista, etc.
  listing_id text null,                     -- External ID from source
  listing_url text null,
  listing_date date null,
  days_on_market int null,
  -- Property features
  furnished boolean null,
  has_parking boolean null,
  has_garden boolean null,
  has_terrace boolean null,
  floor_number int null,
  total_floors int null,
  year_built int null,
  energy_class text null,
  -- Raw data
  raw_data jsonb null,
  created_at timestamptz not null default now()
);

create index if not exists rental_long_term_municipality_idx
  on raw.rental_long_term (municipality_id);
create index if not exists rental_long_term_period_idx
  on raw.rental_long_term (period_id);
create index if not exists rental_long_term_ingestion_idx
  on raw.rental_long_term (ingestion_run_id);
create index if not exists rental_long_term_source_idx
  on raw.rental_long_term (listing_source);

-- Raw: Short-term rental data (Airbnb, VRBO, etc.) - for future expansion
create table if not exists raw.rental_short_term (
  id bigserial primary key,
  ingestion_run_id bigint references admin.ingestion_runs(ingestion_run_id),
  municipality_id text not null,
  period_id text not null,                  -- Month: e.g. "2024-07"
  property_type text null,
  bedrooms int null,
  -- Pricing
  adr_eur numeric null,                     -- Average Daily Rate
  occupancy_rate numeric null,              -- 0-1
  rev_par_eur numeric null,                 -- Revenue per Available Room
  monthly_revenue_eur numeric null,
  -- Listing details
  listing_source text null,
  listing_id text null,
  -- Aggregation metadata
  listings_count int null,                  -- If aggregated data
  raw_data jsonb null,
  created_at timestamptz not null default now()
);

create index if not exists rental_short_term_municipality_idx
  on raw.rental_short_term (municipality_id);
create index if not exists rental_short_term_period_idx
  on raw.rental_short_term (period_id);

-- ============================================================================
-- MART SCHEMA - Aggregated rental data
-- ============================================================================

-- Mart: Municipality rents by month (aggregated from rental listings)
create table if not exists mart.municipality_rents_month (
  municipality_id text not null,
  period_id text not null,                  -- Month: e.g. "2024-07"
  property_segment text not null default 'residential',

  -- Long-term rental metrics
  rent_min_eur_sqm_month numeric null,
  rent_max_eur_sqm_month numeric null,
  rent_mid_eur_sqm_month numeric null,
  rent_median_eur_sqm_month numeric null,
  rent_percentile_25 numeric null,
  rent_percentile_75 numeric null,

  -- Volume metrics
  listings_count int null,
  new_listings_count int null,
  avg_days_on_market numeric null,

  -- Size breakdown
  avg_surface_sqm numeric null,
  avg_rent_total_eur numeric null,

  -- Change metrics
  rent_pct_change_1m numeric null,
  rent_pct_change_3m numeric null,
  rent_pct_change_12m numeric null,

  -- Short-term rental metrics (if available)
  str_adr_eur numeric null,
  str_occupancy_rate numeric null,
  str_rev_par_eur numeric null,
  str_listings_count int null,

  -- Data quality
  data_source text null,                    -- 'omi', 'listings', 'both'
  sample_size int null,
  data_quality_score numeric null,

  updated_at timestamptz not null default now(),
  primary key (municipality_id, period_id, property_segment)
);

create index if not exists municipality_rents_period_idx
  on mart.municipality_rents_month (period_id);
create index if not exists municipality_rents_segment_idx
  on mart.municipality_rents_month (property_segment);

-- ============================================================================
-- MODEL SCHEMA - Add yield-related columns to forecasts
-- ============================================================================

-- Add yield columns to forecasts table if they don't exist
alter table model.forecasts_municipality
  add column if not exists forecast_rent_eur_sqm_month numeric null,
  add column if not exists forecast_rent_change_pct numeric null,
  add column if not exists forecast_net_yield_pct numeric null,
  add column if not exists yield_spread_vs_region numeric null;

-- Add columns for comparison feature
alter table model.forecasts_municipality
  add column if not exists opportunity_strategy text null,
  add column if not exists score_factors jsonb null;

-- ============================================================================
-- FEATURE STORE - Add rental/yield features
-- ============================================================================

-- Add rental-specific features to feature store
alter table model.features_municipality_semester
  add column if not exists rent_mid_eur_sqm_month numeric null,
  add column if not exists rent_pct_change_1s numeric null,
  add column if not exists rent_pct_change_2s numeric null,
  add column if not exists rent_volatility_4s numeric null,
  add column if not exists str_adr_eur numeric null,
  add column if not exists str_occupancy_rate numeric null,
  add column if not exists str_rev_par_eur numeric null,
  add column if not exists yield_vs_province_pct numeric null,
  add column if not exists yield_vs_region_pct numeric null;

-- ============================================================================
-- ADMIN SCHEMA - Add rental-specific configs
-- ============================================================================

-- Create a table for yield calculation parameters
create table if not exists admin.yield_parameters (
  parameter_id serial primary key,
  property_segment text not null,
  region_code text null,                    -- null = national default

  -- Cost assumptions for net yield
  vacancy_rate_pct numeric default 5,       -- Expected vacancy %
  management_fee_pct numeric default 8,     -- Property management
  maintenance_pct numeric default 1,        -- Annual maintenance
  insurance_pct numeric default 0.3,        -- Annual insurance
  imu_rate_pct numeric default 0.86,        -- Property tax (IMU) - varies by type
  cedolare_secca_rate numeric default 21,   -- Flat tax on rental income

  -- Assumptions metadata
  valid_from date not null default current_date,
  valid_to date null,
  notes text null,
  created_at timestamptz not null default now(),

  unique (property_segment, region_code, valid_from)
);

-- Insert default national parameters
insert into admin.yield_parameters (property_segment, region_code, vacancy_rate_pct, management_fee_pct, maintenance_pct, insurance_pct, imu_rate_pct, cedolare_secca_rate, notes)
values
  ('residential', null, 5, 8, 1, 0.3, 0.86, 21, 'National average for residential'),
  ('commercial', null, 10, 10, 1.5, 0.5, 1.06, null, 'National average for commercial - no cedolare secca')
on conflict do nothing;

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Function to calculate gross yield
create or replace function model.calculate_gross_yield(
  annual_rent numeric,
  property_value numeric
) returns numeric as $$
begin
  if property_value is null or property_value <= 0 then
    return null;
  end if;
  return round((annual_rent / property_value) * 100, 2);
end;
$$ language plpgsql immutable;

-- Function to calculate net yield (after costs)
create or replace function model.calculate_net_yield(
  monthly_rent numeric,
  property_value numeric,
  vacancy_rate_pct numeric default 5,
  management_fee_pct numeric default 8,
  maintenance_pct numeric default 1,
  insurance_pct numeric default 0.3,
  imu_rate_pct numeric default 0.86
) returns numeric as $$
declare
  annual_gross_rent numeric;
  effective_rent numeric;
  annual_costs numeric;
  net_income numeric;
begin
  if property_value is null or property_value <= 0 or monthly_rent is null then
    return null;
  end if;

  -- Calculate annual gross rent
  annual_gross_rent := monthly_rent * 12;

  -- Apply vacancy rate
  effective_rent := annual_gross_rent * (1 - vacancy_rate_pct / 100);

  -- Calculate annual costs
  annual_costs := (
    (management_fee_pct / 100 * effective_rent) +
    (maintenance_pct / 100 * property_value) +
    (insurance_pct / 100 * property_value) +
    (imu_rate_pct / 100 * property_value)
  );

  -- Net income
  net_income := effective_rent - annual_costs;

  return round((net_income / property_value) * 100, 2);
end;
$$ language plpgsql immutable;

-- Grant execute permissions
grant execute on function model.calculate_gross_yield(numeric, numeric) to anon;
grant execute on function model.calculate_gross_yield(numeric, numeric) to authenticated;
grant execute on function model.calculate_net_yield(numeric, numeric, numeric, numeric, numeric, numeric, numeric) to anon;
grant execute on function model.calculate_net_yield(numeric, numeric, numeric, numeric, numeric, numeric, numeric) to authenticated;

commit;
