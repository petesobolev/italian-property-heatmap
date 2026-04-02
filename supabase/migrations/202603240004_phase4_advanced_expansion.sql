-- Italian Property Heatmap - Phase 4: Advanced Expansion
-- Adds short-term rental modeling, regulation scoring, and OMI zone drilldown

begin;

-- ============================================================================
-- SHORT-TERM RENTAL MODELING
-- ============================================================================

-- Mart: Detailed short-term rental data by municipality and month
create table if not exists mart.municipality_str_month (
  municipality_id text not null references core.municipalities(municipality_id),
  period_id text not null,                    -- Month: e.g. "2024-07"

  -- Core STR metrics
  adr_eur numeric null,                       -- Average Daily Rate
  adr_median_eur numeric null,
  adr_percentile_25 numeric null,
  adr_percentile_75 numeric null,

  -- Occupancy metrics
  occupancy_rate numeric null,                -- 0-1
  occupancy_median numeric null,

  -- Revenue metrics
  rev_par_eur numeric null,                   -- Revenue per Available Room (ADR * occupancy)
  monthly_revenue_avg_eur numeric null,
  annual_revenue_estimate_eur numeric null,

  -- Supply metrics
  active_listings_count int null,
  new_listings_count int null,
  delisted_count int null,
  entire_home_pct numeric null,               -- % that are entire homes vs rooms

  -- Property breakdown
  avg_bedrooms numeric null,
  avg_guests_capacity numeric null,
  avg_minimum_nights numeric null,

  -- Seasonality metrics
  is_peak_season boolean null,
  seasonality_factor numeric null,            -- Multiplier vs annual average (1.0 = average)
  peak_month_flag boolean null,
  shoulder_month_flag boolean null,
  off_peak_month_flag boolean null,

  -- Competitive metrics
  review_score_avg numeric null,              -- Average review score
  superhost_pct numeric null,                 -- % of listings by superhosts
  instant_book_pct numeric null,

  -- Change metrics
  adr_pct_change_1m numeric null,
  adr_pct_change_12m numeric null,
  occupancy_pct_change_1m numeric null,
  occupancy_pct_change_12m numeric null,
  listings_pct_change_12m numeric null,

  -- Yield calculation (using property values from mart.municipality_values_semester)
  str_gross_yield_pct numeric null,           -- Annual revenue / property value
  str_net_yield_pct numeric null,             -- After STR-specific costs
  yield_vs_long_term_pct numeric null,        -- Premium vs long-term rental yield

  -- Data quality
  data_source text null,                      -- 'airdna', 'mashvisor', 'scraped', 'demo'
  sample_size int null,
  data_quality_score numeric null,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (municipality_id, period_id)
);

create index if not exists municipality_str_period_idx
  on mart.municipality_str_month (period_id);
create index if not exists municipality_str_occupancy_idx
  on mart.municipality_str_month (occupancy_rate) where occupancy_rate is not null;
create index if not exists municipality_str_yield_idx
  on mart.municipality_str_month (str_gross_yield_pct) where str_gross_yield_pct is not null;

-- Mart: STR seasonality profile per municipality (annual summary)
create table if not exists mart.municipality_str_seasonality (
  municipality_id text not null references core.municipalities(municipality_id),
  reference_year int not null,

  -- Annual averages
  annual_avg_adr_eur numeric null,
  annual_avg_occupancy numeric null,
  annual_avg_rev_par_eur numeric null,
  total_annual_revenue_estimate_eur numeric null,

  -- Seasonality analysis
  seasonality_score numeric null,             -- 0-100, higher = more seasonal
  peak_months text[] null,                    -- e.g. ['07', '08']
  shoulder_months text[] null,
  off_peak_months text[] null,

  -- Peak vs off-peak ratios
  peak_to_offpeak_adr_ratio numeric null,
  peak_to_offpeak_occupancy_ratio numeric null,
  peak_to_offpeak_revenue_ratio numeric null,

  -- Monthly breakdown (for charts)
  monthly_adr_profile jsonb null,             -- {"01": 80, "02": 85, ...}
  monthly_occupancy_profile jsonb null,
  monthly_revenue_profile jsonb null,

  -- Booking window
  avg_booking_lead_days numeric null,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (municipality_id, reference_year)
);

-- Add STR yield columns to forecasts table
alter table model.forecasts_municipality
  add column if not exists forecast_str_yield_pct numeric null,
  add column if not exists str_vs_ltr_premium_pct numeric null,
  add column if not exists str_opportunity_score numeric null,
  add column if not exists str_risk_score numeric null,
  add column if not exists str_seasonality_score numeric null;

-- ============================================================================
-- REGULATION SCORING
-- ============================================================================

-- Raw: Regulatory notes (manual curation)
create table if not exists raw.regulatory_notes (
  id bigserial primary key,
  municipality_id text not null references core.municipalities(municipality_id),

  -- Regulation type
  regulation_type text not null,              -- 'str_license', 'str_limit', 'heritage', 'rent_control', 'building_code', 'zoning'
  regulation_category text null,              -- For grouping similar regulations

  -- Details
  title text not null,
  description text null,
  source_url text null,
  source_document text null,

  -- Severity and impact
  severity text not null default 'medium',    -- 'low', 'medium', 'high', 'critical'
  impact_score numeric null,                  -- 0-100

  -- Dates
  effective_date date null,
  expiry_date date null,
  last_verified_date date null,

  -- STR-specific fields
  str_license_required boolean null,
  str_license_fee_eur numeric null,
  str_max_days_per_year int null,             -- e.g. 90 days in some cities
  str_zone_restricted boolean null,           -- Restricted in certain zones
  str_new_permits_suspended boolean null,     -- Moratorium on new STR permits

  -- Heritage restrictions
  heritage_zone_flag boolean null,
  heritage_restrictions text[] null,          -- ['facade_preservation', 'no_external_signs', etc.]

  -- Rent control
  rent_control_active boolean null,
  rent_ceiling_eur_sqm numeric null,

  -- Metadata
  verified_by text null,
  notes text null,
  raw_data jsonb null,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists regulatory_notes_municipality_idx
  on raw.regulatory_notes (municipality_id);
create index if not exists regulatory_notes_type_idx
  on raw.regulatory_notes (regulation_type);
create index if not exists regulatory_notes_severity_idx
  on raw.regulatory_notes (severity);

-- Mart: Municipality regulation summary scores
create table if not exists mart.municipality_regulations (
  municipality_id text not null references core.municipalities(municipality_id) primary key,

  -- Overall regulation risk score (0-100, higher = more restrictive)
  regulation_risk_score numeric null,
  regulation_risk_level text null,            -- 'low', 'medium', 'high', 'critical'

  -- Component scores
  str_regulation_score numeric null,          -- STR-specific restrictions
  heritage_score numeric null,                -- Heritage/historic restrictions
  building_code_score numeric null,           -- Building regulations
  rent_control_score numeric null,            -- Rent control severity

  -- STR regulation summary
  str_license_required boolean default false,
  str_max_days_per_year int null,
  str_new_permits_allowed boolean default true,
  str_zones_restricted boolean default false,

  -- Heritage summary
  has_heritage_zones boolean default false,
  heritage_zone_pct numeric null,             -- % of municipality in heritage zones

  -- Rent control summary
  has_rent_control boolean default false,

  -- Active regulations count
  active_regulations_count int default 0,
  critical_regulations_count int default 0,

  -- Recent changes
  last_regulation_change_date date null,
  recent_changes_summary text null,

  -- Risk factors (for display)
  risk_factors jsonb null,                    -- [{"factor": "str_limit", "severity": "high", "description": "..."}, ...]

  -- Investment implications
  investor_warning_level text null,           -- 'none', 'caution', 'warning', 'avoid'
  investor_notes text null,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists municipality_regulations_risk_idx
  on mart.municipality_regulations (regulation_risk_score);
create index if not exists municipality_regulations_level_idx
  on mart.municipality_regulations (regulation_risk_level);

-- ============================================================================
-- OMI ZONE DRILLDOWN
-- ============================================================================

-- Mart: OMI zone values by semester (sub-municipality level)
create table if not exists mart.omi_zone_values_semester (
  omi_zone_id text not null references core.omi_zones(omi_zone_id),
  period_id text not null references core.time_periods(period_id),
  property_segment text not null default 'residential',

  -- Values (directly from OMI)
  value_min_eur_sqm numeric null,
  value_max_eur_sqm numeric null,
  value_mid_eur_sqm numeric null,

  -- Rents
  rent_min_eur_sqm_month numeric null,
  rent_max_eur_sqm_month numeric null,
  rent_mid_eur_sqm_month numeric null,

  -- Change metrics
  value_pct_change_1s numeric null,
  value_pct_change_2s numeric null,           -- YoY

  -- Zone context
  zone_type text null,                        -- Centrale, Semicentrale, Periferica
  zone_premium_vs_municipality numeric null,  -- % above/below municipality average

  -- Yield
  gross_yield_pct numeric null,

  -- Data quality
  data_quality_score numeric null,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (omi_zone_id, period_id, property_segment)
);

create index if not exists omi_zone_values_period_idx
  on mart.omi_zone_values_semester (period_id);
create index if not exists omi_zone_values_segment_idx
  on mart.omi_zone_values_semester (property_segment);

-- Add zone_type classification to core.omi_zones if not exists
alter table core.omi_zones
  add column if not exists zone_classification text null,  -- 'central', 'semicentral', 'peripheral', 'rural'
  add column if not exists zone_premium_typical numeric null,
  add column if not exists zone_population_estimate int null,
  add column if not exists zone_area_sqkm numeric null;

-- ============================================================================
-- FEATURE STORE ADDITIONS
-- ============================================================================

-- Add STR and regulation features to feature store
alter table model.features_municipality_semester
  add column if not exists str_adr_avg_eur numeric null,
  add column if not exists str_occupancy_avg numeric null,
  add column if not exists str_rev_par_avg_eur numeric null,
  add column if not exists str_gross_yield_pct numeric null,
  add column if not exists str_seasonality_score numeric null,
  add column if not exists str_listings_count int null,
  add column if not exists str_listings_growth_pct numeric null,
  add column if not exists regulation_risk_score numeric null,
  add column if not exists str_regulation_score numeric null,
  add column if not exists heritage_score numeric null;

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Function to calculate STR gross yield
create or replace function model.calculate_str_gross_yield(
  annual_revenue numeric,
  property_value numeric
) returns numeric as $$
begin
  if property_value is null or property_value <= 0 then
    return null;
  end if;
  return round((annual_revenue / property_value) * 100, 2);
end;
$$ language plpgsql immutable;

-- Function to calculate STR net yield (after STR-specific costs)
create or replace function model.calculate_str_net_yield(
  annual_revenue numeric,
  property_value numeric,
  platform_fee_pct numeric default 3,         -- Airbnb host fee
  cleaning_per_booking_eur numeric default 50,
  bookings_per_year int default 40,
  utilities_monthly_eur numeric default 150,
  management_fee_pct numeric default 20,      -- Higher for STR
  vacancy_rate_pct numeric default 30,        -- STR has higher vacancy
  insurance_pct numeric default 0.5,          -- Higher for STR
  imu_rate_pct numeric default 0.86,
  cedolare_secca_rate numeric default 21
) returns numeric as $$
declare
  gross_revenue numeric;
  platform_fees numeric;
  cleaning_costs numeric;
  utilities numeric;
  management_costs numeric;
  insurance numeric;
  property_tax numeric;
  income_tax numeric;
  net_income numeric;
begin
  if property_value is null or property_value <= 0 or annual_revenue is null then
    return null;
  end if;

  gross_revenue := annual_revenue;

  -- Deduct costs
  platform_fees := gross_revenue * (platform_fee_pct / 100);
  cleaning_costs := cleaning_per_booking_eur * bookings_per_year;
  utilities := utilities_monthly_eur * 12;
  management_costs := gross_revenue * (management_fee_pct / 100);
  insurance := property_value * (insurance_pct / 100);
  property_tax := property_value * (imu_rate_pct / 100);

  -- Taxable income (after deductible costs)
  income_tax := (gross_revenue - platform_fees - cleaning_costs - management_costs) * (cedolare_secca_rate / 100);

  net_income := gross_revenue - platform_fees - cleaning_costs - utilities - management_costs - insurance - property_tax - income_tax;

  return round((net_income / property_value) * 100, 2);
end;
$$ language plpgsql immutable;

-- Function to calculate seasonality score (0-100)
create or replace function model.calculate_seasonality_score(
  monthly_revenues numeric[]  -- Array of 12 monthly revenues
) returns numeric as $$
declare
  avg_revenue numeric;
  variance numeric;
  coefficient_of_variation numeric;
begin
  if monthly_revenues is null or array_length(monthly_revenues, 1) != 12 then
    return null;
  end if;

  -- Calculate average
  select avg(r) into avg_revenue from unnest(monthly_revenues) as r where r is not null;

  if avg_revenue is null or avg_revenue = 0 then
    return null;
  end if;

  -- Calculate variance
  select avg(power(r - avg_revenue, 2)) into variance
  from unnest(monthly_revenues) as r where r is not null;

  -- Coefficient of variation (normalized std dev)
  coefficient_of_variation := sqrt(variance) / avg_revenue;

  -- Convert to 0-100 score (higher CV = higher seasonality)
  -- Typical range: 0.1 (low seasonality) to 0.8 (high seasonality)
  return round(least(coefficient_of_variation / 0.8 * 100, 100), 1);
end;
$$ language plpgsql immutable;

-- Function to calculate regulation risk score
create or replace function model.calculate_regulation_risk_score(
  str_license_required boolean,
  str_max_days_per_year int,
  str_new_permits_allowed boolean,
  has_heritage_zones boolean,
  has_rent_control boolean,
  critical_regulations_count int
) returns numeric as $$
declare
  score numeric := 0;
begin
  -- STR license requirement
  if str_license_required then
    score := score + 15;
  end if;

  -- STR day limits
  if str_max_days_per_year is not null then
    if str_max_days_per_year <= 30 then
      score := score + 30;
    elsif str_max_days_per_year <= 90 then
      score := score + 20;
    elsif str_max_days_per_year <= 180 then
      score := score + 10;
    end if;
  end if;

  -- New permits suspended
  if not coalesce(str_new_permits_allowed, true) then
    score := score + 25;
  end if;

  -- Heritage zones
  if has_heritage_zones then
    score := score + 10;
  end if;

  -- Rent control
  if has_rent_control then
    score := score + 10;
  end if;

  -- Critical regulations
  score := score + least(coalesce(critical_regulations_count, 0) * 5, 20);

  return least(score, 100);
end;
$$ language plpgsql immutable;

-- Grant execute permissions
grant execute on function model.calculate_str_gross_yield(numeric, numeric) to anon;
grant execute on function model.calculate_str_gross_yield(numeric, numeric) to authenticated;
grant execute on function model.calculate_str_net_yield(numeric, numeric, numeric, numeric, int, numeric, numeric, numeric, numeric, numeric, numeric) to anon;
grant execute on function model.calculate_str_net_yield(numeric, numeric, numeric, numeric, int, numeric, numeric, numeric, numeric, numeric, numeric) to authenticated;
grant execute on function model.calculate_seasonality_score(numeric[]) to anon;
grant execute on function model.calculate_seasonality_score(numeric[]) to authenticated;
grant execute on function model.calculate_regulation_risk_score(boolean, int, boolean, boolean, boolean, int) to anon;
grant execute on function model.calculate_regulation_risk_score(boolean, int, boolean, boolean, boolean, int) to authenticated;

-- ============================================================================
-- API FUNCTIONS
-- ============================================================================

-- Function to get OMI zones as GeoJSON for a municipality
create or replace function public.get_omi_zones_geojson(
  p_municipality_id text
) returns jsonb as $$
declare
  result jsonb;
begin
  select jsonb_build_object(
    'type', 'FeatureCollection',
    'features', coalesce(jsonb_agg(
      jsonb_build_object(
        'type', 'Feature',
        'properties', jsonb_build_object(
          'omi_zone_id', z.omi_zone_id,
          'zone_code', z.zone_code,
          'zone_type', z.zone_type,
          'zone_description', z.zone_description,
          'zone_classification', z.zone_classification
        ),
        'geometry', ST_AsGeoJSON(z.geom)::jsonb
      )
    ), '[]'::jsonb)
  ) into result
  from core.omi_zones z
  where z.municipality_id = p_municipality_id
    and z.geom is not null;

  return result;
end;
$$ language plpgsql stable security definer;

grant execute on function public.get_omi_zones_geojson(text) to anon;
grant execute on function public.get_omi_zones_geojson(text) to authenticated;

commit;
