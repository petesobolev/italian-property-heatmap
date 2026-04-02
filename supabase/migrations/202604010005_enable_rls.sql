-- Enable Row-Level Security on all tables
-- This migration secures all tables with appropriate read policies

BEGIN;

-- ============================================================================
-- CORE SCHEMA - Reference data (publicly readable)
-- ============================================================================

ALTER TABLE core.time_periods ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.regions ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.provinces ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.municipalities ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.municipality_neighbors ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.omi_zones ENABLE ROW LEVEL SECURITY;

-- Allow public read access to core reference data
CREATE POLICY "Allow public read access" ON core.time_periods
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON core.regions
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON core.provinces
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON core.municipalities
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON core.municipality_neighbors
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON core.omi_zones
  FOR SELECT USING (true);

-- ============================================================================
-- MART SCHEMA - Aggregated/curated data (publicly readable)
-- ============================================================================

ALTER TABLE mart.municipality_values_semester ENABLE ROW LEVEL SECURITY;
ALTER TABLE mart.municipality_transactions_semester ENABLE ROW LEVEL SECURITY;
ALTER TABLE mart.omi_zone_values_semester ENABLE ROW LEVEL SECURITY;
ALTER TABLE mart.municipality_rents_month ENABLE ROW LEVEL SECURITY;
ALTER TABLE mart.municipality_demographics_year ENABLE ROW LEVEL SECURITY;
ALTER TABLE mart.municipality_str_month ENABLE ROW LEVEL SECURITY;
ALTER TABLE mart.municipality_str_seasonality ENABLE ROW LEVEL SECURITY;
ALTER TABLE mart.municipality_regulations ENABLE ROW LEVEL SECURITY;

-- Allow public read access to mart data
CREATE POLICY "Allow public read access" ON mart.municipality_values_semester
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON mart.municipality_transactions_semester
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON mart.omi_zone_values_semester
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON mart.municipality_rents_month
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON mart.municipality_demographics_year
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON mart.municipality_str_month
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON mart.municipality_str_seasonality
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON mart.municipality_regulations
  FOR SELECT USING (true);

-- ============================================================================
-- MODEL SCHEMA - Forecasts and features (publicly readable)
-- ============================================================================

ALTER TABLE model.forecasts_municipality ENABLE ROW LEVEL SECURITY;
ALTER TABLE model.features_municipality_semester ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow public read access" ON model.forecasts_municipality
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON model.features_municipality_semester
  FOR SELECT USING (true);

-- ============================================================================
-- RAW SCHEMA - Raw ingested data (publicly readable for transparency)
-- ============================================================================

ALTER TABLE raw.omi_property_values ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw.omi_transactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw.istat_population ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw.istat_tourism ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw.rental_long_term ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw.rental_short_term ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw.regulatory_notes ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow public read access" ON raw.omi_property_values
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON raw.omi_transactions
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON raw.istat_population
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON raw.istat_tourism
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON raw.rental_long_term
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON raw.rental_short_term
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON raw.regulatory_notes
  FOR SELECT USING (true);

-- ============================================================================
-- ADMIN SCHEMA - Administrative/audit data (restricted to service role)
-- ============================================================================

ALTER TABLE admin.ingestion_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE admin.model_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE admin.yield_parameters ENABLE ROW LEVEL SECURITY;

-- Admin tables: no public access, only service_role can access
-- (Service role bypasses RLS, so no policy needed for admin operations)
-- If you need authenticated admin users to access, add policies like:
-- CREATE POLICY "Allow authenticated admin access" ON admin.ingestion_runs
--   FOR ALL USING (auth.role() = 'authenticated' AND auth.jwt() ->> 'role' = 'admin');

COMMIT;
