-- Vehicle Arson Metric Schema
-- This implements the data model from the vehicle arson implementation plan

BEGIN;

-- ============================================================================
-- RAW SCHEMA - Raw incident/event data (minimize personal data)
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw.vehicle_fire_events (
  event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_family text NOT NULL,        -- 'vvf_foia', 'police_proxy', 'press_release', 'news', 'insurance'
  source_name text NOT NULL,
  source_url text,                    -- URL or stable identifier
  published_at timestamptz,
  occurred_at date,                   -- if known, else null
  municipality_id text REFERENCES core.municipalities(municipality_id),
  province_code text,
  region_code text,
  event_type text NOT NULL,           -- 'vehicle_fire', 'vehicle_arson_suspected', 'vehicle_arson_confirmed'
  vehicles_involved integer DEFAULT 1,
  arson_likelihood text,              -- 'probabile_dolo', 'unknown', 'probabile_colpa' (VVF-compatible)
  extractor_version text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS vehicle_fire_events_municipality_idx
  ON raw.vehicle_fire_events (municipality_id);
CREATE INDEX IF NOT EXISTS vehicle_fire_events_occurred_at_idx
  ON raw.vehicle_fire_events (occurred_at);
CREATE INDEX IF NOT EXISTS vehicle_fire_events_source_family_idx
  ON raw.vehicle_fire_events (source_family);

-- ============================================================================
-- MART SCHEMA - Aggregated publishable metrics
-- ============================================================================

-- Municipality-level vehicle arson metrics by year
CREATE TABLE IF NOT EXISTS mart.vehicle_arson_municipality_year (
  municipality_id text NOT NULL REFERENCES core.municipalities(municipality_id),
  year int NOT NULL,
  count_vehicle_fire int,
  count_vehicle_arson_suspected int,
  count_vehicle_arson_confirmed int,
  rate_per_100k_residents numeric,
  confidence_grade text NOT NULL DEFAULT 'C',  -- 'A', 'B', 'C'
  sources_used text[],                          -- e.g., ['vvf_foia', 'press_release']
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (municipality_id, year)
);

CREATE INDEX IF NOT EXISTS vehicle_arson_muni_year_idx
  ON mart.vehicle_arson_municipality_year (year);
CREATE INDEX IF NOT EXISTS vehicle_arson_muni_confidence_idx
  ON mart.vehicle_arson_municipality_year (confidence_grade);

-- Province-level official proxy (nationwide coverage)
CREATE TABLE IF NOT EXISTS mart.arson_proxy_province_year (
  province_code text NOT NULL,
  year int NOT NULL,
  count_incendio int,                           -- INC crime code
  count_danneggiamento_seguito_da_incendio int, -- DSI crime code
  count_total_arson_related int,
  rate_per_100k_residents numeric,
  source_version text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (province_code, year)
);

CREATE INDEX IF NOT EXISTS arson_proxy_province_year_idx
  ON mart.arson_proxy_province_year (year);

-- ============================================================================
-- ROW LEVEL SECURITY
-- ============================================================================

ALTER TABLE raw.vehicle_fire_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE mart.vehicle_arson_municipality_year ENABLE ROW LEVEL SECURITY;
ALTER TABLE mart.arson_proxy_province_year ENABLE ROW LEVEL SECURITY;

-- Read-only public access for mart tables
CREATE POLICY "Allow public read access" ON mart.vehicle_arson_municipality_year
  FOR SELECT USING (true);

CREATE POLICY "Allow public read access" ON mart.arson_proxy_province_year
  FOR SELECT USING (true);

-- Raw events: restrict to service role only (no public access)
-- Service role bypasses RLS

COMMIT;
