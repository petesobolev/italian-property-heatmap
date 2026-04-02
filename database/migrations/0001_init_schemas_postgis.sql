-- Italian Property Heatmap (MVP) - initial bootstrap migration
-- Creates required schemas and enables PostGIS.

begin;

-- Extensions
create extension if not exists postgis;

-- Schemas
create schema if not exists core;
create schema if not exists raw;
create schema if not exists mart;
create schema if not exists model;
create schema if not exists admin;

-- Core: time periods (minimal seed-ready dimension)
create table if not exists core.time_periods (
  period_id text primary key,            -- e.g. "2024S1", "2024S2", "2024-01"
  period_type text not null,             -- "month" | "semester" | "year"
  period_start_date date not null,
  period_end_date date not null,
  year int not null,
  month int null,
  semester int null,
  created_at timestamptz not null default now(),
  constraint time_periods_type_check check (period_type in ('month', 'semester', 'year')),
  constraint time_periods_month_check check (
    (period_type = 'month' and month between 1 and 12 and semester is null)
    or (period_type <> 'month')
  ),
  constraint time_periods_semester_check check (
    (period_type = 'semester' and semester in (1, 2) and month is null)
    or (period_type <> 'semester')
  )
);

-- Core: municipalities (canonical grain: ISTAT municipality code)
create table if not exists core.municipalities (
  municipality_id text primary key,      -- ISTAT code as text to preserve leading zeros
  municipality_name text not null,
  province_code text null,
  region_code text null,
  coastal_flag boolean not null default false,
  mountain_flag boolean not null default false,
  geom geometry(MultiPolygon, 4326) null,
  geom_simplified geometry(MultiPolygon, 4326) null,
  updated_at timestamptz not null default now()
);

create index if not exists municipalities_geom_gix on core.municipalities using gist (geom);
create index if not exists municipalities_geom_simplified_gix on core.municipalities using gist (geom_simplified);
create index if not exists municipalities_region_idx on core.municipalities (region_code);
create index if not exists municipalities_province_idx on core.municipalities (province_code);

-- Admin: ingestion runs audit (minimal)
create table if not exists admin.ingestion_runs (
  ingestion_run_id bigserial primary key,
  source_name text not null,
  source_version text null,
  status text not null default 'started',
  rows_loaded int not null default 0,
  rows_rejected int not null default 0,
  error_notes text null,
  started_at timestamptz not null default now(),
  finished_at timestamptz null,
  constraint ingestion_runs_status_check check (status in ('started', 'succeeded', 'failed'))
);

-- Admin: model runs audit (minimal)
create table if not exists admin.model_runs (
  model_run_id bigserial primary key,
  model_name text not null,              -- e.g. "appreciation_12m"
  model_version text not null,
  horizon_months int not null default 12,
  status text not null default 'started',
  metrics jsonb null,
  started_at timestamptz not null default now(),
  finished_at timestamptz null,
  constraint model_runs_status_check check (status in ('started', 'succeeded', 'failed'))
);

-- Model: forecasts (minimal shape aligned with spec; expand later)
create table if not exists model.forecasts_municipality (
  municipality_id text not null references core.municipalities(municipality_id),
  forecast_date date not null,
  horizon_months int not null,
  property_segment text not null default 'residential',
  model_version text not null,
  value_mid_eur_sqm numeric null,
  forecast_appreciation_pct numeric null,
  forecast_gross_yield_pct numeric null,
  opportunity_score numeric null,
  confidence_score numeric null,
  drivers jsonb null,
  risks jsonb null,
  publishable_flag boolean not null default true,
  created_at timestamptz not null default now(),
  primary key (municipality_id, forecast_date, horizon_months, property_segment, model_version)
);

create index if not exists forecasts_municipality_latest_idx
  on model.forecasts_municipality (forecast_date desc, horizon_months, property_segment);

commit;

