-- Demo seed data (MVP scaffolding)
-- Inserts 2 municipalities and 1 forecast snapshot each so the map choropleth can render.

begin;

insert into core.municipalities (
  municipality_id,
  municipality_name,
  province_code,
  region_code,
  coastal_flag,
  mountain_flag
)
values
  ('015146', 'Milano', 'MI', 'Lombardia', false, false),
  ('058091', 'Roma', 'RM', 'Lazio', false, false)
on conflict (municipality_id) do update
set municipality_name = excluded.municipality_name,
    province_code = excluded.province_code,
    region_code = excluded.region_code,
    coastal_flag = excluded.coastal_flag,
    mountain_flag = excluded.mountain_flag,
    updated_at = now();

-- One "latest" snapshot date for both rows
insert into model.forecasts_municipality (
  municipality_id,
  forecast_date,
  horizon_months,
  property_segment,
  model_version,
  value_mid_eur_sqm,
  forecast_appreciation_pct,
  forecast_gross_yield_pct,
  opportunity_score,
  confidence_score,
  publishable_flag
)
values
  ('015146', '2026-01-01', 12, 'residential', 'demo_v1', 5200, 3.1, 3.4, 62, 0.72, true),
  ('058091', '2026-01-01', 12, 'residential', 'demo_v1', 4100, 3.8, 3.0, 65, 0.68, true)
on conflict (municipality_id, forecast_date, horizon_months, property_segment, model_version) do update
set value_mid_eur_sqm = excluded.value_mid_eur_sqm,
    forecast_appreciation_pct = excluded.forecast_appreciation_pct,
    forecast_gross_yield_pct = excluded.forecast_gross_yield_pct,
    opportunity_score = excluded.opportunity_score,
    confidence_score = excluded.confidence_score,
    publishable_flag = excluded.publishable_flag;

commit;

