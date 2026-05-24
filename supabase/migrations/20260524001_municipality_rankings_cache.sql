-- Materialized view for fast rankings queries
-- This caches the expensive aggregation calculations for the rankings page

DROP MATERIALIZED VIEW IF EXISTS mart.municipality_rankings_cache;

CREATE MATERIALIZED VIEW mart.municipality_rankings_cache AS
WITH
-- Get distinct periods first (needed for proper ROW_NUMBER)
distinct_periods AS (
  SELECT DISTINCT period_id
  FROM mart.municipality_values_semester
  WHERE property_segment = 'residential'
),
-- Get available periods ordered by recency
periods AS (
  SELECT period_id,
    ROW_NUMBER() OVER (ORDER BY period_id DESC) as period_rank
  FROM distinct_periods
),
-- Get the 4 most recent periods
recent_periods AS (
  SELECT period_id FROM periods WHERE period_rank <= 4
),
-- Aggregate municipality values across periods
municipality_aggregates AS (
  SELECT
    v.municipality_id,
    AVG(v.value_mid_eur_sqm) as avg_value_mid_eur_sqm,
    AVG(v.rent_mid_eur_sqm_month) as avg_rent_mid_eur_sqm_month,
    MAX(v.period_id) as latest_period,
    MIN(v.period_id) as earliest_period,
    COUNT(DISTINCT v.period_id) as periods_count,
    SUM(v.zones_with_data) as zones_with_data
  FROM mart.municipality_values_semester v
  JOIN recent_periods rp ON v.period_id = rp.period_id
  WHERE v.property_segment = 'residential'
  GROUP BY v.municipality_id
),
-- Get earliest and latest values for price change calculation
price_change AS (
  SELECT
    ma.municipality_id,
    (SELECT value_mid_eur_sqm FROM mart.municipality_values_semester
     WHERE municipality_id = ma.municipality_id
       AND period_id = ma.latest_period AND property_segment = 'residential') as latest_value,
    (SELECT value_mid_eur_sqm FROM mart.municipality_values_semester
     WHERE municipality_id = ma.municipality_id
       AND period_id = ma.earliest_period AND property_segment = 'residential') as earliest_value,
    ma.periods_count
  FROM municipality_aggregates ma
),
-- Get province transaction data (using latest available period)
province_sales AS (
  SELECT DISTINCT ON (province_code)
    province_code,
    ntn_per_1000_pop
  FROM mart.province_transactions_semester
  WHERE property_segment = 'residential' AND ntn_per_1000_pop IS NOT NULL
  ORDER BY province_code, period_id DESC
)
SELECT
  m.municipality_id,
  m.municipality_name,
  m.region_code,
  r.region_name,
  m.province_code,
  p.province_name,
  m.coastal_flag,
  m.mountain_flag,
  ma.avg_value_mid_eur_sqm as value_mid_eur_sqm,
  ma.avg_rent_mid_eur_sqm_month as rent_mid_eur_sqm_month,
  CASE
    WHEN ma.avg_rent_mid_eur_sqm_month > 0 AND ma.avg_value_mid_eur_sqm > 0
    THEN (ma.avg_rent_mid_eur_sqm_month * 12 / ma.avg_value_mid_eur_sqm) * 100
    ELSE NULL
  END as gross_yield_pct,
  CASE
    WHEN pc.periods_count > 1 AND pc.earliest_value > 0 AND pc.latest_value > 0
         AND pc.earliest_value != pc.latest_value
    THEN (POWER(pc.latest_value / pc.earliest_value, 2.0 / (pc.periods_count - 1)) - 1) * 100
    ELSE NULL
  END as annualized_price_change_pct,
  ps.ntn_per_1000_pop,
  ma.zones_with_data::int,
  ma.latest_period,
  ma.earliest_period,
  ma.periods_count::int
FROM municipality_aggregates ma
JOIN core.municipalities m ON ma.municipality_id = m.municipality_id
JOIN core.regions r ON m.region_code = r.region_code
JOIN core.provinces p ON m.province_code = p.province_code
LEFT JOIN price_change pc ON ma.municipality_id = pc.municipality_id
LEFT JOIN province_sales ps ON m.province_code = ps.province_code
WHERE ma.avg_value_mid_eur_sqm IS NOT NULL;

-- Create indexes for fast lookups
CREATE INDEX idx_rankings_cache_municipality ON mart.municipality_rankings_cache(municipality_id);
CREATE INDEX idx_rankings_cache_region ON mart.municipality_rankings_cache(region_code);
CREATE INDEX idx_rankings_cache_province ON mart.municipality_rankings_cache(province_code);
CREATE INDEX idx_rankings_cache_value ON mart.municipality_rankings_cache(value_mid_eur_sqm DESC NULLS LAST);
CREATE INDEX idx_rankings_cache_yield ON mart.municipality_rankings_cache(gross_yield_pct DESC NULLS LAST);
CREATE INDEX idx_rankings_cache_change ON mart.municipality_rankings_cache(annualized_price_change_pct DESC NULLS LAST);
CREATE INDEX idx_rankings_cache_sales ON mart.municipality_rankings_cache(ntn_per_1000_pop DESC NULLS LAST);
CREATE INDEX idx_rankings_cache_name ON mart.municipality_rankings_cache(municipality_name);

-- NOTE: This view should be refreshed after new OMI data is ingested:
-- REFRESH MATERIALIZED VIEW mart.municipality_rankings_cache;
