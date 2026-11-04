-- Fix opportunity_score calculation to exactly match municipality detail API
-- Uses latest period values and same appreciation calculation

DROP MATERIALIZED VIEW IF EXISTS mart.municipality_rankings_cache;

CREATE MATERIALIZED VIEW mart.municipality_rankings_cache AS
WITH
-- Get distinct periods first
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
-- Get the latest period
latest_period AS (
  SELECT period_id FROM periods WHERE period_rank = 1
),
-- Get the 4 most recent periods for averaging (used for value/yield/change columns)
recent_periods AS (
  SELECT period_id, period_rank FROM periods WHERE period_rank <= 4
),
-- Get latest period data for each municipality (for opportunity score)
latest_values AS (
  SELECT
    v.municipality_id,
    v.value_mid_eur_sqm,
    v.value_min_eur_sqm,
    v.value_max_eur_sqm,
    v.rent_mid_eur_sqm_month,
    v.zones_with_data
  FROM mart.municipality_values_semester v
  JOIN latest_period lp ON v.period_id = lp.period_id
  WHERE v.property_segment = 'residential'
),
-- Get historical values for appreciation calculation (up to 5 periods)
historical_values AS (
  SELECT
    v.municipality_id,
    v.period_id,
    v.value_mid_eur_sqm,
    ROW_NUMBER() OVER (PARTITION BY v.municipality_id ORDER BY v.period_id DESC) as period_rank
  FROM mart.municipality_values_semester v
  WHERE v.property_segment = 'residential'
),
-- Calculate semester-over-semester changes for appreciation
semester_changes AS (
  SELECT
    h1.municipality_id,
    ((h1.value_mid_eur_sqm - h2.value_mid_eur_sqm) / NULLIF(h2.value_mid_eur_sqm, 0)) * 100 as pct_change
  FROM historical_values h1
  JOIN historical_values h2 ON h1.municipality_id = h2.municipality_id
    AND h2.period_rank = h1.period_rank + 1
  WHERE h1.period_rank <= 4  -- Use up to 4 most recent changes
    AND h1.value_mid_eur_sqm > 0
    AND h2.value_mid_eur_sqm > 0
),
-- Calculate annualized appreciation (avg semester change * 2)
appreciation_calc AS (
  SELECT
    municipality_id,
    GREATEST(-30, LEAST(30, AVG(pct_change) * 2)) as appreciation_pct
  FROM semester_changes
  GROUP BY municipality_id
),
-- Aggregate municipality values across 4 periods (for display columns)
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
-- Get earliest and latest values for annualized price change display column
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
-- Get province transaction data
province_sales AS (
  SELECT DISTINCT ON (province_code)
    province_code,
    ntn_per_1000_pop
  FROM mart.province_transactions_semester
  WHERE property_segment = 'residential' AND ntn_per_1000_pop IS NOT NULL
  ORDER BY province_code, period_id DESC
),
-- Calculate opportunity score using LATEST period values (to match sidebar)
opportunity_calc AS (
  SELECT
    lv.municipality_id,
    -- Gross yield from latest period
    CASE
      WHEN lv.rent_mid_eur_sqm_month > 0 AND lv.value_mid_eur_sqm > 0
      THEN (lv.rent_mid_eur_sqm_month * 12 / lv.value_mid_eur_sqm) * 100
      ELSE NULL
    END as latest_gross_yield,
    -- 1. Yield Component (0-35 points): yield * 5, capped at 35
    CASE
      WHEN lv.rent_mid_eur_sqm_month > 0 AND lv.value_mid_eur_sqm > 0
      THEN LEAST(35, (lv.rent_mid_eur_sqm_month * 12 / lv.value_mid_eur_sqm) * 100 * 5)
      ELSE 0
    END as yield_score,
    -- 2. Appreciation Component (0-25 points): peaks at 5%
    CASE
      WHEN ac.appreciation_pct IS NULL THEN 0
      WHEN ac.appreciation_pct >= 0 AND ac.appreciation_pct <= 10
        THEN GREATEST(0, 25 - ABS(ac.appreciation_pct - 5) * 3)
      WHEN ac.appreciation_pct < 0 AND ac.appreciation_pct >= -5
        THEN GREATEST(0, 10 + ac.appreciation_pct * 2)
      ELSE 0
    END as appreciation_score,
    -- 3. Affordability Component (0-20 points): tiered by price
    CASE
      WHEN lv.value_mid_eur_sqm < 1000 THEN 20
      WHEN lv.value_mid_eur_sqm < 2000 THEN 20 - (lv.value_mid_eur_sqm - 1000) / 100.0
      WHEN lv.value_mid_eur_sqm < 4000 THEN 10 - (lv.value_mid_eur_sqm - 2000) / 400.0
      ELSE GREATEST(0, 5 - (lv.value_mid_eur_sqm - 4000) / 2000.0)
    END as affordability_score,
    -- 4. Price Stability Component (0-10 points): based on spread ratio
    CASE
      WHEN lv.value_min_eur_sqm IS NOT NULL AND lv.value_max_eur_sqm IS NOT NULL
           AND lv.value_mid_eur_sqm > 0
      THEN GREATEST(0, 10 - ((lv.value_max_eur_sqm - lv.value_min_eur_sqm) / lv.value_mid_eur_sqm) * 10)
      ELSE 10
    END as stability_score,
    -- 5. Data Quality Component (0-10 points): based on zones with data
    LEAST(10, COALESCE(lv.zones_with_data, 1) / 3.0) as data_quality_score
  FROM latest_values lv
  LEFT JOIN appreciation_calc ac ON lv.municipality_id = ac.municipality_id
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
  ma.periods_count::int,
  -- Opportunity score: sum of all components, clamped to 0-100
  CASE
    WHEN oc.yield_score IS NOT NULL
    THEN LEAST(100, GREATEST(0, ROUND(
      oc.yield_score + oc.appreciation_score + oc.affordability_score +
      oc.stability_score + oc.data_quality_score
    )))
    ELSE NULL
  END::int as opportunity_score
FROM municipality_aggregates ma
JOIN core.municipalities m ON ma.municipality_id = m.municipality_id
JOIN core.regions r ON m.region_code = r.region_code
JOIN core.provinces p ON m.province_code = p.province_code
LEFT JOIN price_change pc ON ma.municipality_id = pc.municipality_id
LEFT JOIN province_sales ps ON m.province_code = ps.province_code
LEFT JOIN opportunity_calc oc ON ma.municipality_id = oc.municipality_id
WHERE ma.avg_value_mid_eur_sqm IS NOT NULL;

-- Create indexes
CREATE INDEX idx_rankings_cache_municipality ON mart.municipality_rankings_cache(municipality_id);
CREATE INDEX idx_rankings_cache_region ON mart.municipality_rankings_cache(region_code);
CREATE INDEX idx_rankings_cache_province ON mart.municipality_rankings_cache(province_code);
CREATE INDEX idx_rankings_cache_value ON mart.municipality_rankings_cache(value_mid_eur_sqm DESC NULLS LAST);
CREATE INDEX idx_rankings_cache_yield ON mart.municipality_rankings_cache(gross_yield_pct DESC NULLS LAST);
CREATE INDEX idx_rankings_cache_change ON mart.municipality_rankings_cache(annualized_price_change_pct DESC NULLS LAST);
CREATE INDEX idx_rankings_cache_sales ON mart.municipality_rankings_cache(ntn_per_1000_pop DESC NULLS LAST);
CREATE INDEX idx_rankings_cache_opportunity ON mart.municipality_rankings_cache(opportunity_score DESC NULLS LAST);
CREATE INDEX idx_rankings_cache_name ON mart.municipality_rankings_cache(municipality_name);
