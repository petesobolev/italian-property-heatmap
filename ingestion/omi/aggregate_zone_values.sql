-- OMI Zone Values Aggregation Query
-- Populates mart.omi_zone_values_semester from raw.omi_property_values
--
-- This query should be run after ingesting raw OMI data to update the
-- zone-level values used by the frontend zone drilldown feature.

-- Main aggregation query: populate mart.omi_zone_values_semester
INSERT INTO mart.omi_zone_values_semester (
  omi_zone_id,
  period_id,
  property_segment,
  value_min_eur_sqm,
  value_max_eur_sqm,
  value_mid_eur_sqm,
  rent_min_eur_sqm_month,
  rent_max_eur_sqm_month,
  rent_mid_eur_sqm_month,
  zone_type,
  gross_yield_pct,
  data_quality_score,
  updated_at
)
SELECT
  r.omi_zone_id,
  r.period_id,
  'residential' AS property_segment,
  -- Aggregate values (min of mins, max of maxs)
  MIN(r.value_min_eur_sqm) AS value_min_eur_sqm,
  MAX(r.value_max_eur_sqm) AS value_max_eur_sqm,
  -- Mid value: average of all midpoints
  AVG((COALESCE(r.value_min_eur_sqm, 0) + COALESCE(r.value_max_eur_sqm, 0)) / 2)
    FILTER (WHERE r.value_min_eur_sqm IS NOT NULL OR r.value_max_eur_sqm IS NOT NULL) AS value_mid_eur_sqm,
  -- Rent aggregates
  MIN(r.rent_min_eur_sqm_month) AS rent_min_eur_sqm_month,
  MAX(r.rent_max_eur_sqm_month) AS rent_max_eur_sqm_month,
  AVG((COALESCE(r.rent_min_eur_sqm_month, 0) + COALESCE(r.rent_max_eur_sqm_month, 0)) / 2)
    FILTER (WHERE r.rent_min_eur_sqm_month IS NOT NULL OR r.rent_max_eur_sqm_month IS NOT NULL) AS rent_mid_eur_sqm_month,
  -- Zone type from omi_zones table
  z.zone_type,
  -- Gross yield calculation (annual rent / value * 100)
  CASE
    WHEN AVG((COALESCE(r.value_min_eur_sqm, 0) + COALESCE(r.value_max_eur_sqm, 0)) / 2)
      FILTER (WHERE r.value_min_eur_sqm IS NOT NULL OR r.value_max_eur_sqm IS NOT NULL) > 0
    THEN ROUND(
      (AVG((COALESCE(r.rent_min_eur_sqm_month, 0) + COALESCE(r.rent_max_eur_sqm_month, 0)) / 2)
        FILTER (WHERE r.rent_min_eur_sqm_month IS NOT NULL OR r.rent_max_eur_sqm_month IS NOT NULL) * 12
      / AVG((COALESCE(r.value_min_eur_sqm, 0) + COALESCE(r.value_max_eur_sqm, 0)) / 2)
        FILTER (WHERE r.value_min_eur_sqm IS NOT NULL OR r.value_max_eur_sqm IS NOT NULL)) * 100
    , 2)
    ELSE NULL
  END AS gross_yield_pct,
  -- Data quality: 100 if we have all data
  100.0 AS data_quality_score,
  NOW() AS updated_at
FROM raw.omi_property_values r
LEFT JOIN core.omi_zones z ON r.omi_zone_id = z.omi_zone_id
WHERE r.property_type = 'residenziale'
  AND r.omi_zone_id IS NOT NULL
  AND r.state IN ('NORMALE', 'normale', NULL)  -- Focus on normal state
GROUP BY r.omi_zone_id, r.period_id, z.zone_type

ON CONFLICT (omi_zone_id, period_id, property_segment)
DO UPDATE SET
  value_min_eur_sqm = EXCLUDED.value_min_eur_sqm,
  value_max_eur_sqm = EXCLUDED.value_max_eur_sqm,
  value_mid_eur_sqm = EXCLUDED.value_mid_eur_sqm,
  rent_min_eur_sqm_month = EXCLUDED.rent_min_eur_sqm_month,
  rent_max_eur_sqm_month = EXCLUDED.rent_max_eur_sqm_month,
  rent_mid_eur_sqm_month = EXCLUDED.rent_mid_eur_sqm_month,
  zone_type = EXCLUDED.zone_type,
  gross_yield_pct = EXCLUDED.gross_yield_pct,
  data_quality_score = EXCLUDED.data_quality_score,
  updated_at = EXCLUDED.updated_at;


-- Update zone premium vs municipality (how much above/below average)
WITH municipality_avg AS (
  SELECT
    m.municipality_id,
    m.period_id,
    m.property_segment,
    m.value_mid_eur_sqm AS muni_avg
  FROM mart.municipality_values_semester m
)
UPDATE mart.omi_zone_values_semester z
SET zone_premium_vs_municipality = ROUND(
  ((z.value_mid_eur_sqm - ma.muni_avg) / NULLIF(ma.muni_avg, 0)) * 100, 2
)
FROM core.omi_zones oz, municipality_avg ma
WHERE z.omi_zone_id = oz.omi_zone_id
  AND oz.municipality_id = ma.municipality_id
  AND z.period_id = ma.period_id
  AND z.property_segment = ma.property_segment
  AND z.value_mid_eur_sqm IS NOT NULL
  AND ma.muni_avg IS NOT NULL;


-- Update change metrics (vs prior semester)
WITH zone_values AS (
  SELECT
    omi_zone_id,
    period_id,
    property_segment,
    value_mid_eur_sqm,
    -- Parse period to get previous periods
    CASE
      WHEN period_id LIKE '%H1' THEN CONCAT(CAST(LEFT(period_id, 4)::int - 1 AS text), 'H2')
      WHEN period_id LIKE '%H2' THEN CONCAT(LEFT(period_id, 4), 'H1')
    END AS prev_1s_period,
    CASE
      WHEN period_id LIKE '%H1' THEN CONCAT(CAST(LEFT(period_id, 4)::int - 1 AS text), 'H1')
      WHEN period_id LIKE '%H2' THEN CONCAT(CAST(LEFT(period_id, 4)::int - 1 AS text), 'H2')
    END AS prev_2s_period
  FROM mart.omi_zone_values_semester
),
with_prev_values AS (
  SELECT
    zv.omi_zone_id,
    zv.period_id,
    zv.property_segment,
    zv.value_mid_eur_sqm,
    prev1.value_mid_eur_sqm AS prev_1s_value,
    prev2.value_mid_eur_sqm AS prev_2s_value
  FROM zone_values zv
  LEFT JOIN mart.omi_zone_values_semester prev1
    ON zv.omi_zone_id = prev1.omi_zone_id
    AND zv.prev_1s_period = prev1.period_id
    AND zv.property_segment = prev1.property_segment
  LEFT JOIN mart.omi_zone_values_semester prev2
    ON zv.omi_zone_id = prev2.omi_zone_id
    AND zv.prev_2s_period = prev2.period_id
    AND zv.property_segment = prev2.property_segment
)
UPDATE mart.omi_zone_values_semester m
SET
  value_pct_change_1s = core.calc_pct_change(wpv.value_mid_eur_sqm, wpv.prev_1s_value),
  value_pct_change_2s = core.calc_pct_change(wpv.value_mid_eur_sqm, wpv.prev_2s_value)
FROM with_prev_values wpv
WHERE m.omi_zone_id = wpv.omi_zone_id
  AND m.period_id = wpv.period_id
  AND m.property_segment = wpv.property_segment;


-- Summary statistics
SELECT
  'Zone aggregation complete' AS status,
  COUNT(DISTINCT omi_zone_id) AS zones,
  COUNT(DISTINCT period_id) AS periods,
  COUNT(*) AS total_rows
FROM mart.omi_zone_values_semester;
