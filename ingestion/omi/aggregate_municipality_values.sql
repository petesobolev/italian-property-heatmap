-- OMI Municipality Values Aggregation Query
-- Populates mart.municipality_values_semester from raw.omi_property_values
--
-- This query should be run after ingesting raw OMI data to update the
-- aggregated municipality-level values used by the frontend.

-- First, create a helper function to calculate percentage change
CREATE OR REPLACE FUNCTION core.calc_pct_change(new_val numeric, old_val numeric)
RETURNS numeric AS $$
BEGIN
  IF old_val IS NULL OR old_val = 0 THEN
    RETURN NULL;
  END IF;
  RETURN ROUND(((new_val - old_val) / old_val * 100)::numeric, 2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- Main aggregation query: populate mart.municipality_values_semester
-- This uses INSERT ... ON CONFLICT to handle upserts
INSERT INTO mart.municipality_values_semester (
  municipality_id,
  period_id,
  property_segment,
  value_min_eur_sqm,
  value_max_eur_sqm,
  value_mid_eur_sqm,
  value_median_eur_sqm,
  rent_min_eur_sqm_month,
  rent_max_eur_sqm_month,
  rent_mid_eur_sqm_month,
  zones_count,
  zones_with_data,
  data_quality_score,
  updated_at
)
SELECT
  r.municipality_id,
  r.period_id,
  'residential' AS property_segment,
  -- Aggregate values (min of mins, max of maxs)
  MIN(r.value_min_eur_sqm) AS value_min_eur_sqm,
  MAX(r.value_max_eur_sqm) AS value_max_eur_sqm,
  -- Mid value: average of all midpoints
  AVG((COALESCE(r.value_min_eur_sqm, 0) + COALESCE(r.value_max_eur_sqm, 0)) / 2)
    FILTER (WHERE r.value_min_eur_sqm IS NOT NULL OR r.value_max_eur_sqm IS NOT NULL) AS value_mid_eur_sqm,
  -- Median: use percentile_cont for median calculation
  PERCENTILE_CONT(0.5) WITHIN GROUP (
    ORDER BY (COALESCE(r.value_min_eur_sqm, 0) + COALESCE(r.value_max_eur_sqm, 0)) / 2
  ) FILTER (WHERE r.value_min_eur_sqm IS NOT NULL OR r.value_max_eur_sqm IS NOT NULL) AS value_median_eur_sqm,
  -- Rent aggregates
  MIN(r.rent_min_eur_sqm_month) AS rent_min_eur_sqm_month,
  MAX(r.rent_max_eur_sqm_month) AS rent_max_eur_sqm_month,
  AVG((COALESCE(r.rent_min_eur_sqm_month, 0) + COALESCE(r.rent_max_eur_sqm_month, 0)) / 2)
    FILTER (WHERE r.rent_min_eur_sqm_month IS NOT NULL OR r.rent_max_eur_sqm_month IS NOT NULL) AS rent_mid_eur_sqm_month,
  -- Zone counts
  COUNT(DISTINCT r.omi_zone_id) AS zones_count,
  COUNT(DISTINCT r.omi_zone_id) FILTER (WHERE r.value_min_eur_sqm IS NOT NULL) AS zones_with_data,
  -- Data quality score (% of zones with data)
  ROUND(
    100.0 * COUNT(DISTINCT r.omi_zone_id) FILTER (WHERE r.value_min_eur_sqm IS NOT NULL)
    / NULLIF(COUNT(DISTINCT r.omi_zone_id), 0),
    1
  ) AS data_quality_score,
  NOW() AS updated_at
FROM raw.omi_property_values r
WHERE r.property_type = 'residenziale'
  AND r.state IN ('NORMALE', 'normale', NULL)  -- Focus on normal state
GROUP BY r.municipality_id, r.period_id

ON CONFLICT (municipality_id, period_id, property_segment)
DO UPDATE SET
  value_min_eur_sqm = EXCLUDED.value_min_eur_sqm,
  value_max_eur_sqm = EXCLUDED.value_max_eur_sqm,
  value_mid_eur_sqm = EXCLUDED.value_mid_eur_sqm,
  value_median_eur_sqm = EXCLUDED.value_median_eur_sqm,
  rent_min_eur_sqm_month = EXCLUDED.rent_min_eur_sqm_month,
  rent_max_eur_sqm_month = EXCLUDED.rent_max_eur_sqm_month,
  rent_mid_eur_sqm_month = EXCLUDED.rent_mid_eur_sqm_month,
  zones_count = EXCLUDED.zones_count,
  zones_with_data = EXCLUDED.zones_with_data,
  data_quality_score = EXCLUDED.data_quality_score,
  updated_at = EXCLUDED.updated_at;


-- Update percentage change metrics (requires prior period data)
-- This calculates YoY and semester-over-semester changes
WITH period_values AS (
  SELECT
    municipality_id,
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
  FROM mart.municipality_values_semester
),
with_prev_values AS (
  SELECT
    pv.municipality_id,
    pv.period_id,
    pv.property_segment,
    pv.value_mid_eur_sqm,
    prev1.value_mid_eur_sqm AS prev_1s_value,
    prev2.value_mid_eur_sqm AS prev_2s_value
  FROM period_values pv
  LEFT JOIN mart.municipality_values_semester prev1
    ON pv.municipality_id = prev1.municipality_id
    AND pv.prev_1s_period = prev1.period_id
    AND pv.property_segment = prev1.property_segment
  LEFT JOIN mart.municipality_values_semester prev2
    ON pv.municipality_id = prev2.municipality_id
    AND pv.prev_2s_period = prev2.period_id
    AND pv.property_segment = prev2.property_segment
)
UPDATE mart.municipality_values_semester m
SET
  value_pct_change_1s = core.calc_pct_change(wpv.value_mid_eur_sqm, wpv.prev_1s_value),
  value_pct_change_2s = core.calc_pct_change(wpv.value_mid_eur_sqm, wpv.prev_2s_value)
FROM with_prev_values wpv
WHERE m.municipality_id = wpv.municipality_id
  AND m.period_id = wpv.period_id
  AND m.property_segment = wpv.property_segment;


-- Also aggregate by property segment for commercial properties
INSERT INTO mart.municipality_values_semester (
  municipality_id,
  period_id,
  property_segment,
  value_min_eur_sqm,
  value_max_eur_sqm,
  value_mid_eur_sqm,
  zones_count,
  zones_with_data,
  updated_at
)
SELECT
  r.municipality_id,
  r.period_id,
  'commercial' AS property_segment,
  MIN(r.value_min_eur_sqm) AS value_min_eur_sqm,
  MAX(r.value_max_eur_sqm) AS value_max_eur_sqm,
  AVG((COALESCE(r.value_min_eur_sqm, 0) + COALESCE(r.value_max_eur_sqm, 0)) / 2)
    FILTER (WHERE r.value_min_eur_sqm IS NOT NULL OR r.value_max_eur_sqm IS NOT NULL) AS value_mid_eur_sqm,
  COUNT(DISTINCT r.omi_zone_id) AS zones_count,
  COUNT(DISTINCT r.omi_zone_id) FILTER (WHERE r.value_min_eur_sqm IS NOT NULL) AS zones_with_data,
  NOW() AS updated_at
FROM raw.omi_property_values r
WHERE r.property_type IN ('commerciale', 'negozi', 'uffici')
  AND r.state IN ('NORMALE', 'normale', NULL)
GROUP BY r.municipality_id, r.period_id

ON CONFLICT (municipality_id, period_id, property_segment)
DO UPDATE SET
  value_min_eur_sqm = EXCLUDED.value_min_eur_sqm,
  value_max_eur_sqm = EXCLUDED.value_max_eur_sqm,
  value_mid_eur_sqm = EXCLUDED.value_mid_eur_sqm,
  zones_count = EXCLUDED.zones_count,
  zones_with_data = EXCLUDED.zones_with_data,
  updated_at = EXCLUDED.updated_at;


-- Summary statistics
SELECT
  'Aggregation complete' AS status,
  COUNT(DISTINCT municipality_id) AS municipalities,
  COUNT(DISTINCT period_id) AS periods,
  COUNT(*) AS total_rows
FROM mart.municipality_values_semester;
