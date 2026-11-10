-- ============================================================================
-- Reaggregate Municipality Values from Zone Data
-- ============================================================================
-- This script recalculates mart.municipality_values_semester from
-- mart.omi_zone_values_semester, fixing incorrect aggregations that were
-- based on deleted raw data.
--
-- Run this script to fix municipality-level values.
-- Usage: psql -f reaggregate_from_zones.sql
-- ============================================================================

BEGIN;

-- Ensure helper function exists
CREATE OR REPLACE FUNCTION core.calc_pct_change(new_val numeric, old_val numeric)
RETURNS numeric AS $$
BEGIN
  IF old_val IS NULL OR old_val = 0 THEN
    RETURN NULL;
  END IF;
  RETURN ROUND(((new_val - old_val) / old_val * 100)::numeric, 2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- Step 1: Reaggregate residential values from zone data
-- ============================================================================

-- Use a temp table for the aggregation to avoid locking issues
CREATE TEMP TABLE temp_municipality_values AS
SELECT
  split_part(z.omi_zone_id, '_', 1) AS municipality_id,
  z.period_id,
  z.property_segment,
  -- Value aggregations
  MIN(z.value_min_eur_sqm) AS value_min_eur_sqm,
  MAX(z.value_max_eur_sqm) AS value_max_eur_sqm,
  -- Mid value: average of zone midpoints (simple average, not weighted)
  AVG(z.value_mid_eur_sqm) FILTER (WHERE z.value_mid_eur_sqm IS NOT NULL) AS value_mid_eur_sqm,
  -- Median: percentile of zone midpoints
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY z.value_mid_eur_sqm)
    FILTER (WHERE z.value_mid_eur_sqm IS NOT NULL) AS value_median_eur_sqm,
  -- Rent aggregations
  MIN(z.rent_min_eur_sqm_month) AS rent_min_eur_sqm_month,
  MAX(z.rent_max_eur_sqm_month) AS rent_max_eur_sqm_month,
  AVG(z.rent_mid_eur_sqm_month) FILTER (WHERE z.rent_mid_eur_sqm_month IS NOT NULL) AS rent_mid_eur_sqm_month,
  -- Zone counts (these are now accurate)
  COUNT(*) AS zones_count,
  COUNT(*) FILTER (WHERE z.value_mid_eur_sqm IS NOT NULL) AS zones_with_data,
  -- Data quality: average of zone data quality scores
  AVG(z.data_quality_score) FILTER (WHERE z.data_quality_score IS NOT NULL) AS data_quality_score,
  NOW() AS updated_at
FROM mart.omi_zone_values_semester z
GROUP BY split_part(z.omi_zone_id, '_', 1), z.period_id, z.property_segment;

-- Log what we're about to update
DO $$
DECLARE
  row_count INTEGER;
  period_count INTEGER;
  muni_count INTEGER;
BEGIN
  SELECT COUNT(*), COUNT(DISTINCT period_id), COUNT(DISTINCT municipality_id)
  INTO row_count, period_count, muni_count
  FROM temp_municipality_values;

  RAISE NOTICE 'Prepared % rows for % municipalities across % periods', row_count, muni_count, period_count;
END $$;

-- ============================================================================
-- Step 2: Update municipality_values_semester with correct values
-- ============================================================================

-- Update existing rows
UPDATE mart.municipality_values_semester m
SET
  value_min_eur_sqm = t.value_min_eur_sqm,
  value_max_eur_sqm = t.value_max_eur_sqm,
  value_mid_eur_sqm = t.value_mid_eur_sqm,
  value_median_eur_sqm = t.value_median_eur_sqm,
  rent_min_eur_sqm_month = t.rent_min_eur_sqm_month,
  rent_max_eur_sqm_month = t.rent_max_eur_sqm_month,
  rent_mid_eur_sqm_month = t.rent_mid_eur_sqm_month,
  zones_count = t.zones_count,
  zones_with_data = t.zones_with_data,
  data_quality_score = t.data_quality_score,
  updated_at = t.updated_at
FROM temp_municipality_values t
WHERE m.municipality_id = t.municipality_id
  AND m.period_id = t.period_id
  AND m.property_segment = t.property_segment;

-- Insert any new rows that don't exist yet
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
  t.municipality_id,
  t.period_id,
  t.property_segment,
  t.value_min_eur_sqm,
  t.value_max_eur_sqm,
  t.value_mid_eur_sqm,
  t.value_median_eur_sqm,
  t.rent_min_eur_sqm_month,
  t.rent_max_eur_sqm_month,
  t.rent_mid_eur_sqm_month,
  t.zones_count,
  t.zones_with_data,
  t.data_quality_score,
  t.updated_at
FROM temp_municipality_values t
WHERE NOT EXISTS (
  SELECT 1 FROM mart.municipality_values_semester m
  WHERE m.municipality_id = t.municipality_id
    AND m.period_id = t.period_id
    AND m.property_segment = t.property_segment
);

-- ============================================================================
-- Step 3: Recalculate percentage change metrics
-- ============================================================================

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

-- Clean up
DROP TABLE temp_municipality_values;

COMMIT;

-- ============================================================================
-- Step 4: Summary statistics
-- ============================================================================

SELECT
  'Reaggregation complete' AS status,
  COUNT(DISTINCT municipality_id) AS municipalities,
  COUNT(DISTINCT period_id) AS periods,
  property_segment,
  COUNT(*) AS total_rows
FROM mart.municipality_values_semester
GROUP BY property_segment;

-- Spot check: compare a few municipalities before/after
-- (Uncomment to see sample values)
/*
SELECT
  m.municipality_id,
  m.period_id,
  m.value_mid_eur_sqm AS muni_value,
  m.zones_with_data,
  (
    SELECT AVG(z.value_mid_eur_sqm)
    FROM mart.omi_zone_values_semester z
    WHERE split_part(z.omi_zone_id, '_', 1) = m.municipality_id
      AND z.period_id = m.period_id
      AND z.property_segment = m.property_segment
  ) AS zone_avg
FROM mart.municipality_values_semester m
WHERE m.municipality_id IN ('078138', '052033', '058091', '015146', '001272')
  AND m.period_id = '2026H1'
  AND m.property_segment = 'residential';
*/
