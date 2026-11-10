-- ============================================================================
-- Reaggregate Municipality Values from Zone Data (BATCHED VERSION)
-- ============================================================================
-- This script recalculates mart.municipality_values_semester from
-- mart.omi_zone_values_semester in batches to avoid statement timeouts.
-- ============================================================================

-- Increase statement timeout for this session
SET statement_timeout = '10min';

-- ============================================================================
-- Step 1: Create temp table with reaggregated values
-- ============================================================================

DROP TABLE IF EXISTS temp_reaggregated_values;

CREATE TEMP TABLE temp_reaggregated_values AS
SELECT
  split_part(z.omi_zone_id, '_', 1) AS municipality_id,
  z.period_id,
  z.property_segment,
  MIN(z.value_min_eur_sqm) AS value_min_eur_sqm,
  MAX(z.value_max_eur_sqm) AS value_max_eur_sqm,
  AVG(z.value_mid_eur_sqm) FILTER (WHERE z.value_mid_eur_sqm IS NOT NULL) AS value_mid_eur_sqm,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY z.value_mid_eur_sqm)
    FILTER (WHERE z.value_mid_eur_sqm IS NOT NULL) AS value_median_eur_sqm,
  MIN(z.rent_min_eur_sqm_month) AS rent_min_eur_sqm_month,
  MAX(z.rent_max_eur_sqm_month) AS rent_max_eur_sqm_month,
  AVG(z.rent_mid_eur_sqm_month) FILTER (WHERE z.rent_mid_eur_sqm_month IS NOT NULL) AS rent_mid_eur_sqm_month,
  COUNT(*) AS zones_count,
  COUNT(*) FILTER (WHERE z.value_mid_eur_sqm IS NOT NULL) AS zones_with_data,
  AVG(z.data_quality_score) FILTER (WHERE z.data_quality_score IS NOT NULL) AS data_quality_score
FROM mart.omi_zone_values_semester z
GROUP BY split_part(z.omi_zone_id, '_', 1), z.period_id, z.property_segment;

CREATE INDEX ON temp_reaggregated_values (municipality_id, period_id, property_segment);

-- Log what we created
DO $$
DECLARE
  row_count INTEGER;
BEGIN
  SELECT COUNT(*) INTO row_count FROM temp_reaggregated_values;
  RAISE NOTICE 'Created temp table with % rows', row_count;
END $$;

-- ============================================================================
-- Step 2: Batch update - Process one period at a time
-- ============================================================================

DO $$
DECLARE
  period_rec RECORD;
  updated_count INTEGER;
  total_updated INTEGER := 0;
BEGIN
  FOR period_rec IN
    SELECT DISTINCT period_id
    FROM mart.municipality_values_semester
    ORDER BY period_id DESC
  LOOP
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
      updated_at = NOW()
    FROM temp_reaggregated_values t
    WHERE m.municipality_id = t.municipality_id
      AND m.period_id = t.period_id
      AND m.property_segment = t.property_segment
      AND m.period_id = period_rec.period_id;

    GET DIAGNOSTICS updated_count = ROW_COUNT;
    total_updated := total_updated + updated_count;
    RAISE NOTICE 'Updated % rows for period %', updated_count, period_rec.period_id;
  END LOOP;

  RAISE NOTICE 'Total rows updated: %', total_updated;
END $$;

-- ============================================================================
-- Step 3: Insert any new records that don't exist yet (also batched)
-- ============================================================================

DO $$
DECLARE
  period_rec RECORD;
  inserted_count INTEGER;
  total_inserted INTEGER := 0;
BEGIN
  FOR period_rec IN
    SELECT DISTINCT period_id FROM temp_reaggregated_values ORDER BY period_id DESC
  LOOP
    INSERT INTO mart.municipality_values_semester (
      municipality_id, period_id, property_segment,
      value_min_eur_sqm, value_max_eur_sqm, value_mid_eur_sqm, value_median_eur_sqm,
      rent_min_eur_sqm_month, rent_max_eur_sqm_month, rent_mid_eur_sqm_month,
      zones_count, zones_with_data, data_quality_score, updated_at
    )
    SELECT
      t.municipality_id, t.period_id, t.property_segment,
      t.value_min_eur_sqm, t.value_max_eur_sqm, t.value_mid_eur_sqm, t.value_median_eur_sqm,
      t.rent_min_eur_sqm_month, t.rent_max_eur_sqm_month, t.rent_mid_eur_sqm_month,
      t.zones_count, t.zones_with_data, t.data_quality_score, NOW()
    FROM temp_reaggregated_values t
    WHERE t.period_id = period_rec.period_id
      AND NOT EXISTS (
        SELECT 1 FROM mart.municipality_values_semester m
        WHERE m.municipality_id = t.municipality_id
          AND m.period_id = t.period_id
          AND m.property_segment = t.property_segment
      );

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    total_inserted := total_inserted + inserted_count;
  END LOOP;

  RAISE NOTICE 'Total rows inserted: %', total_inserted;
END $$;

-- ============================================================================
-- Step 4: Recalculate percentage changes (also batched by period)
-- ============================================================================

-- First ensure the helper function exists
CREATE OR REPLACE FUNCTION core.calc_pct_change(new_val numeric, old_val numeric)
RETURNS numeric AS $$
BEGIN
  IF old_val IS NULL OR old_val = 0 THEN
    RETURN NULL;
  END IF;
  RETURN ROUND(((new_val - old_val) / old_val * 100)::numeric, 2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

DO $$
DECLARE
  period_rec RECORD;
  updated_count INTEGER;
BEGIN
  FOR period_rec IN
    SELECT DISTINCT period_id
    FROM mart.municipality_values_semester
    ORDER BY period_id DESC
  LOOP
    WITH period_values AS (
      SELECT
        municipality_id,
        period_id,
        property_segment,
        value_mid_eur_sqm,
        CASE
          WHEN period_id LIKE '%H1' THEN CONCAT(CAST(LEFT(period_id, 4)::int - 1 AS text), 'H2')
          WHEN period_id LIKE '%H2' THEN CONCAT(LEFT(period_id, 4), 'H1')
        END AS prev_1s_period,
        CASE
          WHEN period_id LIKE '%H1' THEN CONCAT(CAST(LEFT(period_id, 4)::int - 1 AS text), 'H1')
          WHEN period_id LIKE '%H2' THEN CONCAT(CAST(LEFT(period_id, 4)::int - 1 AS text), 'H2')
        END AS prev_2s_period
      FROM mart.municipality_values_semester
      WHERE period_id = period_rec.period_id
    ),
    with_prev AS (
      SELECT
        pv.municipality_id, pv.period_id, pv.property_segment,
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
      value_pct_change_1s = core.calc_pct_change(w.value_mid_eur_sqm, w.prev_1s_value),
      value_pct_change_2s = core.calc_pct_change(w.value_mid_eur_sqm, w.prev_2s_value)
    FROM with_prev w
    WHERE m.municipality_id = w.municipality_id
      AND m.period_id = w.period_id
      AND m.property_segment = w.property_segment;

    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RAISE NOTICE 'Updated pct changes for period %: % rows', period_rec.period_id, updated_count;
  END LOOP;
END $$;

-- Clean up temp table
DROP TABLE IF EXISTS temp_reaggregated_values;

-- ============================================================================
-- Step 5: Summary
-- ============================================================================

SELECT
  'Reaggregation complete' AS status,
  COUNT(DISTINCT municipality_id) AS municipalities,
  COUNT(DISTINCT period_id) AS periods,
  property_segment,
  COUNT(*) AS total_rows
FROM mart.municipality_values_semester
GROUP BY property_segment;

-- Spot check a few municipalities
SELECT
  m.municipality_id,
  CASE m.municipality_id
    WHEN '078138' THEN 'Scalea'
    WHEN '052033' THEN 'Torrita di Siena'
    WHEN '058091' THEN 'Roma'
    WHEN '015146' THEN 'Milano'
    WHEN '001272' THEN 'Torino'
  END AS name,
  m.value_mid_eur_sqm AS muni_value,
  m.zones_with_data,
  (
    SELECT ROUND(AVG(z.value_mid_eur_sqm)::numeric, 2)
    FROM mart.omi_zone_values_semester z
    WHERE split_part(z.omi_zone_id, '_', 1) = m.municipality_id
      AND z.period_id = m.period_id
      AND z.property_segment = m.property_segment
  ) AS zone_avg,
  m.value_pct_change_2s AS yoy_change
FROM mart.municipality_values_semester m
WHERE m.municipality_id IN ('078138', '052033', '058091', '015146', '001272')
  AND m.period_id = '2026H1'
  AND m.property_segment = 'residential'
ORDER BY m.municipality_id;
