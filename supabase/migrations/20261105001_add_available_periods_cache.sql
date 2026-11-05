-- Create materialized view for available periods (faster map loading)
-- This caches the distinct period_id values per property_segment

DROP MATERIALIZED VIEW IF EXISTS mart.available_periods_cache;

CREATE MATERIALIZED VIEW mart.available_periods_cache AS
SELECT DISTINCT property_segment, period_id
FROM mart.municipality_values_semester
WHERE value_mid_eur_sqm IS NOT NULL
ORDER BY property_segment, period_id DESC;

CREATE INDEX available_periods_cache_idx
ON mart.available_periods_cache (property_segment, period_id DESC);

-- Rewrite the function to use the cache (250x faster: 1.7s -> 7ms)
CREATE OR REPLACE FUNCTION mart.get_available_periods(p_segment text)
RETURNS TABLE(period_id text)
LANGUAGE sql STABLE
AS $$
  SELECT apc.period_id
  FROM mart.available_periods_cache apc
  WHERE apc.property_segment = p_segment
  ORDER BY apc.period_id DESC;
$$;

-- Add composite index for common query pattern
CREATE INDEX IF NOT EXISTS municipality_values_period_segment_idx
ON mart.municipality_values_semester (period_id, property_segment);

-- Add covering index for the available periods query
CREATE INDEX IF NOT EXISTS municipality_values_segment_period_covering_idx
ON mart.municipality_values_semester (property_segment, period_id)
WHERE value_mid_eur_sqm IS NOT NULL;

-- Initial refresh
REFRESH MATERIALIZED VIEW mart.available_periods_cache;
