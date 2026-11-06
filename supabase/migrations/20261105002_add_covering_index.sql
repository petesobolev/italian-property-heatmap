-- Add covering composite index for faster map layer queries
-- INCLUDE clause adds columns to index leaf pages for index-only scans
-- This index supports the common query pattern: filter by period+segment, return values

CREATE INDEX CONCURRENTLY IF NOT EXISTS municipality_values_covering_idx
ON mart.municipality_values_semester
(period_id, property_segment, municipality_id)
INCLUDE (value_mid_eur_sqm, rent_mid_eur_sqm_month, value_min_eur_sqm, value_max_eur_sqm)
WHERE value_mid_eur_sqm IS NOT NULL;
