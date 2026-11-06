-- RPC function for efficient map layer metric calculations
-- Replaces multiple batch fetches + JS calculations with a single SQL query
-- Handles: value_mid_eur_sqm, rent_mid_eur_sqm_month, gross_yield_pct,
--          condition_premium_pct, price_variance_pct

CREATE OR REPLACE FUNCTION mart.get_layer_metric_values(
  p_metric text,
  p_segment text,
  p_semesters_to_average int DEFAULT 1
)
RETURNS TABLE(
  municipality_id text,
  metric_value numeric,
  semesters_with_data int
)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  v_periods text[];
BEGIN
  -- Get the periods to average (most recent N semesters)
  SELECT ARRAY_AGG(apc.period_id ORDER BY apc.period_id DESC)
  INTO v_periods
  FROM (
    SELECT period_id
    FROM mart.available_periods_cache
    WHERE property_segment = p_segment
    ORDER BY period_id DESC
    LIMIT p_semesters_to_average
  ) apc;

  IF v_periods IS NULL OR array_length(v_periods, 1) = 0 THEN
    RETURN;
  END IF;

  -- Return appropriate metric based on p_metric
  IF p_metric = 'value_mid_eur_sqm' THEN
    RETURN QUERY
    SELECT
      mv.municipality_id,
      AVG(mv.value_mid_eur_sqm)::numeric AS metric_value,
      COUNT(*)::int AS semesters_with_data
    FROM mart.municipality_values_semester mv
    WHERE mv.period_id = ANY(v_periods)
      AND mv.property_segment = p_segment
      AND mv.value_mid_eur_sqm IS NOT NULL
    GROUP BY mv.municipality_id;

  ELSIF p_metric = 'rent_mid_eur_sqm_month' THEN
    RETURN QUERY
    SELECT
      mv.municipality_id,
      AVG(mv.rent_mid_eur_sqm_month)::numeric AS metric_value,
      COUNT(*)::int AS semesters_with_data
    FROM mart.municipality_values_semester mv
    WHERE mv.period_id = ANY(v_periods)
      AND mv.property_segment = p_segment
      AND mv.rent_mid_eur_sqm_month IS NOT NULL
    GROUP BY mv.municipality_id;

  ELSIF p_metric = 'gross_yield_pct' THEN
    -- Gross yield = (annual rent / price) * 100
    RETURN QUERY
    SELECT
      mv.municipality_id,
      CASE
        WHEN AVG(mv.value_mid_eur_sqm) > 0
        THEN (AVG(mv.rent_mid_eur_sqm_month) * 12 / AVG(mv.value_mid_eur_sqm) * 100)::numeric
        ELSE NULL
      END AS metric_value,
      COUNT(*)::int AS semesters_with_data
    FROM mart.municipality_values_semester mv
    WHERE mv.period_id = ANY(v_periods)
      AND mv.property_segment = p_segment
      AND mv.value_mid_eur_sqm IS NOT NULL
      AND mv.rent_mid_eur_sqm_month IS NOT NULL
    GROUP BY mv.municipality_id
    HAVING AVG(mv.value_mid_eur_sqm) > 0;

  ELSIF p_metric = 'condition_premium_pct' THEN
    RETURN QUERY
    SELECT
      mv.municipality_id,
      AVG(mv.condition_premium_pct)::numeric AS metric_value,
      COUNT(*)::int AS semesters_with_data
    FROM mart.municipality_values_semester mv
    WHERE mv.period_id = ANY(v_periods)
      AND mv.property_segment = p_segment
      AND mv.condition_premium_pct IS NOT NULL
    GROUP BY mv.municipality_id;

  ELSIF p_metric = 'price_variance_pct' THEN
    -- Price variance = (max - min) / mid * 100
    RETURN QUERY
    SELECT
      mv.municipality_id,
      CASE
        WHEN AVG(mv.value_mid_eur_sqm) > 0
        THEN ((AVG(mv.value_max_eur_sqm) - AVG(mv.value_min_eur_sqm)) / AVG(mv.value_mid_eur_sqm) * 100)::numeric
        ELSE NULL
      END AS metric_value,
      COUNT(*)::int AS semesters_with_data
    FROM mart.municipality_values_semester mv
    WHERE mv.period_id = ANY(v_periods)
      AND mv.property_segment = p_segment
      AND mv.value_mid_eur_sqm IS NOT NULL
      AND mv.value_min_eur_sqm IS NOT NULL
      AND mv.value_max_eur_sqm IS NOT NULL
    GROUP BY mv.municipality_id
    HAVING AVG(mv.value_mid_eur_sqm) > 0;

  ELSE
    RAISE EXCEPTION 'Unknown metric: %', p_metric;
  END IF;
END;
$$;
