-- Migration: Add population data to municipalities GeoJSON endpoint
-- Purpose: Enable 7% flat tax filtering by population (threshold: 30,000)

-- Drop old 4-parameter function if it exists (may have been created by earlier migrations)
drop function if exists public.get_municipalities_geojson(text, text, text, text);

-- Update the get_municipalities_geojson function to include population
create or replace function public.get_municipalities_geojson(
  geom_column text default 'geom_simplified',
  where_clause text default '',
  region_filter text default null,
  province_filter text default null,
  bbox_filter text default null
) returns jsonb as $$
declare
  result jsonb;
begin
  -- Build and execute dynamic query for GeoJSON
  -- Join with demographics to get latest population
  execute format(
    $sql$
    select jsonb_build_object(
      'type', 'FeatureCollection',
      'features', coalesce(jsonb_agg(
        jsonb_build_object(
          'type', 'Feature',
          'properties', jsonb_build_object(
            'municipality_id', m.municipality_id,
            'name', m.municipality_name,
            'province_code', m.province_code,
            'region_code', m.region_code,
            'coastal_flag', m.coastal_flag,
            'mountain_flag', m.mountain_flag,
            'population', d.total_population
          ),
          'geometry', ST_AsGeoJSON(m.%I)::jsonb
        )
      ), '[]'::jsonb)
    )
    from core.municipalities m
    left join lateral (
      select total_population
      from mart.municipality_demographics_year
      where municipality_id = m.municipality_id
      order by reference_year desc
      limit 1
    ) d on true
    where m.%I is not null
      and ($1 is null or m.region_code = $1)
      and ($2 is null or m.province_code = $2)
    $sql$,
    geom_column,
    geom_column
  ) into result using region_filter, province_filter;

  return result;
end;
$$ language plpgsql stable security definer;

-- Grant execute permission
grant execute on function public.get_municipalities_geojson(text, text, text, text, text) to anon;
grant execute on function public.get_municipalities_geojson(text, text, text, text, text) to authenticated;

comment on function public.get_municipalities_geojson is
  'Returns municipalities as GeoJSON FeatureCollection with population data for flat tax eligibility filtering';
