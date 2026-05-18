-- Optimize zone geometry queries by using simplified geometry
-- This reduces the vertex count significantly for faster rendering

-- Update the RPC function to use simplified geometry
create or replace function public.get_omi_zones_geojson(
  p_municipality_id text
) returns jsonb as $$
declare
  result jsonb;
begin
  select jsonb_build_object(
    'type', 'FeatureCollection',
    'features', coalesce(jsonb_agg(
      jsonb_build_object(
        'type', 'Feature',
        'properties', jsonb_build_object(
          'omi_zone_id', z.omi_zone_id,
          'zone_code', z.zone_code,
          'zone_type', z.zone_type,
          'zone_description', z.zone_description,
          'zone_classification', z.zone_classification
        ),
        -- Simplify geometry with 0.0001 degree tolerance (~10m at Italian latitudes)
        -- This dramatically reduces vertex count while preserving shape
        'geometry', ST_AsGeoJSON(
          ST_SimplifyPreserveTopology(z.geom, 0.0001)
        )::jsonb
      )
    ), '[]'::jsonb)
  ) into result
  from core.omi_zones z
  where z.municipality_id = p_municipality_id
    and z.geom is not null;

  return result;
end;
$$ language plpgsql stable security definer;

comment on function public.get_omi_zones_geojson(text) is 
  'Returns OMI zones as GeoJSON with simplified geometry for faster rendering';
