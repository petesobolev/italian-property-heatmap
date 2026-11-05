# Project Context

## OMI Historical Data Ingestion (Zone-Level)
- **2016**: Complete
- **2017**: Complete
- **2018**: Complete
- **2019**: Complete
- **2020**: Complete
- **2021**: Complete
- **2022**: Complete
- **2023**: Complete
- **2024**: Complete
- **2025**: Complete
- **2026**: Complete

### Ingestion Script Details
- Location: `ingestion/omi/load_omi_values.py`
- Uses `--skip-loaded` flag to skip already-processed municipalities
- Auto-deletes raw data after aggregation to save storage
- `--force-zone-reload` flag: Re-ingest to populate zone-level historical data
- `--calculate-zone-changes` flag: Calculate zone change metrics after ingestion
- `--zone-types` flag: Filter to specific zone types (B, C, D, E, R) for batched processing of large municipalities

### Zone Historical Re-ingestion
To populate zone-level value change data, run:
```bash
cd ingestion/omi
# Re-ingest each year with zone aggregation
for year in 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025; do
    python load_omi_values.py --year $year --force-zone-reload --skip-loaded 2>&1 | tee omi_zone_${year}.log
done

# After all years complete, calculate change metrics
python load_omi_values.py --calculate-zone-changes
```

## Recent Features Added

### User Location Feature (June 2026)
- "Use my location" button in command palette (Cmd+K)
- Browser geolocation with IP-based fallback (ipapi.co)
- Yellow pulsing CircleMarker shows user's location on map
- Files modified:
  - `frontend/src/components/map/CommandPalette.tsx`
  - `frontend/src/app/map/MapInner.tsx`

### Value Change Metric
- Plan file: `~/.claude/plans/harmonic-brewing-melody.md`
- Shows property price trends over selectable time periods (1yr, 2yr, 3yr, 5yr, since 2016)
- Diverging color scale: red (declining) → gray (stable) → green (growing)

## Future Data Roadmap

### Next: NTN Transaction Volume Data
After zone ingestion completes (2019-2025), ingest NTN (Numero Transazioni Normalizzate) data:
- **Source:** OMI Forniture Dati (free, requires SPID auth)
- **Data:** Transaction counts by municipality, quarterly from 2011
- **Value:** Shows market activity/liquidity indicators
- **Download:** CSV via https://www.agenziaentrate.gov.it/portale/schede/fabbricatiterreni/omi/forniture-dati-omi

### Deferred Until App Growth
- **Actual transaction prices** - OpenAPI.com API (€0.20-4/call, expensive at scale)
- **Catasto geometries** - Cadastral parcel boundaries
- **APE energy certificates** - Building energy ratings
- **Conservatoria data** - Mortgage/lien records

## Database Notes
- Using Supabase (upgraded plan with expanded storage)
- Raw data is auto-deleted after aggregation to conserve space
- Zone values stored in `mart.omi_zone_values_semester`
- `spatial_ref_sys` RLS warning is a known PostGIS/Supabase limitation (cannot fix)

## Tech Stack
- Frontend: Next.js, React-Leaflet, TypeScript
- Backend: Supabase (PostgreSQL with PostGIS)
- Data: OMI (Osservatorio Mercato Immobiliare) property values
