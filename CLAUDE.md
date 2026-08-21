# Project Context

## OMI Historical Data Ingestion
- **2016**: Complete
- **2017**: Complete
- **2018**: In progress (province 2/103 - ALESSANDRIA, ~52% through)
- **2019**: Queued after 2018

### Smart Monitor
The smart monitor script handles auto-recovery from stalls and crashes:
- Script: `ingestion/omi/smart_monitor.sh`
- Log: `ingestion/omi/smart_monitor.log`
- Ingestion log: `ingestion/omi/omi_2018_ingestion.log`

### Useful Commands
```bash
# Check if ingestion is running
ps aux | grep load_omi

# Check smart monitor status
tail -10 ingestion/omi/smart_monitor.log

# Check ingestion progress
tail -20 ingestion/omi/omi_2018_ingestion.log
```

### Ingestion Script Details
- Location: `ingestion/omi/load_omi_values.py`
- Uses `--skip-loaded` flag to skip already-processed municipalities
- Auto-deletes raw data after aggregation to save storage
- `--force-zone-reload` flag: Re-ingest to populate zone-level historical data
- `--calculate-zone-changes` flag: Calculate zone change metrics after ingestion

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

## Database Notes
- Using Supabase (upgraded plan with expanded storage)
- Raw data is auto-deleted after aggregation to conserve space
- Zone values stored in `mart.omi_zone_values_semester`
- `spatial_ref_sys` RLS warning is a known PostGIS/Supabase limitation (cannot fix)

## Tech Stack
- Frontend: Next.js, React-Leaflet, TypeScript
- Backend: Supabase (PostgreSQL with PostGIS)
- Data: OMI (Osservatorio Mercato Immobiliare) property values
