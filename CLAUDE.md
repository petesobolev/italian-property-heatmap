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
- Auto-deletes raw data after aggregation to save storage (0.5GB limit)

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
- Using Supabase with 0.5GB storage limit on free plan
- Raw data is auto-deleted after aggregation to stay under limit
- `spatial_ref_sys` RLS warning is a known PostGIS/Supabase limitation (cannot fix)

## Tech Stack
- Frontend: Next.js, React-Leaflet, TypeScript
- Backend: Supabase (PostgreSQL with PostGIS)
- Data: OMI (Osservatorio Mercato Immobiliare) property values
