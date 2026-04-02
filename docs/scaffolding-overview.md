# Italian Property Heatmap – Scaffolding Overview

## 1. High-Level Architecture

- **Frontend**: Next.js 16 (App Router) + React + TypeScript + Tailwind
- **Backend (web)**: Next.js API route handlers (for map, rankings, municipality detail, etc.)
- **Backend (analytics / ML)**: Python + FastAPI (separate), with room for batch jobs and modeling
- **Database**: Supabase PostgreSQL + PostGIS
- **Map rendering**: Leaflet + React-Leaflet (browser), consuming GeoJSON + metrics from API
- **Storage**: Supabase DB now; later Vercel Blob for cached GeoJSON / exports

The project is designed to match the spec: municipality-level analytics product with euro/m², appreciation forecasts, rental ROI, and confidence scoring.

---

## 2. Repository Layout (Current)

- **Project root**
  - `README.md` – brief project description and layout
  - `requirements.txt` – Python backend + geo stack deps
  - `.gitignore` – Python, Node, env files
  - `src/backend/main.py` – minimal FastAPI backend
  - `frontend/` – Next.js app (primary UI)
  - `database/` – human-oriented DB assets (migrations + seeds for reference)
  - `supabase/` – Supabase CLI config + migrations applied to your project

---

## 3. Python Backend (FastAPI)

**File:** `src/backend/main.py`

- **Purpose**: placeholder backend for health check and future Python analytics / jobs.
- **Key endpoints**:
  - `GET /health` → `{"status": "ok"}`
  - `GET /` → `{"message": "Italian Property Heatmap backend placeholder"}`

**Run** (inside the venv, from project root):

```bash
uvicorn src.backend.main:app --reload
```

This is separate from the Next.js app and will later be used for Python-heavy tasks (feature engineering, modeling APIs, etc.).

---

## 4. Next.js Frontend

**Directory:** `frontend/`

Created with:

```bash
npm create next-app@latest frontend -- --ts --app --src-dir --eslint --tailwind --import-alias "@/*" --use-npm
```

### 4.1 Pages

- **`src/app/page.tsx`**
  - Landing page.
  - Highlights the product’s purpose and links to:
    - `/map`
    - `/rankings`
    - `/methodology`

- **`src/app/map/page.tsx`**
  - Client component wrapper for the map view.
  - Uses `next/dynamic` to load a client-only `MapInner` (React-Leaflet) to avoid SSR `window` issues.
  - Explains that the choropleth is driven by Supabase data and demo GeoJSON.

- **`src/app/map/MapInner.tsx`**
  - Pure client component (`"use client"`).
  - Loads:
    - `public/demo/municipalities.geojson` (demo geometries)
    - `/api/map/layer?metric=value_mid_eur_sqm&horizonMonths=12&segment=residential`
  - Maintains `valuesByMunicipality` state from API.
  - Computes a simple blue color ramp based on `value_mid_eur_sqm`.
  - Renders:
    - `MapContainer` + `TileLayer` (OSM tiles)
    - `GeoJSON` layer with:
      - Styled polygons (fill color by value)
      - Tooltips: `"{name}: €{value}/m²"` or `"no data"`

- **`src/app/rankings/page.tsx`**
  - Placeholder page describing future rankings UI.
  - Links back to `/map`.

- **`src/app/methodology/page.tsx`**
  - Placeholder page summarizing future methodology content:
    - Sources (OMI, ISTAT, rentals, etc.)
    - Features and models
    - Back-testing and confidence

---

## 5. Supabase Integration (Frontend)

### 5.1 Dependencies

In `frontend/package.json`:

- `"@supabase/supabase-js"`
- `"leaflet"`, `"react-leaflet"`, `"@types/leaflet"`
- `"geojson"` (for TypeScript types)

### 5.2 Environment variables

**File:** `frontend/.env.local` (not committed)

```env
NEXT_PUBLIC_SUPABASE_URL=https://vewcbnclnqikufpgzzyu.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=...        # anon public key
SUPABASE_SERVICE_ROLE_KEY=...            # service role key (server-side only)
```

**Template:** `frontend/.env.example`.

### 5.3 Supabase server helper

**File:** `src/lib/supabase/server.ts`

- Creates a Supabase client for server-side use in Next.js route handlers.
- Prefers `SUPABASE_SERVICE_ROLE_KEY` (for broader schema access), falling back to anon key if needed.
- Disables browser-style auth/session persistence.

---

## 6. Database & Migrations

### 6.1 Schemas

Initial migration creates:

- `core`
- `raw`
- `mart`
- `model`
- `admin`

PostGIS enabled.

**Supabase CLI migration file:**

- `supabase/migrations/202603170001_init_schemas_postgis.sql`

**Reference copy:**

- `database/migrations/0001_init_schemas_postgis.sql`

### 6.2 Key tables

- **`core.time_periods`**
  - Canonical time dimension for months/semesters/years.

- **`core.municipalities`**
  - Primary grain: ISTAT municipality code (`municipality_id`).
  - Fields for name, province, region, flags, `geom`, `geom_simplified`, and indexes on geometry and region/province.

- **`admin.ingestion_runs`**
  - Tracks ingestion jobs, counts, status, error notes.

- **`admin.model_runs`**
  - Tracks model runs, versions, horizons, metrics, status.

- **`model.forecasts_municipality`**
  - Core forecasts table for the app:
    - Keys: `municipality_id`, `forecast_date`, `horizon_months`, `property_segment`, `model_version`.
    - Fields: `value_mid_eur_sqm`, `forecast_appreciation_pct`, `forecast_gross_yield_pct`, `opportunity_score`, `confidence_score`, `drivers`, `risks`, `publishable_flag`.

Supabase config was updated so that:

- PostgREST exposes schemas: `public,core,model,mart,raw,admin`.
- Roles (`anon`, `authenticated`) have `USAGE` on `model` and `core`, and `SELECT` on:
  - `model.forecasts_municipality`
  - `core.municipalities` (for future queries)

---

## 7. Demo Seed Data (for Choropleth)

**File:** `database/seeds/0001_demo_seed.sql`

- Inserts two municipalities:
  - `015146` – Milano
  - `058091` – Roma

- Inserts a single forecast snapshot for each:
  - `forecast_date = '2026-01-01'`
  - `horizon_months = 12`
  - `property_segment = 'residential'`
  - `model_version = 'demo_v1'`
  - `value_mid_eur_sqm`, `forecast_appreciation_pct`, `forecast_gross_yield_pct`, `opportunity_score`, `confidence_score`

This gives the `/api/map/layer` endpoint real values to return for the demo.

---

## 8. Demo GeoJSON

**File:** `frontend/public/demo/municipalities.geojson`

- Simple `FeatureCollection` with 2 polygon features:
  - Demo polygon around Milan (`municipality_id = "015146"`)
  - Demo polygon around Rome (`municipality_id = "058091"`)

- Used only for scaffolding so the map can render a choropleth before real geometries are ingested into PostGIS.

---

## 9. API Routes (Next.js)

- **`src/app/api/supabase/ping/route.ts`**
  - Connectivity check endpoint.
  - Returns `ok: true` and a note; useful to verify env vars and Supabase wiring.

- **`src/app/api/filters/route.ts`**
  - Placeholder endpoint for map/rankings filters.
  - Returns hard-coded strategy and property segment options (regions / provinces are empty for now).

- **`src/app/api/map/layer/route.ts`**
  - Core map data endpoint (scaffolding).
  - **Request:** Query params: `metric`, `horizonMonths`, `segment`.
  - **Behavior:**
    1. Queries `model.forecasts_municipality` for the latest `forecast_date` matching:
       - `horizon_months`
       - `property_segment`
       - `publishable_flag = true`
    2. If none found, returns `features: []` + note.
    3. Otherwise, selects for that date:
       - `municipality_id`
       - `value_mid_eur_sqm`
       - `forecast_appreciation_pct`
       - `forecast_gross_yield_pct`
       - `opportunity_score`
       - `confidence_score`
    4. Maps to:
       ```json
       {
         "features": [
           { "municipalityId": "015146", "value": 5200 },
           ...
         ]
       }
       ```
       with `value` chosen based on `metric`.

This endpoint is what the `/map` page uses to color the demo polygons.

---

## 10. End-to-End Behavior (Scaffold)

When you open `http://localhost:3000/map`:

1. **Frontend:**
   - `MapInner` loads demo GeoJSON + `/api/map/layer` values.
   - Joins by `municipality_id`.
   - Renders OSM tiles + choropleth polygons.
   - Tooltips show municipality name + `€ / m²`.

2. **API:**
   - `/api/map/layer` reads from Supabase `model.forecasts_municipality`.
   - Uses the most recent `forecast_date` for the selected horizon/segment.

3. **Database:**
   - Demo rows exist for Milan and Rome.
   - Permissions and exposed schemas allow the API to read from `model` and `core`.

At this point, the **scaffolding is complete** and ready for:

- Ingestion pipelines (OMI, ISTAT, rentals, etc.).
- Real municipality geometries in `core.municipalities.geom`.
- Additional endpoints (`/api/municipality/[istatCode]`, `/api/rankings`, `/api/compare`, admin actions).
