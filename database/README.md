## Database (Supabase / Postgres + PostGIS)

This project follows the spec's schema layout:

- `core` – reference dimensions (municipalities, provinces/regions, time periods, neighbors)
- `raw` – source-fidelity ingested datasets
- `mart` – curated analytics marts
- `model` – feature store + forecasts
- `admin` – operational audit tables

### Recommended workflow (Supabase CLI)

1. Install CLI (macOS):

```bash
brew install supabase/tap/supabase
```

2. Initialize in project root:

```bash
cd /Users/pete/Projects/italian-property-heatmap
supabase init
```

3. Link to your Supabase project:

```bash
supabase link --project-ref vewcbnclnqikufpgzzyu
```

4. Apply migrations:

```bash
supabase db push
```

### Fallback (SQL editor)

If you prefer not to use the CLI, you can copy/paste the SQL from `database/migrations/*.sql`
into the Supabase SQL editor and run it.

