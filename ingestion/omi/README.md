# OMI Data Ingestion

Scripts for loading property valuation data from Agenzia delle Entrate's OMI (Osservatorio Mercato Immobiliare).

## Data Source

- **Base URL**: `https://www1.agenziaentrate.gov.it/servizi/geopoi_omi/`
- **Data**: Property price quotations (EUR/sqm) by zone and property type
- **Coverage**: All Italian municipalities, available from 2016-S1 to present
- **Update Frequency**: Twice per year (S1: Jan-Jun, S2: Jul-Dec)

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure environment variables are set in `frontend/.env.local`:
```
NEXT_PUBLIC_SUPABASE_URL=your_supabase_url
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
```

## Usage

### Test Mode (Recommended for first run)
Load data for Roma only:
```bash
python ingestion/omi/load_omi_values.py --test
```

### Load Specific Provinces
```bash
python ingestion/omi/load_omi_values.py --provinces RM MI TO --semesters 20242 20241
```

### Load All Provinces (Full ingestion)
```bash
python ingestion/omi/load_omi_values.py --semesters 20242
```

### Options
- `--provinces` / `-p`: Province codes to process (e.g., RM, MI, TO)
- `--semesters` / `-s`: Semesters to load in YYYYS format (e.g., 20242 = 2024-H2)
- `--test` / `-t`: Test mode - only process Roma with limited data
- `--skip-geometries`: Skip fetching zone geometry data
- `--skip-values`: Only load zone definitions, skip value scraping
- `--verbose` / `-v`: Enable debug logging

## Database Schema

### Tables Populated

1. **`core.omi_zones`** - Zone definitions and boundaries
   - `omi_zone_id`: Primary key (format: `{municipality_id}_{zone_code}`)
   - `municipality_id`: ISTAT code (6 digits)
   - `zone_code`: OMI zone code (e.g., B1, C2, D1)
   - `zone_type`: B (central), C (semi-central), D (peripheral), E (suburban), R (rural)
   - `geom`: Zone geometry (MultiPolygon)

2. **`raw.omi_property_values`** - Raw value observations
   - One row per zone/semester/property_type/state combination
   - Values in EUR per square meter

3. **`mart.municipality_values_semester`** - Aggregated municipality values
   - Populated by running the aggregation SQL script
   - Includes min/max/mid values and change percentages

### Aggregation

After ingestion, run the aggregation query to populate the mart table:
```sql
\i ingestion/omi/aggregate_municipality_values.sql
```

Or via Python:
```python
# The script automatically aggregates as it loads data
```

## Rate Limiting

The script includes respectful rate limiting:
- 1.5 second delay between API requests
- Automatic retries with exponential backoff
- Max 3 retries per request

## Logging

Logs are written to:
- Console (stdout)
- `omi_ingestion.log` file

## Property Types

The script focuses on residential properties:
- Abitazioni civili (standard residential)
- Abitazioni di tipo economico (economy residential)
- Abitazioni signorili (upscale residential)
- Ville e Villini (villas)
- Box (garages)
- Posti auto (parking spaces)

## Error Handling

- Failed requests are logged and skipped
- Ingestion continues even if individual zones fail
- Final summary shows loaded vs rejected rows
- Ingestion runs are tracked in `admin.ingestion_runs`

## Example Output

```
2024-12-15 10:30:00 - INFO - Starting OMI Property Values Ingestion
2024-12-15 10:30:01 - INFO - Loaded environment from frontend/.env.local
2024-12-15 10:30:02 - INFO - Started ingestion run 42
2024-12-15 10:30:03 - INFO - Processing semesters: ['20242']
2024-12-15 10:30:05 - INFO - [1/2] Processing province: Roma (RM)
2024-12-15 10:30:06 - INFO -   [1/121] Roma (058091)
...
2024-12-15 12:45:30 - INFO - Ingestion Complete!
2024-12-15 12:45:30 - INFO -   Rows loaded: 15234
2024-12-15 12:45:30 - INFO -   Rows rejected: 12
```
