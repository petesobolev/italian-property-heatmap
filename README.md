## Italian Property Heatmap

This project will display Italian property data on an interactive web map with a heatmap overlay, allowing exploration by location and filters (price, size, etc.).

### Project layout (initial)

- `src/backend` – Python FastAPI backend (APIs, data loading, aggregation).
- `src/frontend` – Web frontend (map UI, filters, charts).
- `requirements.txt` – Python dependencies.
- `.gitignore` – Ignore common Python / Node artifacts.

### Getting started (high level)

1. Create and activate a virtualenv in this folder.
2. Install Python dependencies with `pip install -r requirements.txt`.
3. Implement and run the backend (e.g. `uvicorn src.backend.main:app --reload`).
4. Implement and run the frontend (e.g. using Vite/React or another preferred stack).

The detailed implementation (endpoints, data model, UI, etc.) should follow the `italian_property_heatmap_complete_implementation_package` spec you placed in this directory.
