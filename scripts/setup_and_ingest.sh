#!/bin/bash
# Italian Property Heatmap - Data Ingestion Setup Script
# This script helps you set up and run the data ingestion pipeline.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
INGEST_DIR="$PROJECT_ROOT/src/backend/ingest"
DATA_DIR="$PROJECT_ROOT/data"

echo "=============================================="
echo "  Italian Property Heatmap - Data Ingestion"
echo "=============================================="
echo ""

# Check for DATABASE_URL
if [ -z "$DATABASE_URL" ]; then
    echo "ERROR: DATABASE_URL environment variable is not set."
    echo ""
    echo "To get your database URL:"
    echo "1. Go to https://supabase.com/dashboard"
    echo "2. Select your project: vewcbnclnqikufpgzzyu"
    echo "3. Go to Settings > Database"
    echo "4. Copy the 'Connection string' (URI format)"
    echo "5. Export it: export DATABASE_URL='postgresql://...'"
    echo ""
    echo "Or you can set individual variables:"
    echo "  export DB_HOST=db.vewcbnclnqikufpgzzyu.supabase.co"
    echo "  export DB_PORT=5432"
    echo "  export DB_NAME=postgres"
    echo "  export DB_USER=postgres"
    echo "  export DB_PASSWORD='your-password'"
    echo ""
    exit 1
fi

# Create data directory
mkdir -p "$DATA_DIR"

# Check Python dependencies
echo "Checking Python dependencies..."
cd "$PROJECT_ROOT"
pip3 install -q -r requirements.txt 2>/dev/null || {
    echo "Installing Python dependencies..."
    pip3 install -r requirements.txt
}

echo "Dependencies OK."
echo ""

# Menu
echo "What would you like to do?"
echo ""
echo "1) Download & load ISTAT municipal boundaries (~8,000 municipalities)"
echo "2) Load OMI property values (requires data file)"
echo "3) Load OMI transactions (requires data file)"
echo "4) Load ISTAT population data (requires data file)"
echo "5) Run all available ingestions"
echo "q) Quit"
echo ""
read -p "Select option: " choice

case $choice in
    1)
        echo ""
        echo "Starting ISTAT boundaries ingestion..."
        echo "This will download ~50MB of data and may take a few minutes."
        echo ""
        cd "$INGEST_DIR"
        python3 istat_boundaries.py --year 2024 --data-dir "$DATA_DIR" --skip-neighbors
        echo ""
        echo "Done! Municipalities loaded. Refresh your map to see real boundaries."
        ;;
    2)
        echo ""
        read -p "Enter path to OMI values file (CSV/XLSX): " omi_file
        read -p "Enter semester (e.g., 2024S1): " semester
        if [ -f "$omi_file" ]; then
            cd "$INGEST_DIR"
            python3 omi_values.py --file "$omi_file" --semester "$semester"
        else
            echo "File not found: $omi_file"
        fi
        ;;
    3)
        echo ""
        read -p "Enter path to OMI transactions file (CSV/XLSX): " ntn_file
        read -p "Enter semester (e.g., 2024S1): " semester
        if [ -f "$ntn_file" ]; then
            cd "$INGEST_DIR"
            python3 omi_transactions.py --file "$ntn_file" --semester "$semester"
        else
            echo "File not found: $ntn_file"
        fi
        ;;
    4)
        echo ""
        read -p "Enter path to ISTAT population file (CSV/XLSX): " pop_file
        read -p "Enter year (e.g., 2023): " year
        if [ -f "$pop_file" ]; then
            cd "$INGEST_DIR"
            python3 istat_population.py --file "$pop_file" --year "$year"
        else
            echo "File not found: $pop_file"
        fi
        ;;
    5)
        echo ""
        echo "Running ISTAT boundaries ingestion..."
        cd "$INGEST_DIR"
        python3 istat_boundaries.py --year 2024 --data-dir "$DATA_DIR" --skip-neighbors || true
        echo ""
        echo "Boundaries complete. For OMI and population data, you need to provide data files."
        ;;
    q|Q)
        echo "Bye!"
        exit 0
        ;;
    *)
        echo "Invalid option"
        exit 1
        ;;
esac
