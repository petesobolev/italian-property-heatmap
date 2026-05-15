#!/usr/bin/env python3
"""
Process ISTAT POSAS (Population by Age and Sex) data.

Aggregates single-year age data into demographic categories for our database.

Input: POSAS_2026_it_Comuni.csv (population by single year of age per municipality)
Output: CSV ready for load_demographics.py ingestion
"""

import pandas as pd
import sys
from pathlib import Path

def process_posas(input_file: Path, output_file: Path, year: int = 2026):
    """Process POSAS file and aggregate by age groups."""

    print(f"Reading {input_file}...")

    # Read the CSV, skip the title row
    df = pd.read_csv(
        input_file,
        sep=';',
        skiprows=1,
        encoding='utf-8-sig',
        dtype={'Codice comune': str}
    )

    print(f"Loaded {len(df)} rows")
    print(f"Columns: {list(df.columns)}")

    # Rename columns
    df = df.rename(columns={
        'Codice comune': 'municipality_code',
        'Comune': 'municipality_name',
        'Età': 'age',
        'Totale maschi': 'male',
        'Totale femmine': 'female',
        'Totale': 'total'
    })

    # Convert age to int (handle "100 e più" or similar)
    df['age'] = pd.to_numeric(df['age'], errors='coerce').fillna(100).astype(int)

    # IMPORTANT: Filter out age=999 rows which are TOTAL summary rows in ISTAT data
    before_filter = len(df)
    df = df[df['age'] < 200]  # Keep only valid ages (0-150 or so)
    print(f"Filtered out {before_filter - len(df)} summary rows (age=999)")

    # Aggregate by municipality
    print("Aggregating by municipality...")

    results = []
    for code, group in df.groupby('municipality_code'):
        name = group['municipality_name'].iloc[0]

        # Age groups
        pop_0_14 = group[group['age'] <= 14]['total'].sum()
        pop_15_64 = group[(group['age'] >= 15) & (group['age'] <= 64)]['total'].sum()
        pop_65_plus = group[group['age'] >= 65]['total'].sum()
        total_pop = group['total'].sum()

        # Gender totals
        male_total = group['male'].sum()
        female_total = group['female'].sum()

        results.append({
            'Codice comune': code,
            'Denominazione': name,
            'Totale': int(total_pop),
            'Maschi': int(male_total),
            'Femmine': int(female_total),
            '0-14': int(pop_0_14),
            '15-64': int(pop_15_64),
            '65 e oltre': int(pop_65_plus),
            'Stranieri': '',  # Not in this dataset
            'Nati': '',       # Not in this dataset
            'Morti': '',      # Not in this dataset
            'Famiglie': ''    # Not in this dataset
        })

    result_df = pd.DataFrame(results)

    print(f"Aggregated into {len(result_df)} municipalities")

    # Save to output file
    result_df.to_csv(output_file, sep=';', index=False, encoding='utf-8')
    print(f"Saved to {output_file}")

    # Show sample
    print("\nSample output:")
    print(result_df.head(5).to_string())

    return result_df


if __name__ == "__main__":
    # Default paths
    docs_dir = Path(__file__).parent.parent.parent / "docs" / "POSAS_2026_en_All_files"
    input_file = docs_dir / "POSAS_2026_it_Comuni.csv"
    output_file = Path(__file__).parent / "demographics_2026.csv"

    if len(sys.argv) > 1:
        input_file = Path(sys.argv[1])
    if len(sys.argv) > 2:
        output_file = Path(sys.argv[2])

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        sys.exit(1)

    process_posas(input_file, output_file)
