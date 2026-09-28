#!/usr/bin/env python3
"""
7% Flat Tax Regime Eligibility Calculator

Determines which Italian municipalities qualify for the 7% flat tax regime
for foreign retirees. All qualifying municipalities must have population < 30,000
(raised from 20,000 by Law 34/2026, Art. 26, comma 1, effective April 7, 2026).

The "comunque" clause in TUIR art. 24-ter, comma 1 means the 30,000 limit
applies universally to all three groups:

1. SOUTHERN ITALY (Mezzogiorno) - Law 145/2018, Art. 1, comma 273
   - Regions: Abruzzo, Molise, Campania, Puglia, Basilicata, Calabria, Sicilia, Sardegna

2. SISMA 2009 (L'Aquila) - D.L. n. 4/2022 (Decreto Sostegni Ter), art. 6-ter
   - 57 municipalities in provinces of L'Aquila, Teramo, Pescara
   - Affected by April 6, 2009 earthquake

3. SISMA 2016 (Central Italy) - Decree 189/2016
   - 140+ municipalities in: Abruzzo, Lazio, Marche, Umbria
   - Affected by 2016 earthquakes (August 24 and October 26-30)

Note: The Sisma 2016 Commissioner's website (sisma2016.gov.it) still shows
the old 20,000 limit from DL 4/2022, but this was superseded by Law 34/2026.

Sources:
- TUIR art. 24-ter, comma 1 (Brocardi, updated July 2026)
- https://sisma2016.gov.it/flat-tax-7-ita/
- https://osservatoriosisma.it/la-liste-dei-140-comuni-inseriti-nel-cratere-del-terremoto/
- https://sisma2009.governo.it/it/
"""

import os
import sys
import csv
import logging
from pathlib import Path
from typing import Set, Dict, List, Tuple

from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Southern Italy regions (Mezzogiorno)
SOUTHERN_REGIONS = {
    'Abruzzo', 'Molise', 'Campania', 'Puglia', 'Basilicata',
    'Calabria', 'Sicilia', 'Sardegna'
}

# Region codes (ISTAT first 2 digits of province code)
REGION_CODE_TO_NAME = {
    '01': 'Piemonte', '02': 'Valle d\'Aosta', '03': 'Lombardia', '04': 'Trentino-Alto Adige',
    '05': 'Veneto', '06': 'Friuli-Venezia Giulia', '07': 'Liguria', '08': 'Emilia-Romagna',
    '09': 'Toscana', '10': 'Umbria', '11': 'Marche', '12': 'Lazio',
    '13': 'Abruzzo', '14': 'Molise', '15': 'Campania', '16': 'Puglia',
    '17': 'Basilicata', '18': 'Calabria', '19': 'Sicilia', '20': 'Sardegna'
}

# Sisma 2016 affected municipalities (by name, normalized)
# Source: https://osservatoriosisma.it/la-liste-dei-140-comuni-inseriti-nel-cratere-del-terremoto/
SISMA_2016_MUNICIPALITIES_ABRUZZO = {
    'barete', 'cagnano amiterno', 'campli', 'campotosto', 'capitignano',
    'castelcastagna', 'castelli', 'civitella del tronto', 'colledara', 'cortino',
    'crognaleto', 'fano adriano', 'farindola', 'isola del gran sasso d\'italia',
    'isola del gran sasso', 'montereale', 'montorio al vomano', 'pietracamela',
    'pizzoli', 'rocca santa maria', 'teramo', 'torricella sicura', 'tossicia',
    'valle castellana'
}

SISMA_2016_MUNICIPALITIES_LAZIO = {
    'accumoli', 'amatrice', 'antrodoco', 'borbona', 'borgo velino',
    'cantalice', 'castel sant\'angelo', 'cittaducale', 'cittareale',
    'leonessa', 'micigliano', 'poggio bustone', 'posta', 'rieti', 'rivodutri'
}

SISMA_2016_MUNICIPALITIES_MARCHE = {
    'acquacanina', 'acquasanta terme', 'amandola', 'apiro', 'appignano del tronto',
    'arquata del tronto', 'ascoli piceno', 'belforte del chienti', 'belmonte piceno',
    'bolognola', 'caldarola', 'camerino', 'camporotondo di fiastrone', 'castel di lama',
    'castelraimondo', 'castelsantangelo sul nera', 'castignano', 'castorano',
    'cerreto d\'esi', 'cessapalombo', 'cingoli', 'colli del tronto', 'colmurano',
    'comunanza', 'corridonia', 'cossignano', 'esanatoglia', 'fabriano', 'falerone',
    'fiastra', 'fiordimonte', 'fiuminata', 'folignano', 'force', 'gagliole', 'gualdo',
    'loro piceno', 'macerata', 'maltignano', 'massa fermana', 'matelica', 'mogliano',
    'monsapietro morico', 'montalto delle marche', 'montappone', 'monte rinaldo',
    'monte san martino', 'monte vidon corrado', 'montecavallo', 'montedinove',
    'montefalcone appennino', 'montefortino', 'montegallo', 'montegiorgio',
    'monteleone', 'monteleone di fermo', 'montelparo', 'montemonaco', 'muccia',
    'offida', 'ortezzano', 'palmiano', 'penna san giovanni', 'petriolo',
    'pieve torina', 'pievebovigliana', 'pioraco', 'poggio san vicino', 'pollenza',
    'ripe san ginesio', 'roccafluvione', 'rotella', 'san ginesio',
    'san severino marche', 'santa vittoria in matenano', 'sant\'angelo in pontano',
    'sarnano', 'sefro', 'serrapetrona', 'serravalle del chienti', 'servigliano',
    'smerillo', 'tolentino', 'treia', 'urbisaglia', 'ussita', 'valfornace',
    'venarotta', 'visso'
}

SISMA_2016_MUNICIPALITIES_UMBRIA = {
    'arrone', 'cascia', 'cerreto di spoleto', 'ferentillo', 'montefranco',
    'monteleone di spoleto', 'norcia', 'poggiodomo', 'polino', 'preci',
    'sant\'anatolia di narco', 'scheggino', 'sellano', 'spoleto', 'vallo di nera'
}

# Combined set for quick lookup - Sisma 2016
SISMA_2016_ALL = (
    SISMA_2016_MUNICIPALITIES_ABRUZZO |
    SISMA_2016_MUNICIPALITIES_LAZIO |
    SISMA_2016_MUNICIPALITIES_MARCHE |
    SISMA_2016_MUNICIPALITIES_UMBRIA
)

# Sisma 2009 (L'Aquila earthquake) affected municipalities
# Source: Decree of the Commissario Delegato n. 3 of April 16, 2009 (integrated by Decree n. 11 of July 17, 2009)
# Extended to flat tax regime by D.L. n. 4/2022 (Decreto Sostegni Ter), art. 6-ter
# NO population limit for earthquake-affected municipalities

# Province of L'Aquila
SISMA_2009_MUNICIPALITIES_LAQUILA = {
    'acciano', 'barete', 'barisciano', 'campotosto', 'capestrano', 'caporciano',
    'carapelle calvisio', 'castel del monte', 'castel di ieri', 'castelvecchio calvisio',
    'castelvecchio subequo', 'cocullo', 'collarmele', 'fagnano alto', 'fossa',
    'gagliano aterno', 'goriano sicoli', 'l\'aquila', 'lucoli', 'navelli', 'ocre',
    'ofena', 'ovindoli', 'pizzoli', 'poggio picenze', 'prata d\'ansidonia',
    'rocca di cambio', 'rocca di mezzo', 'san demetrio ne\' vestini', 'san pio delle camere',
    'sant\'eusanio forconese', 'santo stefano di sessanio', 'scoppito',
    'tione degli abruzzi', 'tornimparte', 'villa sant\'angelo', 'villa santa lucia degli abruzzi'
}

# Province of Teramo (some overlap with Sisma 2016)
SISMA_2009_MUNICIPALITIES_TERAMO = {
    'arsita', 'castelli', 'montorio al vomano', 'pietracamela', 'tossicia'
}

# Province of Pescara
SISMA_2009_MUNICIPALITIES_PESCARA = {
    'brittoli', 'bussi sul tirino', 'civitella casanova', 'cugnoli',
    'montebello di bertona', 'popoli', 'torre de\' passeri'
}

# Combined set for quick lookup - Sisma 2009
SISMA_2009_ALL = (
    SISMA_2009_MUNICIPALITIES_LAQUILA |
    SISMA_2009_MUNICIPALITIES_TERAMO |
    SISMA_2009_MUNICIPALITIES_PESCARA
)

# All earthquake-affected municipalities (both 2009 and 2016)
SISMA_ALL = SISMA_2016_ALL | SISMA_2009_ALL

# Population threshold for Southern Italy (raised from 20,000 to 30,000 by Law 34/2026)
POPULATION_THRESHOLD = 30000


def normalize_municipality_name(name: str) -> str:
    """Normalize municipality name for matching."""
    return name.lower().strip().replace('`', "'").replace('\u2019', "'")


def get_region_from_istat_code(istat_code: str) -> str:
    """Get region name from ISTAT municipality code."""
    # ISTAT codes: first 3 digits are province, which maps to region
    province_code = istat_code[:3]

    # Province to region mapping
    province_to_region = {
        # Piemonte
        '001': 'Piemonte', '002': 'Piemonte', '003': 'Piemonte', '004': 'Piemonte',
        '005': 'Piemonte', '006': 'Piemonte', '096': 'Piemonte', '103': 'Piemonte',
        # Valle d'Aosta
        '007': 'Valle d\'Aosta',
        # Lombardia
        '012': 'Lombardia', '013': 'Lombardia', '014': 'Lombardia', '015': 'Lombardia',
        '016': 'Lombardia', '017': 'Lombardia', '018': 'Lombardia', '019': 'Lombardia',
        '020': 'Lombardia', '097': 'Lombardia', '098': 'Lombardia', '108': 'Lombardia',
        # Trentino-Alto Adige
        '021': 'Trentino-Alto Adige', '022': 'Trentino-Alto Adige',
        # Veneto
        '023': 'Veneto', '024': 'Veneto', '025': 'Veneto', '026': 'Veneto',
        '027': 'Veneto', '028': 'Veneto', '029': 'Veneto',
        # Friuli-Venezia Giulia
        '030': 'Friuli-Venezia Giulia', '031': 'Friuli-Venezia Giulia',
        '032': 'Friuli-Venezia Giulia', '093': 'Friuli-Venezia Giulia',
        # Liguria
        '008': 'Liguria', '009': 'Liguria', '010': 'Liguria', '011': 'Liguria',
        # Emilia-Romagna
        '033': 'Emilia-Romagna', '034': 'Emilia-Romagna', '035': 'Emilia-Romagna',
        '036': 'Emilia-Romagna', '037': 'Emilia-Romagna', '038': 'Emilia-Romagna',
        '039': 'Emilia-Romagna', '040': 'Emilia-Romagna', '099': 'Emilia-Romagna',
        # Toscana
        '045': 'Toscana', '046': 'Toscana', '047': 'Toscana', '048': 'Toscana',
        '049': 'Toscana', '050': 'Toscana', '051': 'Toscana', '052': 'Toscana',
        '053': 'Toscana', '100': 'Toscana',
        # Umbria
        '054': 'Umbria', '055': 'Umbria',
        # Marche
        '041': 'Marche', '042': 'Marche', '043': 'Marche', '044': 'Marche', '109': 'Marche',
        # Lazio
        '056': 'Lazio', '057': 'Lazio', '058': 'Lazio', '059': 'Lazio', '060': 'Lazio',
        # Abruzzo
        '066': 'Abruzzo', '067': 'Abruzzo', '068': 'Abruzzo', '069': 'Abruzzo',
        # Molise
        '070': 'Molise', '094': 'Molise',
        # Campania
        '061': 'Campania', '062': 'Campania', '063': 'Campania', '064': 'Campania', '065': 'Campania',
        # Puglia
        '071': 'Puglia', '072': 'Puglia', '073': 'Puglia', '074': 'Puglia', '075': 'Puglia', '110': 'Puglia',
        # Basilicata
        '076': 'Basilicata', '077': 'Basilicata',
        # Calabria
        '078': 'Calabria', '079': 'Calabria', '080': 'Calabria', '101': 'Calabria', '102': 'Calabria',
        # Sicilia
        '081': 'Sicilia', '082': 'Sicilia', '083': 'Sicilia', '084': 'Sicilia',
        '085': 'Sicilia', '086': 'Sicilia', '087': 'Sicilia', '088': 'Sicilia', '089': 'Sicilia',
        # Sardegna
        '090': 'Sardegna', '091': 'Sardegna', '092': 'Sardegna', '095': 'Sardegna',
        '104': 'Sardegna', '105': 'Sardegna', '106': 'Sardegna', '107': 'Sardegna', '111': 'Sardegna',
    }

    return province_to_region.get(province_code, 'Unknown')


def is_eligible_southern_italy(region: str, population: int) -> bool:
    """Check if municipality qualifies via Southern Italy path."""
    return region in SOUTHERN_REGIONS and population < POPULATION_THRESHOLD


def is_eligible_earthquake_zone(name: str, population: int) -> tuple[bool, str]:
    """Check if municipality qualifies via earthquake zone path (Sisma 2009 or 2016).

    Per Law 34/2026, the 30,000 population limit applies to earthquake zones
    as well (the "comunque" clause in TUIR art. 24-ter, comma 1).

    Returns:
        Tuple of (is_eligible, earthquake_type) where earthquake_type is
        'sisma_2009', 'sisma_2016', or 'sisma_2009+2016' if affected by both.
        Returns (False, '') if population >= 30,000 or not in earthquake zone.
    """
    # Population limit applies to all categories per Law 34/2026
    if population >= POPULATION_THRESHOLD:
        return False, ''

    normalized = normalize_municipality_name(name)

    in_2009 = normalized in SISMA_2009_ALL
    in_2016 = normalized in SISMA_2016_ALL

    if in_2009 and in_2016:
        return True, 'sisma_2009+2016'
    elif in_2009:
        return True, 'sisma_2009'
    elif in_2016:
        return True, 'sisma_2016'
    else:
        return False, ''


def check_eligibility(
    municipality_name: str,
    istat_code: str,
    population: int
) -> Tuple[bool, str]:
    """
    Check if a municipality is eligible for the 7% flat tax regime.

    Returns:
        Tuple of (is_eligible, eligibility_reason)
    """
    region = get_region_from_istat_code(istat_code)
    normalized_name = normalize_municipality_name(municipality_name)

    # Check both paths (both have 30,000 population limit per Law 34/2026)
    southern_eligible = is_eligible_southern_italy(region, population)
    earthquake_eligible, earthquake_type = is_eligible_earthquake_zone(municipality_name, population)

    if southern_eligible and earthquake_eligible:
        return True, f"southern_italy+{earthquake_type}"
    elif southern_eligible:
        return True, "southern_italy"
    elif earthquake_eligible:
        return True, earthquake_type
    else:
        # Not eligible - determine reason
        in_eligible_zone = region in SOUTHERN_REGIONS or normalized_name in SISMA_ALL
        if in_eligible_zone and population >= POPULATION_THRESHOLD:
            return False, f"population_exceeds_{POPULATION_THRESHOLD}"
        elif not in_eligible_zone:
            return False, "not_in_eligible_region_or_zone"
        else:
            return False, "unknown"


def generate_eligible_municipalities_from_db():
    """Query database and generate list of eligible municipalities."""
    try:
        import psycopg2
    except ImportError:
        logger.error("psycopg2 not installed. Run: pip install psycopg2-binary")
        return

    # Load environment
    env_paths = [
        Path(__file__).parent.parent / "frontend" / ".env.local",
        Path(__file__).parent / ".env",
    ]
    for env_path in env_paths:
        if env_path.exists():
            load_dotenv(env_path)
            break

    # Connect to database
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT', '6543')
    db_name = os.getenv('DB_NAME', 'postgres')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    conn = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name,
        user=db_user, password=db_password
    )

    cursor = conn.cursor()

    # Get all municipalities with population data
    cursor.execute("""
        SELECT
            m.municipality_id as istat_code,
            m.municipality_name as name,
            COALESCE(d.total_population, 0) as population
        FROM core.municipalities m
        LEFT JOIN mart.municipality_demographics_year d
            ON m.municipality_id = d.municipality_id
            AND d.reference_year = (
                SELECT MAX(reference_year)
                FROM mart.municipality_demographics_year
            )
        ORDER BY m.municipality_name
    """)

    eligible = []
    ineligible_count = 0

    for istat_code, name, population in cursor.fetchall():
        is_eligible, reason = check_eligibility(name, istat_code, population or 0)

        if is_eligible:
            region = get_region_from_istat_code(istat_code)
            eligible.append({
                'istat_code': istat_code,
                'name': name,
                'region': region,
                'population': population or 0,
                'eligibility_reason': reason
            })
        else:
            ineligible_count += 1

    cursor.close()
    conn.close()

    # Summary
    logger.info(f"Total eligible municipalities: {len(eligible)}")
    logger.info(f"Total ineligible: {ineligible_count}")

    # Breakdown by reason
    by_reason = {}
    for m in eligible:
        reason = m['eligibility_reason']
        by_reason[reason] = by_reason.get(reason, 0) + 1

    logger.info("Breakdown by eligibility path:")
    for reason, count in sorted(by_reason.items()):
        logger.info(f"  {reason}: {count}")

    # Write to CSV
    output_path = Path(__file__).parent.parent / "docs" / "7pct_flat_tax_eligible_municipalities_v2.csv"
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['istat_code', 'name', 'region', 'population', 'eligibility_reason'])
        writer.writeheader()
        writer.writerows(eligible)

    logger.info(f"Written to {output_path}")

    return eligible


def update_database_eligibility():
    """Update the mart.flat_tax_eligibility table in the database."""
    try:
        import psycopg2
    except ImportError:
        logger.error("psycopg2 not installed. Run: pip install psycopg2-binary")
        return

    # Load environment
    env_paths = [
        Path(__file__).parent.parent / "frontend" / ".env.local",
        Path(__file__).parent / ".env",
    ]
    for env_path in env_paths:
        if env_path.exists():
            load_dotenv(env_path)
            break

    # Connect to database
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT', '6543')
    db_name = os.getenv('DB_NAME', 'postgres')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    conn = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name,
        user=db_user, password=db_password
    )
    conn.autocommit = False
    cursor = conn.cursor()

    # Get all municipalities with population data
    cursor.execute("""
        SELECT
            m.municipality_id as istat_code,
            m.municipality_name as name,
            COALESCE(d.total_population, 0) as population
        FROM core.municipalities m
        LEFT JOIN mart.municipality_demographics_year d
            ON m.municipality_id = d.municipality_id
            AND d.reference_year = (
                SELECT MAX(reference_year)
                FROM mart.municipality_demographics_year
            )
        ORDER BY m.municipality_name
    """)

    eligible = []
    for istat_code, name, population in cursor.fetchall():
        is_eligible, reason = check_eligibility(name, istat_code, population or 0)
        if is_eligible:
            eligible.append((istat_code, reason))

    logger.info(f"Found {len(eligible)} eligible municipalities")

    # Clear existing data and insert new
    cursor.execute("DELETE FROM mart.flat_tax_eligibility")
    logger.info("Cleared existing eligibility data")

    # Insert new data
    from psycopg2.extras import execute_values
    execute_values(
        cursor,
        """
        INSERT INTO mart.flat_tax_eligibility (municipality_id, eligibility_reason, is_eligible, updated_at)
        VALUES %s
        """,
        [(m_id, reason, True, 'now()') for m_id, reason in eligible],
        template="(%s, %s, %s, %s)"
    )

    conn.commit()
    logger.info(f"Inserted {len(eligible)} eligible municipalities")

    # Count by reason
    by_reason = {}
    for _, reason in eligible:
        by_reason[reason] = by_reason.get(reason, 0) + 1

    logger.info("Breakdown by eligibility path:")
    for reason, count in sorted(by_reason.items()):
        logger.info(f"  {reason}: {count}")

    cursor.close()
    conn.close()
    logger.info("Database update complete")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='7% Flat Tax Eligibility Calculator')
    parser.add_argument('--generate', action='store_true',
                        help='Generate eligible municipalities list to CSV')
    parser.add_argument('--update-db', action='store_true',
                        help='Update the database with current eligibility rules')
    parser.add_argument('--check', type=str, nargs=3, metavar=('NAME', 'ISTAT', 'POP'),
                        help='Check single municipality: NAME ISTAT_CODE POPULATION')
    args = parser.parse_args()

    if args.generate:
        generate_eligible_municipalities_from_db()
    elif args.update_db:
        update_database_eligibility()
    elif args.check:
        name, istat, pop = args.check
        is_elig, reason = check_eligibility(name, istat, int(pop))
        print(f"{name}: {'ELIGIBLE' if is_elig else 'NOT ELIGIBLE'} ({reason})")
    else:
        parser.print_help()
