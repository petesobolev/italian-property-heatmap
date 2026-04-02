# Municipality-level vehicle arson (“car burnings”) in Italy: data availability and implementation plan

## Executive summary

A single, nationwide **official** dataset that reports the **number of vehicle arsons per year for every Italian municipality** does **not appear to be publicly available** in a way that is both (a) vehicle-specific and (b) Comune-level. The two most authoritative public source families—police “delitti denunciati” statistics and fire brigade intervention statistics—are real, rich, and historically deep, but their **public releases are mostly at province level and/or provincial-capital level**, and they generally **do not isolate “vehicle arson” as its own national municipality-level indicator**. citeturn17search9turn16view0turn29view0turn43view0turn21view2

That said, you *can* build a credible “car burnings risk” heatmap for your app using a **multi-stage data strategy**:

- A **nationwide official-proxy layer** based on **arson-related crime categories** (e.g., “incendio” and “danneggiamento seguito da incendio”) and **fire brigade intervention volumes** at the **province / capoluogo** level. citeturn11view0turn29view0turn16view0turn17search9turn21view2  
- A **Calabria-focused enrichment** to reach **municipality-level** coverage by combining:  
  - **FOIA / accesso civico generalizzato** requests for **aggregated** municipality-year counts from the fire brigade’s intervention system (which tracks “autovetture” and likely “probabile dolo”), citeturn51view0turn51view1turn43view0turn21view2  
  - structured incident extraction from law enforcement **press releases** (high precision, low recall), and  
  - local open data where available (typically low yield for this topic, but worth checking). citeturn23view2turn27view1  

With this approach, your product can (1) show **Italy-wide comparability** using official proxies, and (2) provide **high-resolution Calabria municipality layers** with transparent **confidence grading**.

## Feasibility findings and source-family evaluation

### Official police crime statistics

The police “delitti denunciati” pipeline is well-defined: the data are extracted from the **Sistema di indagine (SDI)** of the entity["organization","Ministero dell'Interno","italy interior ministry"] and delivered to entity["organization","ISTAT","italian national institute of statistics"] in **aggregated form**. The official note describes the unit of analysis (crimes and authors at the time of communication to judicial authority), that contraventions are excluded, and that key variables include **crime type** and **place where the crime was committed**; the current configuration starts in **2004** (SDI era). citeturn29view0

For arson-relevant categories, ISTAT’s analytic crime classification explicitly lists codes for “incendio” and “danneggiamento seguito da incendio,” alongside related codes like “incendio boschivo.” citeturn11view0  
Separately, the Italian criminal code defines “danneggiamento seguito da incendio” (art. 424 c.p.), which is often the closest **crime-statistics proxy** for deliberate property damage by fire. citeturn8search14

**Granularity reality check:** public dissemination of these police-derived datasets is generally **not Comune-complete**. A widely cited limitation is that the interior ministry **does not publish denunciation data for all municipalities**, but only for **province** and **provincial capitals** (capoluoghi). citeturn17search9turn16view0  
This matters for Calabria because most Calabrian municipalities are **not** capoluoghi, so they disappear from any capoluogo-only publication.

**Key implication:** Police crime statistics can support a rigorous **province-level “arson-related crime rate”** indicator nationwide, but they do not, by themselves, give you a reliable annual “car burnings per Comune” metric for all Italy.

### Fire brigade intervention statistics

The entity["organization","Corpo Nazionale dei Vigili del Fuoco","italian fire and rescue service"] maintains an intervention reporting system (“STAT-RI Web”) and publishes an annual statistical yearbook (“Annuario statistico”). The yearbook documents the data source (STAT‑RI Web), and it explicitly notes that intervention reports can include fields such as **type of incident**, **cause**, **place**, and **substance involved**. citeturn43view0turn19view3

Crucially for your “car burnings” use case, the yearbook includes national counts for vehicle involvement within the “incendi ed esplosioni” domain. In its “substance involved” table, “Mezzi di trasporto: Autovetture” appears explicitly (e.g., **15,709** interventions in 2023 as shown in that table excerpt). citeturn21view0turn21view2  
It also shows that “cause” can include an arson-likeness dimension (“probabile dolo” vs. “probabile colpa”), though a large share of causes may be “not ascertainable immediately.” citeturn21view2

**Granularity:** the published yearbook provides outputs at **national, regional, and provincial** levels (sections explicitly enumerate “a livello regionale” and “a livello provinciale”). citeturn43view0  
In addition, Calabria is explicitly mentioned among regions where “incendi ed esplosioni” interventions exceed a threshold per unit area in 2023 (a signal that the region is non-trivial in the intervention landscape). citeturn44view1

**Municipality-level availability:** municipality-level intervention counts for “autovetture” (and especially “probabile dolo” for those) **are not published** as an open dataset in the yearbook. However, because the underlying system contains “place” fields and the service is part of SISTAN-style statistical processes, a feasible path is to request **aggregated municipality-year tables** via FOIA-style requests (see “Collection and validation plan”). citeturn43view0turn51view0turn51view1

**SPID note:** the fire brigade’s “Prevenzione OnLine” portal supports SPID login, but it is for prevention procedures (not intervention statistics). It may help establish an authenticated relationship with the platform ecosystem, but it does **not** appear to expose intervention-level datasets for public download. citeturn49view0turn43view0

### Calabria municipal and regional open data portals

Calabria municipalities do operate open data portals (e.g., entity["city","Catanzaro","Calabria, Italy"] has a CKAN instance), but the typical datasets highlighted are administrative (budget, acts, transport) rather than public safety incident logs. citeturn23view2  
Region-level geodata portals in Calabria exist, but a prominent “fire” dataset (“catasto incendi”) is oriented to land/forest fire footprints (and may be marked “dataset not available” for open download), which is not a direct proxy for *vehicle arsons*. citeturn22search37

**Practical expectation:** municipal open data in Calabria is worth checking, but the probability of finding a ready-made “incendi autovetture” annual series by Comune is **low**.

### Law enforcement press releases and logs

Press releases from entity["organization","Polizia di Stato","italian state police"] Questure and entity["organization","Arma dei Carabinieri","italian gendarmerie"] can contain high-signal references to “incendio di autovettura” and “danneggiamento seguito da incendio,” including Calabria-related cases. citeturn25search1turn25search0

However, automated collection feasibility differs:

- Questure pages show anti-bot / protection mechanisms that can block scripted access (“request blocked by protection systems”). This materially reduces the feasibility of continuous scraping at scale without explicit permission or a partnership. citeturn27view1turn27view4  
- Carabinieri press releases are paginated and, in practice, generally easier to navigate programmatically, but they cover only a subset of all incidents (those that become communications). citeturn25search4turn25search11

**Key implication:** press releases are excellent for **high-precision incident confirmation** and **manual validation**, but they are not a complete “count” of all car burnings.

### Local news and social media

Local news can provide much broader coverage of vehicle fires (including smaller towns), enabling Comune-level incident extraction via NLP (place and date recognition, deduplication, etc.). The downsides are well-known: incomplete archives, duplicates, sensational bias, and legal/copyright constraints. Because this is not an official dataset, you must treat it as a **supporting layer** with explicit uncertainty, and you should avoid reproducing article text in your database beyond what’s needed for verification metadata.

Social media can add early signals but is unreliable for counts and often lacks consistent geolocation; it is best used for investigation prompts rather than production metrics.

### Insurance and financial aggregates

Insurance statistics can be useful **proxies** for “vehicle fire loss” risk, but they are not equivalent to “vehicle arson count”:

- entity["organization","ANIA","italian insurance association"] publishes “Corpi Veicoli Terrestri” statistics intended to monitor, at **provincial level**, insured risk distribution and claim frequency/cost for major guarantees, including “incendio.” citeturn26search8turn26search4  
- entity["organization","IVASS","italian insurance supervisory authority"] publishes material on “criminalità settore assicurativo,” i.e., crime phenomena in insurance claims across lines including “corpi di veicoli terrestri” and “incendio.” citeturn26search9  

These are valuable for calibration but suffer from: (a) incomplete coverage because many vehicles lack “furto/incendio” add-ons; (b) claims include accidental fires; and (c) confidentiality often limits granularity below province.

### Other proxies and administrative data

Emergency call volumes (112/115) or municipal incident registries are conceptually strong but are rarely open. In Italy, a realistic route is targeted requests via **accesso civico generalizzato (FOIA)** for **aggregated** counts. The official FOIA guidance stresses that anyone can request data/documents held by public administrations, within statutory limits protecting other interests. citeturn51view0turn51view1

## Data-source catalog

The table below lists prioritized sources (top = most authoritative for your goal) and includes raw URLs in a printable format. Where scraping is risky, the notes reflect the operational constraint.

```text
Table: prioritized sources (source_name | url | fields | granularity | license | update_cadence | notes)

1) ISTAT analytic crime classification (codes for "INC", "DSI", etc.)
   url: https://www.istat.it/classificazione/classificazione-analitica-dei-reati/
   fields: code, italian label, english label, validity dates
   granularity: n/a (classification, not counts)
   license: as published on ISTAT site (check legal notes on page)
   update_cadence: occasional
   notes: authoritative mapping for arson-related categories; includes "INC incendio" and "DSI danneggiamento seguito da incendio"

2) ISTAT “Delitti denunciati…” methodology note (SDI-based)
   url: https://www.istat.it/it/files/2011/02/nota_descrittiva2.pdf
   fields: source ownership, unit of analysis, key variables, periodicity, start year
   granularity: n/a (metadata)
   license: ISTAT publication terms (see document/site)
   update_cadence: occasional
   notes: states data are transmitted to ISTAT aggregated and extracted from SDI; key variable includes "luogo del commesso delitto"

3) “Delitti denunciati per capoluogo e provincia” (CKAN, DFP / Min. Interni)
   url: https://dati-coll.dfp.gov.it/dataset/delitti
   fields: regione, capoluogo (capoluogo di provincia), reato (codice+descrizione), totale_delitti
   granularity: capoluogo di provincia + province (as published)
   license: CC BY 4.0
   update_cadence: annual (metadata says annual; verify actual refresh)
   notes: provides downloadable CSV/JSON + API; good starting point for arson-proxy at capoluogo/province

4) Ufficio Centrale di Statistica (Min. Interno) “Numero dei Delitti denunciati…” (INT 00062)
   url: https://ucs.interno.gov.it/ucs/contenuti/Numero_dei_delitti_denunciati_all_autorita_giudiziaria_dalle_forze_di_polizia_int_00062-7730889.htm
   fields: crime categories; territory; counts
   granularity: generally province + capoluogo (public releases)
   license: depends on portal terms
   update_cadence: annual
   notes: primary interior-ministry publication; may be easier to access via mirrored CKAN extracts when portal blocks automation

5) Vigili del Fuoco “Annuario statistico” (example: reference year 2023, published 2024)
   url: https://www.vigilfuoco.it/sites/default/files/2024-08/Annuario%20statistico%202024_3.pdf
   fields: interventions by type; cross-tabs with place/cause/substance; regional/provincial breakdowns
   granularity: national + regional + provincial (published); underlying system likely more granular
   license: document/site terms
   update_cadence: annual
   notes: includes explicit “Autovetture” and cause “probabile dolo” in tables (for “incendi ed esplosioni” domain)

6) VVF “Prevenzione OnLine” (SPID login)
   url: https://prevenzioneonline.vigilfuoco.it/prevenzione-online/login
   fields: prevention-procedure services (not incident stats)
   granularity: user/procedure level (authenticated)
   license: service terms
   update_cadence: continuous
   notes: confirms SPID authentication is supported; not a direct source for intervention counts

7) ANIA “Statistica annuale Corpi Veicoli Terrestri” (provincial monitoring)
   url: https://www.ania.it/pubblicazioni/-/asset_publisher/xWCafIRLBS4g/content/id/529483
   fields: insured risks, claim frequency/cost by guarantee (including incendio), often provincial-level
   granularity: typically province (for insured portfolio)
   license: ANIA publication terms
   update_cadence: annual
   notes: proxy for vehicle-fire claims; not equivalent to arson; biased by insurance coverage rates

8) IVASS “Criminalità settore assicurativo”
   url: https://www.ivass.it/pubblicazioni-e-statistiche/statistiche/elaborazioni-statistiche/csa/index.html
   fields: crime-related claims indicators across insurance lines
   granularity: usually national / macro (verify tables)
   license: IVASS site terms
   update_cadence: periodic
   notes: useful contextual proxy; rarely municipality-level

9) FOIA guidance (Dipartimento Funzione Pubblica) and ANAC access pages
   url: https://docs.italia.it/italia/funzione-pubblica/foia-circolare2-docs/it/stabile/
   url: https://www.anticorruzione.it/-/accesso-civico-e-accesso-civico-generalizzato
   fields: rules, limits, how to request
   granularity: n/a
   license: site terms
   update_cadence: occasional
   notes: supports obtaining aggregated municipality-year tables from VVF/police if not publicly released

10) Questure press releases (Polizia di Stato) and Carabinieri comunicati stampa
   url: https://questure.poliziadistato.it/
   url: https://www.carabinieri.it/in-vostro-aiuto/informazioni/comunicati-stampa
   fields: narrative incidents (often place/date/crime)
   granularity: often municipality-level in text
   license: site terms; beware copyright/personal data
   update_cadence: daily
   notes: high precision but incomplete; Questure site may block scraping; treat as validation layer

11) Calabria region geoportal (fire-related, mainly land/forest context)
   url: https://geoportale.regione.calabria.it/dataset
   fields: geospatial layers (some “incendi” are land/forest-related)
   granularity: spatial layers (varies)
   license: dataset-specific
   update_cadence: varies
   notes: not a direct car-arson source but useful for broader fire-risk context
```

The most decision-critical elements for your app are: (a) the police statistics being SDI-derived and aggregated citeturn29view0, (b) the arson-relevant offense classes (INC/DSI) existing and being standardized citeturn11view0turn8search14, and (c) the fire brigade (VVF) explicitly tracking “autovetture” and “probabile dolo” within its intervention statistics citeturn21view2turn43view0.

## Recommended proxy metrics and multi-stage data strategy

### What you should measure

Because “vehicle arson” is not consistently published as its own Comune-level count, define three complementary metrics and expose them as separate toggles:

- **Vehicle-fire interventions (VVF proxy):** annual count of VVF interventions where the “substance involved” is **Autovetture**; optionally split into “probabile dolo” vs other/unknown causes. citeturn21view2  
- **Arson-related crime proxy (police proxy):** annual count/rate of police-registered crimes in arson-related classes such as **INC (incendio)** and **DSI (danneggiamento seguito da incendio)**. citeturn11view0turn29view0turn8search14  
- **Confirmed enforcement events (press proxy):** annual count of “vehicle arson/vehicle fire” incidents referenced in police/carabinieri communications (higher precision, incomplete). citeturn25search1turn27view1  

Then build a **composite indicator** only as a *derived view* (with strong disclaimers), not as your primary statistic.

### Multi-stage strategy tailored to Italy and Calabria

**Stage A: Nationwide official-proxy layer (publishable quickly)**  
Use the best-available official public granularity:

- Province-level rates: INC + DSI per 100k residents (police proxy). citeturn29view0turn11view0  
- Capoluogo-level rates where available (the “capoluogo/provincia” dataset is explicitly designed to publish those). citeturn16view0turn30view0  
- Fire brigade province-level “incendi ed esplosioni” volume + contextual shares (autovetture and probable dolo shares from the yearbook) as a secondary context layer, clearly labeled. citeturn21view2turn43view0  

**Stage B: Calabria municipality-level enrichment (the path to Comune granularity)**  
Pursue municipality-year tables via **FOIA / accesso civico generalizzato** requests for aggregated data (not personal data), targeting:

- VVF aggregated intervention counts by **Comune × year** filtered to:  
  - “incendi ed esplosioni” with **substance = autovetture**, and  
  - “cause = probabile dolo” (and/or a broader arson-likeness flag). citeturn21view2turn43view0turn51view0turn51view1  

This is the most promising route to a **true municipality-level “car burnings” metric**, because VVF’s own published tables show the key dimensions exist in the source system. citeturn21view2turn19view3

Enrich and validate with:

- Police/carabinieri press events geocoded to municipality (validation + storytelling). citeturn25search1turn27view4  
- Local news event extraction (coverage expansion), but keep it as lower confidence.

**Stage C: Confidence grading and map publishing rules**  
Publish each municipality-year value with an explicit confidence grade.

```text
Table: confidence grading rubric (grade | criteria | typical sources | mapping rule)

A | Official aggregated counts at Comune×year (or finer) with clear definitions and consistent coverage
  | FOIA-delivered VVF tables; official municipal incident registry (rare)
  | Full color on map; show exact counts + rate; include methodological notes

B | Mixed evidence: partial official + multi-source validation; definitions stable but coverage incomplete
  | Capoluogo/province official + press-release counts + curated news; reconciled to province totals
  | Color on map but add “estimated/partial” badge; show uncertainty band or “min–max”

C | Derived mainly from non-official sources; high bias or sparse coverage; cannot reconcile to official totals
  | News + social only; unvalidated user reports
  | Display as hatched/gray overlay or “low confidence”; default hidden unless user toggles “experimental”
```

## Collection, validation, and QA plan

### Ordered ETL checklist

```text
Table: recommended ETL steps (step | description | output | QA gates)

Ingest official classifications
  - Pull ISTAT analytic classification; retain codes for INC/DSI/etc.
  - Output: dim_crime_codes
  - QA: code list versioned; unit tests for lookup

Ingest police proxy dataset (province/capoluogo)
  - Use CKAN dataset + API; store raw + normalized
  - Output: raw_delitti_records; agg_delitti_province_year
  - QA: schema validation; completeness by year; anomaly detection vs prior year

Ingest VVF yearbook-derived national/province context
  - Parse yearbook tables for “autovetture” share and “probabile dolo” share (contextual priors)
  - Output: vvf_context_year (national priors); vvf_province_totals (if available)
  - QA: cross-check totals; store page references

Calabria FOIA acquisition (VVF)
  - Request Comune×year aggregated tables (autovetture fires; probable dolo subset)
  - Output: raw_foia_files; agg_vvf_vehicle_fires_municipality_year
  - QA: reconcile to regional totals where possible; document definitions

Press-release ingestion (Calabria focus)
  - Collect Carabinieri releases; optionally curated Questure releases (manual/partner due to blocks)
  - Output: raw_press_docs; press_incident_events
  - QA: deduplicate; verify municipality; tag “confirmed enforcement”

News extraction (optional expansion)
  - Crawl allowed sources; run NER + geocoding; dedupe
  - Output: news_incident_events
  - QA: double-source confirmation rule; manual sampling

Build published map tiles / API views
  - Aggregate to municipality-year; compute per-100k residents; attach confidence grade
  - Output: metrics_vehicle_arson_muni_year; vector tile layers
  - QA: leakage checks; small-number suppression; audit logs

Monitoring
  - Track per-source updates and gaps; alert on missing refresh
  - Output: data_freshness_dashboard
  - QA: SLA checks; lineage completeness
```

### Validation rules that make the metric defensible

- **Deduplication**: treat the same incident reported by multiple channels as one event; match by (municipality, date window, keywords, number of vehicles) and optionally fuzzy street matching when available.  
- **Reconciliation**: where you have province totals (official) and municipality sums (from FOIA or events), reconcile and flag discrepancies as either “missing municipalities” or “source overcount.”  
- **Sampling audits**: each quarter, manually validate a stratified sample of municipalities (high/medium/low) against source documents.

### Calabria-specific candidate municipalities to check for open data and local reporting

To maximize early Calabria coverage, prioritize these towns for portal checks and local-source tuning (single mention list; generally aligned with population and reporting density):  
entity["city","Reggio Calabria","Calabria, Italy"], entity["city","Catanzaro","Calabria, Italy"], entity["city","Cosenza","Calabria, Italy"], entity["city","Crotone","Calabria, Italy"], entity["city","Vibo Valentia","Calabria, Italy"], entity["city","Lamezia Terme","Calabria, Italy"], entity["city","Corigliano-Rossano","Calabria, Italy"], entity["city","Gioia Tauro","Calabria, Italy"], entity["city","Palmi","Calabria, Italy"], entity["city","Rosarno","Calabria, Italy"], entity["city","Locri","Calabria, Italy"], entity["city","Siderno","Calabria, Italy"].

### Sample search queries in Italian for incident discovery

```text
"incendio autovettura" Calabria "Comune di <NOME>"
"auto bruciata" "<NOME COMUNE>" "nella notte"
"danneggiamento seguito da incendio" "<NOME COMUNE>"
site:questure.poliziadistato.it "incendio" "autovettura" Calabria
site:carabinieri.it "incendio" "autovettura" Calabria
"incendio" "autovetture" "Vigili del Fuoco" "<NOME COMUNE>"
```

### Workflow diagram

```mermaid
flowchart LR
  A[Official code lists (INC/DSI)] --> B[Police proxy ingestion (province/capoluogo)]
  A --> C[VVF yearbook context (vehicle fire + probable dolo shares)]
  B --> D[Nationwide proxy layer]
  C --> D
  E[FOIA requests to VVF: Comune×year tables] --> F[Calabria municipality layer]
  G[Press releases + curated news] --> H[Validation + enrichment]
  F --> I[Confidence grading + publish rules]
  H --> I
  D --> I
  I --> J[Heatmap tiles / API]
```

## Legal, privacy, and licensing considerations

Publishing **aggregated annual counts by municipality** is generally far safer than publishing incident-level narratives. The VVF yearbook itself states that data collected under the statistical system should not be externalized except in aggregated form so that no individual reference is possible. citeturn43view0

For FOIA acquisition, the Italian FOIA guidance confirms that accesso civico generalizzato allows requesting data and documents held by public administrations, within limits protecting relevant public/private interests. citeturn51view0turn51view1  
This strongly supports a strategy of requesting *aggregated Comune×year counts*, which minimizes privacy concerns and reduces refusal risk.

**Key legal/privacy checklist for your implementation:**

- Do not store personal names or identifying details from press releases/news in your main analytics tables; store only event metadata (date, municipality, source URL hash).  
- Respect licensing: the capoluogo/province CKAN dataset is explicitly CC BY 4.0. citeturn16view0turn30view0turn32view0  
- For news sources, store only what is necessary for verification (URL, timestamp, extracted municipality/date) and avoid reproducing full text; show source attribution in UI.  
- If you use Questure pages, note that automated access can be blocked; treat systematic scraping as a partnership/permission activity rather than a hidden crawler. citeturn27view1turn27view4  

## Implementation guidance for mapping and storage

### Data model recommendations

Use two layers: (1) raw events/documents, (2) publishable aggregates.

```sql
-- Raw incident/event table (minimize personal data)
create table raw_vehicle_fire_events (
  event_id uuid primary key,
  source_family text not null,        -- 'vvf_foia', 'police_proxy', 'press_release', 'news', 'insurance'
  source_name text not null,
  source_url text,                    -- store URL or stable identifier; consider hashing if needed
  published_at timestamptz,
  occurred_at date,                   -- if known, else null
  muni_istat_code text,               -- ISTAT municipality code
  province_code text,                 -- e.g., ITA province code
  region_code text,
  event_type text not null,           -- 'vehicle_fire', 'vehicle_arson_suspected', 'vehicle_arson_confirmed'
  vehicles_involved integer,
  arson_likelihood text,              -- 'probabile_dolo', 'unknown', 'probabile_colpa' (VVF-compatible)
  extractor_version text,
  created_at timestamptz default now()
);

-- Aggregated publishable metrics at Comune×year
create table metrics_vehicle_arson_muni_year (
  muni_istat_code text not null,
  year int not null,
  count_vehicle_fire int,
  count_vehicle_arson_suspected int,
  count_vehicle_arson_confirmed int,
  rate_per_100k_residents numeric,
  confidence_grade text not null,     -- 'A','B','C'
  sources_used text[],                -- e.g., ['vvf_foia','press_release']
  notes text,
  updated_at timestamptz default now(),
  primary key (muni_istat_code, year)
);

-- Province-level official proxy (nationwide coverage)
create table metrics_arson_proxy_province_year (
  province_code text not null,
  year int not null,
  count_incendio int,
  count_danneggiamento_seguito_da_incendio int,
  rate_per_100k_residents numeric,
  source_version text,
  primary key (province_code, year)
);
```

### Map UX and disclaimers

- Provide distinct toggles: “**Vehicle fire interventions** (VVF proxy)”, “**Arson-related crimes** (police proxy)”, “**Confirmed enforcement events** (press proxy)”.  
- Default to showing **confidence A/B** and let users opt into “low confidence (C)” overlays.  
- Show **data coverage badges**: “Comune-level official”, “Province-level only”, “Estimated/partial”.  
- Always show “How this is measured” with: source attribution, last update, and known limitations (e.g., underreporting, unknown causes). Police statistics are known to be influenced by the propensity to report; this limitation is discussed in ISTAT’s integrated publications on delitti denunciati. citeturn12view0turn13view0

### Prioritized next actions

- Submit a Calabria-targeted FOIA request to VVF for **Comune×year aggregated counts** of interventions with “Autovetture” and “Probabile dolo” classification (and definitions). citeturn21view2turn51view0turn51view1  
- Build the nationwide province/capoluogo arson-proxy layer using the CKAN dataset and ISTAT classification mapping for “INC/DSI” (or the dataset’s own “reato” codes). citeturn16view0turn11view0turn30view0  
- Implement the confidence rubric and publish as an explicit user-facing layer so the Calabria municipality enrichment can go live incrementally without overclaiming.  
- Treat Questure scraping as “permission/partnership only” due to active blocking signals. citeturn27view1turn27view4