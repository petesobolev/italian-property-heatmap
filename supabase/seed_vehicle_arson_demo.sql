-- Demo data for vehicle arson metric
-- Focus on Calabria region (region_code = '18') as per implementation plan

-- Insert demo vehicle arson data for Calabria municipalities
-- Data is illustrative only - not real statistics

INSERT INTO mart.vehicle_arson_municipality_year
  (municipality_id, year, count_vehicle_fire, count_vehicle_arson_suspected, count_vehicle_arson_confirmed, rate_per_100k_residents, confidence_grade, sources_used, notes)
VALUES
  -- Reggio Calabria province (080)
  ('080063', 2023, 145, 89, 34, 78.5, 'B', ARRAY['vvf_foia', 'press_release'], 'Reggio Calabria city'),
  ('080039', 2023, 28, 18, 7, 112.3, 'B', ARRAY['vvf_foia', 'press_release'], 'Gioia Tauro'),
  ('080056', 2023, 22, 14, 5, 95.2, 'B', ARRAY['vvf_foia'], 'Palmi'),
  ('080068', 2023, 19, 12, 4, 124.8, 'B', ARRAY['vvf_foia', 'press_release'], 'Rosarno'),
  ('080046', 2023, 15, 9, 3, 88.4, 'C', ARRAY['press_release', 'news'], 'Locri'),
  ('080075', 2023, 14, 8, 3, 82.1, 'C', ARRAY['press_release', 'news'], 'Siderno'),

  -- Catanzaro province (079)
  ('079023', 2023, 52, 31, 12, 56.8, 'B', ARRAY['vvf_foia', 'press_release'], 'Catanzaro city'),
  ('079071', 2023, 38, 24, 9, 52.4, 'B', ARRAY['vvf_foia'], 'Lamezia Terme'),

  -- Cosenza province (078)
  ('078045', 2023, 67, 42, 16, 98.2, 'B', ARRAY['vvf_foia', 'press_release'], 'Cosenza city'),
  ('078044', 2023, 31, 19, 7, 41.5, 'B', ARRAY['vvf_foia'], 'Corigliano-Rossano'),

  -- Crotone province (101)
  ('101014', 2023, 35, 22, 8, 54.3, 'B', ARRAY['vvf_foia', 'press_release'], 'Crotone city'),

  -- Vibo Valentia province (102)
  ('102046', 2023, 18, 11, 4, 54.8, 'B', ARRAY['vvf_foia'], 'Vibo Valentia city'),

  -- Additional municipalities with lower confidence data
  ('080001', 2023, 3, 2, 1, 45.2, 'C', ARRAY['news'], 'Africo'),
  ('080005', 2023, 4, 3, 1, 67.8, 'C', ARRAY['news'], 'Anoia'),
  ('080012', 2023, 2, 1, 0, 38.5, 'C', ARRAY['news'], 'Bovalino'),
  ('080018', 2023, 5, 3, 1, 52.3, 'C', ARRAY['news'], 'Cardeto'),
  ('080025', 2023, 3, 2, 1, 71.4, 'C', ARRAY['news'], 'Cittanova'),
  ('080032', 2023, 2, 1, 0, 44.6, 'C', ARRAY['news'], 'Gerace'),
  ('080051', 2023, 4, 2, 1, 58.9, 'C', ARRAY['news'], 'Melito di Porto Salvo'),
  ('080059', 2023, 3, 2, 1, 49.2, 'C', ARRAY['news'], 'Platì'),
  ('080071', 2023, 6, 4, 2, 83.7, 'C', ARRAY['news'], 'San Luca'),
  ('080088', 2023, 2, 1, 0, 35.8, 'C', ARRAY['news'], 'Taurianova')
ON CONFLICT (municipality_id, year) DO UPDATE SET
  count_vehicle_fire = EXCLUDED.count_vehicle_fire,
  count_vehicle_arson_suspected = EXCLUDED.count_vehicle_arson_suspected,
  count_vehicle_arson_confirmed = EXCLUDED.count_vehicle_arson_confirmed,
  rate_per_100k_residents = EXCLUDED.rate_per_100k_residents,
  confidence_grade = EXCLUDED.confidence_grade,
  sources_used = EXCLUDED.sources_used,
  notes = EXCLUDED.notes,
  updated_at = now();

-- Insert province-level proxy data for all Italian provinces (sample)
-- Using arson-related crime categories (INC + DSI)
INSERT INTO mart.arson_proxy_province_year
  (province_code, year, count_incendio, count_danneggiamento_seguito_da_incendio, count_total_arson_related, rate_per_100k_residents, source_version)
VALUES
  -- Calabria provinces (higher rates)
  ('080', 2023, 342, 198, 540, 98.2, 'ISTAT_2023'),
  ('079', 2023, 187, 112, 299, 84.5, 'ISTAT_2023'),
  ('078', 2023, 256, 156, 412, 59.8, 'ISTAT_2023'),
  ('101', 2023, 98, 62, 160, 94.1, 'ISTAT_2023'),
  ('102', 2023, 78, 48, 126, 79.5, 'ISTAT_2023'),

  -- Other southern provinces (medium-high rates)
  ('063', 2023, 1245, 756, 2001, 65.8, 'ISTAT_2023'), -- Napoli
  ('075', 2023, 412, 248, 660, 52.3, 'ISTAT_2023'),   -- Bari
  ('087', 2023, 567, 342, 909, 45.2, 'ISTAT_2023'),   -- Palermo
  ('089', 2023, 234, 145, 379, 78.4, 'ISTAT_2023'),   -- Catania

  -- Central/Northern provinces (lower rates)
  ('058', 2023, 892, 534, 1426, 32.8, 'ISTAT_2023'),  -- Roma
  ('015', 2023, 567, 345, 912, 28.1, 'ISTAT_2023'),   -- Milano
  ('001', 2023, 312, 189, 501, 22.4, 'ISTAT_2023'),   -- Torino
  ('048', 2023, 145, 87, 232, 23.1, 'ISTAT_2023'),    -- Firenze
  ('027', 2023, 98, 62, 160, 18.9, 'ISTAT_2023')      -- Venezia
ON CONFLICT (province_code, year) DO UPDATE SET
  count_incendio = EXCLUDED.count_incendio,
  count_danneggiamento_seguito_da_incendio = EXCLUDED.count_danneggiamento_seguito_da_incendio,
  count_total_arson_related = EXCLUDED.count_total_arson_related,
  rate_per_100k_residents = EXCLUDED.rate_per_100k_residents,
  source_version = EXCLUDED.source_version,
  updated_at = now();

-- Output summary
SELECT 'Vehicle arson municipality data inserted: ' || COUNT(*) FROM mart.vehicle_arson_municipality_year;
SELECT 'Province arson proxy data inserted: ' || COUNT(*) FROM mart.arson_proxy_province_year;
