-- Seed OMI zone demo data for existing municipalities

-- First ensure the time period exists
INSERT INTO core.time_periods (period_id, period_type, period_start_date, period_end_date, year, semester)
VALUES ('2024H2', 'semester', '2024-07-01', '2024-12-31', 2024, 2)
ON CONFLICT (period_id) DO NOTHING;

-- Insert demo zones for Milano (015146)
INSERT INTO core.omi_zones (omi_zone_id, municipality_id, zone_code, zone_type, zone_description, microzone_code)
VALUES
  ('015146_B1', '015146', 'B1', 'B', 'Centro Storico', 'B1.1'),
  ('015146_B2', '015146', 'B2', 'B', 'Duomo - San Babila', 'B2.1'),
  ('015146_C1', '015146', 'C1', 'C', 'Porta Venezia', 'C1.1'),
  ('015146_C2', '015146', 'C2', 'C', 'Navigli', 'C2.1'),
  ('015146_C3', '015146', 'C3', 'C', 'Isola', 'C3.1'),
  ('015146_D1', '015146', 'D1', 'D', 'Città Studi', 'D1.1'),
  ('015146_D2', '015146', 'D2', 'D', 'Lambrate', 'D2.1'),
  ('015146_D3', '015146', 'D3', 'D', 'Lorenteggio', 'D3.1'),
  ('015146_E1', '015146', 'E1', 'E', 'Quarto Oggiaro', 'E1.1'),
  ('015146_E2', '015146', 'E2', 'E', 'Baggio', 'E2.1'),
  ('015146_R1', '015146', 'R1', 'R', 'Chiaravalle', 'R1.1')
ON CONFLICT (omi_zone_id) DO NOTHING;

-- Insert demo zones for Roma (058091)
INSERT INTO core.omi_zones (omi_zone_id, municipality_id, zone_code, zone_type, zone_description, microzone_code)
VALUES
  ('058091_B1', '058091', 'B1', 'B', 'Centro Storico', 'B1.1'),
  ('058091_B2', '058091', 'B2', 'B', 'Trastevere', 'B2.1'),
  ('058091_B3', '058091', 'B3', 'B', 'Prati', 'B3.1'),
  ('058091_C1', '058091', 'C1', 'C', 'Testaccio', 'C1.1'),
  ('058091_C2', '058091', 'C2', 'C', 'San Giovanni', 'C2.1'),
  ('058091_C3', '058091', 'C3', 'C', 'Trieste', 'C3.1'),
  ('058091_C4', '058091', 'C4', 'C', 'Parioli', 'C4.1'),
  ('058091_D1', '058091', 'D1', 'D', 'Tuscolano', 'D1.1'),
  ('058091_D2', '058091', 'D2', 'D', 'Montesacro', 'D2.1'),
  ('058091_D3', '058091', 'D3', 'D', 'Ostiense', 'D3.1'),
  ('058091_D4', '058091', 'D4', 'D', 'Magliana', 'D4.1'),
  ('058091_E1', '058091', 'E1', 'E', 'Tor Bella Monaca', 'E1.1'),
  ('058091_E2', '058091', 'E2', 'E', 'Casal Palocco', 'E2.1'),
  ('058091_R1', '058091', 'R1', 'R', 'Campagna Romana', 'R1.1')
ON CONFLICT (omi_zone_id) DO NOTHING;

-- Insert zone values for Milano zones (2024H2)
INSERT INTO mart.omi_zone_values_semester (omi_zone_id, period_id, property_segment, value_mid_eur_sqm, value_min_eur_sqm, value_max_eur_sqm, rent_mid_eur_sqm_month, value_pct_change_1s)
VALUES
  ('015146_B1', '2024H2', 'residential', 8500, 6800, 12000, 34.0, 3.2),
  ('015146_B2', '2024H2', 'residential', 9200, 7500, 13500, 38.0, 4.1),
  ('015146_C1', '2024H2', 'residential', 6200, 4800, 8500, 25.0, 2.8),
  ('015146_C2', '2024H2', 'residential', 6800, 5200, 9200, 27.5, 3.5),
  ('015146_C3', '2024H2', 'residential', 5900, 4500, 8000, 24.0, 5.2),
  ('015146_D1', '2024H2', 'residential', 4200, 3200, 5800, 17.0, 1.5),
  ('015146_D2', '2024H2', 'residential', 4500, 3400, 6000, 18.0, 2.8),
  ('015146_D3', '2024H2', 'residential', 3500, 2700, 4800, 14.0, 0.8),
  ('015146_E1', '2024H2', 'residential', 2800, 2100, 3800, 11.5, -0.5),
  ('015146_E2', '2024H2', 'residential', 2600, 2000, 3500, 10.5, -0.2),
  ('015146_R1', '2024H2', 'residential', 2200, 1600, 3000, 9.0, 0.3)
ON CONFLICT (omi_zone_id, period_id, property_segment) DO UPDATE SET
  value_mid_eur_sqm = EXCLUDED.value_mid_eur_sqm,
  value_min_eur_sqm = EXCLUDED.value_min_eur_sqm,
  value_max_eur_sqm = EXCLUDED.value_max_eur_sqm,
  rent_mid_eur_sqm_month = EXCLUDED.rent_mid_eur_sqm_month,
  value_pct_change_1s = EXCLUDED.value_pct_change_1s;

-- Insert zone values for Roma zones (2024H2)
INSERT INTO mart.omi_zone_values_semester (omi_zone_id, period_id, property_segment, value_mid_eur_sqm, value_min_eur_sqm, value_max_eur_sqm, rent_mid_eur_sqm_month, value_pct_change_1s)
VALUES
  ('058091_B1', '2024H2', 'residential', 7200, 5800, 10500, 30.0, 2.5),
  ('058091_B2', '2024H2', 'residential', 6800, 5200, 9500, 28.0, 3.8),
  ('058091_B3', '2024H2', 'residential', 6500, 5000, 9000, 27.0, 2.2),
  ('058091_C1', '2024H2', 'residential', 5200, 4000, 7000, 21.5, 2.0),
  ('058091_C2', '2024H2', 'residential', 4800, 3600, 6500, 19.5, 1.8),
  ('058091_C3', '2024H2', 'residential', 5500, 4200, 7500, 22.5, 2.5),
  ('058091_C4', '2024H2', 'residential', 6200, 4800, 8500, 25.5, 1.5),
  ('058091_D1', '2024H2', 'residential', 3500, 2700, 4800, 14.5, 0.8),
  ('058091_D2', '2024H2', 'residential', 3200, 2400, 4400, 13.0, 1.2),
  ('058091_D3', '2024H2', 'residential', 3400, 2600, 4600, 14.0, 3.5),
  ('058091_D4', '2024H2', 'residential', 2900, 2200, 4000, 12.0, 0.5),
  ('058091_E1', '2024H2', 'residential', 2200, 1600, 3100, 9.0, -1.2),
  ('058091_E2', '2024H2', 'residential', 2800, 2100, 3800, 11.5, 0.8),
  ('058091_R1', '2024H2', 'residential', 1800, 1200, 2600, 7.5, 0.2)
ON CONFLICT (omi_zone_id, period_id, property_segment) DO UPDATE SET
  value_mid_eur_sqm = EXCLUDED.value_mid_eur_sqm,
  value_min_eur_sqm = EXCLUDED.value_min_eur_sqm,
  value_max_eur_sqm = EXCLUDED.value_max_eur_sqm,
  rent_mid_eur_sqm_month = EXCLUDED.rent_mid_eur_sqm_month,
  value_pct_change_1s = EXCLUDED.value_pct_change_1s;

-- Output summary
SELECT 'Zones inserted: ' || COUNT(*) FROM core.omi_zones;
SELECT 'Zone values inserted: ' || COUNT(*) FROM mart.omi_zone_values_semester;
