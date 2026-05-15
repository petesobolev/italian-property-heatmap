-- Provincial-level transaction data from OMI
-- Public data showing NTN (Numero Transazioni Normalizzate) by province

CREATE TABLE IF NOT EXISTS mart.province_transactions_semester (
  province_code text NOT NULL,
  period_id text NOT NULL,              -- YYYYS format (e.g., 20241)
  is_capoluogo boolean NOT NULL,        -- true = capital city, false = other municipalities
  property_segment text NOT NULL DEFAULT 'residential',

  -- Transaction metrics
  ntn_total numeric NULL,               -- Total normalized transactions
  ntn_per_1000_pop numeric NULL,        -- Per capita metric (calculated)

  -- Metadata
  updated_at timestamptz DEFAULT now(),

  PRIMARY KEY (province_code, period_id, is_capoluogo, property_segment)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS province_transactions_period_idx
  ON mart.province_transactions_semester (period_id);
CREATE INDEX IF NOT EXISTS province_transactions_province_idx
  ON mart.province_transactions_semester (province_code);

-- Enable RLS
ALTER TABLE mart.province_transactions_semester ENABLE ROW LEVEL SECURITY;

-- Allow public read access
CREATE POLICY "Allow public read access" ON mart.province_transactions_semester
  FOR SELECT USING (true);

-- Allow service role to insert/update
CREATE POLICY "Allow service role write access" ON mart.province_transactions_semester
  FOR ALL USING (auth.role() = 'service_role')
  WITH CHECK (auth.role() = 'service_role');

-- Grant permissions
GRANT SELECT ON mart.province_transactions_semester TO anon;
GRANT SELECT ON mart.province_transactions_semester TO authenticated;
GRANT ALL ON mart.province_transactions_semester TO service_role;

COMMENT ON TABLE mart.province_transactions_semester IS 'Provincial-level transaction volumes (NTN) from OMI public data';
COMMENT ON COLUMN mart.province_transactions_semester.is_capoluogo IS 'true = provincial capital city transactions, false = rest of province';
