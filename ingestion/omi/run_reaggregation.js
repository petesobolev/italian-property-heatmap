const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

async function runReaggregation() {
  console.log("Starting reaggregation from zone data...");

  // Get list of periods first
  const { data: periods, error: periodError } = await supabase
    .schema("mart")
    .rpc("get_available_periods", { p_segment: "residential" });

  if (periodError) {
    console.error("Error fetching periods:", periodError);
    return;
  }

  const periodIds = periods.map(p => p.period_id);
  console.log(`Found ${periodIds.length} periods to process`);

  // Process each period separately to avoid timeouts
  const allZoneData = [];

  for (const periodId of periodIds) {
    console.log(`  Fetching period ${periodId}...`);

    let offset = 0;
    let hasMore = true;
    const batchSize = 1000;

    while (hasMore) {
      const { data: batch, error } = await supabase
        .schema("mart")
        .from("omi_zone_values_semester")
        .select("omi_zone_id, period_id, property_segment, value_min_eur_sqm, value_max_eur_sqm, value_mid_eur_sqm, rent_min_eur_sqm_month, rent_max_eur_sqm_month, rent_mid_eur_sqm_month, data_quality_score")
        .eq("period_id", periodId)
        .range(offset, offset + batchSize - 1);

      if (error) {
        console.error(`Error fetching ${periodId}:`, error);
        break;
      }

      if (batch && batch.length > 0) {
        allZoneData.push(...batch);
        offset += batchSize;
        hasMore = batch.length === batchSize;
      } else {
        hasMore = false;
      }
    }
  }

  console.log(`  Total zone records: ${allZoneData.length}`);

  // Group by municipality/period/segment
  console.log("\nStep 2: Grouping by municipality/period/segment...");
  const grouped = new Map();

  for (const zone of allZoneData) {
    const municipalityId = zone.omi_zone_id.split("_")[0];
    const key = `${municipalityId}|${zone.period_id}|${zone.property_segment}`;

    if (!grouped.has(key)) {
      grouped.set(key, {
        municipality_id: municipalityId,
        period_id: zone.period_id,
        property_segment: zone.property_segment,
        zones: []
      });
    }
    grouped.get(key).zones.push(zone);
  }

  console.log(`  Created ${grouped.size} municipality/period/segment groups`);

  // Calculate aggregates
  console.log("\nStep 3: Calculating aggregates...");
  const aggregated = [];

  for (const [key, group] of grouped) {
    const zones = group.zones;
    const valuesWithData = zones.filter(z => z.value_mid_eur_sqm != null);
    const rentsWithData = zones.filter(z => z.rent_mid_eur_sqm_month != null);

    if (valuesWithData.length === 0) continue;

    const valuesMid = valuesWithData.map(z => z.value_mid_eur_sqm);
    const rentsMid = rentsWithData.map(z => z.rent_mid_eur_sqm_month);

    // Calculate median
    const sortedValues = [...valuesMid].sort((a, b) => a - b);
    const mid = Math.floor(sortedValues.length / 2);
    const median = sortedValues.length % 2 === 0
      ? (sortedValues[mid - 1] + sortedValues[mid]) / 2
      : sortedValues[mid];

    // Get min/max values
    const valsMin = zones.filter(z => z.value_min_eur_sqm != null).map(z => z.value_min_eur_sqm);
    const valsMax = zones.filter(z => z.value_max_eur_sqm != null).map(z => z.value_max_eur_sqm);
    const rentsMin = rentsWithData.map(z => z.rent_min_eur_sqm_month).filter(v => v != null);
    const rentsMax = rentsWithData.map(z => z.rent_max_eur_sqm_month).filter(v => v != null);
    const qualityScores = zones.map(z => z.data_quality_score).filter(v => v != null);

    aggregated.push({
      municipality_id: group.municipality_id,
      period_id: group.period_id,
      property_segment: group.property_segment,
      value_min_eur_sqm: valsMin.length > 0 ? Math.min(...valsMin) : null,
      value_max_eur_sqm: valsMax.length > 0 ? Math.max(...valsMax) : null,
      value_mid_eur_sqm: valuesMid.reduce((a, b) => a + b, 0) / valuesMid.length,
      value_median_eur_sqm: median,
      rent_min_eur_sqm_month: rentsMin.length > 0 ? Math.min(...rentsMin) : null,
      rent_max_eur_sqm_month: rentsMax.length > 0 ? Math.max(...rentsMax) : null,
      rent_mid_eur_sqm_month: rentsMid.length > 0 ? rentsMid.reduce((a, b) => a + b, 0) / rentsMid.length : null,
      zones_count: zones.length,
      zones_with_data: valuesWithData.length,
      data_quality_score: qualityScores.length > 0 ? qualityScores.reduce((a, b) => a + b, 0) / qualityScores.length : null,
      updated_at: new Date().toISOString()
    });
  }

  console.log(`  Calculated ${aggregated.length} municipality aggregates`);

  // Step 4: Update municipality_values_semester
  console.log("\nStep 4: Updating municipality_values_semester...");

  let updated = 0;
  let errors = 0;
  const upsertBatchSize = 100;

  for (let i = 0; i < aggregated.length; i += upsertBatchSize) {
    const batch = aggregated.slice(i, i + upsertBatchSize);

    const { error: upsertError } = await supabase
      .schema("mart")
      .from("municipality_values_semester")
      .upsert(batch, {
        onConflict: "municipality_id,period_id,property_segment",
        ignoreDuplicates: false
      });

    if (upsertError) {
      console.error(`  Batch ${i}-${i + batch.length} error:`, upsertError.message);
      errors += batch.length;
    } else {
      updated += batch.length;
    }

    if ((i + upsertBatchSize) % 5000 === 0 || i + upsertBatchSize >= aggregated.length) {
      console.log(`  Progress: ${Math.min(i + upsertBatchSize, aggregated.length)}/${aggregated.length} (${errors} errors)`);
    }
  }

  console.log("\n=== Summary ===");
  console.log(`Total zones processed: ${allZoneData.length}`);
  console.log(`Municipality/period combinations: ${aggregated.length}`);
  console.log(`Successfully upserted: ${updated}`);
  console.log(`Errors: ${errors}`);

  // Spot check
  console.log("\n=== Spot Check (2026H1 residential) ===");
  const testMunis = ["078138", "052033", "058091", "015146", "001272"];
  const names = ["Scalea", "Torrita di Siena", "Roma", "Milano", "Torino"];

  for (let i = 0; i < testMunis.length; i++) {
    const munId = testMunis[i];
    const name = names[i];

    const { data: munData } = await supabase
      .schema("mart")
      .from("municipality_values_semester")
      .select("value_mid_eur_sqm, zones_with_data")
      .eq("municipality_id", munId)
      .eq("property_segment", "residential")
      .eq("period_id", "2026H1")
      .single();

    const { data: zoneCheck } = await supabase
      .schema("mart")
      .from("omi_zone_values_semester")
      .select("value_mid_eur_sqm")
      .like("omi_zone_id", munId + "_%")
      .eq("property_segment", "residential")
      .eq("period_id", "2026H1");

    const zoneAvg = zoneCheck && zoneCheck.length > 0
      ? zoneCheck.reduce((sum, z) => sum + z.value_mid_eur_sqm, 0) / zoneCheck.length
      : null;

    const munVal = munData?.value_mid_eur_sqm;
    const diff = munVal && zoneAvg ? ((munVal - zoneAvg) / zoneAvg * 100).toFixed(1) : "N/A";

    console.log(`${name} (${munId}): muni=${munVal?.toFixed(0) || "N/A"} zone_avg=${zoneAvg?.toFixed(0) || "N/A"} diff=${diff}% zones=${munData?.zones_with_data || 0}`);
  }
}

runReaggregation().catch(console.error);
