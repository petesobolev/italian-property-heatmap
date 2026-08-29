#!/bin/bash
# zone_ingestion_chain.sh - Chains zone-level ingestion across years
#
# Monitors the current 2016 ingestion and automatically starts 2017-2025
# when each year completes.

cd /Users/pete/Projects/italian-property-heatmap/ingestion/omi

CHAIN_LOG="zone_ingestion_chain.log"
CHECK_INTERVAL=180  # Check every 3 minutes
STALL_THRESHOLD=600 # 10 minutes without log activity = stalled

YEARS=(2016 2017 2018 2019 2020 2021 2022 2023 2024 2025)

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$CHAIN_LOG"
}

notify() {
    # Send macOS notification
    osascript -e "display notification \"$1\" with title \"OMI Ingestion\" sound name \"Ping\"" 2>/dev/null || true
}

get_log_age() {
    local log_file="$1"
    if [ -f "$log_file" ]; then
        local last_mod=$(stat -f %m "$log_file" 2>/dev/null)
        local now=$(date +%s)
        echo $((now - last_mod))
    else
        echo 999999
    fi
}

is_year_running() {
    local year="$1"
    pgrep -f "load_omi_values.py.*${year}1.*${year}2" > /dev/null 2>&1
}

is_year_complete() {
    local year="$1"
    local log_file="omi_${year}_full.log"

    # Check if log file contains completion message
    if [ -f "$log_file" ]; then
        grep -q "Ingestion complete\|Processing complete\|All .* provinces processed" "$log_file" 2>/dev/null
        return $?
    fi
    return 1
}

start_year() {
    local year="$1"
    local log_file="omi_${year}_full.log"

    log "Starting $year ingestion..."
    nohup python3 load_omi_values.py \
        --semesters ${year}1 ${year}2 \
        --force-zone-reload \
        --skip-loaded \
        > "$log_file" 2>&1 &

    local pid=$!
    log "Started $year ingestion with PID $pid, logging to $log_file"
}

kill_stalled() {
    local year="$1"
    log "Killing stalled $year ingestion..."
    pkill -f "load_omi_values.py.*${year}1.*${year}2"
    sleep 5
}

log "=========================================="
log "Zone Ingestion Chain Monitor starting"
log "Years to process: ${YEARS[*]}"
log "Check interval: ${CHECK_INTERVAL}s, Stall threshold: ${STALL_THRESHOLD}s"
log "=========================================="

for year in "${YEARS[@]}"; do
    log "--- Processing year $year ---"

    # Check if already complete
    if is_year_complete "$year"; then
        log "$year already complete, skipping"
        continue
    fi

    # Check if already running (e.g., 2016 started manually)
    if ! is_year_running "$year"; then
        start_year "$year"
        sleep 30  # Give it time to start
    else
        log "$year already running, monitoring..."
    fi

    # Monitor until complete
    while true; do
        sleep "$CHECK_INTERVAL"

        if is_year_complete "$year"; then
            log "$year completed successfully!"
            notify "$year ingestion completed!"
            break
        fi

        if ! is_year_running "$year"; then
            log "$year process died unexpectedly, restarting..."
            notify "$year process died - restarting"
            start_year "$year"
            sleep 30
            continue
        fi

        # Check for stalls
        log_file="omi_${year}_full.log"
        log_age=$(get_log_age "$log_file")

        if [ "$log_age" -gt "$STALL_THRESHOLD" ]; then
            log "STALL DETECTED: $log_file not updated for ${log_age}s"
            notify "Stall detected in $year - restarting"
            kill_stalled "$year"
            start_year "$year"
            sleep 30
        else
            # Show brief progress
            last_province=$(grep -E "Processing province:" "$log_file" 2>/dev/null | tail -1 | sed 's/.*Processing province: //')
            log "Year $year running OK (log age: ${log_age}s) - $last_province"
        fi
    done
done

log "=========================================="
log "All years complete!"
log "=========================================="

# Run change metrics calculation
log "Calculating zone change metrics..."
python3 load_omi_values.py --calculate-zone-changes >> "$CHAIN_LOG" 2>&1
log "Zone change metrics calculation complete"
