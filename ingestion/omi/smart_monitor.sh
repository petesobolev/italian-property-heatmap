#!/bin/bash
# smart_monitor.sh - Monitors ingestion and auto-recovers from stalls
#
# Features:
# - Detects stuck processes via log file freshness (not just process existence)
# - Auto-restarts on stall detection
# - Chains to next year when current year completes
# - Logs all monitor activity

cd /Users/pete/Projects/italian-property-heatmap/ingestion/omi

# Configuration
STALL_THRESHOLD_SECONDS=600  # 10 minutes without log activity = stalled
CHECK_INTERVAL=300           # Check every 5 minutes
MONITOR_LOG="smart_monitor.log"

# Years to process in order
YEARS=(2016 2017 2018 2019)

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$MONITOR_LOG"
}

get_log_age() {
    local log_file="$1"
    if [ -f "$log_file" ]; then
        local last_mod=$(stat -f %m "$log_file" 2>/dev/null)
        local now=$(date +%s)
        echo $((now - last_mod))
    else
        echo 0
    fi
}

is_process_running() {
    local semesters="$1"
    pgrep -f "$semesters" > /dev/null 2>&1
}

start_ingestion() {
    local year="$1"
    local semesters="${year}1 ${year}2"
    local log_file="omi_${year}_ingestion.log"

    nohup python3 load_omi_values.py --semesters $semesters --skip-loaded > "$log_file" 2>&1 &
    local pid=$!
    log "Started $year ingestion with PID $pid"
}

kill_ingestion() {
    local year="$1"
    local semesters="${year}1 ${year}2"
    pkill -f "$semesters" 2>/dev/null
    sleep 3
}

check_completed() {
    local year="$1"
    local log_file="omi_${year}_ingestion.log"

    if [ -f "$log_file" ]; then
        # Check for completion message
        if grep -q "Ingestion Complete" "$log_file" 2>/dev/null; then
            return 0  # Completed successfully
        fi
        # Check for failed status (process died without completing)
        if ! is_process_running "${year}1 ${year}2"; then
            # Process not running - check if it was a crash or completion
            local last_line=$(tail -1 "$log_file" 2>/dev/null)
            if echo "$last_line" | grep -q "Complete\|succeeded"; then
                return 0  # Completed
            fi
        fi
    fi
    return 1  # Not completed
}

# Main loop
log "=========================================="
log "Smart Monitor starting"
log "Stall threshold: ${STALL_THRESHOLD_SECONDS}s, Check interval: ${CHECK_INTERVAL}s"
log "Years to process: ${YEARS[*]}"
log "=========================================="

for YEAR in "${YEARS[@]}"; do
    SEMESTERS="${YEAR}1 ${YEAR}2"
    LOG_FILE="omi_${YEAR}_ingestion.log"

    log "--- Processing year $YEAR ---"

    # Check if already completed
    if check_completed "$YEAR"; then
        log "$YEAR already completed, skipping to next year"
        continue
    fi

    # Start if not running
    if ! is_process_running "$SEMESTERS"; then
        start_ingestion "$YEAR"
    else
        log "$YEAR ingestion already running"
    fi

    # Monitor loop for this year
    while true; do
        sleep $CHECK_INTERVAL

        # Check if completed
        if check_completed "$YEAR"; then
            log "$YEAR completed successfully!"
            break  # Move to next year
        fi

        # Check if process died (without completing)
        if ! is_process_running "$SEMESTERS"; then
            log "$YEAR process died unexpectedly, restarting..."
            start_ingestion "$YEAR"
            continue
        fi

        # Check for stall (process running but no log activity)
        LOG_AGE=$(get_log_age "$LOG_FILE")
        if [ "$LOG_AGE" -gt "$STALL_THRESHOLD_SECONDS" ]; then
            log "STALL DETECTED: $YEAR log unchanged for ${LOG_AGE}s (threshold: ${STALL_THRESHOLD_SECONDS}s)"
            log "Killing stalled process and restarting..."
            kill_ingestion "$YEAR"
            start_ingestion "$YEAR"
        elif [ "$LOG_AGE" -gt $((STALL_THRESHOLD_SECONDS / 2)) ]; then
            log "WARNING: $YEAR slow progress - ${LOG_AGE}s since last log activity"
        fi
    done
done

log "=========================================="
log "All years complete!"
log "=========================================="
