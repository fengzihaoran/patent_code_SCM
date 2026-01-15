#!/usr/bin/env bash
set -euo pipefail

# =========================
# Experiment 1: load 10M/20M/30M, each 3 runs
# =========================

# ---- Basic paths (adjust if needed) ----
YCSB_BIN="/home/femu/rocksdb/YCSB-cpp/ycsb"
WORKLOAD="workloads/workloada"
PROP_FILE="rocksdb/rocksdb.properties"
YCSB_DIR="/home/femu/rocksdb/YCSB-cpp"

# ---- Experiment params ----
PHASE="load"
THREADS=16
STATUS_INTERVAL=2
RUNS=3

# Insert sizes for exp1
RECORDCOUNTS=(10000000 20000000 30000000)

# SCM mount point for df sampling (for Ours); if baseline has no SCM mount, you can still keep it.
MOUNT_POINT="/home/femu/mnt/optane"

# 修改的地方：
# scheme name used in file names: znh2_default / znh2_tuned / ours_default / ours_tuned ...
SCHEME_NAME="ours_tuned"  # SCM_normalParam

# output root
OUTDIR="logs/exp1_load"

# cool-down between runs (seconds)
SLEEP_BETWEEN_RUNS=180

# -------------------------
build_cmd() {
  local rc="$1"
  echo "stdbuf -oL -eL $YCSB_BIN -load -db rocksdb -P $WORKLOAD -P $PROP_FILE \
    -p recordcount=$rc -threads $THREADS -p status.interval=$STATUS_INTERVAL -s"
}

reset_zenfs() {
  (
    cd "${YCSB_DIR}/.."
    mkdir -p "${YCSB_DIR}/logs"
    echo "[`date '+%F %T'`] reset zenfs..."
    ./zenfs_setup.sh 2>&1 | tee -a "${YCSB_DIR}/logs/zenfs_setup_$(date +%Y%m%d).log"
  )
}

mkdir -p "$OUTDIR/$SCHEME_NAME"

for rc in "${RECORDCOUNTS[@]}"; do
  for run in $(seq 1 "$RUNS"); do
    reset_zenfs
    sleep "$SLEEP_BETWEEN_RUNS"

    TS="$(date +%Y%m%d_%H%M%S)"
    PREFIX="${OUTDIR}/${SCHEME_NAME}/${SCHEME_NAME}_${PHASE}_rc${rc}_t${THREADS}_i${STATUS_INTERVAL}_run${run}_${TS}"
    LOG_FILE="${PREFIX}.log"
    DF_CSV="${PREFIX}_df.csv"
    CMD_TXT="${PREFIX}_cmd.txt"

    echo "=== ${SCHEME_NAME} | load rc=${rc} | run ${run}/${RUNS} ==="
    echo "log_file=$LOG_FILE"
    echo "df_csv=$DF_CSV"

    YCSB_CMD="$(build_cmd "$rc")"
    echo "$YCSB_CMD" | tee "$CMD_TXT"

    # start ycsb in background, tee to log
    bash -c "$YCSB_CMD" 2>&1 | tee "$LOG_FILE" &
    YCSB_PID=$!

    # df sampling header
    echo "ts_epoch,ts_iso,mount,used_bytes,avail_bytes,total_bytes,used_pct" > "$DF_CSV"

    # sample df while ycsb is running
    while kill -0 "$YCSB_PID" 2>/dev/null; do
      TS_EPOCH=$(date +%s)
      TS_ISO=$(date "+%F %T")

      LINE=$(df -B1 "$MOUNT_POINT" | tail -n 1 || true)
      if [[ -n "$LINE" ]]; then
        TOTAL=$(echo "$LINE" | awk '{print $2}')
        USED=$(echo  "$LINE" | awk '{print $3}')
        AVAIL=$(echo "$LINE" | awk '{print $4}')
        USEP=$(echo  "$LINE" | awk '{print $5}' | tr -d '%')
        echo "${TS_EPOCH},\"${TS_ISO}\",\"${MOUNT_POINT}\",${USED},${AVAIL},${TOTAL},${USEP}" >> "$DF_CSV"
      fi

      sleep "$STATUS_INTERVAL"
    done

    wait "$YCSB_PID" || true
    echo "Done. Saved:"
    echo "  $LOG_FILE"
    echo "  $DF_CSV"
    echo
  done
done

echo "All done. Logs in: $OUTDIR/$SCHEME_NAME"
