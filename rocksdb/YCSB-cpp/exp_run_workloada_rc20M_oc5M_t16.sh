#!/usr/bin/env bash
set -euo pipefail

# =========================
# Experiment: RUN comparison (4 schemes)
# Command: -load -run
# SCHEME_NAME=ours_tuned   ENABLE_DF=1 ./exp_run_workloada_rc20M_oc5M_t16.sh
# 小技巧：znh2 没 SCM，ENABLE_DF=0 能少生成垃圾 csv。
# =========================

# ---- Basic paths (adjust if needed) ----
YCSB_BIN="/home/femu/rocksdb/YCSB-cpp/ycsb"
WORKLOAD="workloads/workloada"
PROP_FILE="rocksdb/rocksdb.properties"
YCSB_DIR="/home/femu/rocksdb/YCSB-cpp"

# ---- Experiment params ----
THREADS=16
STATUS_INTERVAL=2
RUNS=3

RECORDCOUNT=20000000
OPCOUNT=5000000

# SCM mount point for df sampling (mainly for ours)
MOUNT_POINT="/home/femu/mnt/optane"

# scheme name used in file names:
# znh2_default / znh2_tuned / ours_default / ours_tuned
SCHEME_NAME="ours_tuned"

# output root
OUTDIR="logs/exp_run"

# cool-down between runs (seconds)
SLEEP_BETWEEN_RUNS=180

# df sampling: 1 enable, 0 disable
ENABLE_DF=1

# -------------------------
build_cmd() {
  echo "stdbuf -oL -eL $YCSB_BIN -load -run -db rocksdb -P $WORKLOAD -P $PROP_FILE \
    -p recordcount=$RECORDCOUNT -p operationcount=$OPCOUNT \
    -threads $THREADS -p status.interval=$STATUS_INTERVAL -s"
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

for run in $(seq 1 "$RUNS"); do
  reset_zenfs
  sleep "$SLEEP_BETWEEN_RUNS"

  TS="$(date +%Y%m%d_%H%M%S)"
  PREFIX="${OUTDIR}/${SCHEME_NAME}/${SCHEME_NAME}_loadrun_workloada_rc${RECORDCOUNT}_oc${OPCOUNT}_t${THREADS}_i${STATUS_INTERVAL}_run${run}_${TS}"

  LOG_FILE="${PREFIX}.log"
  DF_CSV="${PREFIX}_df.csv"
  CMD_TXT="${PREFIX}_cmd.txt"

  echo "=== ${SCHEME_NAME} | load+run workloada rc=${RECORDCOUNT} oc=${OPCOUNT} | run ${run}/${RUNS} ==="
  echo "log_file=$LOG_FILE"
  echo "df_csv=$DF_CSV"

  YCSB_CMD="$(build_cmd)"
  echo "$YCSB_CMD" | tee "$CMD_TXT"

  # start ycsb in background, tee to log
  bash -c "$YCSB_CMD" 2>&1 | tee "$LOG_FILE" &
  YCSB_PID=$!

  if [[ "$ENABLE_DF" == "1" ]]; then
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
  fi

  wait "$YCSB_PID" || true
  echo "Done. Saved:"
  echo "  $LOG_FILE"
  [[ "$ENABLE_DF" == "1" ]] && echo "  $DF_CSV"
  echo
done

echo "All done. Logs in: $OUTDIR/$SCHEME_NAME"
