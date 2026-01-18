#!/usr/bin/env bash
set -euo pipefail

# ==========================================
# YCSB RUN experiment:
#   dist in {zipfian, uniform, latest}
#   workload in {A,B,C,D,E,F}
# For ONE scheme at a time (you switch code/config manually)
  #SCHEME_NAME=znh2_default ENABLE_DF=0 ./exp_ycsb_run_3dist_6wl.sh
  #SCHEME_NAME=znh2_tuned   ENABLE_DF=0 ./exp_ycsb_run_3dist_6wl.sh
  #SCHEME_NAME=ours_default ENABLE_DF=1 ./exp_ycsb_run_3dist_6wl.sh
  #SCHEME_NAME=ours_tuned   ENABLE_DF=1 ./exp_ycsb_run_3dist_6wl.sh
# ==========================================

# --------- Paths (adjust if needed) ---------
YCSB_BIN="${YCSB_BIN:-/home/femu/rocksdb/YCSB-cpp/ycsb}"
YCSB_DIR="${YCSB_DIR:-/home/femu/rocksdb/YCSB-cpp}"
PROP_FILE="${PROP_FILE:-rocksdb/rocksdb.properties}"
WORKLOAD_DIR="${WORKLOAD_DIR:-workloads}"

# --------- Experiment params ---------
THREADS="${THREADS:-16}"
STATUS_INTERVAL="${STATUS_INTERVAL:-10}"     # RUN 柱状图不需要 2s，这里用 10s 更省
RUNS="${RUNS:-3}"

RECORDCOUNT="${RECORDCOUNT:-20000000}"
OPCOUNT="${OPCOUNT:-5000000}"

# scheme name: znh2_default / znh2_tuned / ours_default / ours_tuned
SCHEME_NAME="${SCHEME_NAME:-ours_tuned}"

# output dir (organized by scheme)
OUTDIR="${OUTDIR:-logs/exp_ycsb_run_3dist}"

# If you want to sample df for ours, enable it; for znh2 set 0
ENABLE_DF="${ENABLE_DF:-0}"
MOUNT_POINT="${MOUNT_POINT:-/home/femu/mnt/optane}"

# sleep between runs (to cool down)
SLEEP_BETWEEN="${SLEEP_BETWEEN:-60}"

# --------- Workloads / dists ---------
# Map A-F -> workload file path
declare -A WL_MAP=(
  ["A"]="${WORKLOAD_DIR}/workloada"
  ["B"]="${WORKLOAD_DIR}/workloadb"
  ["C"]="${WORKLOAD_DIR}/workloadc"
  ["D"]="${WORKLOAD_DIR}/workloadd"
  ["E"]="${WORKLOAD_DIR}/workloade"
  ["F"]="${WORKLOAD_DIR}/workloadf"
)

DISTS=("zipfian" "uniform" "latest")
WLS=("A" "B" "C" "D" "E" "F")

# --------- Helpers ---------
reset_zenfs() {
  (
    cd "${YCSB_DIR}/.."
    mkdir -p "${YCSB_DIR}/logs"
    echo "[`date '+%F %T'`] reset zenfs..."
    ./zenfs_setup.sh 2>&1 | tee -a "${YCSB_DIR}/logs/zenfs_setup_$(date +%Y%m%d).log"
  )
}

build_cmd() {
  local wl_file="$1"
  local dist="$2"
  # 用 -load -run 一次性跑，保证每个 workload 是“干净起点”（不会被别的 workload 污染）
  echo "stdbuf -oL -eL $YCSB_BIN -load -run -db rocksdb \
    -P $wl_file -P $PROP_FILE \
    -p recordcount=$RECORDCOUNT -p operationcount=$OPCOUNT \
    -p requestdistribution=$dist \
    -threads $THREADS -p status.interval=$STATUS_INTERVAL -s"
}

mkdir -p "$OUTDIR/$SCHEME_NAME"

echo "=== Scheme: $SCHEME_NAME ==="
echo "recordcount=$RECORDCOUNT opcount=$OPCOUNT threads=$THREADS runs=$RUNS"
echo "outdir=$OUTDIR/$SCHEME_NAME"
echo

for dist in "${DISTS[@]}"; do
  for wl in "${WLS[@]}"; do
    wl_file="${WL_MAP[$wl]}"
    if [[ ! -f "$wl_file" ]]; then
      echo "ERROR: workload file not found: $wl_file"
      exit 1
    fi

    for run in $(seq 1 "$RUNS"); do
      reset_zenfs
      sleep "$SLEEP_BETWEEN"

      TS="$(date +%Y%m%d_%H%M%S)"
      PREFIX="${OUTDIR}/${SCHEME_NAME}/${SCHEME_NAME}_dist${dist}_wl${wl}_rc${RECORDCOUNT}_oc${OPCOUNT}_t${THREADS}_run${run}_${TS}"
      LOG_FILE="${PREFIX}.log"
      CMD_TXT="${PREFIX}_cmd.txt"
      DF_CSV="${PREFIX}_df.csv"

      echo ">>> $SCHEME_NAME | dist=$dist | wl=$wl | run $run/$RUNS"
      YCSB_CMD="$(build_cmd "$wl_file" "$dist")"
      echo "$YCSB_CMD" | tee "$CMD_TXT"

      bash -c "$YCSB_CMD" 2>&1 | tee "$LOG_FILE" &
      YCSB_PID=$!

      if [[ "$ENABLE_DF" == "1" ]]; then
        echo "ts_epoch,ts_iso,mount,used_bytes,avail_bytes,total_bytes,used_pct" > "$DF_CSV"
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
          sleep 2
        done
      fi

      wait "$YCSB_PID" || true
      echo "Saved: $LOG_FILE"
      echo
    done
  done
done

echo "All done. Logs in: $OUTDIR/$SCHEME_NAME"
