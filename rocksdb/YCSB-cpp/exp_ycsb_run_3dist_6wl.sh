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

#!/usr/bin/env bash
set -euo pipefail

# ==========================================
# YCSB RUN experiment:
#   dist in {zipfian, uniform, latest}
#   workload in {A,B,C,D,E,F}
# For ONE scheme at a time (you switch code/config manually)
#
# Examples:
#   SCHEME_NAME=znh2_default ENABLE_DF=0 RUNS=3 ./exp_ycsb_run_3dist_6wl.sh
#   SCHEME_NAME=ours_tuned   ENABLE_DF=1 RUNS=3 ./exp_ycsb_run_3dist_6wl.sh
# ==========================================

# --------- Paths (adjust if needed) ---------
YCSB_BIN="${YCSB_BIN:-/home/femu/rocksdb/YCSB-cpp/ycsb}"
YCSB_DIR="${YCSB_DIR:-/home/femu/rocksdb/YCSB-cpp}"
PROP_FILE="${PROP_FILE:-rocksdb/rocksdb.properties}"
WORKLOAD_DIR="${WORKLOAD_DIR:-workloads}"

# --------- Experiment params ---------
THREADS="${THREADS:-16}"
STATUS_INTERVAL="${STATUS_INTERVAL:-10}"     # RUN 柱状图不需要 2s，10s 更省
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
SLEEP_BETWEEN="${SLEEP_BETWEEN:-240}"

# --------- Workloads / dists ---------
declare -A WL_MAP=(
  ["A"]="${WORKLOAD_DIR}/workloada"
  ["B"]="${WORKLOAD_DIR}/workloadb"
  ["C"]="${WORKLOAD_DIR}/workloadc"
  ["D"]="${WORKLOAD_DIR}/workloadd"
  ["E"]="${WORKLOAD_DIR}/workloade"
  ["F"]="${WORKLOAD_DIR}/workloadf"
)

# ✅ 默认全跑（你也可以在命令行覆盖）
DISTS=(${DISTS_OVERRIDE:-zipfian uniform latest})
WLS=(${WLS_OVERRIDE:-A B C E F D})  # 和你给的图一样的顺序：A B C E F | A B C E F | D

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
  echo "stdbuf -oL -eL $YCSB_BIN -load -run -db rocksdb \
    -P $wl_file -P $PROP_FILE \
    -p recordcount=$RECORDCOUNT -p operationcount=$OPCOUNT \
    -p requestdistribution=$dist \
    -threads $THREADS -p status.interval=$STATUS_INTERVAL -s"
}

# ---- Error log ----
mkdir -p "$OUTDIR/$SCHEME_NAME"
ERR_LOG="$OUTDIR/$SCHEME_NAME/errors_${SCHEME_NAME}.log"
: > "$ERR_LOG"  # truncate

log_error() {
  # $1: message
  echo "[`date '+%F %T'`] $1" | tee -a "$ERR_LOG" >&2
}

is_log_failed() {
  # $1: log file
  # If log contains common failure signatures, treat as failed even if exit code = 0
  local lf="$1"
  grep -qE "No space left on device|Caught exception|IO error:" "$lf" 2>/dev/null
}

echo "=== Scheme: $SCHEME_NAME ==="
echo "recordcount=$RECORDCOUNT opcount=$OPCOUNT threads=$THREADS runs=$RUNS"
echo "dists=${DISTS[*]}"
echo "workloads=${WLS[*]}"
echo "outdir=$OUTDIR/$SCHEME_NAME"
echo "error_log=$ERR_LOG"
echo

for dist in "${DISTS[@]}"; do
  for wl in "${WLS[@]}"; do
    wl_file="${WL_MAP[$wl]}"
    if [[ ! -f "$wl_file" ]]; then
      log_error "FATAL: workload file not found: $wl_file (wl=$wl)"
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

      # Start ycsb
      set +e
      bash -c "$YCSB_CMD" 2>&1 | tee "$LOG_FILE"
      YCSB_RC=${PIPESTATUS[0]}
      set -e

      # Optional df sampling (only if you really need it for run bars you can turn it off)
      # NOTE: if you want df sampling during run, you need background sampling loop like before.
      # Here we keep df OFF by default for run-bar experiments.
      if [[ "$ENABLE_DF" == "1" ]]; then
        echo "NOTE: ENABLE_DF=1 but this simplified runner does not sample df in foreground mode." >> "$LOG_FILE"
      fi

      # Decide failure
      FAIL=0
      if [[ "$YCSB_RC" -ne 0 ]]; then
        FAIL=1
      fi
      if is_log_failed "$LOG_FILE"; then
        FAIL=1
      fi

      if [[ "$FAIL" -eq 1 ]]; then
        log_error "FAILED: scheme=$SCHEME_NAME dist=$dist wl=$wl run=$run exit_code=$YCSB_RC log=$LOG_FILE"
        log_error "---- tail(80) of $LOG_FILE ----"
        tail -n 80 "$LOG_FILE" | tee -a "$ERR_LOG" >&2
        log_error "---- end tail ----"
        # 继续跑后续（不 exit），这样你能得到尽可能多的数据
      else
        echo "OK: $LOG_FILE"
      fi

      echo
    done
  done
done

echo "All done."
echo "Logs in: $OUTDIR/$SCHEME_NAME"
echo "Error log: $ERR_LOG"
