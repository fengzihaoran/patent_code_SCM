#!/usr/bin/env bash
set -euo pipefail

# =========================
# 可配置参数（你常改的都在这）
# =========================
YCSB_BIN="/home/femu/rocksdb/YCSB-cpp/ycsb"
WORKLOAD="workloads/workloada"
PROP_FILE="rocksdb/rocksdb.properties"
YCSB_DIR="/home/femu/rocksdb/YCSB-cpp"

PHASE="load"                 # load 或 run
RECORDCOUNT=30000000         # load 用
OPCOUNT=5000000             # run 用（如果 PHASE=run）
THREADS=16
STATUS_INTERVAL=2            # 秒
MOUNT_POINT="/home/femu/mnt/optane"
RUNS=3

SCHEME_NAME="ours_default"   # 用于命名：znH2_default / ours_full / naive_scm 等
OUTDIR="logs"

# =========================
# 组装 YCSB 命令
# =========================
build_cmd() {
  if [[ "$PHASE" == "load" ]]; then
    echo "stdbuf -oL -eL $YCSB_BIN -load -db rocksdb -P $WORKLOAD -P $PROP_FILE -p recordcount=$RECORDCOUNT -threads $THREADS -p status.interval=$STATUS_INTERVAL -s"
  else
    echo "stdbuf -oL -eL $YCSB_BIN -run -db rocksdb -P $WORKLOAD -P $PROP_FILE -p operationcount=$OPCOUNT -threads $THREADS -p status.interval=$STATUS_INTERVAL -s"
  fi
}

mkdir -p "$OUTDIR"

reset_zenfs () {
  (
    cd "${YCSB_DIR}/.."
    mkdir -p "${YCSB_DIR}/logs"
    echo "[`date '+%F %T'`] reset zenfs..."
    ./zenfs_setup.sh 2>&1 | tee -a "${YCSB_DIR}/logs/zenfs_setup_$(date +%Y%m%d).log"
  )
}


for i in $(seq 2 "$RUNS"); do
  reset_zenfs
  sleep 500

  TS="$(date +%Y%m%d_%H%M%S)"
  PREFIX="${OUTDIR}/${SCHEME_NAME}_${PHASE}_rc${RECORDCOUNT}_oc${OPCOUNT}_t${THREADS}_i${STATUS_INTERVAL}_run${i}_${TS}"

  LOG_FILE="${PREFIX}.log"
  DF_CSV="${PREFIX}_df.csv"

  echo "=== Run $i/$RUNS ==="
  echo "log_file=$LOG_FILE"
  echo "df_csv=$DF_CSV"

  YCSB_CMD="$(build_cmd)"
  echo "cmd=$YCSB_CMD" | tee "${PREFIX}_cmd.txt"

  # 启动 ycsb（后台），同时 tee 到 log
  bash -c "$YCSB_CMD" 2>&1 | tee "$LOG_FILE" &
  YCSB_PID=$!

  # df 采样 CSV 表头
  echo "ts_epoch,ts_iso,mount,used_bytes,avail_bytes,total_bytes,used_pct" > "$DF_CSV"

  # 采样直到 ycsb 结束
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
  echo "Done run $i. Saved $LOG_FILE and $DF_CSV"
  echo

done

echo "All done. Logs in: $OUTDIR"
#!/usr/bin/env bash
set -euo pipefail