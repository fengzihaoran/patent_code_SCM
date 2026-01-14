#!/usr/bin/env bash
set -euo pipefail

# 用法示例：
# ./run_ycsb_with_df_sampling.sh \
#   "stdbuf -oL -eL /home/femu/rocksdb/cmake-build-release/YCSB-cpp/ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p recordcount=30000000 -threads 16 -p status.interval=2 -s" \
#   /home/femu/mnt/optane \
#   2 \
#   logs/ours_load30M_t16

#YCSB_CMD="$1"
#MOUNT_POINT="$2"
#INTERVAL_SEC="$3"
#OUT_PREFIX="$4"

YCSB_CMD="stdbuf -oL -eL /home/femu/rocksdb/YCSB-cpp/ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p recordcount=30000000 -threads 16 -p status.interval=2 -s"
MOUNT_POINT="/home/femu/mnt/optane"
INTERVAL_SEC="2"
OUT_PREFIX="logs/ours_SCMFULL_load30M_t16"

mkdir -p "$(dirname "$OUT_PREFIX")"

LOG_FILE="${OUT_PREFIX}.log"
DF_CSV="${OUT_PREFIX}_df.csv"

echo "log_file=$LOG_FILE"
echo "df_csv=$DF_CSV"
echo "mount=$MOUNT_POINT"
echo "interval=$INTERVAL_SEC sec"

# 先后台启动 ycsb（输出 tee 到 log）
bash -c "$YCSB_CMD" 2>&1 | tee "$LOG_FILE" &
YCSB_PID=$!
echo "ycsb_pid=$YCSB_PID"

# 写 CSV 表头
echo "ts_epoch,ts_iso,mount,used_bytes,avail_bytes,total_bytes,used_pct" > "$DF_CSV"

# df 采样循环：只要 ycsb 还活着就一直采
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

  sleep "$INTERVAL_SEC"
done

wait "$YCSB_PID" || true
echo "Done."
echo "Saved:"
echo "  $LOG_FILE"
echo "  $DF_CSV"
