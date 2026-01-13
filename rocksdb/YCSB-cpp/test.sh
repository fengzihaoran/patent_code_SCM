#!/usr/bin/env bash
set -euo pipefail

YCSB_BIN="/home/femu/rocksdb/YCSB-cpp/ycsb"
YCSB_DIR="/home/femu/rocksdb/YCSB-cpp"
WORKLOAD="${YCSB_DIR}/workloads/workloada"
ROCKS_PROPS="${YCSB_DIR}/rocksdb/rocksdb.properties"

THREADS=16
SLEEP_SEC=300
MAX_RETRY=2   # 失败后最多重试次数（每轮最多跑 1 + MAX_RETRY 次）

mkdir -p "${YCSB_DIR}/logs"

reset_zenfs () {
  cd "${YCSB_DIR}/.."
  echo "[`date '+%F %T'`] reset zenfs..."
  ./zenfs_setup.sh 2>&1 | tee -a "${YCSB_DIR}/logs/zenfs_setup_$(date +%Y%m%d).log"
}

run_load_once () {
  local rc="$1"
  local tag="$2"
  local logfile="${YCSB_DIR}/logs/load_workloada_rc${rc}_t${THREADS}_${tag}_$(date +%Y%m%d_%H%M%S).log"

  cd "${YCSB_DIR}"
  echo "[`date '+%F %T'`] start load rc=${rc} tag=${tag}"
  # 这段让 set -e 不会直接把整个脚本杀掉，我们自己接管失败处理
  set +e
  stdbuf -oL -eL "${YCSB_BIN}" \
    -load -db rocksdb \
    -P "${WORKLOAD}" \
    -P "${ROCKS_PROPS}" \
    -p recordcount="${rc}" \
    -threads "${THREADS}" \
    -s 2>&1 | tee "${logfile}"
  local rc_exit=${PIPESTATUS[0]}   # ycsb 进程的退出码
  set -e

  return "${rc_exit}"
}

run_load_with_retry () {
  local rc="$1"
  local tag="$2"

  local attempt=0
  while true; do
    attempt=$((attempt + 1))
    if run_load_once "${rc}" "${tag}_try${attempt}"; then
      echo "[`date '+%F %T'`] load success rc=${rc} tag=${tag} attempt=${attempt}"
      return 0
    else
      echo "[`date '+%F %T'`] load FAILED rc=${rc} tag=${tag} attempt=${attempt}"
      # 失败就重置
      reset_zenfs
      sleep "${SLEEP_SEC}"

      if (( attempt > MAX_RETRY )); then
        echo "[`date '+%F %T'`] giving up rc=${rc} tag=${tag} after ${attempt} attempts"
        return 1
      fi
    fi
  done
}

########################################
# 执行计划：1x30M, 3x40M, 1x50M
########################################

# 如果你希望每轮开始前都先 reset（更干净），取消注释下一行
reset_zenfs; sleep "${SLEEP_SEC}"

# 3x 10M
for i in 1 2 3; do
  run_load_with_retry 10000000 "10M_scmNormalParam_load${i}" || true
  reset_zenfs
  sleep "${SLEEP_SEC}"
done

# 3x 20M
for i in 1 2 3; do
  run_load_with_retry 20000000 "20M_scmNormalParam_load${i}" || true
  reset_zenfs
  sleep "${SLEEP_SEC}"
done

# 3x 30M
for i in 1 2 3; do
  run_load_with_retry 30000000 "30M_scmNormalParam_load${i}" || true
  reset_zenfs
  sleep "${SLEEP_SEC}"
done

## 1x 50M
#run_load_with_retry 50000000 "50M_run1" || true
#reset_zenfs

echo "OKOKOK!!!"
