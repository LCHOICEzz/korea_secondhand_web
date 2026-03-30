#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

MODE="${1:-batch}"
shift || true

PYTHON_BIN="${PYTHON_BIN:-python3}"
SLUG_FILE="${SLUG_FILE:-$ROOT_DIR/output/daangn_region_slug_list.txt}"
OUT_ROOT="${OUT_ROOT:-$ROOT_DIR/output/brand_runs}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/logs}"

SLEEP_BETWEEN_BRANDS="${SLEEP_BETWEEN_BRANDS:-35}"
REGION_WORKERS="${REGION_WORKERS:-8}"
DETAIL_WORKERS="${DETAIL_WORKERS:-5}"
FINAL_WORKERS="${FINAL_WORKERS:-8}"
FIX_WORKERS="${FIX_WORKERS:-2}"
REQUEST_INTERVAL="${REQUEST_INTERVAL:-0.32}"
REQUEST_JITTER="${REQUEST_JITTER:-0.05}"
REGION_BATCH_SIZE="${REGION_BATCH_SIZE:-120}"
REGION_BATCH_SLEEP="${REGION_BATCH_SLEEP:-3}"
DETAIL_BATCH_SIZE="${DETAIL_BATCH_SIZE:-80}"
DETAIL_BATCH_SLEEP="${DETAIL_BATCH_SLEEP:-2}"

mkdir -p "$OUT_ROOT" "$LOG_DIR"

usage() {
  cat <<'EOF'
Usage:
  ./run_pipeline.sh batch [extra args...]
  ./run_pipeline.sh batch-bg [extra args...]
  ./run_pipeline.sh brand <brand> [variant1 variant2 ...]
  ./run_pipeline.sh scrape <brand> [variant1 variant2 ...]
  ./run_pipeline.sh postprocess <brand> <run_dir> [variant1 variant2 ...]
  ./run_pipeline.sh status

Environment overrides:
  PYTHON_BIN
  SLUG_FILE
  OUT_ROOT
  LOG_DIR
  SLEEP_BETWEEN_BRANDS
  REGION_WORKERS
  DETAIL_WORKERS
  FINAL_WORKERS
  FIX_WORKERS
  REQUEST_INTERVAL
  REQUEST_JITTER
  REGION_BATCH_SIZE
  REGION_BATCH_SLEEP
  DETAIL_BATCH_SIZE
  DETAIL_BATCH_SLEEP

Examples:
  ./run_pipeline.sh batch
  ./run_pipeline.sh batch-bg
  ./run_pipeline.sh brand 헤이에스 heys
  ./run_pipeline.sh scrape 헤이에스 heys
  ./run_pipeline.sh postprocess 헤이에스 output/brand_runs/헤이에스_20260315_120000 heys
  ./run_pipeline.sh status
EOF
}

require_slug_file() {
  if [[ ! -f "$SLUG_FILE" ]]; then
    echo "[ERROR] slug file not found: $SLUG_FILE" >&2
    exit 1
  fi
}

build_common_args() {
  cat <<EOF
--slug-file $SLUG_FILE
--out-root $OUT_ROOT
--region-workers $REGION_WORKERS
--detail-workers $DETAIL_WORKERS
--final-workers $FINAL_WORKERS
--fix-workers $FIX_WORKERS
--request-interval $REQUEST_INTERVAL
--request-jitter $REQUEST_JITTER
--region-batch-size $REGION_BATCH_SIZE
--region-batch-sleep $REGION_BATCH_SLEEP
--detail-batch-size $DETAIL_BATCH_SIZE
--detail-batch-sleep $DETAIL_BATCH_SLEEP
EOF
}

append_variants() {
  local cmd_ref="$1"
  shift
  local variant
  for variant in "$@"; do
    cmd_ref+=" --search-variant "
    cmd_ref+="$(printf '%q' "$variant")"
  done
  printf '%s' "$cmd_ref"
}

run_batch() {
  require_slug_file
  local cmd
  cmd="$PYTHON_BIN -u run_multi_brand_pipeline.py $(build_common_args) --sleep-between-brands $SLEEP_BETWEEN_BRANDS"
  eval "$cmd $*"
}

run_batch_bg() {
  require_slug_file
  local ts log_file cmd
  ts="$(date +%Y%m%d_%H%M%S)"
  log_file="$LOG_DIR/karrot_batch_${ts}.log"
  cmd="$PYTHON_BIN -u run_multi_brand_pipeline.py $(build_common_args) --sleep-between-brands $SLEEP_BETWEEN_BRANDS"
  nohup bash -lc "$cmd $*" >"$log_file" 2>&1 &
  echo "[STARTED] pid=$! log=$log_file"
}

run_brand_mode() {
  local mode="$1"
  local brand="$2"
  shift 2
  require_slug_file
  local cmd
  cmd="$PYTHON_BIN -u run_brand_pipeline.py $(build_common_args) --brand $(printf '%q' "$brand") --mode $mode"
  cmd="$(append_variants "$cmd" "$@")"
  eval "$cmd"
}

run_postprocess() {
  local brand="$1"
  local run_dir="$2"
  shift 2
  local cmd
  cmd="$PYTHON_BIN -u run_brand_pipeline.py $(build_common_args) --brand $(printf '%q' "$brand") --mode postprocess --run-dir $(printf '%q' "$run_dir")"
  cmd="$(append_variants "$cmd" "$@")"
  eval "$cmd"
}

run_status() {
  "$PYTHON_BIN" "$ROOT_DIR/pipeline_status.py" --log-dir "$LOG_DIR" --out-root "$OUT_ROOT"
}

case "$MODE" in
  batch)
    run_batch "$@"
    ;;
  batch-bg)
    run_batch_bg "$@"
    ;;
  brand)
    [[ $# -ge 1 ]] || { usage; exit 1; }
    run_brand_mode all "$@"
    ;;
  scrape)
    [[ $# -ge 1 ]] || { usage; exit 1; }
    run_brand_mode scrape "$@"
    ;;
  postprocess)
    [[ $# -ge 2 ]] || { usage; exit 1; }
    run_postprocess "$@"
    ;;
  status)
    run_status
    ;;
  help|-h|--help)
    usage
    ;;
  *)
    echo "[ERROR] unknown mode: $MODE" >&2
    usage
    exit 1
    ;;
esac
