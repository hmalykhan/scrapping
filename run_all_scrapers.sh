#!/usr/bin/env bash
#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/var/www/scrapping"
PY="/root/miniconda3/envs/scraper/bin/python"
LOG="/var/log/scrapers_master.log"

cd "$PROJECT_DIR"

# Load env vars for cron/non-interactive shells
set -a
source .env
set +a

run_one () {
  local name="$1"; shift
  echo "[$(date -u '+%F %T')] START $name" >> "$LOG"
  if "$@"; then
    echo "[$(date -u '+%F %T')] OK    $name" >> "$LOG"
  else
    rc=$?
    echo "[$(date -u '+%F %T')] FAIL  $name rc=$rc" >> "$LOG"
    return 0
  fi
}

#run_one "scrape_course"         "$PY" manage.py scrape_course --delay 0.7
run_one "scrape"                "$PY" manage.py scrape --delay 0.7
#run_one "scrape_apprenticeship" "$PY" manage.py scrape_apprenticeship --delay 0.7
#run_one "scrape_job"            "$PY" manage.py scrape_job --delay 0.7
