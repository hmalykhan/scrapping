#!/usr/bin/env bash
set -euo pipefail

cd /var/www/scrapping

source /root/miniconda3/etc/profile.d/conda.sh
conda activate scraper

echo "=== Geo enrich started: $(date) ==="

python manage.py enrich_geo_category \
  --categories-file /var/www/scrapping/categories.json \
  --state-file /var/www/scrapping/geo_enrich_state.json \
  --rps 5 \
  --per-key-limit 3000 \
  --progress-every 500

echo "=== Geo enrich finished: $(date) ==="
