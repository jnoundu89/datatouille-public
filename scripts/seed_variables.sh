#!/usr/bin/env bash
# Seed Airflow Variables with default configuration values (Public Test Version).
# Safe to re-run: skips variables that already exist.

set -euo pipefail

SCHEDULER_CONTAINER="${SCHEDULER_CONTAINER:-airflow-scheduler}"

seed_variable() {
    local key="$1"
    local json="$2"

    if docker exec "$SCHEDULER_CONTAINER" airflow variables get "$key" >/dev/null 2>&1; then
        echo "  SKIP  $key (already exists)"
    else
        docker exec "$SCHEDULER_CONTAINER" airflow variables set "$key" "$json"
        echo "  SET   $key"
    fi
}

echo "Seeding Airflow Variables..."
echo ""

seed_variable "platform_config" '{
    "default_max_retries": 3,
    "default_request_timeout": 30
}'

seed_variable "meteo_config" '{
    "vigilance_bucket_name": "vigilance-meteo",
    "georisques_bucket_name": "georisques-report",
    "meteo_api_url": "https://rwg.meteofrance.com/wsft/v3/warning/timelaps",
    "api_params": {"domain": "FRA", "warning_type": "vigilance", "formatDate": "timestamp", "echeance": "J0", "depth": "1"},
    "vigilance_website_url": "https://vigilance.meteofrance.fr",
    "georisques_rapport_url": "https://georisques.gouv.fr/api/v1/resultats_rapport_risque",
    "ban_geocode_url": "https://api-adresse.data.gouv.fr/search/",
    "request_timeout_connect": 10,
    "request_timeout_read": 30,
    "max_workers": 6,
    "rate_limit_delay": 0.1
}'

echo ""
echo "Done! Check Airflow UI > Admin > Variables to verify."
