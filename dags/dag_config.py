"""Central configuration via Airflow Variables with hardcoded defaults.

Each domain function returns a dict of operational constants.
Values can be overridden at runtime via Airflow UI (Admin > Variables)
by setting a JSON variable with the corresponding key (e.g. "meteo_config").

Unknown keys in overrides are silently ignored to prevent typo-driven bugs.
"""

from airflow.sdk import Variable


def _get_config(variable_key: str, defaults: dict) -> dict:
    """Merge Airflow Variable overrides into hardcoded defaults.

    Only keys present in `defaults` are accepted from overrides.
    Returns defaults as-is when the variable doesn't exist.
    """
    overrides = Variable.get(variable_key, deserialize_json=True, default={})
    merged = {**defaults}
    for key, value in overrides.items():
        if key in defaults:
            merged[key] = value
    return merged


def get_platform_config() -> dict:
    """Platform-wide defaults shared across domains."""
    return _get_config(
        "platform_config",
        {
            "default_max_retries": 3,
            "default_request_timeout": 30,
        },
    )


def get_meteo_config() -> dict:
    """Meteo vigilance and GeoRisques configuration."""
    return _get_config(
        "meteo_config",
        {
            "vigilance_bucket_name": "vigilance-meteo",
            "georisques_bucket_name": "georisques-report",
            "meteo_api_url": "https://rwg.meteofrance.com/wsft/v3/warning/timelaps",
            "api_params": {
                "domain": "FRA",
                "warning_type": "vigilance",
                "formatDate": "timestamp",
                "echeance": "J0",
                "depth": "1",
            },
            "vigilance_website_url": "https://vigilance.meteofrance.fr",
            "georisques_rapport_url": "https://georisques.gouv.fr/api/v1/resultats_rapport_risque",
            "ban_geocode_url": "https://api-adresse.data.gouv.fr/search/",
            "request_timeout_connect": 10,
            "request_timeout_read": 30,
            "max_workers": 6,
            "rate_limit_delay": 0.1,
        },
    )
