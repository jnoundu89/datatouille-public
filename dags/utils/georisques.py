import logging
import time
from typing import Any

import requests
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

REQUEST_HEADERS = {
    "User-Agent": "GeoRisquesReport/1.0 (Airflow DAG)",
    "Accept": "application/json",
}


def extract_severity(status_label: str | None) -> tuple[str, int]:
    """Extract severity level from status label."""
    if not status_label:
        return "inconnu", 0

    label_lower = status_label.lower()
    if "important" in label_lower or "élevé" in label_lower or "eleve" in label_lower:
        return "eleve", 3
    if "modéré" in label_lower or "modere" in label_lower or "moyen" in label_lower:
        return "modere", 2
    if "faible" in label_lower or "existant" in label_lower:
        return "faible", 1
    return "inconnu", 0


def geocode_address(address: str, ban_url: str) -> tuple[float, float] | None:
    """Geocode address to (longitude, latitude) via BAN API."""
    try:
        params: dict[str, str | int] = {"q": address, "limit": 1}
        response = requests.get(
            ban_url,
            params=params,
            headers=REQUEST_HEADERS,
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        features = data.get("features", [])
        if features:
            coords = features[0].get("geometry", {}).get("coordinates", [])
            if len(coords) >= 2:
                return float(coords[0]), float(coords[1])
    except (requests.exceptions.RequestException, ConnectionError, TimeoutError, ValueError):
        pass
    return None


def fetch_risk_report(
    insee_code: str,
    georisques_url: str,
    ban_url: str,
    request_timeout: float,
    rate_limit_delay: float,
    commune_name: str = "",
    postal_code: str = "",
) -> dict | None:
    """Fetch complete risk report for a commune from GeoRisques API.

    Tries INSEE code first, falls back to address geocoding if 404.
    """
    time.sleep(rate_limit_delay)

    # Try with INSEE code first
    try:
        response = requests.get(
            georisques_url,
            params={"code_insee": insee_code},
            headers=REQUEST_HEADERS,
            timeout=request_timeout,
        )
        if response.status_code == 200:
            return response.json()
    except requests.exceptions.RequestException:
        pass

    # Fallback: try with address geocoding
    if commune_name and postal_code:
        address = f"{commune_name}, {postal_code}"
        coords = geocode_address(address, ban_url)
        if coords:
            lon, lat = coords
            try:
                response = requests.get(
                    georisques_url,
                    params={"latlon": f"{lon},{lat}"},
                    headers=REQUEST_HEADERS,
                    timeout=request_timeout,
                )
                if response.status_code == 200:
                    logger.info("Fallback geocode OK for %s (%s)", insee_code, address)
                    return response.json()
            except requests.exceptions.RequestException:
                pass

    logger.warning("Failed to fetch risk report for INSEE %s", insee_code)
    return None


def normalize_risk_report(raw_data: dict, insee_code: str) -> dict[str, Any]:
    """Normalize raw API response into structured risk profile."""
    profile: dict[str, Any] = {
        "insee_code": insee_code,
        "commune": "",
        "code_postal": "",
        "risks": [],
        "metrics": {
            "naturels_count": 0,
            "technologiques_count": 0,
            "max_severity_naturels": 0,
            "max_severity_technologiques": 0,
        },
    }

    # Extract commune info
    commune_info = raw_data.get("commune") or {}
    profile["commune"] = commune_info.get("libelle", "")
    profile["code_postal"] = commune_info.get("codePostal", "")

    # Process natural risks
    risques_naturels = raw_data.get("risquesNaturels") or {}
    for risk_key, risk_data in risques_naturels.items():
        if not isinstance(risk_data, dict):
            continue

        present = risk_data.get("present", False)
        libelle = risk_data.get("libelle", risk_key)
        status_commune = risk_data.get("libelleStatutCommune", "")
        status_adresse = risk_data.get("libelleStatutAdresse", "")

        severity_label, severity_score = extract_severity(status_commune or status_adresse)

        profile["risks"].append(
            {
                "category": "naturel",
                "key": risk_key,
                "libelle": libelle,
                "present": present,
                "statut_commune": status_commune,
                "statut_adresse": status_adresse,
                "severity_label": severity_label,
                "severity_score": severity_score,
            }
        )

        if present:
            profile["metrics"]["naturels_count"] += 1
            profile["metrics"]["max_severity_naturels"] = max(
                profile["metrics"]["max_severity_naturels"], severity_score
            )

    # Process technological risks
    risques_techno = raw_data.get("risquesTechnologiques") or {}
    for risk_key, risk_data in risques_techno.items():
        if not isinstance(risk_data, dict):
            continue

        present = risk_data.get("present", False)
        libelle = risk_data.get("libelle", risk_key)
        status_commune = risk_data.get("libelleStatutCommune", "")
        status_adresse = risk_data.get("libelleStatutAdresse", "")

        severity_label, severity_score = extract_severity(status_commune or status_adresse)

        profile["risks"].append(
            {
                "category": "technologique",
                "key": risk_key,
                "libelle": libelle,
                "present": present,
                "statut_commune": status_commune,
                "statut_adresse": status_adresse,
                "severity_label": severity_label,
                "severity_score": severity_score,
            }
        )

        if present:
            profile["metrics"]["technologiques_count"] += 1
            profile["metrics"]["max_severity_technologiques"] = max(
                profile["metrics"]["max_severity_technologiques"], severity_score
            )

    return profile


def insert_commune_profiles(cur, commune_profiles: list[dict[str, Any]]) -> None:
    """Insert commune risk profiles into PostgreSQL."""
    if not commune_profiles:
        return

    commune_values = [
        (
            r["insee_code"],
            r["commune"],
            r["code_postal"],
            r["department_code"],
            r["department_name"],
            r["region"],
            r["naturels_count"],
            r["technologiques_count"],
            r["total_risks"],
            r["max_severity_naturels"],
            r["max_severity_technologiques"],
            r.get("seisme_present", False),
            r.get("seisme_severity", 0),
            r.get("rga_present", False),
            r.get("rga_severity", 0),
            r.get("rga_statut", ""),
            r.get("radon_present", False),
            r.get("radon_severity", 0),
            r.get("inondation_present", False),
            r.get("inondation_severity", 0),
            r.get("icpe_present", False),
            r.get("nucleaire_present", False),
            r["extracted_at"],
            r["extraction_date"],
        )
        for r in commune_profiles
    ]

    execute_values(
        cur,
        """
        INSERT INTO georisques_commune_profiles (
            insee_code, commune, code_postal, department_code, department_name, region,
            naturels_count, technologiques_count, total_risks,
            max_severity_naturels, max_severity_technologiques,
            seisme_present, seisme_severity, rga_present, rga_severity, rga_statut,
            radon_present, radon_severity, inondation_present, inondation_severity,
            icpe_present, nucleaire_present, extracted_at, extraction_date
        ) VALUES %s
        ON CONFLICT (insee_code, extraction_date)
        DO UPDATE SET
            commune = EXCLUDED.commune,
            naturels_count = EXCLUDED.naturels_count,
            technologiques_count = EXCLUDED.technologiques_count,
            total_risks = EXCLUDED.total_risks,
            max_severity_naturels = EXCLUDED.max_severity_naturels,
            max_severity_technologiques = EXCLUDED.max_severity_technologiques,
            seisme_present = EXCLUDED.seisme_present,
            seisme_severity = EXCLUDED.seisme_severity,
            rga_present = EXCLUDED.rga_present,
            rga_severity = EXCLUDED.rga_severity,
            rga_statut = EXCLUDED.rga_statut,
            radon_present = EXCLUDED.radon_present,
            radon_severity = EXCLUDED.radon_severity,
            inondation_present = EXCLUDED.inondation_present,
            inondation_severity = EXCLUDED.inondation_severity,
            icpe_present = EXCLUDED.icpe_present,
            nucleaire_present = EXCLUDED.nucleaire_present,
            extracted_at = EXCLUDED.extracted_at
        """,
        commune_values,
    )


def insert_gie_risks(cur, gie_records: list[dict[str, Any]]) -> None:
    """Insert GIE risk mappings into PostgreSQL."""
    if not gie_records:
        return

    gie_values = [
        (
            r["gie_name"],
            r["insee_code"],
            r["commune"],
            r["postal_code"],
            r["department_code"],
            r["department_name"],
            r["region"],
            r["naturels_count"],
            r["technologiques_count"],
            r["total_risks"],
            r["max_severity_naturels"],
            r["max_severity_technologiques"],
            r["seisme_present"],
            r["seisme_severity"],
            r["rga_present"],
            r["rga_severity"],
            r["rga_statut"],
            r["radon_present"],
            r["radon_severity"],
            r["inondation_present"],
            r["inondation_severity"],
            r["icpe_present"],
            r["nucleaire_present"],
            r["extracted_at"],
            r["extraction_date"],
        )
        for r in gie_records
    ]

    execute_values(
        cur,
        """
        INSERT INTO georisques_gie_risks (
            gie_name, insee_code, commune, postal_code,
            department_code, department_name, region,
            naturels_count, technologiques_count, total_risks,
            max_severity_naturels, max_severity_technologiques,
            seisme_present, seisme_severity, rga_present, rga_severity, rga_statut,
            radon_present, radon_severity, inondation_present, inondation_severity,
            icpe_present, nucleaire_present, extracted_at, extraction_date
        ) VALUES %s
        ON CONFLICT (gie_name, extraction_date)
        DO UPDATE SET
            insee_code = EXCLUDED.insee_code,
            commune = EXCLUDED.commune,
            naturels_count = EXCLUDED.naturels_count,
            technologiques_count = EXCLUDED.technologiques_count,
            total_risks = EXCLUDED.total_risks,
            max_severity_naturels = EXCLUDED.max_severity_naturels,
            max_severity_technologiques = EXCLUDED.max_severity_technologiques,
            seisme_present = EXCLUDED.seisme_present,
            seisme_severity = EXCLUDED.seisme_severity,
            rga_present = EXCLUDED.rga_present,
            rga_severity = EXCLUDED.rga_severity,
            rga_statut = EXCLUDED.rga_statut,
            radon_present = EXCLUDED.radon_present,
            radon_severity = EXCLUDED.radon_severity,
            inondation_present = EXCLUDED.inondation_present,
            inondation_severity = EXCLUDED.inondation_severity,
            icpe_present = EXCLUDED.icpe_present,
            nucleaire_present = EXCLUDED.nucleaire_present,
            extracted_at = EXCLUDED.extracted_at
        """,
        gie_values,
    )


def insert_risk_details(cur, risk_details: list[dict[str, Any]]) -> None:
    """Insert nested risk details into PostgreSQL."""
    if not risk_details:
        return

    detail_values = [
        (
            r["insee_code"],
            r["risk_category"],
            r["risk_key"],
            r["risk_libelle"],
            r["present"],
            r["statut_commune"],
            r["statut_adresse"],
            r["severity_label"],
            r["severity_score"],
            r["extracted_at"],
            r["extraction_date"],
        )
        for r in risk_details
    ]

    execute_values(
        cur,
        """
        INSERT INTO georisques_risk_details (
            insee_code, risk_category, risk_key, risk_libelle,
            present, statut_commune, statut_adresse,
            severity_label, severity_score, extracted_at, extraction_date
        ) VALUES %s
        ON CONFLICT (insee_code, risk_key, extraction_date)
        DO UPDATE SET
            risk_category = EXCLUDED.risk_category,
            risk_libelle = EXCLUDED.risk_libelle,
            present = EXCLUDED.present,
            statut_commune = EXCLUDED.statut_commune,
            statut_adresse = EXCLUDED.statut_adresse,
            severity_label = EXCLUDED.severity_label,
            severity_score = EXCLUDED.severity_score,
            extracted_at = EXCLUDED.extracted_at
        """,
        detail_values,
    )
