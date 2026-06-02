"""Run Soda data quality checks from within Airflow tasks."""

import logging
import subprocess

logger = logging.getLogger(__name__)

SODA_CONFIG = "/opt/airflow/soda/configuration_docker.yml"
SODA_CHECKS_DIR = "/opt/airflow/soda/checks"


def run_soda_check(contract_file: str) -> dict:
    """Run a Soda check file and return results.

    Args:
        contract_file: Name of the check file (e.g., 'gaming_contract.yml')

    Returns:
        dict with keys: passed (bool), output (str), return_code (int)
    """
    check_path = f"{SODA_CHECKS_DIR}/{contract_file}"
    cmd = ["soda", "scan", "-d", "postgres", "-c", SODA_CONFIG, check_path]

    logger.info("Running Soda check: %s", contract_file)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

    passed = result.returncode == 0
    output = result.stdout + result.stderr

    if passed:
        logger.info("Soda check PASSED: %s", contract_file)
    else:
        logger.warning("Soda check FAILED: %s\n%s", contract_file, output[-500:])

    return {"passed": passed, "output": output[-2000:], "return_code": result.returncode}
