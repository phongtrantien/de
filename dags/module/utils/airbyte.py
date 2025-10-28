
import requests
import time
from airflow.exceptions import AirflowException
from requests.auth import HTTPBasicAuth

from dags.module.config_loader import load_env
from module.utils.logger_utils import get_logger

config = load_env()
logger = get_logger("airbyte_sync")

user = config['airbyte']['user']
password = config['airbyte']['password']


def trigger_sync(connection_id: str) -> str:
    url = f"{config['airbyte']['api']}/connections/sync"
    payload = {"connectionId": connection_id}
    logger.info(f"Triggering sync for connection {connection_id} at {url}")

    resp = requests.post(url, json=payload, auth=HTTPBasicAuth(user, password))
    if resp.status_code != 200:
        raise AirflowException(f"Failed to trigger Airbyte sync: {resp.text}")

    job_id = resp.json().get("job", {}).get("id")
    if not job_id:
        raise AirflowException(f"Cannot extract job_id from Airbyte response: {resp.text}")

    logger.info(f"Sync triggered for connection {connection_id}, job_id={job_id}")
    return job_id


def wait_for_job(job_id: str, poll_interval: int = 20, timeout: int = 1800):
    url = f"{config['airbyte']['api']}/jobs/get"
    start_time = time.time()

    logger.info(f"Waiting for job {job_id} to complete...")
    while time.time() - start_time < timeout:
        resp = requests.post(url, json={"id": job_id}, auth=HTTPBasicAuth(user, password))
        if resp.status_code != 200:
            raise AirflowException(f"Failed to get job status: {resp.text}")

        job = resp.json().get("job", {})
        status = job.get("status")
        logger.info(f"Job {job_id} status: {status}")

        if status in ["succeeded", "failed", "cancelled"]:
            if status != "succeeded":
                raise AirflowException(f"Airbyte job {job_id} failed with status: {status}")
            logger.info(f"Job {job_id} succeeded!")
            return status

        time.sleep(poll_interval)

    raise AirflowException(f"Timeout waiting for Airbyte job {job_id} to finish")
