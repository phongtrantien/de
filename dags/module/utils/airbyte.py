import requests
import time
from airflow.exceptions import AirflowException
from dags.module.config_loader import load_env

config = load_env()


def trigger_sync(connection_id: str) -> str:

    url = f"{config['airbyte']['api']}/connections/sync"
    payload = {"connectionId": connection_id}

    print(f"[Airbyte] Triggering sync for connection {connection_id} at {url}")
    resp = requests.post(url, json=payload)

    if resp.status_code != 200:
        raise AirflowException(f"Failed to trigger Airbyte sync: {resp.text}")

    job_id = resp.json().get("job", {}).get("id")
    if not job_id:
        raise AirflowException(f"Cannot extract job_id from Airbyte response: {resp.text}")

    print(f"[Airbyte] Sync triggered for connection {connection_id}, job_id={job_id}")
    return job_id


def wait_for_job(job_id: str, poll_interval: int = 20, timeout: int = 1800):

    url = f"{config['airbyte']['api']}/jobs/get"
    start_time = time.time()

    print(f"[Airbyte] Waiting for job {job_id} to complete...")
    while time.time() - start_time < timeout:
        resp = requests.post(url, json={"id": job_id})
        if resp.status_code != 200:
            raise AirflowException(f"Failed to get job status: {resp.text}")

        job = resp.json().get("job", {})
        status = job.get("status")
        print(f"[Airbyte] Job {job_id} status: {status}")

        if status in ["succeeded", "failed", "cancelled"]:
            if status != "succeeded":
                raise AirflowException(f"Airbyte job {job_id} failed with status: {status}")
            print(f"[Airbyte] Job {job_id} succeeded!")
            return status

        time.sleep(poll_interval)

    raise AirflowException(f"Timeout waiting for Airbyte job {job_id} to finish")
