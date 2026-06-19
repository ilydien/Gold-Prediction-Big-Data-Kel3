import os
import time
import urllib.request

os.environ["PREFECT_API_URL"] = os.getenv("PREFECT_API_URL", "http://prefect:4200/api")

from prefect.client.schemas.schedules import CronSchedule
from storage.prefect.flows import gold_pipeline


def _wait_for_server():
    api_url = os.environ["PREFECT_API_URL"]
    for attempt in range(30):
        try:
            req = urllib.request.urlopen(f"{api_url}/health", timeout=5)
            if req.status == 200:
                print(f"Prefect server ready at {api_url}")
                return
        except Exception:
            if attempt == 29:
                raise
            print(f"  Waiting for Prefect server... ({attempt+1}/30)")
            time.sleep(2)


if __name__ == "__main__":
    print(f"Registering deployment to {os.environ['PREFECT_API_URL']}...")
    _wait_for_server()

    deployment = gold_pipeline.to_deployment(
        name="gold-training-daily",
        work_pool_name="gold-pool",
        schedule=CronSchedule(cron="0 3 * * *", timezone="Asia/Jakarta"),
    )
    deployment_id = deployment.apply()
    print(f"Deployment registered: gold-training-daily (id={deployment_id})")

    # Fix: set path to /app so worker subprocess can find flow code
    from prefect.client.orchestration import PrefectClient
    from prefect.client.schemas.actions import DeploymentUpdate
    client = PrefectClient(api=os.environ["PREFECT_API_URL"])
    import asyncio
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(client.update_deployment(deployment_id, DeploymentUpdate(path="/app")))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        loop.run_until_complete(client.update_deployment(deployment_id, DeploymentUpdate(path="/app")))
    print(f"  path: /app")
    print(f"  work_pool: gold-pool  cron: 0 3 * * * Asia/Jakarta")
