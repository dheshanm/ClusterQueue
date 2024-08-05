"""
Pipeline ochestration module.
"""

import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from scheduler.helpers import cli, db
from scheduler.helpers.config import config
from scheduler.models.job import Job
from scheduler.models.node import Node

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def snooze(config_file: Path) -> None:
    """
    Sleeps for a specified amount of time.

    Args:
        config_file (str): The path to the configuration file.

    Returns:
        None
    """
    params = config(config_file, section="orchestration")
    snooze_time_seconds = int(params["snooze_time_seconds"])

    if snooze_time_seconds == 0:
        logger.info(
            "[bold green]Snooze time is set to 0. Exiting...", extra={"markup": True}
        )
        sys.exit(0)

    logger.info(
        f"[bold green]No jobs. Snoozing for {snooze_time_seconds} seconds...",
        extra={"markup": True},
    )

    # Sleep for snooze_time_seconds
    # Catch KeyboardInterrupt to allow the user to stop snoozing
    try:
        time.sleep(snooze_time_seconds)
    except KeyboardInterrupt:
        try:
            logger.info("[bold red]Snooze interrupted by user.", extra={"markup": True})
            logger.info("[red]Interrupt again to exit.", extra={"markup": True})
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("[bold red]Exiting...", extra={"markup": True})
            sys.exit(0)
        logger.info("[bold green]Resuming...", extra={"markup": True})


def update_node(hostname: str, config_file: Path, status: str, tags: List[str]) -> None:
    """
    Registers the compute node with the scheduler.

    Args:
        hostname (str): The hostname of the compute node.
        config_file (str): The path to the configuration file.
        status (str): The status of the compute node.

    Returns:
        None
    """

    node = Node(
        hostname=hostname,
        status=status,
        last_seen=datetime.now(),
        tags=tags,
    )

    query = node.insert_query()

    db.execute_queries(
        config_file=config_file,
        queries=[query],
        silent=True,
        show_commands=False,
    )


def get_pending_jobs(
    config_file: Path, tags: List[str], limit: int = 10
) -> Optional[List[Job]]:
    """
    Get the pending jobs from the database.

    Args:
        config_file (str): The path to the configuration file.
        limit (int): The number of jobs to return.

    Returns:
        List[Job]: A list of pending jobs.
    """

    available_jobs = Job.get_pending_jobs(
        config_file=config_file,
        tags=tags,
        limit=limit,
    )

    if len(available_jobs) == 0:
        return None

    return available_jobs


def claim_job(hostname: str, job_id: int, config_file: Path) -> bool:
    """
    Claims a job for a compute node.

    Args:
        hostname (str): The hostname of the compute node.
        job_id (int): The ID of the job to claim.

    Returns:
        bool: True if the job was claimed, False otherwise.
    """

    query = f"""
    UPDATE jobs
    SET job_assigned_node = '{hostname}',
        job_status = 'CLAIMED',
        job_last_updated = '{datetime.now()}'
    WHERE job_id = {job_id} AND job_status = 'PENDING'
    """

    query = db.execute_queries(
        config_file=config_file,
        queries=[query],
        silent=True,
        show_commands=False,
    )

    # check if the job was claimed

    query = f"""
    SELECT job_assigned_node
    FROM jobs
    WHERE job_id = {job_id}
    """

    result = db.fetch_record(
        config_file=config_file,
        query=query,
    )

    if result == hostname:
        return True

    return False


def handle_job(config_file: Path, job: Job) -> None:
    """
    Handles a job.

    Args:
        config_file (str): The path to the configuration file.
        job (Job): The job to handle.

    Returns:
        None
    """

    # Update the job status to RUNNING
    query = f"""
    UPDATE jobs
    SET job_status = 'RUNNING',
        job_last_updated = '{datetime.now()}'
    WHERE job_id = {job.job_id}
    """

    db.execute_queries(
        config_file=config_file,
        queries=[query],
        silent=True,
        show_commands=False,
    )

    # Execute the job
    job_metadata = job.job_metadata
    if job_metadata is None:
        job_metadata = {}

    orchestrator_params = config(config_file, section="orchestration")
    logs_root = Path(orchestrator_params["job_logs_root"])

    log_stdout = logs_root / f"job_{job.job_id}_stdout.log"
    log_stderr = logs_root / f"job_{job.job_id}_stderr.log"

    if "CWD" in job_metadata:
        cwd = Path(job_metadata["CWD"])
    else:
        cwd = Path(".")

    if job.job_env_variables is None:
        # clone current environment variables
        job.job_env_variables = os.environ.copy()

    result = cli.execute_job(
        payload=job.job_payload,
        env_variables=job.job_env_variables,
        cwd=cwd,
        stdout=log_stdout,
        stderr=log_stderr,
    )

    result_metadata = {}
    result_metadata["returncode"] = result.returncode

    result_metadata_str = db.sanitize_json(result_metadata)

    # Update the job status to COMPLETED
    query = f"""
    UPDATE jobs
    SET job_status = 'COMPLETED',
        job_last_updated = '{datetime.now()}',
        job_result_metadata = '{result_metadata_str}'
    WHERE job_id = {job.job_id}
    """

    db.execute_queries(
        config_file=config_file,
        queries=[query],
        silent=True,
        show_commands=False,
    )
