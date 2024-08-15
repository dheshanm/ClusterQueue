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

import pandas as pd

from scheduler.helpers import cli, db
from scheduler.helpers.config import config
from scheduler.models.job import Job
from scheduler.models.node import Node
from scheduler.models.processor import Processor

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def snooze(config_file: Path, interruptible: bool = True) -> None:
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
        if not interruptible:
            logger.info(
                "KeyboardInterrupt received. Aborting...", extra={"markup": True}
            )
            sys.exit(0)
        else:
            try:
                logger.info(
                    "[bold red]Snooze interrupted by user.", extra={"markup": True}
                )
                logger.info("[red]Interrupt again to exit.", extra={"markup": True})
                time.sleep(5)
            except KeyboardInterrupt:
                logger.info("[bold red]Exiting...", extra={"markup": True})
                sys.exit(0)
            logger.info("[bold green]Resuming...", extra={"markup": True})


def update_node_last_seen(hostname: str, config_file: Path) -> None:
    """
    Updates the last seen time of the compute node.

    Args:
        hostname (str): The hostname of the compute node.
        config_file (str): The path to the configuration file.

    Returns:
        None
    """

    query = Node.update_last_seen_query(hostname)

    db.execute_queries(
        config_file=config_file,
        queries=[query],
        silent=True,
        show_commands=False,
    )


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


def update_node_processor(
    hostname: str, processor_idx: int, config_file: Path, status: str
) -> None:
    """
    Registers the compute node with the scheduler.

    Args:
        hostname (str): The hostname of the compute node.
        config_file (str): The path to the configuration file.
        status (str): The status of the compute node.

    Returns:
        None
    """

    processor = Processor(
        processor_parent_node=hostname,
        processor_id=processor_idx,
        processor_status=status,
        processor_last_seen=datetime.now(),
    )

    query = processor.insert_query()

    db.execute_queries(
        config_file=config_file,
        queries=[query],
        silent=True,
        show_commands=False,
    )

    # Update the last seen time of the compute node
    update_node_last_seen(hostname, config_file)


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


def claim_job(
    hostname: str, processor_idx: int, job_id: int, config_file: Path
) -> bool:
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
        job_assigned_node_processor = {processor_idx},
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
    SELECT job_assigned_node, job_assigned_node_processor
    FROM jobs
    WHERE job_id = {job_id}
    """

    result: pd.DataFrame = db.execute_sql(
        config_file=config_file,
        query=query,
    )

    if result.empty:
        return False

    if (
        result["job_assigned_node"].iloc[0] == hostname
        and result["job_assigned_node_processor"].iloc[0] == processor_idx
    ):
        return True

    return False


def update_job_status(job_id: int, status: str, config_file: Path) -> None:
    """
    Updates the status of a job.

    Args:
        job_id (int): The ID of the job.
        status (str): The new status of the job.
        config_file (str): The path to the configuration file.

    Returns:
        None
    """

    query = f"""
    UPDATE jobs
    SET job_status = '{status}',
        job_last_updated = '{datetime.now()}'
    WHERE job_id = {job_id}
    """

    db.execute_queries(
        config_file=config_file,
        queries=[query],
        silent=True,
        show_commands=False,
    )


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

    start_timestamp = datetime.now()

    with open(log_stdout, "w", encoding="utf-8") as f:
        f.write("-" * 80)
        f.write("\n")
        f.write(f"Job ID: {job.job_id}\n")
        f.write(f"Job Payload: {job.job_payload}\n")
        f.write(f"Job Tags: {job.job_tags}\n")
        f.write(f"Job Submission Time: {job.job_submission_time}\n")
        f.write(f"Job Started at: {start_timestamp}\n")
        f.write(f"Job Metadata: {job_metadata}\n")
        f.write("+" * 80)
        f.write("\n")

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

    end_timestamp = datetime.now()
    duration = end_timestamp - start_timestamp

    result_metadata["start_timestamp"] = start_timestamp
    result_metadata["end_timestamp"] = end_timestamp
    result_metadata["duration_s"] = duration.total_seconds()

    result_metadata_str = db.sanitize_json(result_metadata)

    with open(log_stdout, "a", encoding="utf-8") as f:
        f.write("+" * 80)
        f.write("\n")
        f.write(f"Job Result Metadata: {result_metadata}\n")
        f.write(f"Job Completed at: {end_timestamp}\n")
        f.write("-" * 80)
        f.write("\n")

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


def stop_node(hostname: str, config_file: Path) -> None:
    """
    Stops a compute node.

    Args:
        hostname (str): The hostname of the compute node.
        config_file (str): The path to the configuration file.

    Returns:
        None
    """
    queries: List[str] = []

    stop_node_query = f"""
    UPDATE nodes
    SET node_status = 'STOPPED',
        node_last_seen = '{datetime.now()}'
    WHERE node_hostname = '{hostname}'
    """

    interrupt_jobs_query = f"""
    UPDATE jobs
    SET job_status = 'INTERRUPTED',
        job_last_updated = '{datetime.now()}'
    WHERE job_assigned_node = '{hostname}'
        AND job_status = 'RUNNING'
    """

    queries.append(stop_node_query)
    queries.append(interrupt_jobs_query)

    logger.info(f"Stopping node {hostname}")
    logger.debug(f"Interrupting jobs on node {hostname}")

    db.execute_queries(
        config_file=config_file,
        queries=queries,
        silent=True,
        show_commands=False,
    )


def submit_job(job: Job, config_file: Path) -> None:
    """
    Submits a job to the database.

    Args:
        job (Job): The job to submit.
        config_file (str): The path to the configuration file.

    Returns:
        None
    """

    query = job.insert_query()

    db.execute_queries(
        config_file=config_file,
        queries=[query],
        silent=True,
        show_commands=False,
    )
