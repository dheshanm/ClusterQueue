#!/usr/bin/env python
"""
Starts the compute node.
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == "ClusterQueue":
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
from typing import List

from rich.logging import RichHandler

from scheduler import orchestrator
from scheduler.helpers import utils, cli

MODULE_NAME = "scheduler.runners.compute_node"
INSTANCE_NAME = MODULE_NAME

console = utils.get_console()


logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    HOSTNAME = cli.get_hostname()
    TAGS: List[str] = ["gpu"]
    logger.info(f"Starting compute node @ {HOSTNAME}")

    orchestrator.update_node(
        hostname=HOSTNAME, config_file=config_file, status="started", tags=TAGS
    )
    logger.info("Node registered with the scheduler. Starting main loop...")
    while True:
        available_jobs = orchestrator.get_pending_jobs(
            config_file=config_file, tags=TAGS, limit=1
        )
        if available_jobs is None:
            logger.info("No jobs available.")
            orchestrator.update_node(
                hostname=HOSTNAME, config_file=config_file, status="snoozing", tags=TAGS
            )
            orchestrator.snooze(config_file=config_file)
            orchestrator.update_node(
                hostname=HOSTNAME, config_file=config_file, status="idle", tags=TAGS
            )
            continue

        job = available_jobs[0]
        logger.info(f"Attempt claiming job {job.job_id}: {job.job_payload}")

        claimed_sucessfully = orchestrator.claim_job(
            job_id=job.job_id,  # type: ignore
            hostname=HOSTNAME,
            config_file=config_file,
        )

        if not claimed_sucessfully:
            logger.info(f"Failed to claim job {job.job_id}: {job.job_payload}")
            continue

        logger.info(f"Claimed job {job.job_id}: {job.job_payload}")

        orchestrator.handle_job(
            job=job,
            config_file=config_file,
        )

        logger.info(f"Job {job.job_id} completed.")
    
    logger.info("Done!")
