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
import multiprocessing
import argparse

from rich.logging import RichHandler
from pydantic import BaseModel

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

# ----------------------------------------------

TAGS: List[str] = ["cpu"]
NUM_PARALLEL_JOBS = 4

# ----------------------------------------------


class ProcessorContext(BaseModel):
    """
    Represents the context for a single processor.

    Contains metadata about the node.
    """

    hostname: str
    config_file: Path
    tags: List[str]
    num_parallel_jobs: int
    processor_idx: int


def processor(
    context: ProcessorContext,
) -> None:
    """
    Represents a single processor, that can do 1 job at a time.

    Spin up multiple processors to handle multiple jobs in parallel.

    Args:
        context (ProcessorContext): The context for the processor.

    Returns:
        None
    """
    config_file = context.config_file
    processor_idx = context.processor_idx
    hostname = context.hostname
    tags = context.tags

    logger.info(f"Starting processor {processor_idx} on {hostname}")

    while True:
        available_jobs = orchestrator.get_pending_jobs(
            config_file=config_file, tags=tags, limit=1
        )
        if available_jobs is None:
            logger.info("No jobs available.")
            orchestrator.update_node_processor(
                hostname=hostname,
                processor_idx=processor_idx,
                config_file=config_file,
                status="snoozing",
            )
            orchestrator.snooze(config_file=config_file, interruptible=False)
            orchestrator.update_node_processor(
                hostname=hostname,
                processor_idx=processor_idx,
                config_file=config_file,
                status="idle",
            )
            continue

        job = available_jobs[0]
        logger.info(f"Attempt claiming job {job.job_id}: {job.job_payload}")

        claimed_sucessfully = orchestrator.claim_job(
            job_id=job.job_id,  # type: ignore
            processor_idx=processor_idx,
            hostname=hostname,
            config_file=config_file,
        )

        if not claimed_sucessfully:
            logger.info(f"Failed to claim job {job.job_id}: {job.job_payload}")
            continue

        logger.info(f"Claimed job {job.job_id}: {job.job_payload}")

        orchestrator.update_node_processor(
            hostname=hostname,
            processor_idx=processor_idx,
            config_file=config_file,
            status=f"handling {job.job_id}",
        )

        try:
            orchestrator.handle_job(
                job=job,
                config_file=config_file,
            )
        except Exception as e:
            logger.exception(f"Error handling job {job.job_id}: {e}")
            orchestrator.update_job_status(
                job_id=job.job_id,  # type: ignore
                config_file=config_file,
                status="FAILED",
            )

        logger.info(f"Job {job.job_id} completed.")

        orchestrator.update_node_processor(
            hostname=hostname,
            processor_idx=processor_idx,
            config_file=config_file,
            status="idle",
        )


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    argparser = argparse.ArgumentParser(description="Start the compute node.")
    argparser.add_argument(
        "--num_parallel_jobs",
        type=int,
        default=NUM_PARALLEL_JOBS,
        help="The number of parallel jobs to run.",
    )
    argparser.add_argument(
        "--tags",
        type=str,
        default="cpu",
        help="The tags to use for the node.",
    )
    args = argparser.parse_args()

    NUM_PARALLEL_JOBS = args.num_parallel_jobs
    TAGS = args.tags.split(",")

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    HOSTNAME = cli.get_hostname()
    logger.info(f"Starting compute node @ {HOSTNAME}")

    orchestrator.update_node(
        hostname=HOSTNAME, config_file=config_file, status="started", tags=TAGS
    )
    logger.info("Node registered with the scheduler. Starting main loop...")

    # Start multiple processors
    with multiprocessing.Pool(processes=NUM_PARALLEL_JOBS) as pool:
        params = [
            ProcessorContext(
                hostname=HOSTNAME,
                config_file=config_file,
                tags=TAGS,
                num_parallel_jobs=NUM_PARALLEL_JOBS,
                processor_idx=i,
            )
            for i in range(NUM_PARALLEL_JOBS)
        ]
        try:
            pool.map(processor, params)
        except KeyboardInterrupt:
            logger.info("Exiting...")
            orchestrator.stop_node(hostname=HOSTNAME, config_file=config_file)

    logger.info("Done!")
