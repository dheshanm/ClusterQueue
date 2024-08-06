#!/usr/bin/env python
"""
Initializes the database.
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
from datetime import datetime

from rich.logging import RichHandler

from scheduler.helpers import utils, db
from scheduler.models import Job

MODULE_NAME = "test"
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

    new_job = Job(
        job_payload="/data/predict1/home/dm1447/dev/ClusterQueue/workflows/run_fmriprep.py sub-YA52868 ses-202404051",
        job_status="PENDING",
        job_last_updated=datetime.now(),
        job_submission_time=datetime.now(),
    )

    insert_query = new_job.insert_query()
    db.execute_queries(config_file=config_file, queries=[insert_query])

    logger.info("Done!")
