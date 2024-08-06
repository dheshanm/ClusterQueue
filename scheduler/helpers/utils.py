"""
Helper functions for the scheduler
"""

import logging
from datetime import datetime
from pathlib import Path
import platform

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from scheduler.helpers import cli
from scheduler.helpers.config import config

_console = Console(color_system="standard")

logger = logging.getLogger(__name__)


class HostnameFilter(logging.Filter):
    hostname = platform.node()

    def filter(self, record):
        record.hostname = HostnameFilter.hostname
        return True


def get_progress_bar(transient: bool = False) -> Progress:
    """
    Returns a rich Progress object with standard columns.

    Returns:
        Progress: A rich Progress object with standard columns.
    """
    return Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        transient=transient,
    )


def get_console() -> Console:
    """
    Returns a Console object with standard color system.

    Returns:
        Console: A Console object with standard color system.
    """
    return _console


def configure_logging(config_file: Path, module_name: str, logger: logging.Logger):
    """
    Configures logging for a given module using the specified configuration file.

    Args:
        config_file (str): The path to the configuration file.
        module_name (str): The name of the module to configure logging for.
        logger (logging.Logger): The logger object to use for logging.

    Returns:
        None
    """
    log_params = config(config_file, "logging")
    log_path = log_params[module_name]

    if log_path[0] == ".":
        log_path = cli.get_repo_root() + log_path[1:]
    log_file = Path(log_path)

    if log_file.exists() and log_file.stat().st_size > 10000000:  # 10MB
        archive_file = (
            log_file.parent
            / "archive"
            / f"{log_file.stem}_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
        )
        logger.info(f"Rotating log file to {archive_file}")

        archive_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.rename(archive_file)

    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.addFilter(HostnameFilter())

    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s  - %(process)d @ %(hostname)s - \
%(name)s - %(levelname)s - %(message)s - [%(filename)s:%(lineno)d]"
        )
    )

    logging.getLogger().addHandler(file_handler)
    logger.info(f"Logging to {log_file}")


def get_config_file_path() -> Path:
    """
    Returns the path to the config file.

    Returns:
        str: The path to the config file.

    Raises:
        ConfigFileNotFoundExeption: If the config file is not found.
    """
    repo_root = cli.get_repo_root()
    config_file_path = repo_root + "/config.ini"

    # Check if config_file_path exists
    if not Path(config_file_path).is_file():
        raise FileNotFoundError(f"Config file not found at {config_file_path}")

    return Path(config_file_path)
