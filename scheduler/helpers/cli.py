"""
Module providing command line interface for the scheduler.
"""

import subprocess
import sys
import logging
from typing import Callable, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)


def get_repo_root() -> str:
    """
    Returns the root directory of the current Git repository.

    Uses the command `git rev-parse --show-toplevel` to get the root directory.
    """

    try:
        repo_root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"])
    except subprocess.CalledProcessError:
        logger.error("Not in a Git repository.")
        raise
    repo_root = repo_root.decode("utf-8").strip()
    return repo_root


def execute_commands(
    command_array: list,
    shell: bool = False,
    on_fail: Callable = lambda: sys.exit(1),
) -> subprocess.CompletedProcess:
    """
    Executes a command and returns the result.

    Args:
        command_array (list): The command to execute as a list of strings.
        shell (bool, optional): Whether to execute the command in a shell. Defaults to False.
        logger (Optional[logging.Logger], optional): The logger to use for logging.
            Defaults to None.
        on_fail (Callable, optional): The function to call if the command fails.
            Defaults to lambda: sys.exit(1).

    Returns:
        subprocess.CompletedProcess: The result of the command execution.

    """
    logger.debug("Executing command:")
    # cast to str to avoid error when command_array is a list of Path objects
    command_array = [str(x) for x in command_array]

    if logger:
        logger.debug(" ".join(command_array))

    if shell:
        result = subprocess.run(
            " ".join(command_array),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            check=False,
        )
    else:
        result = subprocess.run(
            command_array, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False
        )

    if result.returncode != 0:
        logger.error("=====================================")
        logger.error("Command: %s", " ".join(command_array))
        logger.error("=====================================")
        logger.error("stdout:")
        logger.error(result.stdout.decode("utf-8"))
        logger.error("=====================================")
        logger.error("stderr:")
        logger.error(result.stderr.decode("utf-8"))
        logger.error("=====================================")
        logger.error("Exit code: %s", str(result.returncode))
        logger.error("=====================================")

        if on_fail:
            on_fail()

    return result


def get_hostname() -> str:
    """
    Returns the hostname of the current machine.
    """
    return subprocess.check_output(["hostname"]).decode("utf-8").strip()


def execute_job(
    payload: str,
    env_variables: Dict[str, Any],
    cwd: Path,
    stdout: Path,
    stderr: Path,
) -> subprocess.CompletedProcess:
    """
    Executes a job.

    Args:
        payload (str): The payload to execute.
        env_variables (Dict[str, Any]): The environment variables to set.
        cwd (Path): The working directory to use.
        stdout (Path): The path to the stdout file.
        stderr (Path): The path to the stderr file.

    Returns:
        subprocess.CompletedProcess: The result of the job execution.
    """

    # Set the environment variables
    env: Dict[str, str] = {}
    for key, value in env_variables.items():
        env[key] = value

    logger.debug("Executing job:")
    logger.debug("Payload: %s", payload)
    logger.debug("Environment variables:")
    for key, value in env.items():
        logger.debug(f"{key}={value}")
    logger.debug("Working directory: %s", cwd)
    logger.debug("stdout: %s", stdout)
    logger.debug("stderr: %s", stderr)

    # Execute the command
    result = subprocess.run(
        payload,
        stdout=stdout.open("w"),
        stderr=stderr.open("w"),
        cwd=cwd,
        env=env,
        shell=True,
        check=False,
    )

    if result.returncode != 0:
        logger.error("=====================================")
        logger.error("Command: %s", payload)
        logger.error("=====================================")
        logger.error("Environment variables:")
        for key, value in env.items():
            logger.error(f"{key}={value}")
        logger.error("=====================================")
        logger.error("stdout:")
        with stdout.open("r") as f:
            logger.error(f.read())
        logger.error("=====================================")
        logger.error("stderr:")
        with stderr.open("r") as f:
            logger.error(f.read())
        logger.error("=====================================")
        logger.error("Exit code: %s", str(result.returncode))
        logger.error("=====================================")

    return result
