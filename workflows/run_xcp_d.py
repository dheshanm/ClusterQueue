#!/usr/bin/env python
"""
Run SCP-D on the a single subject/session pair.
"""

import argparse
import logging
import random
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import IO, Callable

logger = logging.getLogger("fmriprep")
logargs = {
    "level": logging.DEBUG,
    "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    # "format": "%(message)s",
}
logging.basicConfig(**logargs)

SINGULARITY_IMGAGE_PATH = "/data/predict1/home/kcho/singularity_images/xcp_d-0.8.3.simg"
LOGS_DIR = Path("/data/predict1/home/dm1447/xcp_d/logs")
MRI_ROOT = Path("/data/predict1/data_from_nda/MRI_ROOT")
FMRIPREP_OUTPUT_DIR = Path("/data/predict2/MRI_ROOT/derivatives/fmriprep_24_0_0")
OUT_ROOT = Path("/data/predict2/MRI_ROOT/derivatives/xcp_d_0_8_3")

SINGULARITY_FALLBACK_PATH = (
    "/apps/released/gcc-toolchain/gcc-4.x/singularity/singularity-3.7.0/bin/singularity"
)

# local
# check if '~/scratch' exists
if Path("~/scratch").expanduser().exists():
    TEMP_ROOT = Path("~/scratch").expanduser()
elif Path("/tmp").exists():
    TEMP_ROOT = Path("/tmp")
else:
    raise FileNotFoundError("No temporary directory found")


def execute_commands(
    command: str,
    stdout: IO,
    stderr: IO,
    timeout: str = "24h",
    on_fail: Callable = lambda: sys.exit(1),
) -> subprocess.CompletedProcess:
    """
    Executes a command and returns the result.

    Args:
        command (str): The command to execute.
        stdout (IO): The file object to write stdout to.
        stderr (IO): The file object to write stderr to.
        on_fail (Callable, optional): The function to call if the command fails.
            Defaults to lambda: sys.exit(1).

    Returns:
        subprocess.CompletedProcess: The result of the command execution.

    """

    # Add timeout to the command
    command = f"timeout {timeout} {command}"

    logger.debug("Executing command:")
    logger.debug(command)

    result = subprocess.run(
        command,
        stdout=stdout,
        stderr=stderr,
        shell=True,
        check=False,
    )

    # log outputs to sys.stdout and sys.stderr
    with open(stdout.name, "r", encoding="utf-8") as stdout_file:
        sys.stdout.write(stdout_file.read())
    with open(stderr.name, "r", encoding="utf-8") as stderr_file:
        sys.stderr.write(stderr_file.read())

    if result.returncode != 0:
        logger.error("=====================================")
        logger.error("Command: %s", command)
        logger.error("=====================================")
        logger.error("stdout:")
        with open(stdout.name, "r", encoding="utf-8") as stdout_file:
            logger.error(stdout_file.read())
        logger.error("=====================================")
        logger.error("stderr:")
        with open(stderr.name, "r", encoding="utf-8") as stderr_file:
            logger.error(stderr_file.read())
        logger.error("=====================================")
        logger.error("Exit code: %s", str(result.returncode))
        logger.error("=====================================")

        if on_fail:
            on_fail()

    return result


if __name__ == "__main__":
    logger.info(f"Logging to {LOGS_DIR}")
    if Path(LOGS_DIR).exists() is False:
        Path(LOGS_DIR).mkdir(parents=True)

    parser = argparse.ArgumentParser(
        description="Run SCP-D on a single subject/session pair"
    )
    parser.add_argument("subject_id", type=str, help="The subject ID. e.g. sub-XXXXXXX")
    parser.add_argument("session_id", type=str, help="The session ID. e.g. ses-XXXXXXX")

    args = parser.parse_args()

    # subject_id = "sub-YA52868"
    # session_id = "ses-202404051"
    subject_id: str = args.subject_id
    session_id: str = args.session_id

    logger.info(f"Running fmriprep for {subject_id} {session_id}")

    TEMP_DIR = TEMP_ROOT / "XCP_D"
    RANDOM_STR = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=8))
    TEMP_DIR = TEMP_DIR / RANDOM_STR

    SINGULARITY_EXEC = shutil.which("singularity")
    if SINGULARITY_EXEC is None:
        # Try looking at the default location
        SINGULARITY_EXEC = SINGULARITY_FALLBACK_PATH
        if Path(SINGULARITY_EXEC).exists() is False:
            logger.error("Singularity not found")
            sys.exit(404)

    logger.info(f"Singularity executable: {SINGULARITY_EXEC}")

    work_dir = TEMP_DIR / "work"
    work_dir.mkdir(exist_ok=True, parents=True)

    xcp_d_session_output_dir = TEMP_DIR / "output"
    xcp_d_session_output_dir.mkdir(exist_ok=True, parents=True)

    fmriprep_input_dir = FMRIPREP_OUTPUT_DIR / subject_id / session_id
    fmriprep_input_dir_copy = TEMP_DIR / "input" / "fmriprep"
    logger.info(
        f"Copying fmriprep output from {fmriprep_input_dir} to {fmriprep_input_dir_copy}"
    )
    shutil.copytree(fmriprep_input_dir, fmriprep_input_dir_copy)

    subject_id_raw = subject_id.replace("sub-", "")

    command = f"""{SINGULARITY_EXEC} run --cleanenv \
-B {fmriprep_input_dir_copy}:/fmriprep \
-B {xcp_d_session_output_dir}:/out \
-B /data/pnl/soft/pnlpipe3/freesurfer/license.txt:/opt/freesurfer/license.txt \
-B {work_dir}:/work \
{SINGULARITY_IMGAGE_PATH} \
/fmriprep /out participant \
-w /work --participant-label {subject_id_raw} \
--nprocs 8 --omp-nthreads 8 \
--input-type fmriprep \
--mode abcd \
--motion-filter-type none \
--dummy-scans auto \
--fd-thresh 0.3 \
--lower-bpf 0.01 \
--upper-bpf 0.08 \
--nuisance-regressors gsr_only \
--create-matrices all \
--atlases Glasser Gordon 4S456Parcels HCP Tian \
--smoothing 0 \
--fs-license-file /opt/freesurfer/license.txt
"""

    current_time = datetime.now().isoformat()

    stdout_path = LOGS_DIR / f"{subject_id}_{session_id}_{current_time}_stdout.log"
    stderr_path = LOGS_DIR / f"{subject_id}_{session_id}_{current_time}_stderr.log"

    stdout = open(stdout_path, "w", encoding="utf-8")
    stderr = open(stderr_path, "w", encoding="utf-8")

    logger.debug(f"Logging stdout to {stdout_path}")
    logger.debug(f"Logging stderr to {stderr_path}")

    # # log to stdout and stderr
    # stdout = sys.stdout
    # stderr = sys.stderr
    execute_commands(command, stdout=stdout, stderr=stderr)

    stdout.close()
    stderr.close()
    # copy logs to the output directory
    shutil.copy(stdout_path, xcp_d_session_output_dir)
    shutil.copy(stderr_path, xcp_d_session_output_dir)

    logger.info(f"Finished fmriprep for {subject_id} {session_id}")

    output_dir = OUT_ROOT / subject_id / session_id
    logger.info(f"Moving assets from {xcp_d_session_output_dir} to {output_dir}")
    output_dir.parent.mkdir(exist_ok=True, parents=True)

    shutil.copytree(xcp_d_session_output_dir, output_dir)

    logger.info(f"Removing temporary directory {TEMP_DIR}")
    shutil.rmtree(TEMP_DIR)

    logger.info(f"{subject_id} {session_id} finished")
    sys.exit(0)
