#!/usr/bin/env python
"""
Run fmriprep on the a single subject/session pair.

Reuses FS output directory from the previous run.
"""

import argparse
import json
import logging
import random
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import IO, Callable

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))

from scheduler import orchestrator
from scheduler.models.job import Job

logger = logging.getLogger("fmriprep")
logargs = {
    "level": logging.DEBUG,
    "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    # "format": "%(message)s",
}
logging.basicConfig(**logargs)

SINGULARITY_IMGAGE_PATH = (
    "/data/predict1/home/kcho/singularity_images/fmriprep-24.0.0.simg"
)
LOGS_DIR = Path("/data/predict1/home/dm1447/fmriprep/logs")
MRI_ROOT = Path("/data/predict1/data_from_nda/MRI_ROOT")
OUT_ROOT = Path("/data/predict2/MRI_ROOT/derivatives/fmriprep_24_0_0")

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


def create_link(source: Path, destination: Path, softlink: bool = True) -> None:
    """
    Create a link from the source to the destination.

    Note:
    - Both source and destination must be on the same filesystem.
    - The destination must not already exist.

    Args:
        source (Path): The source of the symbolic link.
        destination (Path): The destination of the symbolic link.
        softlink (bool, optional): Whether to create a soft link.
            Defaults to True. If False, a hard link is created.

    Returns:
        None
    """
    if not source.exists():
        logger.error(f"Source path does not exist: {source}")
        raise FileNotFoundError

    if destination.exists():
        logger.error(f"Destination path already exists: {destination}")
        raise FileExistsError

    if softlink:
        logger.debug(f"Creating soft link from {source} to {destination}")
        destination.symlink_to(source)
    else:
        logger.debug(f"Creating hard link from {source} to {destination}")
        destination.hardlink_to(source)


# from:
# qqc/qqc/fmriprep.py
def remove_DataSetTrailingPadding_from_json_files(
    rawdata_dir: Path, subject_id: str, session_id: str
) -> None:
    """
    Remove DataSetTrailingPadding from the existing json files

    Args:
        rawdata_dir (Path): The rawdata directory.
        subject_id (str): The subject ID.
        session_id (str): The session ID.

    Returns:
        None
    """
    session_path = rawdata_dir / subject_id / session_id
    json_files = list(Path(session_path).glob("*/*json"))
    for json_file in json_files:
        with open(json_file, "r", encoding="utf-8") as fp:
            data = json.load(fp)
        if "global" in data.keys():
            # anat
            if "DataSetTrailingPadding" in data["global"]["slices"].keys():
                data["global"]["slices"]["DataSetTrailingPadding"] = "removed"
                with open(json_file, "w", encoding="utf-8") as fp:
                    json.dump(data, fp, indent=1)

        if "time" in data.keys():
            # fmri
            if "DataSetTrailingPadding" in data["time"]["samples"].keys():
                data["time"]["samples"]["DataSetTrailingPadding"] = "removed"
                with open(json_file, "w", encoding="utf-8") as fp:
                    json.dump(data, fp, indent=1)


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
        description="Run fmriprep on a single subject/session pair"
    )
    parser.add_argument("subject_id", type=str, help="The subject ID. e.g. sub-XXXXXXX")
    parser.add_argument("session_id", type=str, help="The session ID. e.g. ses-XXXXXXX")
    parser.add_argument(
        "-c",
        "--config-file",
        type=str,
        help="The configuration file to use to queue jobs",
    )

    args = parser.parse_args()

    # subject_id = "sub-YA52868"
    # session_id = "ses-202404051"
    subject_id: str = args.subject_id
    session_id: str = args.session_id

    config_file: Path = Path(args.config_file)

    logger.info(f"Running fmriprep for {subject_id} {session_id}")
    output_dir = OUT_ROOT / subject_id / session_id

    # Check if the output directory already exists
    if output_dir.exists():
        logger.info(f"Output directory {output_dir} already exists. Skipping.")
        sys.exit(2)

    TEMP_DIR = TEMP_ROOT / "fmriprep"
    RANDOM_STR = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=6))
    TEMP_DIR = TEMP_DIR / RANDOM_STR

    SINGULARITY_EXEC = shutil.which("singularity")
    if SINGULARITY_EXEC is None:
        # Try looking at the default location
        SINGULARITY_EXEC = SINGULARITY_FALLBACK_PATH
        if Path(SINGULARITY_EXEC).exists() is False:
            logger.error("Singularity not found")
            sys.exit(404)

    logger.info(f"Singularity executable: {SINGULARITY_EXEC}")
    logger.info(f"Temporary directory: {TEMP_DIR}")

    work_dir = TEMP_DIR / "work"
    work_dir.mkdir(exist_ok=True, parents=True)

    rawdata_dir = MRI_ROOT / "rawdata"
    # fmriprep_outdir_root = MRI_ROOT / "derivatives" / "fmriprep"
    fmriprep_outdir_root = TEMP_DIR / "output"
    fs_outdir_root = MRI_ROOT / "derivatives" / "freesurfer7_t2w"
    fs_session_dir = fs_outdir_root / subject_id / session_id

    # Link the freesurfer output to a temporary directory to prevent fmriprep from
    # using information from the other sessions
    fs_session_temp = TEMP_DIR / "fsdir" / session_id / subject_id
    fs_session_temp.mkdir(exist_ok=True, parents=True)

    logger.info(f"Copy {fs_session_dir} to {fs_session_temp}")
    shutil.copytree(fs_session_dir, fs_session_temp, dirs_exist_ok=True)
    fs_subject_dir = fs_session_temp.parent

    fmriprep_outdir_root.mkdir(exist_ok=True, parents=True)

    remove_DataSetTrailingPadding_from_json_files(rawdata_dir, subject_id, session_id)

    session_id_digits = session_id.split("-")[1]

    command = f"""{SINGULARITY_EXEC} run -e \
-B {rawdata_dir}:/data:ro \
-B {work_dir}:/work \
-B {fmriprep_outdir_root}:/out \
-B {fs_subject_dir}:/fsdir \
-B /data/pnl/soft/pnlpipe3/freesurfer/license.txt:/opt/freesurfer/license.txt \
{SINGULARITY_IMGAGE_PATH} \
/data /out participant \
-w /work --participant-label {subject_id} \
--nprocs 8 --mem 32G --omp-nthreads 8 \
--fs-subjects-dir /fsdir \
--fs-no-resume \
--output-layout bids \
--verbose \
--skip_bids_validation \
--notrack \
--cifti-output
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

    def on_fail():
        """
        Function to call when the command fails.

        Can be used to clean up temporary directories.
        """
        logger.error(f"Failed to run fmriprep for {subject_id} {session_id}")

        # remove temporary directory
        logger.info(f"Removing temporary directory {TEMP_DIR}")
        shutil.rmtree(TEMP_DIR)

        logger.info("Exiting with status 1")
        sys.exit(1)

    execute_commands(command, stdout=stdout, stderr=stderr, on_fail=on_fail)

    stdout.close()
    stderr.close()
    # copy logs to the output directory
    shutil.copy(stdout_path, fmriprep_outdir_root)
    shutil.copy(stderr_path, fmriprep_outdir_root)

    logger.info(f"Finished fmriprep for {subject_id} {session_id}")

    logger.info(f"Moving assets from {fmriprep_outdir_root} to {output_dir}")
    output_dir.parent.mkdir(exist_ok=True, parents=True)

    shutil.copytree(fmriprep_outdir_root, output_dir)

    logger.info(f"Removing temporary directory {TEMP_DIR}")
    shutil.rmtree(TEMP_DIR)

    logger.info(f"{subject_id} {session_id} finished")

    # Queue XCP-D job
    xcp_d_job = Job(
        job_payload=f"{REPO_ROOT}/workflows/run_xcp_d.py {subject_id} {session_id}",
        job_status="PENDING",
        job_tags=["xcp-d"],
        job_last_updated=datetime.now(),
        job_submission_time=datetime.now(),
    )
    orchestrator.submit_job(job=xcp_d_job, config_file=args.config_file)
    sys.exit(0)
