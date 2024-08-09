#!/usr/bin/env python
"""
Run fmriprep on the a single subject/session pair.

Reuses FS output directory from the previous run.
"""

import json
import logging
import shutil
import subprocess
import sys
import random
from pathlib import Path
from typing import IO, Callable
import argparse

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
LOGS_DIR = "/data/predict1/home/dm1447/fmriprep/logs"
MRI_ROOT = Path("/data/predict1/data_from_nda/MRI_ROOT")
OUT_ROOT = Path("/data/predict1/home/dm1447/fmriprep/output")

# local
TEMP_ROOT = Path("/tmp")


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
    """Remove DataSetTrailingPadding from the existing json files"""
    session_path = rawdata_dir / subject_id / session_id
    json_files = list(Path(session_path).glob("*/*json"))
    for json_file in json_files:
        with open(json_file, "r") as fp:
            data = json.load(fp)
        if "global" in data.keys():
            # anat
            if "DataSetTrailingPadding" in data["global"]["slices"].keys():
                data["global"]["slices"]["DataSetTrailingPadding"] = "removed"
                with open(json_file, "w") as fp:
                    json.dump(data, fp, indent=1)

        if "time" in data.keys():
            # fmri
            if "DataSetTrailingPadding" in data["time"]["samples"].keys():
                data["time"]["samples"]["DataSetTrailingPadding"] = "removed"
                with open(json_file, "w") as fp:
                    json.dump(data, fp, indent=1)


def execute_commands(
    command: str,
    stdout: IO,
    stderr: IO,
    on_fail: Callable = lambda: sys.exit(1),
) -> subprocess.CompletedProcess:
    """
    Executes a command and returns the result.

    Args:
        command (str): The command to execute.
        on_fail (Callable, optional): The function to call if the command fails.
            Defaults to lambda: sys.exit(1).

    Returns:
        subprocess.CompletedProcess: The result of the command execution.

    """
    logger.debug("Executing command:")
    logger.debug(command)

    result = subprocess.run(
        command,
        stdout=stdout,
        stderr=stderr,
        shell=True,
        check=False,
    )

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

    args = parser.parse_args()

    # subject_id = "sub-YA52868"
    # session_id = "ses-202404051"
    subject_id: str = args.subject_id
    session_id: str = args.session_id

    logger.info(f"Running fmriprep for {subject_id} {session_id}")

    SINGULARITY_EXEC = shutil.which("singularity")
    if SINGULARITY_EXEC is None:
        # Try looking at the default location
        SINGULARITY_EXEC = "/apps/released/gcc-toolchain/gcc-4.x/singularity/singularity-3.7.0/bin/singularity"
        if Path(SINGULARITY_EXEC).exists() is False:
            logger.error("Singularity not found")
            sys.exit(404)

    logger.info(f"Singularity executable: {SINGULARITY_EXEC}")

    work_dir = TEMP_ROOT / "fmriprep" / subject_id / session_id
    work_dir.mkdir(exist_ok=True, parents=True)

    rawdata_dir = MRI_ROOT / "rawdata"
    # fmriprep_outdir_root = MRI_ROOT / "derivatives" / "fmriprep"
    fmriprep_outdir_root = OUT_ROOT / subject_id / session_id
    fs_outdir_root = MRI_ROOT / "derivatives" / "freesurfer7_t2w"
    fs_session_dir = fs_outdir_root / subject_id / session_id

    # Link the freesurfer output to a temporary directory to prevent fmriprep from
    # using information from the other sessions
    fs_session_temp = TEMP_ROOT / "freesurfer_temp" / subject_id / session_id

    # isolate the freesurfer output per run
    random_str = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=6))
    fs_session_temp = fs_session_temp / random_str

    fs_session_temp.parent.mkdir(exist_ok=True, parents=True)

    logger.info(f"Linking {fs_session_dir} to {fs_session_temp}")
    create_link(
        source=fs_session_dir,
        destination=fs_session_temp,
        softlink=True,
    )

    fmriprep_outdir_root.mkdir(exist_ok=True, parents=True)

    remove_DataSetTrailingPadding_from_json_files(rawdata_dir, subject_id, session_id)

    session_id_digits = session_id.split("-")[1]
    filter_dict = {
        "t1w": {
            "datatype": "anat",
            "session": session_id_digits,
            "suffix": "T1w",
        },
        "t2w": {
            "datatype": "anat",
            "session": session_id_digits,
            "suffix": "T2w",
        },
        "bold": {
            "datatype": "func",
            "session": session_id_digits,
            "suffix": "bold",
        },
    }

    with open(fmriprep_outdir_root / "filter.json", "w", encoding="utf-8") as fp:
        json.dump(filter_dict, fp, indent=4)

    command = f"""{SINGULARITY_EXEC} run -e \
-B {rawdata_dir}:/data:ro \
-B {work_dir}:/work \
-B {fmriprep_outdir_root}:/out \
-B {fs_session_temp}:/fsdir \
-B /data/pnl/soft/pnlpipe3/freesurfer/license.txt:/opt/freesurfer/license.txt \
-B {fmriprep_outdir_root}/filter.json:/filter.json \
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
--cifti-output \
--level minimal \
--bids-filter-file /filter.json
"""

    stdout = open(f"{LOGS_DIR}/{subject_id}_{session_id}.stdout", "w", encoding="utf-8")
    stderr = open(f"{LOGS_DIR}/{subject_id}_{session_id}.stderr", "w", encoding="utf-8")

    logger.debug(f"Logging stdout to {stdout}")

    execute_commands(command, stdout=stdout, stderr=stderr)

    stdout.close()
    stderr.close()

    # Remove the temporary freesurfer directory
    logger.info(f"Removing {fs_session_temp}")
    shutil.rmtree(fs_session_temp)

    logger.info(f"{subject_id} {session_id} finished")
    sys.exit(0)
