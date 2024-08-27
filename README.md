# ClusterQueue

This is a simple queue system that uses a cluster of servers to process jobs. It is designed to be used in a distributed environment where multiple servers can be used to process jobs. 

## Pre-requisites

The following are required to run the ClusterQueue:
- Shared file system

## Tech Stack

- Python
- PostgreSQL (Database)

## How to Setup ClusterQueue

1. Create a database in PostgreSQL for use with the ClusterQueue
2. Populate a `config.ini` file based on the included `sample.config.ini`
   - Ensure the database connection details from 1 are correct
3. Initialize the database by running:
    ```bash
    python scheduler/scripts/init_db.py
    ```

## How to Submit Jobs

For a complete example, see `scheduler/scripts/submit_test_job.py`.

```python
from scheduler import orchestrator
from scheduler.models import Job

config_file = utils.get_config_file_path()

env_vars = {
    "TEST_VAR_1": "Hello",
    "TEST_VAR_2": "World",
}

new_job = Job(
    job_payload="echo $TEST_VAR_1 $TEST_VAR_2",
    job_env_variables=env_vars,
    job_status="PENDING",
    job_last_updated=datetime.now(),
    job_submission_time=datetime.now(),
    job_tags=["gpu"],
)

orchestrator.submit_job(job=new_job, config_file=config_file)
```

## How to start a worker

```bash
python scheduler/runners/compute_node_multi.py --num_parallel_jobs 4 --tags gpu
```

The above command will start a worker that can process 4 jobs in parallel. The worker will process jobs with no tags or with the tag `gpu`.