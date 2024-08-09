"""
Job Model
"""

from datetime import datetime
from typing import Any, Dict, Optional, List
from pathlib import Path

from pydantic import BaseModel

from scheduler.helpers import db


class Job(BaseModel):
    """
    Represents a job in the cluster.

    Attributes:
        job_payload (str): The command to execute.
        job_env_variables (Optional[Dict[str, str]]): Environment variables.
        job_tags (Optional[str]): Tags associated with the job.
        job_status (str): The status of the job.
        job_last_updated (datetime): The last time the job was updated.
        job_submission_time (datetime): The time the job was submitted.
        job_assigned_node (Optional[str]): The node assigned to the job.
        job_result_metadata (Optional[Dict[str, Any]]): The result metadata.
        job_metadata (Optional[Dict[str, Any]]): Additional metadata.
    """

    job_id: Optional[int] = None
    job_payload: str
    job_env_variables: Optional[Dict[str, str]] = None
    job_tags: Optional[List[str]] = None
    job_status: str
    job_last_updated: datetime
    job_submission_time: datetime
    job_assigned_node: Optional[str] = None
    job_assigned_node_processor: Optional[int] = None
    job_result_metadata: Optional[Dict[str, Any]] = None
    job_metadata: Optional[Dict[str, Any]] = None

    def __str__(self):
        return f"{self.job_payload} ({self.job_status})"

    def __repr__(self):
        return str(self)

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'jobs' table.
        """
        sql_query = """
        CREATE TABLE jobs (
            job_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            job_payload TEXT NOT NULL,
            job_tags TEXT[],
            job_env_variables JSONB,
            job_status TEXT NOT NULL,
            job_last_updated TIMESTAMP NOT NULL,
            job_submission_time TIMESTAMP NOT NULL,
            job_assigned_node TEXT NOT NULL REFERENCES nodes(node_hostname),
            job_assigned_node_processor INT,
            job_result_metadata JSONB,
            job_metadata JSONB
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'jobs' table.
        """
        sql_query = "DROP TABLE IF EXISTS jobs;"

        return sql_query

    def insert_query(self) -> str:
        """
        Return the SQL query to insert the job into the 'jobs' table.
        """

        if self.job_tags is None:
            job_tags = "NULL"
        else:
            job_tags = "{" + ",".join(self.job_tags) + "}"

        if self.job_assigned_node is None:
            job_assigned_node = "UNASSIGNED"
        else:
            job_assigned_node = self.job_assigned_node

        if self.job_result_metadata is None:
            job_result_metadata = "NULL"
        else:
            job_result_metadata = db.sanitize_json(self.job_result_metadata)

        if self.job_metadata is None:
            job_metadata = "NULL"
        else:
            job_metadata = db.sanitize_json(self.job_metadata)

        if self.job_env_variables is None:
            job_env_variables = "NULL"
        else:
            job_env_variables = db.sanitize_json(self.job_env_variables)

        if self.job_assigned_node_processor is None:
            job_assigned_node_processor = "NULL"
        else:
            job_assigned_node_processor = self.job_assigned_node_processor

        sql_query = f"""
        INSERT INTO jobs (
            job_payload, job_env_variables, job_status,
            job_tags, job_last_updated, job_submission_time,
            job_assigned_node, job_assigned_node_processor,
            job_result_metadata, job_metadata
        ) VALUES (
            '{self.job_payload}', '{job_env_variables}', '{self.job_status}',
            '{job_tags}', '{self.job_last_updated}', '{self.job_submission_time}',
            '{job_assigned_node}', {job_assigned_node_processor},
            '{job_result_metadata}', '{job_metadata}'
        );
        """

        sql_query = db.handle_null(sql_query)

        return sql_query

    @staticmethod
    def get_pending_jobs(
        config_file: Path, tags: List[str], limit: int = 10
    ) -> List["Job"]:
        """
        Get the pending jobs from the database.

        Args:
            config_file (str): The path to the configuration file.
            limit (int): The number of jobs to return.

        Returns:
            List[Job]: A list of pending jobs.
        """

        if len(tags) == 0:
            select_query = f"""
            SELECT *
            FROM public.jobs
            WHERE job_status = 'PENDING'
            AND job_tags IS NULL
            ORDER BY job_submission_time
            LIMIT {limit};
        """
        else:
            select_query = f"""
            WITH compatible_tags AS (
            SELECT ARRAY[{','.join([f"'{tag}'" for tag in tags])}] AS tags
            )
            SELECT *
            FROM public.jobs, compatible_tags
            WHERE job_status = 'PENDING'
            AND (
                job_tags IS NULL
                OR (job_tags && tags AND job_tags <@ tags)
            )
            ORDER BY job_submission_time
            LIMIT {limit};
        """

        temp_df = db.execute_sql(config_file=config_file, query=select_query)

        jobs: List[Job] = []
        for _, row in temp_df.iterrows():
            job = Job(
                job_id=row["job_id"],
                job_payload=row["job_payload"],
                job_env_variables=row["job_env_variables"],
                job_tags=row["job_tags"],
                job_status=row["job_status"],
                job_last_updated=row["job_last_updated"],
                job_submission_time=row["job_submission_time"],
                job_assigned_node=row["job_assigned_node"],
                job_assigned_node_processor=row["job_assigned_node_processor"],
                job_result_metadata=row["job_result_metadata"],
                job_metadata=row["job_metadata"],
            )
            jobs.append(job)

        return jobs
