"""
Node model
"""

from datetime import datetime
from typing import List

from pydantic import BaseModel


class Node(BaseModel):
    """
    Represents a node in the cluster.

    Attributes:
        hostname (str): The hostname of the node.
        status (str): The status of the node.
        tags (List[str]): The tags associated with the node.
        last_seen (datetime): The last time the node was seen.
    """

    hostname: str
    status: str
    tags: List[str]
    num_parallel_jobs: int = 1
    last_seen: datetime

    def __str__(self):
        return f"{self.hostname} ({self.status})"

    def __repr__(self):
        return str(self)

    @staticmethod
    def init_table_query() -> List[str]:
        """
        Return the SQL query to create the 'nodes' table.
        """
        create_query = """
        CREATE TABLE nodes (
            node_hostname TEXT PRIMARY KEY,
            node_status TEXT NOT NULL,
            node_tags TEXT[],
            node_num_parallel_jobs INTEGER,
            node_last_seen TIMESTAMP NOT NULL
        );
        """

        prepopulate_query = f"""
        INSERT INTO nodes (node_hostname, node_status, node_last_seen, node_tags, node_num_parallel_jobs)
        VALUES ('UNASSIGNED', 'UNASSIGNED', '{datetime.now()}', '{{virtual}}', 1)
        """

        queries = [create_query, prepopulate_query]

        return queries

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'nodes' table.
        """
        sql_query = "DROP TABLE IF EXISTS nodes;"

        return sql_query

    def insert_query(self) -> str:
        """
        Return the SQL query to insert the node into the 'nodes' table.
        """

        node_tags_str = "{" + ",".join(self.tags) + "}"

        sql_query = f"""
        INSERT INTO nodes (
            node_hostname, node_status, node_last_seen,
            node_tags, node_num_parallel_jobs
        ) VALUES (
            '{self.hostname}', '{self.status}', '{self.last_seen}',
            '{node_tags_str}', {self.num_parallel_jobs}
        ) ON CONFLICT (node_hostname) DO UPDATE
        SET node_status = '{self.status}', node_last_seen = '{self.last_seen}',
            node_tags = '{node_tags_str}' , node_num_parallel_jobs = {self.num_parallel_jobs};
        """

        return sql_query

    @staticmethod
    def update_last_seen_query(hostname: str) -> str:
        """
        Return the SQL query to update the last seen time of the node.
        """
        query = f"""
        UPDATE nodes
        SET node_last_seen = '{datetime.now()}'
        WHERE node_hostname = '{hostname}'
        """

        return query
