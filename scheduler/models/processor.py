"""
Processor model
"""

from datetime import datetime

from pydantic import BaseModel


class Processor(BaseModel):
    """
    Represents a processor in the node.

    Attributes:
        processor_id (int): The ID of the processor.
        processor_parent_node (str): The hostname of the parent node.
        processor_status (str): The status of the processor.
        processor_last_seen (datetime): The last time the processor was seen.
    """

    processor_id: int
    processor_parent_node: str
    processor_status: str
    processor_last_seen: datetime

    def __str__(self):
        return f"{self.processor_id} ({self.processor_status})"

    def __repr__(self):
        return str(self)

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'processors' table.
        """
        sql_query = """
        CREATE TABLE processors (
            processor_id INT NOT NULL,
            processor_parent_node TEXT NOT NULL REFERENCES nodes(node_hostname),
            processor_status TEXT NOT NULL,
            processor_last_seen TIMESTAMP NOT NULL,
            PRIMARY KEY (processor_id, processor_parent_node)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'processors' table.
        """
        sql_query = "DROP TABLE IF EXISTS processors;"

        return sql_query

    def insert_query(self) -> str:
        """
        Return the SQL query to insert the processor into the 'processors' table.
        """

        query = f"""
        INSERT INTO processors (
            processor_id, processor_parent_node,
            processor_status, processor_last_seen)
        VALUES (
            {self.processor_id}, '{self.processor_parent_node}',
            '{self.processor_status}', '{self.processor_last_seen}'
        ) ON CONFLICT (processor_id, processor_parent_node)
        DO UPDATE SET
            processor_status = '{self.processor_status}',
            processor_last_seen = '{self.processor_last_seen}';
        """

        return query
