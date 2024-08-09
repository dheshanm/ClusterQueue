"""
Models Database tables, and contains functions to initialize the database.
"""

from pathlib import Path
from typing import List, Union

from scheduler.helpers import db
from scheduler.models.job import Job
from scheduler.models.node import Node
from scheduler.models.processor import Processor


def flatten_list(coll: list) -> list:
    """
    Flattens a list of lists into a single list.

    Args:
        coll (list): List of lists.

    Returns:
        list: Flattened list.
    """
    flat_list = []
    for i in coll:
        if isinstance(i, list):
            flat_list += flatten_list(i)
        else:
            flat_list.append(i)
    return flat_list


def init_db(config_file: Path):
    """
    Initializes the database.

    WARNING: This will drop all tables and recreate them.
    DO NOT RUN THIS IN PRODUCTION.

    Args:
        config_file (Path): Path to the config file.
    """
    drop_queries_l: List[Union[str, List[str]]] = [
        Job.drop_table_query(),
        Processor.drop_table_query(),
        Node.drop_table_query(),
    ]

    create_queries_l: List[Union[str, List[str]]] = [
        Node.init_table_query(),
        Processor.init_table_query(),
        Job.init_table_query(),
    ]

    drop_queries = flatten_list(drop_queries_l)
    create_queries = flatten_list(create_queries_l)

    sql_queries: List[str] = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)
