"""Text formatting functions"""
import os
from datetime import datetime
import re


def make_parquet_file_name(drill_name: str, task_id_string: str) -> str:
    """
    Make filename for Parquet.

    Arguments
    ---------
    drill_name : str
        Name of the drill.

    task_id_string : str
        Task ID of the task.

    Returns
    -------
    str
        Parquet filename.
    """
    # Parse the task id.
    period, x, y = task_id_string.split("/")

    parquet_file_name = f"{drill_name}_x{x}_y{y}_{period}.pq"

    return parquet_file_name


def task_id_string_from_parquet_file_name(file_name: str):
    base_name = os.path.basename(file_name)
    period = re.search(r"(\d{4}-\d{2}-\d{2})", base_name).group()
    y = re.search(r"(x\d{3})", base_name).group()
    x = re.search(r"(y\d{3})", base_name).group()
    task_id_string = f"{period}/{x}/{y}"
    return task_id_string


def parse_tile_ids(file_path: str) -> str:
    """
    Parse tile ids from a file path.

    Parameters
    ----------
    file_path : str
        File path to get the tile id from.

    Returns
    -------
    str
        Tile id
    """
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    x_id = int(file_name.split("_")[0].lstrip("x"))
    y_id = int(file_name.split("_")[1].lstrip("y"))
    tile_id = (x_id, y_id)
    return tile_id


def task_id_to_string(task_id_tuple: tuple) -> str:
    """
    Transform a task id tuple to a string.

    Parameters
    ----------
    task_id_tuple : tuple
        Task id as a tuple.

    Returns
    -------
    str
        Task id as string.
    """
    period, x, y = task_id_tuple

    task_id_string = f"{period}/{x:03d}/{y:03d}"

    return task_id_string


def task_id_to_tuple(task_id_string: str) -> tuple:
    """
    Transform a task id string to a tuple.

    Parameters
    ----------
    task_id_string : str
        Task id as string.

    Returns
    -------
    tuple
        Task id as a tuple.
    """
    sep = "/" if "/" in task_id_string else ","

    period, x, y = task_id_string.split(sep)

    if period.startswith("x"):
        period, x, y = y, period, x

    x = int(x)
    y = int(y)

    task_id_tuple = (period, x, y)

    return task_id_tuple


def extract_date_from_filename(filename) -> datetime:
    """
    Extract a date in the in the format YYYY-MM-DD from
    a file name.

    Parameters
    ----------
    filename : str
        File name to exctract date from.

    Returns
    -------
    datetime
        Extracted date as a datetime object
    """
    # Define a regex pattern to extract dates in the format YYYY-MM-DD
    date_pattern = r"(\d{4}-\d{2}-\d{2})"

    # Search for the date pattern in the filename
    # Note: re.search() finds the first match only of a pattern
    match = re.search(date_pattern, filename)

    if match:
        # Extract the matched date
        return datetime.strptime(match.group(), "%Y-%m-%d")
    else:
        return None
