"""Text formatting functions"""
import os


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

    parquet_file_name = f"{drill_name}_x{x}y{y}_{period}.pq"

    return parquet_file_name


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
