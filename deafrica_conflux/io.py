"""Input/output for Conflux.

Matthew Alger
Geoscience Australia
2021
"""
import json
import logging
import os
import re
import urllib
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

import boto3
import fsspec
import pandas as pd
import pyarrow
import pyarrow.parquet
from odc.stats.model import DateTimeRange

from deafrica_conflux.text import extract_date_from_filename, make_parquet_file_name

_log = logging.getLogger(__name__)

# File extensions to recognise as Parquet files.
PARQUET_EXTENSIONS = {".pq", ".parquet"}

# File extensions to recognise as CSV files.
CSV_EXTENSIONS = {".csv", ".CSV"}

# File extensions to recognise as GeoTIFF files.
GEOTIFF_EXTENSIONS = {".tif", ".tiff", ".gtiff"}

# Metadata key for Parquet files.
PARQUET_META_KEY = b"conflux.metadata"


def table_exists(drill_name: str, task_id_string: str, output_directory: str) -> bool:
    """
    Check whether tables for  a specific task exist.

    Arguments
    ---------
    drill_name : str
        Name of the drill.

    task_id_string : str
        Task ID of the task.

    output_directory : str
        Path to output directory.

    Returns
    -------
    bool
    """
    # "Support" pathlib Paths.
    output_directory = str(output_directory)

    # Parse the task id.
    period, x, y = task_id_string.split("/")

    file_name = make_parquet_file_name(drill_name=drill_name, task_id_string=task_id_string)
    file_path = os.path.join(output_directory, f"x{x}", f"y{y}", file_name)

    if check_if_s3_uri(file_path):
        fs = fsspec.filesystem("s3")
    else:
        fs = fsspec.filesystem("file")

    if fs.exists(file_path):
        _log.info(f"{file_path} exists.")
    else:
        _log.info(f"{file_path} does not exist.")

    return fs.exists(file_path)


def write_table_to_parquet(
    drill_name: str,
    task_id_string: str,
    table: pd.DataFrame,
    output_directory: str | Path,
) -> list[str]:
    """
    Write a table to Parquet.

    Arguments
    ---------
    drill_name : str
        Name of the drill.

    task_id_string : str
        Task ID of the task.

    table : pd.DataFrame
        Dataframe with index polygons and columns bands.

    output_directory : str | Path
        Path to output directory.

    Returns
    -------
    list[str]
        Paths written to.
    """
    # "Support" pathlib Paths.
    output_directory = str(output_directory)

    # Add the date to the table.
    period, x, y = task_id_string.split("/")  # Parse the task id.
    table["date"] = pd.to_datetime(period)

    # Convert the table to pyarrow.
    table_pa = pyarrow.Table.from_pandas(table)

    # Dump new metadata to JSON.
    meta_json = json.dumps({"drill": drill_name, "task_id_string": task_id_string})

    # Dump existing (Pandas) metadata.
    # https://towardsdatascience.com/
    #   saving-metadata-with-dataframes-71f51f558d8e
    existing_meta = table_pa.schema.metadata
    combined_meta = {
        PARQUET_META_KEY: meta_json.encode(),
        **existing_meta,
    }

    # Replace the metadata.
    table_pa = table_pa.replace_schema_metadata(combined_meta)

    parquet_buffer = BytesIO()
    pyarrow.parquet.write_table(table_pa, parquet_buffer)

    # Write the table.
    is_s3 = check_if_s3_uri(output_directory)
    if is_s3:
        fs = fsspec.filesystem("s3")
    else:
        fs = fsspec.filesystem("file")

    # Check if the parent folder exists.
    parent_folder = os.path.join(output_directory, f"x{x}", f"y{y}")
    if not check_dir_exists(parent_folder):
        fs.makedirs(parent_folder, exist_ok=True)
        _log.info(f"Created directory: {parent_folder}")

    file_name = make_parquet_file_name(drill_name=drill_name, task_id_string=task_id_string)
    output_file_path = os.path.join(parent_folder, file_name)

    if is_s3:
        s3 = boto3.client("s3")
        # Parse the S3 URI
        parsed_uri = urlparse(output_file_path)
        # Extract the bucket name and object key
        bucket_name = parsed_uri.netloc
        object_key = parsed_uri.path.lstrip("/")
        parquet_buffer.seek(0)  # Reset the buffer position
        s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=parquet_buffer,
            ACL="bucket-owner-full-control",  # Set the ACL to bucket-owner-full-control
        )
    else:
        pyarrow.parquet.write_table(table=table_pa, where=output_file_path, compression="GZIP")

    _log.info(f"Table written to {output_file_path}")
    return output_file_path


def read_table_from_parquet(path: str | Path) -> pd.DataFrame:
    """
    Read a Parquet file with Conflux metadata.

    Arguments
    ---------
    path : str | Path
        Path to Parquet file.

    Returns
    -------
    pd.DataFrame
        DataFrame with attrs set.
    """
    # "Support" pathlib Paths.
    path = str(path)

    table = pyarrow.parquet.read_table(path)
    df = table.to_pandas()
    meta_json = table.schema.metadata[PARQUET_META_KEY]
    metadata = json.loads(meta_json)
    for key, val in metadata.items():
        df.attrs[key] = val
    return df


def check_if_s3_uri(file_path: str | Path) -> bool:
    """
    Checks if a file path is an S3 URI.

    Parameters
    ----------
    file_path : str | Path
        File path to check

    Returns
    -------
    bool
        True if the file path is an S3 URI.
    """
    # "Support" pathlib Paths.
    file_path = str(file_path)

    file_scheme = urllib.parse.urlparse(file_path).scheme

    valid_s3_schemes = ["s3"]

    if file_scheme in valid_s3_schemes:
        return True
    else:
        return False


def check_dir_exists(dir_path: str | Path):
    """
    Checks if a specified path is an existing directory.

    Parameters
    ----------
    dir_path : str | Path
        Path to check.

    Returns
    -------
    bool
        True if the path exists and is a directory.
        False if the path does not exists or if the path exists and it is not a directory.
    """
    # "Support" pathlib Paths.
    dir_path = str(dir_path)

    if check_if_s3_uri(dir_path):
        fs = fsspec.filesystem("s3")
    else:
        fs = fsspec.filesystem("file")

    if fs.exists(dir_path):
        if fs.isdir(dir_path):
            return True
        else:
            return False
    else:
        return False


def check_file_exists(file_path: str | Path) -> bool:
    """
    Checks if a specified path is an existing file.

    Parameters
    ----------
    file_path : str | Path
        Path to check.

    Returns
    -------
    bool
        True if the path exists and is a file.
        False if the path does not exists or if the path exists and it is not a file.
    """
    # "Support" pathlib Paths.
    file_path = str(file_path)

    if check_if_s3_uri(file_path):
        fs = fsspec.filesystem("s3")
    else:
        fs = fsspec.filesystem("file")

    if fs.exists(file_path):
        if fs.isfile(file_path):
            return True
        else:
            return False
    else:
        return False


def find_parquet_files(path: str | Path, pattern: str = ".*", verbose: bool = True) -> list[str]:
    """
    Find Parquet files matching a pattern.

    Arguments
    ---------
    path : str | Path
        Path (s3 or local) to search for Parquet files.

    pattern : str
        Regex to match file names against.

    verbose: bool
        Turn on/off logging.

    Returns
    -------
    [str]
        List of paths.
    """
    pattern = re.compile(pattern)

    # "Support" pathlib Paths.
    path = str(path)

    if check_if_s3_uri(path):
        # Find Parquet files on S3.
        file_system = fsspec.filesystem("s3")
    else:
        # Find Parquet files locally.
        file_system = fsspec.filesystem("file")

    pq_file_paths = []

    for root, dirs, files in file_system.walk(path):
        for file in files:
            _, file_extension = os.path.splitext(file)
            if file_extension not in PARQUET_EXTENSIONS:
                continue
            else:
                if not pattern.match(file):
                    continue
                else:
                    pq_file_paths.append(os.path.join(root, file))

    if check_if_s3_uri(path):
        pq_file_paths = [f"s3://{file}" for file in pq_file_paths]

    if verbose:
        _log.info(f"Found {len(pq_file_paths)} parquet files.")
    return pq_file_paths


def find_csv_files(path: str | Path, pattern: str = ".*", verbose: bool = True) -> list[str]:
    """
    Find CSV files matching a pattern.

    Arguments
    ---------
    path : str | Path
        Path (s3 or local) to search for CSV files.

    pattern : str
        Regex to match file names against.

    verbose: bool
        Turn on/off logging.
    Returns
    -------
    [str]
        List of paths.
    """
    pattern = re.compile(pattern)

    # "Support" pathlib Paths.
    path = str(path)

    if check_if_s3_uri(path):
        # Find CSV files on S3.
        file_system = fsspec.filesystem("s3")
    else:
        # Find CSV files locally.
        file_system = fsspec.filesystem("file")

    csv_file_paths = []

    for root, dirs, files in file_system.walk(path):
        for file in files:
            _, file_extension = os.path.splitext(file)
            if file_extension not in CSV_EXTENSIONS:
                continue
            else:
                if not pattern.match(file):
                    continue
                else:
                    csv_file_paths.append(os.path.join(root, file))

    if check_if_s3_uri(path):
        csv_file_paths = [f"s3://{file}" for file in csv_file_paths]

    if verbose:
        _log.info(f"Found {len(csv_file_paths)} csv files.")
    return csv_file_paths


def find_geotiff_files(path: str | Path, pattern: str = ".*", verbose: bool = True) -> list[str]:
    """
    Find GeoTIFF files matching a pattern.

    Arguments
    ---------
    path : str | Path
        Path (s3 or local) to search for GeoTIFF files.

    pattern : str
        Regex to match file names against.

    verbose: bool
        Turn on/off logging.

    Returns
    -------
    [str]
        List of paths.
    """
    pattern = re.compile(pattern)

    # "Support" pathlib Paths.
    path = str(path)

    if check_if_s3_uri(path):
        # Find GeoTIFF files on S3.
        file_system = fsspec.filesystem("s3")
    else:
        # Find GeoTIFF files locally.
        file_system = fsspec.filesystem("file")

    geotiff_file_paths = []

    for root, dirs, files in file_system.walk(path):
        for file in files:
            _, file_extension = os.path.splitext(file)
            file_extension = file_extension.lower()
            if file_extension not in GEOTIFF_EXTENSIONS:
                continue
            else:
                if not pattern.match(file):
                    continue
                else:
                    geotiff_file_paths.append(os.path.join(root, file))

    if check_if_s3_uri(path):
        geotiff_file_paths = [f"s3://{file}" for file in geotiff_file_paths]

    if verbose:
        _log.info(f"Found {len(geotiff_file_paths)} GeoTIFF files.")
    return geotiff_file_paths


def filter_files_by_date_range(file_paths: list[str], temporal_range: str) -> list[str]:
    """
    Filter file paths using a date range.

    Parameters
    ----------
    file_paths : list[str]
        List of files to filter.
    temporal_range : str
        Date range to filter by e.g. 2023-01--P1M filters file by
        the date range 2023-01-01 to 2023-01-31.

    Returns
    -------
    list[str]
        List of files within the defined temporal range.
    """
    temporal_range_ = DateTimeRange(temporal_range)
    start_date = temporal_range_.start
    end_date = temporal_range_.end

    # Filter files based on the dates extracted from filenames
    filtered_file_paths = []
    for path in file_paths:
        file_date = extract_date_from_filename(os.path.basename(path))

        if file_date:
            # Check if the file date is within the specified date range
            if start_date <= file_date <= end_date:
                filtered_file_paths.append(path)

    return filtered_file_paths


def add_missing_metadata(path: str):
    """
    Update existing drill output parquet files with the required metadata.

    Parameters
    ----------
    path : str
        Path to the parquet file

    """
    try:
        read_table_from_parquet(path)
    except KeyError:
        # Parse the drill name and task id from the file path.
        # This only works for files named using `make_parquet_file_name`
        base_name, _ = os.path.splitext(os.path.basename(path))
        drill_name, x, y, period = base_name.split("_")
        task_id_string = f"{period}/{x}/{y}"

        # Read the table from the path
        table = pd.read_parquet(path)

        # Add the date to the table.
        if "date" not in table.columns:
            table["date"] = pd.to_datetime(period)

        # Convert the table to pyarrow.
        table_pa = pyarrow.Table.from_pandas(table)

        # Dump new metadata to JSON.
        meta_json = json.dumps({"drill": drill_name, "task_id_string": task_id_string})

        # Dump existing (Pandas) metadata.
        # https://towardsdatascience.com/
        #   saving-metadata-with-dataframes-71f51f558d8e
        existing_meta = table_pa.schema.metadata
        combined_meta = {
            PARQUET_META_KEY: meta_json.encode(),
            **existing_meta,
        }

        # Replace the metadata.
        table_pa = table_pa.replace_schema_metadata(combined_meta)

        parquet_buffer = BytesIO()
        pyarrow.parquet.write_table(table_pa, parquet_buffer)

        # Write the table back.
        is_s3 = check_if_s3_uri(path)

        if is_s3:
            s3 = boto3.client("s3")
            # Parse the S3 URI
            parsed_uri = urlparse(path)
            # Extract the bucket name and object key
            bucket_name = parsed_uri.netloc
            object_key = parsed_uri.path.lstrip("/")
            parquet_buffer.seek(0)  # Reset the buffer position
            s3.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=parquet_buffer,
                ACL="bucket-owner-full-control",  # Set the ACL to bucket-owner-full-control
            )
        else:
            pyarrow.parquet.write_table(table=table_pa, where=path, compression="GZIP")

        _log.info(f"Metadata for {path} has been updated")
    else:
        _log.info(f"Skipping metadata update for {path}")
