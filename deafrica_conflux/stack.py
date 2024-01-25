"""Stack Parquet scene outputs into other formats.

Lots of this code is domain-specific and not intended to be fully general.

Matthew Alger
Geoscience Australia
2021
"""

import logging
import os
from pathlib import Path

import dask
import dask.dataframe as dd
import fsspec
import numpy as np
import pandas as pd

from deafrica_conflux.io import check_dir_exists, check_if_s3_uri, find_parquet_files

_log = logging.getLogger(__name__)


def update_timeseries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Update the data in the timeseries DataFrame.

    Arguments
    ---------
    df : pd.DataFrame
        The polygon base timeseries result.

    Returns
    -------
    pd.DataFrame
        The polygon base timeseries.
    """
    assert "date" in df.columns

    keep = np.unique(df["date"])

    # Group by day
    df = df.groupby(pd.Grouper(key="date", axis=0, freq="D")).sum()
    # Remove filler rows.
    df = df[df.index.isin(keep)]

    df["pc_wet"] = (df["px_wet"] / df["px_total"]) * 100.0
    df["pc_dry"] = (df["px_dry"] / df["px_total"]) * 100.0
    df["pc_invalid"] = (df["px_invalid"] / df["px_total"]) * 100.0

    # If the proportion of the waterbody missing is greater than 10%,
    # set the values for pc_wet and pc_dry to nan.
    df.loc[df["pc_invalid"] > 10.0, "pc_wet"] = np.nan
    df.loc[df["pc_invalid"] > 10.0, "pc_dry"] = np.nan

    df.sort_index(inplace=True)
    return df


def stack_polygon_timeseries_to_csv(
    polygon_ids: list[str], drill_output_directory: str | Path, output_directory: str | Path
) -> list[str]:
    """
    Stack the timeseries for a polygon from the drill output parquet files
    into a csv file. Best used in parallel processing.

    Parameters
    ----------
    polygon_ids : list[str]
        Unique ids for the polygons.
    drill_output_directory : str | Path
        Directory containing the parquet files i.e. outputs from the drill step.
    output_directory : str | Path
        Directory to write the csv files to.

    Returns
    -------
    list[str]
        File paths of the polygon(s) timeseries csv files.
    """
    # Support pathlib paths.
    drill_output_directory = str(drill_output_directory)
    output_directory = str(output_directory)

    # Get the file system to use to write.
    if check_if_s3_uri(output_directory):
        fs = fsspec.filesystem("s3")
    else:
        fs = fsspec.filesystem("file")

    _log.info(f"Stacking timeseries for polygons: {', '.join(polygon_ids)}")

    # Find all the drill output files.
    drill_output_files = find_parquet_files(path=drill_output_directory, pattern=".*")

    # Read in all the drill output parquet files using dask dataframes.
    df = dd.read_parquet(drill_output_files)

    # Compute the timeseries for each polygon id.
    to_compute = [df.loc[poly_id] for poly_id in polygon_ids]
    polygons_timeseries = dask.compute(*to_compute)

    # Write the timeseries to csv.
    output_file_paths = []
    for frame in polygons_timeseries:
        polygon_id = frame.index[0]
        polygon_timeseries = update_timeseries(frame)

        output_file_parent_directory = os.path.join(output_directory, f"{polygon_id[:4]}")
        output_file_path = os.path.join(output_file_parent_directory, f"{polygon_id}.csv")

        if not check_dir_exists(output_file_parent_directory):
            fs.mkdirs(output_file_parent_directory, exist_ok=True)
            _log.info(f"Created directory: {output_file_parent_directory}")

        with fs.open(output_file_path, "w") as f:
            polygon_timeseries.to_csv(f, index_label="date")

        _log.info(f"CSV file written to {output_file_path}")

        output_file_paths.append(output_file_path)
    return output_file_paths
