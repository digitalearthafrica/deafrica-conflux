import json
import logging

import click
import fsspec

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.io import check_dir_exists, check_file_exists, check_if_s3_uri
from deafrica_conflux.stack import stack_polygon_timeseries_to_csv


@click.command("stack-from-uid", no_args_is_help=True)
@click.option("-v", "--verbose", default=1, count=True)
@click.option(
    "--drill-output-directory",
    type=str,
    # Don't mandate existence since this might be s3://.
    help="Path to the directory containing the parquet files output during polygon drill.",
)
@click.option(
    "--output-directory",
    type=str,
    help="Output directory for waterbodies-style stack",
)
@click.option(
    "--polygon-uids",
    type=str,
    help="Polygon ids (UID) to stack timeseries for. To specify multiple polygons seperate polygon ids using a comma ",
)
@click.option(
    "--polygon-stringids-to-tileids-file",
    type=str,
    help="JSON file mapping string polygons ids (UID) to the idsof the grids/tiles the polygon intersects with.",
)
def stack_from_uid(
    verbose,
    drill_output_directory,
    output_directory,
    polygon_uids,
    polygon_stringids_to_tileids_file,
):
    """
    \b
    Stack outputs of deafrica-conflux into csv formats
    using polygon ids.
    """
    # Set up logger.
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    # Support pathlib Paths
    output_directory = str(output_directory)
    drill_output_directory = str(drill_output_directory)
    polygon_stringids_to_tileids_file = str(polygon_stringids_to_tileids_file)

    if not check_dir_exists(drill_output_directory):
        _log.error(f"Directory {drill_output_directory} does not exist!")
        raise FileNotFoundError(f"Directory {drill_output_directory} does not exist!)")

    # Create the output directory if it does not exist.
    if not check_dir_exists(output_directory):
        if check_if_s3_uri(output_directory):
            fsspec.filesystem("s3").makedirs(output_directory, exist_ok=True)
        else:
            fsspec.filesystem("file").makedirs(output_directory, exist_ok=True)
        _log.info(f"Created directory {output_directory}")

    if not check_file_exists(polygon_stringids_to_tileids_file):
        _log.error(f"File {polygon_stringids_to_tileids_file} does not exist!")
        raise FileNotFoundError(f"File {polygon_stringids_to_tileids_file} does not exist!)")

    if check_if_s3_uri(polygon_stringids_to_tileids_file):
        fs = fsspec.filesystem("s3")
    else:
        fs = fsspec.filesystem("file")

    with fs.open(polygon_stringids_to_tileids_file) as f:
        polygon_stringids_to_tileids = json.load(f)

    polygon_uids = polygon_uids.split(",")
    polygon_uids = [i.strip() for i in polygon_uids]

    for polygon_uid in polygon_uids:
        stack_polygon_timeseries_to_csv(
            polygon_uid=polygon_uid,
            polygon_stringids_to_tileids=polygon_stringids_to_tileids,
            drill_output_directory=drill_output_directory,
            output_directory=output_directory,
        )
