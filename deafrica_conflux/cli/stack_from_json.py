import json
import logging

import click
import fsspec

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.io import check_dir_exists, check_file_exists, check_if_s3_uri
from deafrica_conflux.queues import batch_messages
from deafrica_conflux.stack import stack_polygon_timeseries_to_csv


@click.command("stack-from-json", no_args_is_help=True)
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
    "--polygon-ids-mapping-file",
    type=str,
    help="JSON file mapping numerical polygon ids (WB_ID) to string polygon ids (UID).",
)
def stack_from_json(verbose, drill_output_directory, output_directory, polygon_ids_mapping_file):
    """
    \b
    Stack outputs of deafrica-conflux into csv formats
    using polygon ids from a JSON file.
    """
    # Set up logger.
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    # Support pathlib Paths
    output_directory = str(output_directory)
    drill_output_directory = str(drill_output_directory)
    polygon_ids_mapping_file = str(polygon_ids_mapping_file)

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

    if not check_file_exists(polygon_ids_mapping_file):
        _log.error(f"File {polygon_ids_mapping_file} does not exist!")
        raise FileNotFoundError(f"File {polygon_ids_mapping_file} does not exist!)")

    # Get the polygon ids.
    if check_if_s3_uri(polygon_ids_mapping_file):
        fs = fsspec.filesystem("s3")
    else:
        fs = fsspec.filesystem("file")

    with fs.open(polygon_ids_mapping_file) as f:
        polygon_ids_mapping = json.load(f)

    polygon_ids = list(polygon_ids_mapping.values())

    # Batch the polygon ids into batches of 10.
    batched_polygon_ids = batch_messages(messages=polygon_ids, n=100)

    for batch in batched_polygon_ids:
        stack_polygon_timeseries_to_csv(
            polygon_ids=batch,
            drill_output_directory=drill_output_directory,
            output_directory=output_directory,
        )
