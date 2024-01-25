import json
import logging
from importlib import import_module

import boto3
import click
import datacube
import fsspec
from odc import dscache
from odc.aws import s3_download
from rasterio.errors import RasterioIOError

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.drill import drill
from deafrica_conflux.io import (
    check_dir_exists,
    check_file_exists,
    check_if_s3_uri,
    table_exists,
    write_table_to_parquet,
)
from deafrica_conflux.plugins.utils import run_plugin, validate_plugin
from deafrica_conflux.queues import (
    delete_batch_with_retry,
    get_queue_url,
    move_to_dead_letter_queue,
    receive_messages,
)


@click.command(
    "run-from-sqs-queue",
    no_args_is_help=True,
    help="Run deafrica-conflux on dataset ids from an SQS queue.",
)
@click.option("-v", "--verbose", default=1, count=True)
@click.option("--cachedb-file-path", type=str, help="File path to the cache file database.")
@click.option(
    "--tasks-sqs-queue",
    type=str,
    help="SQS Queue to read task IDs from to run deafrica-conflux on.",
)
@click.option(
    "--plugin-name",
    type=str,
    help="Name of the plugin. Plugin file must be in the \
        deafrica_conflux/plugins/ directory.",
)
@click.option(
    "--polygons-rasters-directory",
    type=str,
    help="Path to the directory containing the polygons raster files.",
)
@click.option("--output-directory", type=str, help="Directory to write the drill outputs to.")
@click.option(
    "--polygon-ids-mapping-file",
    type=str,
    help="JSON file mapping numerical polygon ids (WB_ID) to string polygon ids (UID).",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    help="Rerun tasks that have already been processed.",
)
def run_from_sqs_queue(
    verbose,
    cachedb_file_path,
    tasks_sqs_queue,
    plugin_name,
    polygons_rasters_directory,
    output_directory,
    polygon_ids_mapping_file,
    overwrite,
):
    """
    Run deafrica-conflux on dataset ids from an SQS queue.
    """
    # Set up logger.
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    # Support pathlib Paths
    cachedb_file_path = str(cachedb_file_path)
    polygons_rasters_directory = str(polygons_rasters_directory)
    output_directory = str(output_directory)
    if polygon_ids_mapping_file:
        polygon_ids_mapping_file = str(polygon_ids_mapping_file)

    # Read the plugin as a Python module.
    module = import_module(f"deafrica_conflux.plugins.{plugin_name}")
    plugin_file = module.__file__
    plugin = run_plugin(plugin_file)
    _log.info(f"Using plugin {plugin_file}")
    validate_plugin(plugin)

    # Get the drill name from the plugin
    drill_name = plugin.product_name

    if polygon_ids_mapping_file:
        if not check_file_exists(polygon_ids_mapping_file):
            _log.error(f"File {polygon_ids_mapping_file} does not exist!")
            raise FileNotFoundError(f"File {polygon_ids_mapping_file} does not exist!)")

    if not check_dir_exists(polygons_rasters_directory):
        _log.error(f"Directory {polygons_rasters_directory} does not exist!")
        raise FileNotFoundError(f"Directory {polygons_rasters_directory} does not exist!)")

    # Create the output directory if it does not exist.
    if not check_dir_exists(output_directory):
        if check_if_s3_uri(output_directory):
            fsspec.filesystem("s3").makedirs(output_directory, exist_ok=True)
        else:
            fsspec.filesystem("file").makedirs(output_directory, exist_ok=True)
        _log.info(f"Created directory {output_directory}")

    if not check_file_exists(cachedb_file_path):
        _log.error(f"Could not find the database file {cachedb_file_path}!")
        raise FileNotFoundError(f"{cachedb_file_path} does not exist!")
    else:
        if check_if_s3_uri(cachedb_file_path):
            cachedb_file_path = s3_download(cachedb_file_path)
            if not check_file_exists(cachedb_file_path):
                _log.error(f"{cachedb_file_path} did not download!")
                raise FileNotFoundError(
                    f"{cachedb_file_path} does not exist! File did not download."
                )

    # Read the polygons ids mapping file.
    if polygon_ids_mapping_file:
        if check_if_s3_uri(polygon_ids_mapping_file):
            fs = fsspec.filesystem("s3")
        else:
            fs = fsspec.filesystem("file")

        with fs.open(polygon_ids_mapping_file) as f:
            polygon_ids_mapping = json.load(f)
    else:
        polygon_ids_mapping = {}

    # Create the service client.
    sqs_client = boto3.client("sqs")

    tasks_sqs_queue_url = get_queue_url(queue_name=tasks_sqs_queue, sqs_client=sqs_client)
    # Get the dead-letter queue.
    dead_letter_queue_name = f"{tasks_sqs_queue}-deadletter"
    dead_letter_queue_url = get_queue_url(queue_name=dead_letter_queue_name, sqs_client=sqs_client)

    # Connect to the datacube
    dc = datacube.Datacube(app="deafrica-conflux-drill")

    # Read the cache file
    cache = dscache.open_ro(cachedb_file_path)

    max_retries = 10
    retries = 0
    while retries <= max_retries:
        # Retrieve a single message from the dataset_ids_queue.
        retrieved_message = receive_messages(
            queue_url=tasks_sqs_queue_url,
            max_retries=max_retries,
            visibility_timeout=900,
            max_no_messages=1,
            sqs_client=sqs_client,
        )
        if retrieved_message is None:
            retries += 1
        else:
            retries = 0  # reset the count

            message = retrieved_message[0]

            # Process the task.
            task = message["Body"]
            _log.info(f"Read task id {task} from queue {tasks_sqs_queue_url}")

            entry_to_delete = [
                {"Id": message["MessageId"], "ReceiptHandle": message["ReceiptHandle"]}
            ]

            # Produce the parquet file.
            success_flag = True

            if not overwrite:
                _log.info(f"Checking existence of {task}")
                exists = table_exists(
                    drill_name=drill_name, task_id_string=task, output_directory=output_directory
                )
            if overwrite or not exists:
                try:
                    # Perform the polygon drill.
                    table = drill(
                        plugin=plugin,
                        task_id_string=task,
                        cache=cache,
                        polygons_rasters_directory=polygons_rasters_directory,
                        polygon_ids_mapping=polygon_ids_mapping,
                        dc=dc,
                    )
                    pq_file_name = write_table_to_parquet(  # noqa F841
                        drill_name=drill_name,
                        task_id_string=task,
                        table=table,
                        output_directory=output_directory,
                    )
                except KeyError as keyerr:
                    _log.exception(f"Found task {task} has KeyError: {str(keyerr)}")
                    _log.error(f"Moving {task} to deadletter queue {dead_letter_queue_url}")
                    move_to_dead_letter_queue(
                        dead_letter_queue_url=dead_letter_queue_url,
                        message_body=task,
                        sqs_client=sqs_client,
                    )
                    success_flag = False
                except TypeError as typeerr:
                    _log.exception(f"Found task {task} has TypeError: {str(typeerr)}")
                    _log.error(f"Moving {task} to deadletter queue {dead_letter_queue_url}")
                    move_to_dead_letter_queue(
                        dead_letter_queue_url=dead_letter_queue_url,
                        message_body=task,
                        sqs_client=sqs_client,
                    )
                    success_flag = False
                except RasterioIOError as ioerror:
                    _log.exception(f"Found task {task} has RasterioIOError: {str(ioerror)}")
                    _log.error(f"Moving {task} to deadletter queue {dead_letter_queue_url}")
                    move_to_dead_letter_queue(
                        dead_letter_queue_url=dead_letter_queue_url,
                        message_body=task,
                        sqs_client=sqs_client,
                    )
                    success_flag = False
                except ValueError as valueerror:
                    _log.exception(f"Found task {task} has ValueError: {str(valueerror)}")
                    _log.error(f"Moving {task} to deadletter queue {dead_letter_queue_url}")
                    move_to_dead_letter_queue(
                        dead_letter_queue_url=dead_letter_queue_url,
                        message_body=task,
                        sqs_client=sqs_client,
                    )
                    success_flag = False
            else:
                _log.info(f"Task {task} already exists, skipping")

            if success_flag:
                _log.info(f"Successful, deleting {task} from {tasks_sqs_queue_url}")
            else:
                _log.info(
                    f"Not successful, moved {task} to dead letter queue {dead_letter_queue_url} and deleting from {tasks_sqs_queue_url}"
                )

            (
                successfully_deleted,
                failed_to_delete,
            ) = delete_batch_with_retry(
                queue_url=tasks_sqs_queue_url,
                entries=entry_to_delete,
                max_retries=max_retries,
                sqs_client=sqs_client,
            )
            if failed_to_delete:
                _log.error(f"Failed to delete {task} from queue {tasks_sqs_queue_url}")
                # raise RuntimeError(f"Failed to delete task: {task}")
            else:
                _log.info(f"Deleted task {task} from queue")
