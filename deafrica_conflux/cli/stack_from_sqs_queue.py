import json
import logging

import boto3
import click
import fsspec

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.io import check_dir_exists, check_file_exists, check_if_s3_uri
from deafrica_conflux.queues import (
    delete_batch_with_retry,
    get_queue_url,
    move_to_dead_letter_queue,
    receive_messages,
)
from deafrica_conflux.stack import stack_polygon_timeseries_to_csv


@click.command("stack-from-sqs-queue", no_args_is_help=True)
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
    "--ids-sqs-queue",
    type=str,
    help="SQS Queue to read the polygons ids from.",
)
@click.option(
    "--polygon-stringids-to-tileids-file",
    type=str,
    help="JSON file mapping string polygons ids (UID) to the idsof the grids/tiles the polygon intersects with.",
)
def stack_from_sqs_queue(
    verbose,
    drill_output_directory,
    output_directory,
    ids_sqs_queue,
    polygon_stringids_to_tileids_file,
):
    """
    \b
    Stack outputs of deafrica-conflux into csv formats
    using polygon ids from a SQS queue.
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

    # Create the service client.
    sqs_client = boto3.client("sqs")

    ids_sqs_queue_url = get_queue_url(queue_name=ids_sqs_queue, sqs_client=sqs_client)
    # Get the dead-letter queue.
    dead_letter_queue_name = f"{ids_sqs_queue}-deadletter"
    dead_letter_queue_url = get_queue_url(queue_name=dead_letter_queue_name, sqs_client=sqs_client)

    max_retries = 10
    retries = 0
    while retries <= max_retries:
        # Retrieve a single messages from the queue.
        retrieved_message = receive_messages(
            queue_url=ids_sqs_queue_url,
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

            # Get the polygon ids.
            polygon_uid = message["Body"]
            _log.info(f"Read polygon id {polygon_uid} from queue {ids_sqs_queue_url}")

            entry_to_delete = [
                {"Id": message["MessageId"], "ReceiptHandle": message["ReceiptHandle"]}
            ]

            try:
                stack_polygon_timeseries_to_csv(  # noqa F841
                    polygon_uid=polygon_uid,
                    polygon_stringids_to_tileids=polygon_stringids_to_tileids,
                    drill_output_directory=drill_output_directory,
                    output_directory=output_directory,
                )
                _log.info(f"Successfully stacked timeseries for polygon: {polygon_uid}")

            except Exception as error:
                _log.error(
                    f"Encountered error while stacking timeseries for polygon: {polygon_uid}"
                )
                _log.error(error)
                move_to_dead_letter_queue(
                    dead_letter_queue_url=dead_letter_queue_url,
                    message_body=polygon_uid,
                    sqs_client=sqs_client,
                )
                _log.info(
                    f"Moved polygon id {polygon_uid} to dead letter queue {dead_letter_queue_url}"
                )

            _log.info(f"Deleting polygon id {polygon_uid} from {ids_sqs_queue_url}...")
            (
                successfully_deleted,
                failed_to_delete,
            ) = delete_batch_with_retry(
                queue_url=ids_sqs_queue_url,
                entries=entry_to_delete,
                max_retries=max_retries,
                sqs_client=sqs_client,
            )
            if failed_to_delete:
                _log.error(f"Failed to delete {polygon_uid} from queue {ids_sqs_queue_url}")
                # raise RuntimeError(f"Failed to delete task: {task}")
            else:
                _log.info(f"Deleted polygon id {polygon_uid} from {ids_sqs_queue_url}")
