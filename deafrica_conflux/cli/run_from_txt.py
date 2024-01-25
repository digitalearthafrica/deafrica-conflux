import json
import logging
import os
from importlib import import_module

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


@click.command(
    "run-from-txt",
    no_args_is_help=True,
    help="Run deafrica-conflux on tasks from a text file.",
)
@click.option("-v", "--verbose", default=1, count=True)
@click.option("--cachedb-file-path", type=str, help="File path to the cache file database.")
@click.option(
    "--tasks-text-file",
    type=str,
    help="Text file to get tasks ids from.",
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
    help="JSON file mapping numerical polygons ids (WB_ID) to string polygons ids (UID).",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    help="Rerun tasks that have already been processed.",
)
def run_from_txt(
    verbose,
    cachedb_file_path,
    tasks_text_file,
    plugin_name,
    polygons_rasters_directory,
    output_directory,
    polygon_ids_mapping_file,
    overwrite,
):
    # Set up logger.
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    # Support pathlib Paths
    cachedb_file_path = str(cachedb_file_path)
    tasks_text_file = str(tasks_text_file)
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

    if not check_file_exists(tasks_text_file):
        _log.error(f"Could not find the text file {tasks_text_file}!")
        raise FileNotFoundError(f"Could not find text file {tasks_text_file}!")

    # Read task ids from the S3 URI or File URI.
    if check_if_s3_uri(tasks_text_file):
        fs = fsspec.filesystem("s3")
    else:
        fs = fsspec.filesystem("file")

    with fs.open(tasks_text_file, "r") as file:
        tasks = [line.strip() for line in file]
    _log.info(f"Read {len(tasks)} tasks from file.")
    _log.debug(f"Read {tasks} from file.")

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

    # Connect to the datacube
    dc = datacube.Datacube(app="deafrica-conflux-drill")

    # Read the cache file
    cache = dscache.open_ro(cachedb_file_path)

    failed_tasks = []
    for i, task in enumerate(tasks):
        _log.info(f"Processing task {task} ({i + 1}/{len(tasks)})")

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
                failed_tasks = [].append(task)
            except TypeError as typeerr:
                _log.exception(f"Found task {task} has TypeError: {str(typeerr)}")
                failed_tasks.append(task)
            except RasterioIOError as ioerror:
                _log.exception(f"Found task {task} has RasterioIOError: {str(ioerror)}")
                failed_tasks.append(task)
            except ValueError as valueerror:
                _log.exception(f"Found task {task} has ValueError: {str(valueerror)}")
                failed_tasks.append(task)
            else:
                _log.info(f"Task {task} successful")
        else:
            _log.info(f"Drill outputs for {task} already exist, skipping")

        if failed_tasks:
            # Write the failed dataset ids to a text file.
            parent_folder, file_name = os.path.split(tasks_text_file)
            file, file_extension = os.path.splitext(file_name)
            failed_tasks_text_file = os.path.join(
                parent_folder, file + "_failed_tasks" + file_extension
            )

            with fs.open(failed_tasks_text_file, "a") as file:
                for task in failed_tasks:
                    file.write(f"{task}\n")

            _log.info(f"Failed tasks {failed_tasks} written to: {failed_tasks_text_file}.")
