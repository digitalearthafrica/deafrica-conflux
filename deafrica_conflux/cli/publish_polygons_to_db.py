import click

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.db import add_waterbody_polygons_to_db, get_engine_waterbodies


@click.command(
    "publish-polygons-to-db",
    no_args_is_help=True,
)
@click.option("-v", "--verbose", default=1, count=True)
@click.option(
    "--polygons-file-path",
    type=str,
    help="Path to the shapefile/geojson/geoparquet file containing the waterbodies polygons",
)
@click.option("--drop/--no-drop", default=True, help="Replace duplicate polygons if applicable.")
def publish_polygons_to_db(verbose, polygons_file_path, drop):
    """Publish waterbodies polygons to database."""
    # Set up logger.
    logging_setup(verbose)

    engine = get_engine_waterbodies()
    add_waterbody_polygons_to_db(
        engine=engine,
        waterbodies_polygons_fp=polygons_file_path,
        drop_table=False,
        replace_duplicate_rows=drop,
    )
