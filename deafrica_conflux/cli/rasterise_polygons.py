import collections
import json
import logging
import os

import click
import fsspec
import geopandas as gpd
import numpy as np
import rasterio.features
from datacube.utils.cog import to_cog
from datacube.utils.dask import save_blob_to_file, save_blob_to_s3
from datacube.utils.geometry import Geometry
from odc.dscache.tools.tiling import parse_gridspec_with_name
from odc.geo.geobox import GeoBox
from odc.geo.xr import wrap_xr
from pandas.api.types import is_float_dtype, is_integer_dtype, is_string_dtype

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.filter_polygons import get_intersecting_polygons
from deafrica_conflux.id_field import guess_id_field
from deafrica_conflux.io import check_dir_exists, check_file_exists, check_if_s3_uri


@click.command(
    "rasterise-polygons",
    no_args_is_help=True,
    help="Rasterise a set of waterbodies polygons by tile.",
)
@click.option("-v", "--verbose", default=1, count=True)
@click.option("--grid-name", type=str, help="Grid name africa_{10|20|30|60}", default="africa_30")
@click.option(
    "--product",
    type=str,
    help="Datacube product to get tiles for from the grid.",
    default="wofs_ls",
)
@click.option(
    "--polygons-file-path",
    type=str,
    help="Path to the polygons to be rasterised.",
)
@click.option(
    "--numeric-id",
    type=str,
    default="WB_ID",
    help="Unique key id in polygons vector file which contains either integers or floats.",
)
@click.option(
    "--string-id",
    type=str,
    default="UID",
    help="Unique key id in polygons vector file which contains only string",
)
@click.option(
    "--output-directory", type=str, help="Directory to write the tiled polygons rasters to."
)
@click.option(
    "--overwrite/--no-overwrite",
    default=True,
    help="Overwrite existing polygons raster file.",
)
def rasterise_polygons(
    verbose,
    grid_name,
    product,
    polygons_file_path,
    numeric_id,
    string_id,
    output_directory,
    overwrite,
):
    # Set up logger.
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    # Support pathlib Paths.
    polygons_file_path = str(polygons_file_path)
    output_directory = str(output_directory)

    # Create the output directory if it does not exist.
    is_s3 = check_if_s3_uri(output_directory)
    if is_s3:
        fs = fsspec.filesystem("s3")
    else:
        fs = fsspec.filesystem("file")

    if not check_dir_exists(output_directory):
        fs.makedirs(output_directory, exist_ok=True)
        _log.info(f"Created directory {output_directory}")

    tiles_output_directory = os.path.join(output_directory, "product_tiles")
    if not check_dir_exists(tiles_output_directory):
        fs.makedirs(tiles_output_directory, exist_ok=True)
        _log.info(f"Created directory {tiles_output_directory}")

    rasters_output_directory = os.path.join(output_directory, "historical_extent_rasters")
    if not check_dir_exists(rasters_output_directory):
        fs.mkdirs(rasters_output_directory, exist_ok=True)
        _log.info(f"Created directory {output_directory}")

    # Get the GridSpec.
    grid, gridspec = parse_gridspec_with_name(grid_name)
    _log.info(f"Using the grid {grid} with {gridspec}")

    # From the GridSpec get the crs and resolution.
    crs = gridspec.crs
    resolution = abs(gridspec.resolution[0])

    # Read the product footprint.
    product_footprint = gpd.read_file(
        f"https://explorer.digitalearth.africa/api/footprint/{product}"
    ).to_crs(crs)
    # Get the product footprint geopolygon.
    product_footprint = Geometry(geom=product_footprint.geometry[0], crs=crs)

    # Get the tiles covering the product footprint.
    tiles = gridspec.tiles_from_geopolygon(geopolygon=product_footprint)
    tiles = list(tiles)

    # Get the individual tile geometries.
    tile_geometries = []
    tile_ids = []
    for tile in tiles:
        tile_idx, tile_idy = tile[0]
        tile_geometry = tile[1].extent.geom

        tile_geometries.append(tile_geometry)
        tile_ids.append(f"x{tile_idx:03d}_y{tile_idy:03d}")

    tiles_gdf = gpd.GeoDataFrame(data={"tile_ids": tile_ids, "geometry": tile_geometries}, crs=crs)
    _log.info(f"Tile count: {len(tiles_gdf)}")

    tiles_output_fp = os.path.join(tiles_output_directory, f"{product}_tiles.parquet")
    tiles_gdf.to_parquet(tiles_output_fp)
    _log.info(f"{product} tiles written to {tiles_output_fp}")

    # Load the polygons.
    try:
        polygons_gdf = gpd.read_parquet(polygons_file_path).to_crs(crs)
    except Exception:
        _log.info("Polygons vector file is not a parquet file")
        try:
            polygons_gdf = gpd.read_file(polygons_file_path).to_crs(crs)
        except Exception as error:
            _log.error(f"Could not load file {polygons_file_path}")
            _log.error(error)
            raise error

    _log.info(f"Polygon count {len(polygons_gdf)}")

    # Check the id columns are unique.
    numeric_id = guess_id_field(input_gdf=polygons_gdf, use_id=numeric_id)
    assert is_integer_dtype(polygons_gdf[numeric_id]) or is_float_dtype(polygons_gdf[numeric_id])

    string_id = guess_id_field(input_gdf=polygons_gdf, use_id=string_id)
    assert is_string_dtype(polygons_gdf[string_id])

    polygon_numericids_to_stringids = dict(zip(polygons_gdf[numeric_id], polygons_gdf[string_id]))
    polygon_numericids_to_stringids_fp = os.path.join(
        rasters_output_directory, "polygon_numericids_to_stringids.json"
    )
    with fs.open(polygon_numericids_to_stringids_fp, "w") as fp:
        json.dump(polygon_numericids_to_stringids, fp)
    _log.info(
        f"Polygon numeric IDs (WB_ID) to string IDs (UID) dictionary written to {polygon_numericids_to_stringids_fp}"
    )

    _log.info("Filtering out tiles that do not intersect with any polygon...")
    filtered_tiles_gdf = get_intersecting_polygons(
        region=polygons_gdf, polygons_gdf=tiles_gdf, use_id="tile_ids"
    )
    _log.info(f"Filtered out {len(tiles_gdf) - len(filtered_tiles_gdf)} tiles.")
    _log.info(f"Filtered tiles count: {len(filtered_tiles_gdf)}.")

    # Split each row in the tiles into a GeoDataFrame of its own.
    tiles = np.array_split(filtered_tiles_gdf, len(filtered_tiles_gdf))
    assert len(tiles) == len(filtered_tiles_gdf)

    polygons_stringids_to_tileids = collections.defaultdict(list)
    for i, tile in enumerate(tiles):
        tile_id = tile["tile_ids"].iloc[0]
        tile_geometry = tile.geometry.iloc[0]
        _log.info(f"Rasterizing polygons for tile {tile_id} ({i + 1}/{len(tiles)})")

        tile_raster_fp = os.path.join(rasters_output_directory, f"{tile_id}.tif")

        if not overwrite:
            _log.info(f"Checking existence of {tile_raster_fp}")
            exists = check_file_exists(tile_raster_fp)

        if overwrite or not exists:
            # Get the geobox for the region.
            tile_geobox = GeoBox.from_geopolygon(
                geopolygon=Geometry(geom=tile_geometry, crs=crs),
                resolution=resolution,
                crs=crs,
            )

            # Get the polygons that intersect with the tile.
            tile_polygons = get_intersecting_polygons(
                region=tile, polygons_gdf=polygons_gdf, use_id=numeric_id
            )
            for poly_id in tile_polygons[string_id].to_list():
                polygons_stringids_to_tileids[poly_id].append(tile_id)

            # Rasterise shapes into a numpy array
            shapes = zip(tile_polygons.geometry, tile_polygons[numeric_id])
            tile_raster_np = rasterio.features.rasterize(
                shapes=shapes, out_shape=tile_geobox.shape, transform=tile_geobox.transform
            )

            # Convert numpy array to a full xarray.DataArray
            tile_raster_ds = wrap_xr(im=tile_raster_np, gbox=tile_geobox)

            # Write the raster to disk.
            cog_bytes = to_cog(tile_raster_ds)
            if check_if_s3_uri(tile_raster_fp):
                save_blob_to_s3(data=cog_bytes, url=tile_raster_fp).compute()
            else:
                save_blob_to_file(data=cog_bytes, url=tile_raster_fp).compute()
            _log.info(f"Exported raster data to {tile_raster_fp}")
        else:
            _log.info(f"{tile_raster_fp} already exists, skipping...")

    polygons_stringids_to_tileids_fp = os.path.join(
        rasters_output_directory, "polygons_stringids_to_tileids.json"
    )
    with fs.open(polygons_stringids_to_tileids_fp, "w") as fp:
        json.dump(polygons_stringids_to_tileids, fp)
    _log.info(
        f"Polygon string IDs (UID) to tile ids dictionary written to {polygons_stringids_to_tileids_fp}"
    )
