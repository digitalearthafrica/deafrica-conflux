"""
Run a polygon drill step on a scene.

Matthew Alger, Vanessa Newey
Geoscience Australia
2021
"""

import logging
import warnings
from types import ModuleType

import datacube
import pandas as pd
import rioxarray
from odc.dscache import DatasetCache
from skimage.measure import regionprops

from deafrica_conflux.io import find_geotiff_files
from deafrica_conflux.text import task_id_to_tuple

_log = logging.getLogger(__name__)


def drill(
    plugin: ModuleType,
    task_id_string: str,
    cache: DatasetCache,
    polygons_rasters_directory: str,
    polygon_numericids_to_stringids: dict = {},
    dc: datacube.Datacube | None = None,
) -> pd.DataFrame:
    """
    Perform a polygon drill.

    Parameters
    ----------
    plugin : ModuleType
        A validated plugin to drill with.
    task_id_string : str
        Task id to run drill on in string format.
    cache : DatasetCache
        Dataset cache to read datasets from.
    polygons_rasters_directory : str
        Directory to search for polygons raster files.
    polygon_numericids_to_stringids: dict[str, str]
        Dictionary mapping numeric polygon ids (WB_ID) to string polygon ids (UID)
    dc : datacube.Datacube | None, optional
        Optional existing Datacube., by default None

    Returns
    -------
    Drill table : pd.DataFrame
        Index = polygon ID
        Columns = output bands
    """
    # TODO: Generalize to work with multiple products and
    # products with multiple measurements.

    # Check the plugin does not have multiple products/measurements to load.
    # Using multiple products/measurements is not is not implemented.
    measurements = plugin.measurements
    if len(measurements) > 1:
        raise NotImplementedError("Expected 1 measurement in plugin")
    else:
        measurement = measurements[0]

    # Get a datacube if we don't have one already.
    if dc is None:
        dc = datacube.Datacube(app="deafrica-conflux-drill")

    # Get the gridspec/grid name from the cache.
    grid = cache.get_info_dict("stats/config")["grid"]
    _log.debug(f"Found grid {grid}")

    # Parse the task id tuple from the task string.
    task_id_tuple = task_id_to_tuple(task_id_string)

    # Find the polygons raster tile to use for the task.
    _log.info("Finding polygon raster tile....")
    search_pattern = f".*x{task_id_tuple[1]:03d}_y{task_id_tuple[2]:03d}.*"
    found_raster_tile = find_geotiff_files(
        path=polygons_rasters_directory,
        pattern=search_pattern,
    )
    # The should only be one polygons raster file  for each tile.
    assert len(found_raster_tile) == 1

    # Load the enumerated waterbodies polygons raster tile.
    polygons_raster = rioxarray.open_rasterio(found_raster_tile[0]).squeeze("band", drop=True)
    _log.info(f"Loaded {found_raster_tile[0]}")
    assert polygons_raster.geobox.crs == plugin.output_crs
    assert polygons_raster.geobox.resolution == plugin.resolution

    # Get the datasets for the task.
    required_datasets = [ds for ds in cache.stream_grid_tile(task_id_tuple, grid)]

    # Create a datacube query object to use to load the found datasets.
    query = {}

    if hasattr(plugin, "resampling"):
        query["resampling"] = plugin.resampling
    else:
        query["resampling"] = "nearest"

    if hasattr(plugin, "dask_chunks"):
        query["dask_chunks"] = plugin.dask_chunks

    query["group_by"] = "solar_day"

    query["like"] = polygons_raster.geobox

    query["measurements"] = [measurement]

    query["datasets"] = required_datasets

    _log.info(f"Query object to use for loading data {query}")

    # Load the datasets.
    ds_ = dc.load(**query)

    # TODO: Take care of empty regions or other errors resulting in empty dataframes.
    # if len(filtered_polygons_gdf) == 0:

    # Transform the loaded data.
    # Force warnings to raise exceptions.
    # This means users have to explicitly ignore warnings.
    ds = ds_.isel(time=0)

    with warnings.catch_warnings():
        warnings.filterwarnings("error")
        ds_transformed = plugin.transform(ds)[measurement]

    # For each polygon, perform the summary.
    props = regionprops(
        label_image=polygons_raster.values,
        intensity_image=ds_transformed.values,
        extra_properties=(plugin.summarise,),
    )

    summary_df_list = []
    for region_prop in props:
        polygon_summary_df = region_prop.summarise
        # Get the string unique id of the polygon
        # if the polygons numeric ids to string ids dictionary is provided.
        if polygon_numericids_to_stringids:
            polygon_summary_df.index = [polygon_numericids_to_stringids[str(region_prop.label)]]
        else:
            polygon_summary_df.index = [region_prop.label]
        summary_df_list.append(polygon_summary_df)

    summary_df = pd.concat(summary_df_list, ignore_index=False)
    summary_df.sort_index(inplace=True)

    return summary_df
