"""Database management for the interstitial database.

Matthew Alger
Geoscience Australia
2021
"""

import logging
import os
from pathlib import Path

import geopandas as gpd
from geoalchemy2 import load_spatialite
from pandas.api.types import is_float_dtype, is_integer_dtype, is_string_dtype
from sqlalchemy import MetaData, Table, create_engine, insert, inspect, select
from sqlalchemy.event import listen
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.future import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql.expression import ClauseElement
from tqdm import tqdm

from deafrica_conflux.db_tables import Waterbody, WaterbodyBase, WaterbodyObservation
from deafrica_conflux.id_field import guess_id_field
from deafrica_conflux.io import PARQUET_EXTENSIONS, check_file_exists, read_table_from_parquet

_log = logging.getLogger(__name__)


def get_engine_sqlite_file_db(db_file_path) -> Engine:
    """
    Get a SQLite on-disk database engine.
    """
    # identifying name of the SQLAlchemy dialect,
    dialect = "sqlite"
    # name of the DBAPI to be used to connect to the database
    driver = "pysqlite"
    # dialect+driver://username:password@host:port/database
    # sqlite://<nohostname>/<path>
    # where <path> is relative:
    database_url = f"{dialect}+{driver}:///{db_file_path}"
    engine = create_engine(database_url, echo=True, future=True)
    # listener is responsible for loading the SpatiaLite extension,
    # which is a necessary operation for using SpatiaLite through SQL.
    listen(engine, "connect", load_spatialite)
    return engine


def get_engine_sqlite_in_memory_db() -> Engine:
    """Get a SQLite in-memory database engine."""
    # identifying name of the SQLAlchemy dialect,
    dialect = "sqlite"
    # name of the DBAPI to be used to connect to the database
    driver = "pysqlite"
    # dialect+driver://username:password@host:port/database
    database_url = f"{dialect}+{driver}:///:memory:"
    engine = create_engine(
        database_url,
        connect_args={"check_same_thread": False},
        echo=True,
        future=True,
    )
    # listener is responsible for loading the SpatiaLite extension,
    # which is a necessary operation for using SpatiaLite through SQL.
    listen(engine, "connect", load_spatialite)
    return engine


def get_engine_waterbodies() -> Engine:
    """Get the Waterbodies database engine.

    References environment variables WATERBODIES_DB_USER,
    WATERBODIES_DB_PASS, WATERBODIES_DB_HOST,
    WATERBODIES_DB_PORT, and WATERBODIES_DB_NAME.
    HOST and PORT default to localhost and 5432 respectively.
    """
    username = os.environ.get("WATERBODIES_DB_USER")
    password = os.environ.get("WATERBODIES_DB_PASS")
    host = os.environ.get("WATERBODIES_DB_HOST", "localhost")
    port = os.environ.get("WATERBODIES_DB_PORT", 5432)
    database_name = os.environ.get("WATERBODIES_DB_NAME")

    # identifying name of the SQLAlchemy dialect
    dialect = "postgresql"
    # name of the DBAPI to be used to connect to the database
    driver = "psycopg2"
    # dialect+driver://username:password@host:port/database
    database_url = f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{database_name}"
    return create_engine(database_url, future=True)


def get_engine_waterbodies_dev_sandbox() -> Engine:
    """Get the Waterbodies database engine.

    References environment variables WATERBODIES_DB_USER,
    WATERBODIES_DB_PASS, WATERBODIES_DB_HOST,
    WATERBODIES_DB_PORT, and WATERBODIES_DB_NAME.
    HOST and PORT default to localhost and 5432 respectively.
    """
    username = os.environ.get("DB_USERNAME")
    password = os.environ.get("DB_PASSWORD")
    host = os.environ.get("DB_HOSTNAME", "localhost")
    port = os.environ.get("DB_PORT", 5432)
    database_name = os.environ.get("DB_DATABASE")

    # identifying name of the SQLAlchemy dialect
    dialect = "postgresql"
    # name of the DBAPI to be used to connect to the database
    driver = "psycopg2"
    # dialect+driver://username:password@host:port/database
    database_url = f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{database_name}"
    return create_engine(database_url, future=True)


def get_schemas(engine: Engine) -> list[str]:
    """List the schemas present in the database.

    Parameters
    ----------
    engine : Engine
        Waterbodies database engine
    """
    # Create an inspector
    inspector = inspect(engine)

    # List schemas in the database
    schemas = inspector.get_schema_names()

    if schemas:
        _log.info(f"Schemas in the database: {', '.join(schemas)}")
    else:
        _log.info("No schemas found in database")

    return schemas


def list_public_tables(engine: Engine) -> list[str]:
    """List the tables in present in the public schema of the database.

    Parameters
    ----------
    engine : Engine
        Waterbodies database engine
    """
    # Create an inspector
    inspector = inspect(engine)

    # Get a list of table names in the public schema.
    table_names = inspector.get_table_names(schema="public")

    # Print the list of schemas
    if table_names:
        _log.info(f"Tables in the public schema: {', '.join(table_names)}")
    else:
        _log.info("No tables found in the public schema")

    return table_names


def get_public_table(engine: Engine, table_name: str) -> Table:
    # Create a metadata object
    metadata = MetaData(schema="public")

    # Reflect the table from the database
    _log.info(f"Finding {table_name} table...")
    try:
        table = Table(table_name, metadata, autoload_with=engine)
    except NoSuchTableError:
        _log.error(f"{table_name} table does not exist in database!")
        return None
    else:
        _log.info(f"{table_name} table found.")
        return table


def drop_public_table(engine: Engine, table_name: str):
    table = get_public_table(engine, table_name)

    if table is not None:
        # Reflect the table from the database
        _log.info(f"Dropping {table_name} table...")
        try:
            # Drop the table
            table.drop(engine)
        except Exception as error:  # using a catch all
            _log.exception(error)
            _log.error(f"{table_name} table not dropped")
        else:
            _log.info(f"{table_name} table dropped.")
    else:
        _log.info("Skipping drop operation...")


def create_waterbody_table(engine: Engine, exist_ok=True):
    # Creating individual tables
    # without affecting any other tables defined in the metadata
    Waterbody.__table__.create(engine, checkfirst=exist_ok)
    table_name = Waterbody.__tablename__
    table = get_public_table(engine, table_name)
    return table


def drop_waterbody_table(engine: Engine):
    table_name = Waterbody.__tablename__
    drop_public_table(engine, table_name)


def create_waterbody_obs_table(engine: Engine, exist_ok=True):
    # Creating individual tables
    # without affecting any other tables defined in the metadata
    WaterbodyObservation.__table__.create(engine, checkfirst=exist_ok)
    table_name = WaterbodyObservation.__tablename__
    table = get_public_table(engine, table_name)
    return table


def drop_waterbody_obs_table(engine: Engine):
    table_name = WaterbodyObservation.__tablename__
    drop_public_table(engine, table_name)


def create_all_waterbody_tables(engine: Engine):
    """Create all waterbody tables."""
    return WaterbodyBase.metadata.create_all(engine)


def drop_all_waterbody_tables(engine: Engine):
    """Drop all waterbody tables."""
    # Drop all tables
    return WaterbodyBase.metadata.drop_all(bind=engine)


def get_or_create(session: Session, model, **kwargs):
    """Query a row or create it if it doesn't exist."""
    instance = session.scalars(select(model).filter_by(**kwargs)).one_or_none()
    if instance:
        return instance, False
    else:
        attributes = {k: v for k, v in kwargs.items() if not isinstance(v, ClauseElement)}
        instance = model(**attributes)
        try:
            session.add(instance)
        # The actual exception depends on the specific database
        # so we catch all exceptions. This is similar to the
        # official documentation:
        # https://docs.sqlalchemy.org/en/latest/orm/session_transaction.html
        except Exception:
            session.rollback()
            instance = session.scalars(select(model).filter_by(**kwargs)).one()
            return instance, False
        else:
            session.commit()
            return instance, True


def add_waterbody_polygons_to_db(
    engine: Engine,
    waterbodies_polygons_fp: str | Path,
    drop: bool = True,
):
    """
    Add the waterbody polygon into the waterbodies table.

    Parameters
    ----------
    engine : Engine
    drop : bool, optional
        If True drop the waterbodies table first and create a new table., by default True
    waterbodies_polygons_fp : str | Path | None, optional
                Path to the shapefile/geojson/geoparquet file containing the waterbodies polygons, by default None, by default None
    """
    if not check_file_exists(waterbodies_polygons_fp):
        _log.error(f"File {waterbodies_polygons_fp} does not exist!")
        raise FileNotFoundError(f"File {waterbodies_polygons_fp} does not exist!)")
    else:
        _, file_extension = os.path.splitext(waterbodies_polygons_fp)
        if file_extension in PARQUET_EXTENSIONS:
            try:
                waterbodies = gpd.read_parquet(waterbodies_polygons_fp).to_crs("EPSG:4326")
            except Exception as error:
                _log.error(f"Could not load file {waterbodies_polygons_fp}")
                _log.error(error)
                raise error
        else:
            try:
                waterbodies = gpd.read_file(waterbodies_polygons_fp).to_crs("EPSG:4326")
            except Exception as error:
                _log.error(f"Could not load file {waterbodies_polygons_fp}")
                _log.error(error)
                raise error

        # Check the id columns are  unique.
        numeric_id = "WB_ID"
        string_id = "UID"
        numeric_id = guess_id_field(input_gdf=waterbodies, use_id=numeric_id)
        assert is_integer_dtype(waterbodies[numeric_id]) or is_float_dtype(waterbodies[numeric_id])
        string_id = guess_id_field(input_gdf=waterbodies, use_id=string_id)
        assert is_string_dtype(waterbodies[string_id])

        _log.info(f"Found {len(waterbodies)} polygons in {waterbodies_polygons_fp}")

        # Create a sesssion
        Session = sessionmaker(bind=engine)

        if drop:
            # Drop the waterbodies table
            drop_waterbody_table(engine)

            # Create the table
            table = create_waterbody_table(engine)

            srid = waterbodies.crs.to_epsg()

            objects_list = []
            for row in waterbodies.itertuples():
                object_ = dict(
                    area_m2=row.area_m2,
                    uid=row.UID,
                    wb_id=row.WB_ID,
                    length_m=row.length_m,
                    perim_m=row.perim_m,
                    timeseries=row.timeseries,
                    geometry=f"SRID={srid};{row.geometry.wkt}",
                )
                objects_list.append(object_)

        else:
            # Ensure table exists.
            table = create_waterbody_table(engine)

            # Get the polygon uids in the database table
            with Session() as session:
                uids = session.scalars(select(table.c["uid"])).all()
                _log.info(f"Found {len(uids)} polygon UIDs in the {table.name} table")
            srid = waterbodies.crs.to_epsg()

            objects_list = []
            for row in waterbodies.itertuples():
                if row.UID not in uids:
                    object_ = dict(
                        area_m2=row.area_m2,
                        uid=row.UID,
                        wb_id=row.WB_ID,
                        length_m=row.length_m,
                        perim_m=row.perim_m,
                        timeseries=row.timeseries,
                        geometry=f"SRID={srid};{row.geometry.wkt}",
                    )
                    objects_list.append(object_)
                else:
                    continue

        with Session() as session:
            session.begin()
            try:
                _log.info(f"Adding {len(objects_list)} polygons to {table.name} table")
                session.execute(insert(table), objects_list)
            except Exception:
                session.rollback()
                raise
            else:
                session.commit()
            session.close()


def add_waterbody_observations_to_db_from_pq_file(
    paths: list[str],
    verbose: bool = False,
    engine: Engine = None,
    uids: {str} = None,
    drop: bool = False,
):
    """Write drill output parquet files into the waterbodies interstitial DB.

    Arguments
    ---------
    paths : [str]
        List of paths to Parquet files to stack.

    verbose : bool

    engine: sqlalchemy.engine.Engine
        Database engine. Default postgres, which is
        connected to if engine=None.

    drop : bool
        Whether to drop the database. Default False.
    """
    if verbose:
        paths = tqdm(paths)

    # connect to the db
    if not engine:
        engine = get_engine_waterbodies()

    # drop tables if requested
    if drop:
        # Drop the waterbodies table
        drop_waterbody_obs_table(engine)

        # Create the table
        table = create_waterbody_obs_table(engine)

        for path in paths:
            # read the table in...
            df = read_table_from_parquet(path)
            # parse the date...
            task_id_string = df.attrs["task_id_string"]
            # df is ids x bands
            # for each ID...
            objects_list = []
            for row in df.itertuples():
                obs = dict(
                    obs_id=f"{task_id_string}_{row.Index}",
                    uid=row.Index,
                    px_total=row.px_total,
                    px_wet=row.px_wet,
                    area_wet_m2=row.area_wet_m2,
                    px_dry=row.px_dry,
                    area_dry_m2=row.area_dry_m2,
                    px_invalid=row.px_invalid,
                    area_invalid_m2=row.area_invalid_m2,
                    date=row.date,
                )
                objects_list.append(obs)
            # basically just hoping that these don't exist already
            # TODO: Insert or update
            Session = sessionmaker(bind=engine)
            with Session() as session:
                session.begin()
                try:
                    pass
                    session.execute(insert(table), objects_list)
                except Exception:
                    session.rollback()
                    raise
                else:
                    session.commit()
                session.close()
    else:
        # TODO: How to insert if row already exists in table
        raise NotImplementedError
