"""Database management for the interstitial database.

Matthew Alger
Geoscience Australia
2021
"""

import logging
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from geoalchemy2 import load_spatialite
from pandas.api.types import is_float_dtype, is_integer_dtype, is_string_dtype
from sqlalchemy import MetaData, Table, create_engine, delete, insert, inspect, select
from sqlalchemy.event import listen
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.future import Engine
from sqlalchemy.orm import sessionmaker

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
    engine: sqlalchemy.engine.Engine
        Database engine.
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
    engine: sqlalchemy.engine.Engine
        Database engine.
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
    """Get a table in the public schema using the table name."""
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


def drop_public_table(engine: Engine, model):
    """Drop a table in the public schema"""
    table_name = model.__tablename__
    tables_in_db = list_public_tables(engine)

    if table_name not in tables_in_db:
        _log.info(f"{table_name} table does not exist./nSkipping drop operation")
    else:
        # Reflect the table from the database
        _log.info(f"Dropping {table_name} table...")
        table = get_public_table(engine, table_name)
        try:
            # Drop the table
            table.drop(engine)
        except Exception as error:  # using a catch all
            _log.exception(error)
            _log.error(f"{table_name} table not dropped")
        else:
            _log.info(f"{table_name} table dropped.")


def create_public_table(engine: Engine, model):
    """Create an individual table in the public schema
    without affecting any other tables defined in the metadata."""
    table_name = model.__tablename__
    tables_in_db = list_public_tables(engine)

    if table_name not in tables_in_db:
        _log.info(f"Creating the {table_name} table ...")
        model.__table__.create(engine, checkfirst=False)
        _log.info(f"{table_name} table created")
    else:
        _log.info(f"{table_name} table already exists./nSkipping table creation")

    table = get_public_table(engine, table_name)
    return table


def create_waterbody_table(engine: Engine):
    table = create_public_table(engine, Waterbody)
    return table


def drop_waterbody_table(engine: Engine):
    drop_public_table(engine, Waterbody)


def create_waterbody_obs_table(engine: Engine):
    table = create_public_table(engine, WaterbodyObservation)
    return table


def drop_waterbody_obs_table(engine: Engine):
    drop_public_table(engine, WaterbodyObservation)


def create_all_waterbody_tables(engine: Engine):
    """Create all waterbody tables."""
    return WaterbodyBase.metadata.create_all(engine)


def drop_all_waterbody_tables(engine: Engine):
    """Drop all waterbody tables."""
    # Drop all tables
    return WaterbodyBase.metadata.drop_all(bind=engine)


def add_waterbody_polygons_to_db(
    engine: Engine,
    waterbodies_polygons_fp: str | Path,
    drop_table: bool = True,
    replace_duplicate_rows=True,
):
    """
    Add the waterbody polygon into the waterbodies table.

    Parameters
    ----------
    engine : Engine
    drop_table : bool, optional
        If True drop the waterbodies table first and create a new table., by default True
    replace_duplicate_rows : bool, optional
        If True if the polygon uid already exists in the waterbodies table, it will be replaced,
        else it will be skipped.
    waterbodies_polygons_fp : str | Path | None, optional
                Path to the shapefile/geojson/geoparquet file containing the waterbodies polygons, by default None, by default None
    """
    # connect to the db
    if not engine:
        engine = get_engine_waterbodies()

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

        uids_to_delete = []
        insert_objects_list = []

        if drop_table:
            # Drop the waterbodies table
            drop_waterbody_table(engine)

            # Create the table
            table = create_waterbody_table(engine)

            srid = waterbodies.crs.to_epsg()

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
                insert_objects_list.append(object_)

        else:
            # Ensure table exists.
            table = create_waterbody_table(engine)

            # Get the polygon uids in the database table
            # Note: Getting them all in a list works fine for about 700,000 polygons.
            with Session() as session:
                uids = session.scalars(select(table.c["uid"])).all()
                _log.info(f"Found {len(uids)} polygon UIDs in the {table.name} table")
            srid = waterbodies.crs.to_epsg()

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
                    insert_objects_list.append(object_)
                else:
                    if replace_duplicate_rows:
                        uids_to_delete.append(row.UID)
                        object_ = dict(
                            area_m2=row.area_m2,
                            uid=row.UID,
                            wb_id=row.WB_ID,
                            length_m=row.length_m,
                            perim_m=row.perim_m,
                            timeseries=row.timeseries,
                            geometry=f"SRID={srid};{row.geometry.wkt}",
                        )
                        insert_objects_list.append(object_)
                    else:
                        continue

        if uids_to_delete:
            with Session() as session:
                session.begin()
                try:
                    _log.info(
                        f"Deleting {len(uids_to_delete)} polygons from the {table.name} table"
                    )
                    delete_stmt = delete(table).where(table.c.uid.in_(uids_to_delete))
                    session.execute(delete_stmt)
                except Exception:
                    session.rollback()
                    raise
                else:
                    session.commit()
                session.close()
        else:
            pass

        if insert_objects_list:
            with Session() as session:
                session.begin()
                try:
                    _log.info(f"Adding {len(insert_objects_list)} polygons to {table.name} table")
                    session.execute(insert(table), insert_objects_list)
                except Exception:
                    session.rollback()
                    raise
                else:
                    session.commit()
                session.close()
        else:
            _log.error("No polygons to add to the {table.name} table")
