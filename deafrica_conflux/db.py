"""Database management for the interstitial database.

Matthew Alger
Geoscience Australia
2021
"""

import json
import logging
import os
from pathlib import Path

import fsspec
import geopandas as gpd
from geoalchemy2 import load_spatialite
from pandas.api.types import is_float_dtype, is_integer_dtype, is_string_dtype
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    create_engine,
    insert,
    select,
)
from sqlalchemy.event import listen
from sqlalchemy.future import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.sql.expression import ClauseElement
from tqdm import tqdm

from deafrica_conflux.id_field import guess_id_field
from deafrica_conflux.io import PARQUET_EXTENSIONS, check_file_exists, read_table_from_parquet

_log = logging.getLogger(__name__)

WaterbodyBase = declarative_base()


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


# Define the table for the waterbodies polygons
class Waterbody(WaterbodyBase):
    """Table for the waterbody polygons"""

    __tablename__ = "waterbodies"
    uid = Column(String, primary_key=True)
    wb_id = Column(Integer)

    def __repr__(self):
        return f"<Waterbody uid={self.uid}, wb_id={self.wb_id}"


class WaterbodyObservation(WaterbodyBase):
    """Table for the drill outputs"""

    __tablename__ = "waterbody_observations"
    obs_id = Column(String, primary_key=True)
    uid = Column(String, ForeignKey("waterbodies.uid"), index=True)
    px_total = Column(Integer)
    px_wet = Column(Float)
    area_wet_m2 = Column(Float)
    px_dry = Column(Float)
    area_dry_m2 = Column(Float)
    px_invalid = Column(Float)
    area_invalid_m2 = Column(Float)
    date = Column(DateTime)

    def __repr__(self):
        return (
            f"<WaterbodyObservation obs_id={self.obs_id}, uid={self.uid}, "
            + f"date={self.date}, ...>"
        )


def create_waterbody_tables(engine: Engine):
    """Create all waterbody tables."""
    return WaterbodyBase.metadata.create_all(engine)


def drop_waterbody_tables(engine: Engine):
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
    model,
    waterbodies_polygons_fp: str | Path | None = None,
):
    """
    Add the waterbody polygon UIDs and WB_IDs into the database table.

    Parameters
    ----------
    session : Session
    model :
    waterbodies_polygons_fp : str | Path | None, optional
        Path to the shapefile/geojson/geoparquet file containing the waterbodies polygons, by default None
    polygon_numericids_to_stringids_file : str | Path | None, optional
        Path to the JSON file mapping numeric polygon ids (WB_ID) to string polygon ids (UID), by default None

    """

    if (polygon_numericids_to_stringids_file and waterbodies_polygons_fp) or (
        not polygon_numericids_to_stringids_file and not waterbodies_polygons_fp
    ):
        raise ValueError(
            "Please pass either a path to the shapefile/geojson/geoparquet file containing the waterbodies polygons to `waterbodies_polygons_fp` OR the path\
        to the JSON file mapping numeric polygon ids (WB_ID) to string polygon ids (UID) to `polygon_numericids_to_stringids_file`"
        )
    else:
        if waterbodies_polygons_fp:
            if not check_file_exists(waterbodies_polygons_fp):
                _log.error(f"File {waterbodies_polygons_fp} does not exist!")
                raise FileNotFoundError(f"File {waterbodies_polygons_fp} does not exist!)")
            else:
                _, file_extension = os.path.splitext(waterbodies_polygons_fp)
                if file_extension in PARQUET_EXTENSIONS:
                    try:
                        waterbodies = gpd.read_parquet(waterbodies_polygons_fp)
                    except Exception as error:
                        _log.error(f"Could not load file {waterbodies_polygons_fp}")
                        _log.error(error)
                        raise error
                else:
                    try:
                        waterbodies = gpd.read_file(waterbodies_polygons_fp)
                    except Exception as error:
                        _log.error(f"Could not load file {waterbodies_polygons_fp}")
                        _log.error(error)
                        raise error

                # Check the id columns are unique.
                numeric_id = "WB_ID"
                string_id = "UID"
                numeric_id = guess_id_field(input_gdf=waterbodies, use_id=numeric_id)
                assert is_integer_dtype(waterbodies[numeric_id]) or is_float_dtype(
                    waterbodies[numeric_id]
                )
                string_id = guess_id_field(input_gdf=waterbodies, use_id=string_id)
                assert is_string_dtype(waterbodies[string_id])

                objects_list = []
                for row in waterbodies.itertuples():
                    object_ = dict(wb_id=row.WB_ID, uid=row.UID)
                    objects_list.append(object_)

        else:
            if not check_file_exists(polygon_numericids_to_stringids_file):
                _log.error(f"File {polygon_numericids_to_stringids_file} does not exist!")
                raise FileNotFoundError(
                    f"File {polygon_numericids_to_stringids_file} does not exist!)"
                )
            else:
                with fsspec.open(polygon_numericids_to_stringids_file) as f:
                    polygon_numericids_to_stringids = json.load(f)
                    objects_list = []
                    for wb_id, uid in polygon_numericids_to_stringids.items():
                        object_ = dict(wb_id=wb_id, uid=uid)
                        objects_list.append(object_)

        Session = sessionmaker(bind=engine)
        with Session() as session:
            session.begin()
            try:
                session.execute(insert(model), objects_list)
            except Exception:
                session.rollback()
                raise
            else:
                session.commit()
            session.close()


def add_waterbody_observations_to_db(
    paths: [str],
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
        drop_waterbody_tables(engine)

    # ensure tables exist
    create_waterbody_tables(engine)

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
                session.execute(insert(WaterbodyObservation), objects_list)
            except Exception:
                session.rollback()
                raise
            else:
                session.commit()
            session.close()
