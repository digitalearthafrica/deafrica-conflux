"""Database management for the interstitial database.

Matthew Alger
Geoscience Australia
2021
"""

import os

from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, create_engine, select
from sqlalchemy.future import Engine
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.sql.expression import ClauseElement

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
    return create_engine(database_url, echo=True, future=True)


def get_engine_sqlite_in_memory_db() -> Engine:
    """Get a SQLite in-memory database engine."""
    # identifying name of the SQLAlchemy dialect,
    dialect = "sqlite"
    # name of the DBAPI to be used to connect to the database
    driver = "pysqlite"
    # dialect+driver://username:password@host:port/database
    database_url = f"{dialect}+{driver}:///:memory:"
    return create_engine(
        database_url,
        connect_args={"check_same_thread": False},
        echo=True,
        future=True,
    )


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
