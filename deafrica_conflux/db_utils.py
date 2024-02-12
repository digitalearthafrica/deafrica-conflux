# This are tools to help in testing the database functions.
# Might remove after testing phase is done.
import logging

from sqlalchemy import MetaData, Table, inspect
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.future import Engine

from deafrica_conflux.db_tables import Waterbody, WaterbodyObservation

_log = logging.Logger(__name__)


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

    # Print the list of schemas
    print(f"Schemas in the database: {schemas}")

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
    print(f"Tables in the public schema: {table_names}")

    return table_names


def get_public_table(engine: Engine, table_name: str) -> Table:
    # Create a metadata object
    metadata = MetaData(schema="public")

    # Reflect the table from the database
    try:
        table = Table(table_name, metadata, autoload_with=engine)
    except NoSuchTableError as error:
        _log.exception(error)
        _log.error("f{table_name} does not exist in database")
        return None
    else:
        return table


def drop_public_table(engine: Engine, table_name: str):
    # Create a metadata object
    metadata = MetaData(schema="public")

    # Reflect the table from the database
    try:
        table = Table(table_name, metadata, autoload_with=engine)
    except NoSuchTableError as error:
        _log.error(error)
    else:
        # Drop the table
        table.drop(engine)

        # Check if the table was dropped.
        #  # Create an inspector
        inspector = inspect(engine)
        check = inspector.has_table("waterbodies")

        if check:
            _log.error(f"{table_name} not deleted")
            raise
        else:
            _log.info("f{table_name} deleted.")


def create_waterbody_table(engine: Engine):
    # Creating individual tables
    # without affecting any other tables defined in the metadata
    Waterbody.__table__.create(engine)
    table_name = Waterbody.__tablename__
    table = get_public_table(engine, table_name)
    return table


def delete_waterbody_table(engine: Engine):
    table_name = Waterbody.__tablename__
    drop_public_table(table_name)


def create_waterbody_obs_table(engine: Engine):
    # Creating individual tables
    # without affecting any other tables defined in the metadata
    WaterbodyObservation.__table__.create(engine)
    table_name = WaterbodyObservation.__tablename__
    table = get_public_table(engine, table_name)
    return table


def delete_waterbody_obs_table(engine: Engine):
    table_name = WaterbodyObservation.__tablename__
    drop_public_table(table_name)
