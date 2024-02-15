"""Tables expected in the  waterbodies database"""
import logging

from geoalchemy2 import Geometry
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import declarative_base

_log = logging.getLogger(__name__)

WaterbodyBase = declarative_base()


# Define the table for the waterbodies polygons
class Waterbody(WaterbodyBase):
    """Table for the waterbody polygons"""

    __tablename__ = "waterbodies"
    uid = Column(String, primary_key=True)
    wb_id = Column(Integer)
    area_m2 = Column(Float)
    length_m = Column(Float)
    perim_m = Column(Float)
    timeseries = Column(String)
    geometry = Column(Geometry(geometry_type="POLYGON"))

    def __repr__(self):
        return f"<Waterbody uid={self.uid}, wb_id={self.wb_id}, ...>"


class WaterbodyObservation(WaterbodyBase):
    """Table for the drill outputs"""

    __tablename__ = "waterbody_observations"
    obs_id = Column(String, primary_key=True)
    uid = Column(String, ForeignKey("waterbodies.uid"), index=True)
    uid = Column(String, index=True)
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


class WaterbodyTimeseries(WaterbodyBase):
    """Table for the waterbodies timeseries"""

    __tablename__ = "waterbody_timeseries"
    uid = Column(String, ForeignKey("waterbodies.uid"), primary_key=True, index=True)
    px_total = Column(Integer)
    px_wet = Column(Float)
    area_wet_m2 = Column(Float)
    px_dry = Column(Float)
    area_dry_m2 = Column(Float)
    px_invalid = Column(Float)
    area_invalid_m2 = Column(Float)

    def __repr__(self):
        return f"<WaterbodyTimeseries uid={self.uid}, ...>"
