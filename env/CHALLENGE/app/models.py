# models.py
from sqlalchemy import Column, Integer, String, DateTime, Float, Date
from sqlalchemy.ext.declarative import declarative_base

# Create a base class for declarative models #
Base = declarative_base()

# Create Table Data model #
class Trip(Base):
    __tablename__ = 'trips_data'

    id = Column(Integer, primary_key=True)
    region = Column(String(255))
    origin_coord = Column(String(255))
    destination_coord = Column(String(255))
    datetime = Column(DateTime)
    datasource = Column(String(255))
    group_id = Column(Integer)

# Create Table Data model #
class Average(Base):
    __tablename__ = 'trips_data_average'

    id = Column(Integer, primary_key=True)
    start_of_week = Column(Date)
    region = Column(String(255))
    trip_count = Column(Integer)
    weekly_average = Column(Float)

# Create Table Data model #
class Scalability(Base):
    __tablename__ = 'trips_data_scalability'

    id = Column(Integer, primary_key=True)
    region = Column(String(255))
    origin_coord = Column(String(255))
    destination_coord = Column(String(255))
    datetime = Column(DateTime)
    datasource = Column(String(255))
    group_id = Column(Integer)