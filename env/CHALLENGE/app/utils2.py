# utils.py #
import pandas as pd
from sqlalchemy import func
from database import session
from models import Average

# Set csv file data type #
data_types1 = {
    'start_of_week': 'datetime64',
    'region': 'str',  
    'trip_count': 'int',  
    'weekly_average': 'float'}

# Set the csv file path #
file_path = "/app/trips_average.csv"

def ingest_data_from_csv2(file_path):
    data = pd.read_csv(file_path, usecols=['start_of_week', 'region', 'trip_count', 'weekly_average'])

    for _, row in data.iterrows():
        trips_data_average = Average(start_of_week=row['start_of_week'], region=row['region'], trip_count=row['trip_count'], weekly_average=row['weekly_average'])
        session.add(trips_data_average)

    session.commit()