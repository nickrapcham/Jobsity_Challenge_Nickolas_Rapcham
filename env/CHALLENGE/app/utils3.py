# utils.py #
import pandas as pd
from sqlalchemy import func
from database import session
from models import Scalability

# Set csv file data type #
data_types1 = {
    'region': 'str',
    'origin_coord': 'str',
    'destination_coord': 'str',
    'datetime': 'datetime64',  
    'datasource': 'str' }

# Set the csv file path #
file_path = "/app/trips_scalability.csv"

# Define the automatic file ingestion #
def ingest_data_from_csv3(file_path):
    data = pd.read_csv(file_path, usecols=['region', 'origin_coord', 'destination_coord', 'datetime', 'datasource'], parse_dates=['datetime'])

    #Group trips by origin, destination, and time of day
    data['group_id'] = data.groupby(['origin_coord', 'destination_coord', data['datetime'].dt.time]).ngroup()
    
    for _, row in data.iterrows():
        trips_data_scalability = Scalability(region=row['region'], origin_coord=row['origin_coord'], destination_coord=row['destination_coord'], datetime=row['datetime'], datasource=row['datasource'],group_id=row['group_id'])
        session.add(trips_data_scalability)

    session.commit()
