# app.py #
from flask import Flask, jsonify, request, make_response
from utils import ingest_data_from_csv
from utils2 import ingest_data_from_csv2
from utils3 import ingest_data_from_csv3
from sqlalchemy import create_engine
from faker import Faker
import random
import requests
import pandas as pd
import os
import signal

# Setup Flask for API Rest #
app = Flask(__name__)

# Extraction Endpoint Connections #
connection_string = "mysql+mysqlconnector://challenge:123456@192.168.32.1:3306/trips"
engine = create_engine(connection_string)

### INGESTION ENDPOINTS ###
# Endpoint to Ingest to Trips_Data #
@app.route('/ingest', methods=['POST'])
def ingest():
    data = request.json
    print("Ingesting data:", data)
    return jsonify({"message": "Data ingestion successful"}), 200

# Endpoint to Ingest to Scalability_Trips_Data #
@app.route('/ingest_scalability', methods=['POST'])
def ingest_scalability():
    data = request.json
    print("Ingesting data:", data)
    return jsonify({"message": "Data ingestion successful"}), 200

# Endpoint to load the trips_data_average table #
@app.route('/load_average_table', methods=['POST'])
def ingest_average():
    data = request.json
    print("Ingesting data:", data)
    return jsonify({"message": "Data ingestion successful"}), 200

### STAGE ENDPOINTS ###
# Endpoint to Extract Average from Trips_Data #
@app.route('/extract_data', methods=['GET'])
def extract_data():
    try:
        table_name = "trips_data"

        # Query the data from the table
        query = f'''
                SELECT
                    DATE_FORMAT(datetime, '%Y-%m-%d') AS start_of_week,
                    region,
                    COUNT(*) AS trip_count,
                    COUNT(*) / 7 AS weekly_average
                FROM
                    {table_name}
                GROUP BY
                    start_of_week, region;'''
        df = pd.read_sql_query(query, engine)
        df2 = pd.DataFrame(df, columns=["start_of_week", "region", "trip_count", "weekly_average"])
        # Convert DataFrame to CSV data #
        df2.to_csv("/app/trips_average.csv", index=False)
        return jsonify('DONE') 
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/scalability_data', methods=['POST'])
def scalability_data():
        # Create a Faker instance to generate random data #
        fake = Faker()

        # Define the list of regions and datasources #
        regions = ["Prague", "Turin", "Hamburg", "Berlin", "Paris", "London", "Madrid", "Rome", "Vienna", "Amsterdam", "Londrina"]
        datasources = ["funny_car", "baba_car", "cheap_mobile", "bad_diesel_vehicles", "pt_search_app"]

        # Generate random data #
        data = []
        for _ in range(100000):
            region = random.choice(regions)
            origin_latitude = random.uniform(0, 90)  # Generate a random positive latitude between 0 and 90
            origin_longitude = random.uniform(0, 180)  # Generate a random positive longitude between 0 and 180
            destination_latitude = random.uniform(0, 90)  # Generate a random positive latitude between 0 and 90
            destination_longitude = random.uniform(0, 180)  # Generate a random positive longitude between 0 and 180
            datetime = fake.iso8601()
            datasource = random.choice(datasources)
            data.append({
                'region': region,
                'origin_coord': f"POINT ({origin_longitude} {origin_latitude})",
                'destination_coord': f"POINT ({destination_longitude} {destination_latitude})",
                'datetime': datetime,
                'datasource': datasource
            })

        # Create a DataFrame from the generated data #
        df = pd.DataFrame(data)

        # Save the CSV data to a file on the server #
        df.to_csv("/app/trips_scalability.csv", index=False)
        return jsonify({'status': 'success', 'message': 'Data ingestion process completed successfully.'})
       
### PROCESS AUTOMATION ENDPOINTS ###
# Endpoint to Automate extraction from Trips_Data #
@app.route('/extract_average_from_data_table', methods=['GET'])
def execute_extraction():
    # Set the connection string for the ingestion endpoint #
    extraction_endpoint = 'http://127.0.0.1:5000/extract_data'

    try:
        # Step 1: Execute the ingestion endpoint to ingest the data
        requests.get(extraction_endpoint)
        return jsonify({'status': 'success', 'message': 'Data ingestion process completed successfully.'})

    except requests.exceptions.RequestException as e:
        # If an error occurs during any of the requests, handle the exception
        return jsonify({'status': 'error', 'message': str(e)})

# Endpoint to Automate Ingestion to Trips_Data #
@app.route('/ingestion_to_data_table', methods=['POST'])
def execute_ingestion1():
    # Set the connection string for the ingestion endpoint #
    ingestion_endpoint1 = 'http://127.0.0.1:5000/ingest'

    # Files to be ingested #
    csv_file1 = "trips.csv"
    if csv_file1: ingest_data_from_csv('trips.csv')

    try:
        # Step 1: Execute the ingestion endpoint to ingest the data
        requests.post(ingestion_endpoint1, json=csv_file1)
        return jsonify({'status': 'success', 'message': 'Data ingestion process completed successfully.'})

    except requests.exceptions.RequestException as e:
        # If an error occurs during any of the requests, handle the exception
        return jsonify({'status': 'error', 'message': str(e)})

# Endpoint to Automate Ingestion to Trips_Data_Average #
@app.route('/ingest_data_to_average_table', methods=['POST'])
def execute_ingestion2():
    # Set the connection string for the ingestion endpoint #
    ingestion_endpoint2 = 'http://127.0.0.1:5000/load_average_table'

    # Files to be ingested #
    csv_file2 = "trips_average.csv"
    if csv_file2: ingest_data_from_csv2('trips_average.csv')

    try:
        # Step 1: If the data extraction succeeds, execute the second ingestion endpoint
        # to create and load the average table
        requests.post(ingestion_endpoint2, json=csv_file2)
        return jsonify({'status': 'success', 'message': 'Data ingestion process completed successfully.'})

    except requests.exceptions.RequestException as e:
        # If an error occurs during any of the requests, handle the exception
        return jsonify({'status': 'error', 'message': str(e)})

# Endpoint to Automate Ingestion to Trips_data_scalability #
@app.route('/ingestion_to_scalability_table', methods=['POST'])
def execute_ingestion3():
    # Files to be ingested #
    csv_file3 = "/app/trips_scalability.csv"
    
    try:
        # Step 2: Execute the ingestion function with the DataFrame
        ingest_data_from_csv3(csv_file3)
        return jsonify({'status': 'success', 'message': 'Data ingestion process completed successfully.'})

    except Exception as e:
        # If an error occurs during ingestion, handle the exception
        return jsonify({'status': 'error', 'message': str(e)})
    
# Endpoint to shutdown_server #
@app.route('/shutdown', methods=['POST'])
def shutdown_server():
    # Trigger the shutdown process by raising a SystemExit exception
    os.kill(os.getpid(), signal.SIGINT)
    return 'Server shutting down...'


### ENDPOINT TO RUN THE APP ###
# Endpoint to run the entire ingestion process #
@app.route('/run_process', methods=['POST'])
def run_ingestion_process():
    # Set the URLs for the three endpoints
    ingestion_data = 'http://127.0.0.1:5000/ingestion_to_data_table'
    extraction_average = 'http://127.0.0.1:5000/extract_average_from_data_table'
    ingestion_average = 'http://127.0.0.1:5000/ingest_data_to_average_table'
    scalability_csv_creation = 'http://127.0.0.1:5000/scalability_data'
    scalability_ingestion = 'http://127.0.0.1:5000/ingestion_to_scalability_table'
    close_session = 'http://127.0.0.1:5000/shutdown'

    try:
    # Step 1: Execute the first ingestion endpoint to ingest the data
        response = requests.post(ingestion_data)
        print("Ingestion request sent successfully")
        if response.ok:
            print("Data ingestion complete")
        else:
            print("Data ingestion Failed")
    # Step 2: Execute the data extraction endpoint
        response2 = requests.get(extraction_average)
        print("Data extraction request sent successfully")
        if response2.ok:
            print("Data exctraction complete")
        else:
            print("Data extraction Failed")
    # Step 3: If the data extraction succeeds, execute the second ingestion endpoint
        response3 = requests.post(ingestion_average)
        print("Ingestion request sent successfully")
        if response3.ok:
            print("Average data ingestion complete")
        else:
            print("Average data Failed")
    #Step 4: Execute the scalability csv file creation
        response4 = requests.post(scalability_csv_creation)
        print("CSV Creation sent successfully")
        if response4.ok:
            print("CSV Creation complete")
        else:
            print("CSV Creation Failed")
    #Step 5: Execute the scalability ingestion endpoint
        response0 = requests.post(scalability_ingestion)
        print("Ingestion request sent successfully")
        if response0.ok:
            print("Data ingestion complete")
        else:
            print("Data ingestion Failed")
    # Step 6: If the data the second ingestion endpoint succeds, close the server
        response6 = requests.post(close_session)
        if response6.ok:
            print("Server Shutdown")
                 
        return jsonify({'status': 'success', 'message': 'App completed successfully.'})

    except requests.exceptions.RequestException as e:
        # If an error occurs during any of the requests, handle the exception
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)