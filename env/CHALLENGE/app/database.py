# database.py #
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base
from mysql.connector import Error as msql 
import mysql.connector as msql
from mysql.connector import Error

### Create the MySQL database ###

try:
    # Connect to the MySQL server without specifying a database #
    conn = msql.connect(host='192.168.32.1', user='challenge', password='123456')

    if conn.is_connected():
        cursor = conn.cursor()
        # Check if the database 'TRIPS' exists #
        cursor.execute("SHOW DATABASES")
        databases = [database[0] for database in cursor.fetchall()]

        if 'TRIPS' not in databases:
            # Create the database 'TRIPS' #
            cursor.execute("CREATE DATABASE TRIPS")
            print("Database 'TRIPS' is created")
        else:
            print("Database 'TRIPS' already exists")

except Error as e:
    print("Error while connecting to MySQL", e)

# Configurate DB_STRING #
DB_CONFIG = {
    'host': '192.168.32.1',  # Replace with your MySQL host #
    'user': 'challenge',  # Replace with your MySQL username #
    'password': '123456',  # Replace with your MySQL password #
    'database': 'trips',  # Replace with your MySQL database name #
}

# Generate engine #
engine = create_engine(f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")

Base.metadata.create_all(engine)

# Setup Session #
Session = sessionmaker(bind=engine)
session = Session()

