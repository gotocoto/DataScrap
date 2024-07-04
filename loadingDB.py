import json
import mysql.connector

# Load database configuration from file
with open('db_config.json', 'r') as config_file:
    config = json.load(config_file)

# Connect to MySQL database
try:
    connection = mysql.connector.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database=config['database']
    )
    
    if connection.is_connected():
        print("Connected to the database")
        # Your database operations here

except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if connection.is_connected():
        connection.close()
        print("Connection closed")
