import socket
import os
from dotenv import load_dotenv
import psycopg2 
import json
import schedule
import time 


# loading in .env variable
load_dotenv()
db_url = os.getenv("DATABASE")
print("Database connected!")

def init_database():
    """
    Create a new table named 'sensor_data' on NeonDB, containing payload data from 'KitchenDevices_virtual'.
    """
   
    print("Creating separate table for sensor data...")

    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    # Step 1: Create the table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sensor_data (
            id SERIAL PRIMARY KEY,
            asset_uid TEXT,
            timestamp BIGINT,
            sensor_name TEXT,
            value REAL
        );
    """)
    conn.commit()

    # fetch payloads from KitchenDevices_virtual
    cursor.execute("""
        SELECT payload
        FROM "KitchenDevices_virtual"
    """)
    rows = cursor.fetchall()

    # parse each payload and insert into sensor_data
    for row in rows:
        payload_json = row[0]

        # if stored as a string, convert to dict
        if isinstance(payload_json, str):
            payload_json = json.loads(payload_json)

        asset_uid = payload_json.get('asset_uid')
        timestamp = int(payload_json.get('timestamp'))

        for key, value in payload_json.items():
            if key not in ['timestamp', 'topic', 'parent_asset_uid', 'asset_uid', 'board_name']:
                sensor_name = key
                sensor_value = float(value)
                cursor.execute("""
                    INSERT INTO sensor_data (asset_uid, timestamp, sensor_name, value)
                    VALUES (%s, %s, %s, %s);
                """, (asset_uid, timestamp, sensor_name, sensor_value))

    conn.commit()
    cursor.close()
    conn.close()
    print("sensor_data table populated successfully.")

def init_metadata():
    """
    Returns a dictionary of all device meta data 

    Key: device asset_uid from sensor_data table on neonDB 
    Value: device meta data similar to json format

        metadata = {

            "obj-62t-uf5-4r9": {
                "device_name": "Fridge 1",
                "device_type": "Refrigerator",
                "timezone": "PST",
                "sensors": {
                "ACS712 - Fridge 1": { "unit": "Amperes", "type": "SENSOR" },
                "Moisture Meter - Fridge 1": { "unit": "% RH", "type": "SENSOR" }
                }
            }

        }

    """
    print("parsing metadata information...")
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT "assetUid", "assetType", "customAttributes"
        FROM "KitchenDevices_metadata"; 
    """)

    rows = cursor.fetchall()

    metadata_dict = {}

    for asset_uid, asset_type, custom_attrs in rows:
        try:
            obj = json.loads(custom_attrs) if isinstance(custom_attrs, str) else custom_attrs

            device_name = obj.get("name", asset_uid)
            additional = obj.get("additionalMetadata", {})
            
            # adding meta data into dictionary with asset_uid from KitchenDevices_metadata as key 
            metadata_dict[asset_uid] = {
                "device_name": device_name,
                "device_type": additional.get("device_type", asset_type),
                "timezone": additional.get("timezone", "PST"),
                "location": additional.get("location", ""),
                "sensors": {}
            }

            # traverse board children
            for board in obj.get("children", []):
                for sensor in board.get("customAttributes", {}).get("children", []):

                    sensor_attrs = sensor.get("customAttributes", {})
                    sensor_name = sensor_attrs.get("name", "").strip()
                    unit = sensor_attrs.get("unit", "").strip()

                    if sensor_name:
                        metadata_dict[asset_uid]["sensors"][sensor_name] = {
                            "type": sensor_attrs.get("type", "SENSOR"),
                            "unit": sensor_attrs.get("unit"),
                            "min": sensor_attrs.get("minValue"),
                            "max": sensor_attrs.get("maxValue"),
                            "desiredMin": sensor_attrs.get("desiredMinValue"),
                            "desiredMax": sensor_attrs.get("desiredMaxValue")
                        }

        except Exception as e:
            print(f"[Metadata error for {asset_uid}]: {e}")

    cursor.close()
    conn.close()

    return metadata_dict

def handle_query_one(metadata):
    """
    Prints average moisture values from all fridges within a 3 hour interval.
    Get the device type, timezone, sensor name and information from metadata dictionary.
    Iterates through all sensors within 3 hour timestap, extracts value of sensor using SQL.
    """
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    moisture_values= []

    # iterate through metadata_dict and find refrigerator devices
    for asset_uid, metadata in metadata.items():
        if metadata["device_type"].lower() != "refrigerator":
            continue

        device_name = metadata["device_name"]
        timezone = metadata.get("timezone", "PST")


        # find the moisture sensor and filter for "% RH" units
        for sensor_name, sensor_info in metadata["sensors"].items():
            if sensor_info.get("unit", "").strip() == "% RH":

                # query values from last 3 hours
                cursor.execute("""
                    SELECT payload
                    FROM "KitchenDevices_virtual"
                    WHERE payload::json->> %s IS NOT NULL
                    AND time >= NOW() - INTERVAL '3 hours'
                """, (sensor_name,))

                rows = cursor.fetchall() # in format: [(34.1), (23.1,), ...]

                for (payload,) in rows:
                    payload_data = json.loads(payload) if isinstance(payload, str) else payload
                    value = payload_data.get(sensor_name)

                    if value:
                        moisture_values.append(float(value))
                break
    
    cursor.close()
    conn.close()

    if moisture_values:
        avg = sum(moisture_values) / len(moisture_values)
        return f"Average fridge moisture from all fridges over the past 3 hours: {avg:.2f}% RH"
    else:
        return "No recent moisture data found for your kitchen fridge."

def handle_query_two(metadata):
    """
    Assuming a cycle is 30 minutes with a 1-minute sample rate
    """
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    gallon_values = []

    for asset_uid, metadata in metadata.items():

        # iterate through metadata_dict and find dishwasher devices
        if metadata["device_type"].lower() != "dishwasher":
            continue

        device_name = metadata["device_name"]

        for sensor_name, sensor_info in metadata["sensors"].items():
            if sensor_info.get("unit", "").strip() == "Liters Per Minute":

                # grab only 30 values to simulate 30 minutes
                cursor.execute("""
                    SELECT payload
                    FROM "KitchenDevices_virtual"
                    WHERE payload::json->> %s IS NOT NULL
                    LIMIT 30
                """, (sensor_name,))

                rows = cursor.fetchall();

                # track total water consumption per cycle
                cycle_water_consumption = 0

                for (payload,) in rows:
                    payload_data = json.loads(payload) if isinstance(payload, str) else payload
                    cycle_water_consumption += float(payload_data.get(sensor_name))

                    # convert liters into gallons
                    gallon_values.append(cycle_water_consumption * 0.264172)
                break

    cursor.close()
    conn.close()

    if gallon_values:
        avg = sum(gallon_values) / len(gallon_values)
        return f"Average water consumption per cycle in smart dishwasher: {avg:.2f} gallons"
    else:
        return "No recent water consumption data found for your smart dishwasher."

def handle_query_three(metadata):
    


    return 


def main():
   
    # create database for sensor data 
    # init_database()

    # get metadata dictionary containing metadata attributes
    metadata = init_metadata()

    # ask the user for IP address and port number
    ip = "0.0.0.0"
    port = 4445
    # ip = input("Enter IP address: ")
    # port = int(input("Enter port number: "))
    
    # setting up socket and connecting an IP with a PORT number
    TCPSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    TCPSocket.bind((ip, port))
    
    # listening for 5 connections
    print(f"Server listening on {ip}:{port}")
    TCPSocket.listen(5)
    
    try:
        while True:
            incomingSocket, incomingAddress = TCPSocket.accept()
            print(f"Connected to {incomingAddress}")
            
            try:
                while True:
                    # receive data from client in string form
                    query = incomingSocket.recv(8192).decode()
                    if not query:
                        break  # Exit loop if client disconnects

                    # ADD NEW CODE HERE

                    if query == "1":
                        response = handle_query_one(metadata)
                    elif query == "2":
                        response = handle_query_two(metadata)

                    # sending back to client
                    incomingSocket.send(response.encode("utf-8"))
            except Exception as e:
                print(f"Error: {e}")
            finally:
                incomingSocket.close()
                print(f"{incomingAddress} disconnected.")
    except KeyboardInterrupt:
        print("\nServer shutting down...")
    finally:
        TCPSocket.close()

if __name__ == "__main__":
    main()


    """
    running a function every t time

    schedule.every(5).seconds.do(test)
    while True:
        schedule.run_pending()
        time.sleep(1)
    """"



