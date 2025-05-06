import socket
import os
from dotenv import load_dotenv
import psycopg2 
import json
import schedule
import time 
from collections import defaultdict
import threading 


# loading in .env variable
load_dotenv()
db_url = os.getenv("DATABASE")
print("Database connected!")

# global device dictionaries 
dict_1 = defaultdict(lambda: defaultdict(list))
dict_2 = defaultdict(lambda: defaultdict(list))
dict_3 = defaultdict(lambda: defaultdict(list))



def update_dict(device_name, target_dict, metadata):
    """
    Updates dict_1 to contain device name as key and sensor readings as lists of (timestamp, value) tuples.
        
    Example:
        {
            "Fridge 2": {
            "Moisture Meter - Fridge 2": [(timestamp, 1.2), ...],
            "ACS712 - Fridge 2": [(timestamp, 1.2), ...),
            "Other Sensor - Fridge 2": [(timestamp, 1.2), ...]
            }
        }
    """
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    
    for asset_uid, device_info in metadata.items():
        if device_info["device_name"].lower() != device_name.lower():
            continue

        print(f"Found matching device: {device_name}")

        for sensor_name, sensor_info in device_info.get("sensors", {}).items():
            if not sensor_info.get("unit") or sensor_info.get("type") != "SENSOR":
                continue

            # query values from last 3 hours
            cursor.execute("""
                SELECT time, payload
                FROM "KitchenDevices_virtual"
                WHERE payload::json->> %s IS NOT NULL
                AND time >= NOW() - INTERVAL '3 hours'
            """, (sensor_name,))

            rows = cursor.fetchall()
            print(f"{sensor_name}: {len(rows)} rows found")

            for time_obj, payload in rows:
                payload_data = json.loads(payload) if isinstance(payload, str) else payload
                value = payload_data.get(sensor_name)

                # convert datetime to UNIX epoch
                timestamp = time_obj.timestamp()
                target_dict[device_name][sensor_name].append((timestamp, float(value)))


    cursor.close()
    conn.close()


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



def handle_query_one(metadata, dict_1):
    """
    Returns the average moisture from all fridges using the data in dict_1
    collected within the past 3 hours.
    """
    now = time.time()
    three_hours_ago = now - 3 * 3600
    moisture_values = []

    for asset_uid, device_info in metadata.items():
        if device_info["device_type"].lower() != "refrigerator":
            continue

        device_name = device_info["device_name"]

        for sensor_name, sensor_info in device_info["sensors"].items():
            if sensor_info.get("unit", "").strip() == "% RH":
                readings = dict_1.get(device_name, {}).get(sensor_name, [])
                recent_readings = [val for ts, val in readings if ts >= three_hours_ago]

                print(f"{device_name} - {sensor_name}: {len(recent_readings)} readings in last 3 hours")

                moisture_values.extend(recent_readings)
                break  # only one RH sensor per fridge
    

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


def main():

    # get metadata dictionary containing metadata attributes
    metadata = init_metadata()
   
    # ask the user for IP address and port number
    ip = "0.0.0.0"
    port = 4444
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
                        response = handle_query_one(metadata, dict_1)

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


def update(metadata):
    update_dict("Fridge 2", dict_1, metadata)
    update_dict("Fridge 1", dict_2, metadata)
    update_dict("Dishwasher", dict_3, metadata)

def start_scheduler(metadata):
    schedule.every(5).seconds.do(lambda: update(metadata))
    while True:
        schedule.run_pending()
        time.sleep(1)
    print("schedule update")


if __name__ == "__main__":
    metadata = init_metadata()

    # updating dictionaries for recent data retrieval on background thread
    scheduler_thread = threading.Thread(target=start_scheduler, args=(metadata,), daemon=True)
    scheduler_thread.start()

    main()






