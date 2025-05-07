import socket
import os
from dotenv import load_dotenv
import psycopg2 
import json
import schedule
import time 
from collections import defaultdict
import threading

# Load .env variable
load_dotenv()
db_url = os.getenv("DATABASE")
print("Database connected!")

# Global device dictionaries
fridge1_dict = defaultdict(list)
fridge2_dict = defaultdict(list)
dishwasher_dict = defaultdict(list)


def update_dict_for_device(device_name, target_dict, metadata):
    """
    Updates a target dictionary to contain sensor readings as lists of (timestamp, value) tuples
    for a single device only.

    Only includes sensors with a valid unit and of type SENSOR.
    Values are pulled from the last 3 hours from KitchenDevices_virtual table.

    Example output:
        {
            "Sensor 1": [(timestamp, 1.2), ...],
            "Sensor 2": [(timestamp, 2.4), ...],
        }
    """
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    
    for asset_uid, device_info in metadata.items():
        # skip all devices except the one matching device_name
        if device_info["device_name"].lower() != device_name.lower():
            continue

        print(f"Updating device: {device_name}")

        # iterate over all sensors for this device
        for sensor_name, sensor_info in device_info.get("sensors", {}).items():
            # skip sensors without a unit or of the wrong type
            if not sensor_info.get("unit") or sensor_info.get("type") != "SENSOR":
                continue

            # query recent sensor readings from the last 3 hours
            cursor.execute("""
                SELECT time, payload
                FROM "KitchenDevices_virtual"
                WHERE payload::json->> %s IS NOT NULL
                AND time >= NOW() - INTERVAL '3 hours'
            """, (sensor_name,))

            rows = cursor.fetchall()

            # iterate over result rows
            for time_obj, payload in rows:
                # parse JSON payload if needed
                payload_data = json.loads(payload) if isinstance(payload, str) else payload
                
                # extract sensor value
                value = payload_data.get(sensor_name)

                if value is not None:
                    timestamp = time_obj.timestamp()  # convert datetime to UNIX time
                    try:
                        target_dict[sensor_name].append((timestamp, float(value)))
                    except ValueError:
                        print(f"Warning: Skipped non-numeric value for {sensor_name}: {value}")

        break  # stop after processing the matching device

    cursor.close()
    conn.close()

def init_metadata():
    """
    Returns a dictionary of all device metadata.
    """
    print("Parsing metadata information...")
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
            
            metadata_dict[asset_uid] = {
                "device_name": device_name,
                "device_type": additional.get("device_type", asset_type),
                "timezone": additional.get("timezone", "PST"),
                "location": additional.get("location", ""),
                "sensors": {}
            }

            for board in obj.get("children", []):
                for sensor in board.get("customAttributes", {}).get("children", []):
                    sensor_attrs = sensor.get("customAttributes", {})
                    sensor_name = sensor_attrs.get("name", "").strip()
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
    Returns the average moisture from all fridges over the past 3 hours.
    """
    now = time.time()
    three_hours_ago = now - 3 * 3600
    moisture_values = []

    for asset_uid, device_info in metadata.items():
        if device_info["device_type"].lower() != "refrigerator":
            continue

        device_name = device_info["device_name"]

        if device_name.lower() == "fridge 1":
            sensor_dict = fridge1_dict
        elif device_name.lower() == "fridge 2":
            sensor_dict = fridge2_dict
        else:
            continue

        for sensor_name, sensor_info in device_info["sensors"].items():
            if sensor_info.get("unit", "").strip() == "% RH":
                readings = sensor_dict.get(sensor_name, [])
                recent_readings = [val for ts, val in readings if ts >= three_hours_ago]

                print(f"{device_name} - {sensor_name}: {len(recent_readings)} readings")

                moisture_values.extend(recent_readings)
                break

    if moisture_values:
        avg = sum(moisture_values) / len(moisture_values)
        return f"Average fridge moisture over past 3 hours: {avg:.2f}% RH"
    else:
        return "No recent moisture data found for your kitchen fridge."


def handle_query_two(metadata):
    """
    Returns average dishwasher water consumption per cycle.
    """
    gallon_values = []

    for asset_uid, device_info in metadata.items():
        if device_info["device_type"].lower() != "dishwasher":
            continue

        device_name = device_info["device_name"]

        for sensor_name, sensor_info in device_info["sensors"].items():
            if sensor_info.get("unit", "").strip() == "Liters Per Minute":
                readings = dishwasher_dict.get(sensor_name, [])
                values = [val for _, val in sorted(readings)]

                for i in range(0, len(values), 30):
                    cycle = values[i:i+30]
                    if len(cycle) == 30:
                        total_liters = sum(cycle)
                        total_gallons = total_liters * 0.264172
                        gallon_values.append(total_gallons)

    if gallon_values:
        avg = sum(gallon_values) / len(gallon_values)
        return f"Average dishwasher water usage per cycle: {avg:.2f} gallons"
    else:
        return "No recent dishwasher water data available."


def update_all(metadata):
    update_dict_for_device("Fridge 1", fridge1_dict, metadata)
    update_dict_for_device("Fridge 2", fridge2_dict, metadata)
    update_dict_for_device("Dishwasher", dishwasher_dict, metadata)
    print("Updated all dictionaries.")

def start_scheduler(metadata):
    schedule.every(5).seconds.do(lambda: update_all(metadata))
    while True:
        schedule.run_pending()
        time.sleep(1)

def main():
    metadata = init_metadata()
    scheduler_thread = threading.Thread(target=start_scheduler, args=(metadata,), daemon=True)
    scheduler_thread.start()

    ip = "0.0.0.0"
    port = 4444

    TCPSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    TCPSocket.bind((ip, port))
    print(f"Server listening on {ip}:{port}")
    TCPSocket.listen(5)

    try:
        while True:
            incomingSocket, incomingAddress = TCPSocket.accept()
            print(f"Connected to {incomingAddress}")

            try:
                while True:
                    query = incomingSocket.recv(8192).decode()
                    if not query:
                        break

                    if query == "1":
                        response = handle_query_one(metadata)
                    elif query == "2":
                        response = handle_query_two(metadata)
                    else:
                        response = "Invalid query."

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

