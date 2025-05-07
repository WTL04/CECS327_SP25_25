import socket
import os
import json
import threading
import time
import datetime
import pytz
from dotenv import load_dotenv
import psycopg2

def connect_db():
    load_dotenv()
    conn = psycopg2.connect(os.getenv("DATABASE"))
    print("Database connected!")
    return conn

def load_metadata(cursor):
    """
    Returns two dicts:
      - sensor_map: board_asset_uid -> list of sensor names
      - name_map:   board_asset_uid -> human device name (e.g. "Fridge 1")
    """
    cursor.execute(
        'SELECT "customAttributes" FROM "KitchenDevices_metadata";'
    )
    sensor_map = {}
    name_map = {}
    
    # search through all metadata
    for (raw_meta,) in cursor.fetchall():

        rec = raw_meta if isinstance(raw_meta, dict) else json.loads(raw_meta)
        device_name = rec.get("name")

        # searching through board and sensors 
        for board in rec.get("children", []):
            board_uid = board.get("assetUid")
            sensors = board.get("customAttributes", {}).get("children", [])
            names = []

            # appending sensors to sensor_map and device_name to name_map
            for s in sensors:
                ca = s.get("customAttributes", {})
                if ca.get("type") == "SENSOR":
                    names.append(ca.get("name"))
            if board_uid and names:
                sensor_map[board_uid] = names
                name_map[board_uid] = device_name

    print(f"Loaded metadata for {len(sensor_map)} boards/devices")
    return sensor_map, name_map

def populate_initial_cache(cursor, device_data):
    """
    Load last 3 hours of readings into device_data.
    Return the most recent timestamp seen.
    """
    cursor.execute("""
        SELECT payload, time
        FROM "KitchenDevices_virtual"
        WHERE time > NOW() - INTERVAL '3 hours'
        ORDER BY time ASC;
    """)

    rows = cursor.fetchall()
    last_time = None
    count = 0 # tracks number of readings
    
    # from virtual data, grab the payload and timestamp 
    for raw_payload, ts in rows:
        payload = raw_payload if isinstance(raw_payload, dict) else json.loads(raw_payload)
        uid = payload.get("asset_uid")

        # skip unknown devices
        if uid not in device_data:
            continue

        # search for sensors in device, add values to cache
        for sensor in device_data[uid]:
            if sensor in payload:
                try:
                    val = float(payload[sensor])
                except (TypeError, ValueError):
                    continue
                device_data[uid][sensor].append((ts, val))
                count += 1
        last_time = ts

    # Trim to at most 180 entries per sensor
    for sensors in device_data.values():
        for lst in sensors.values():
            if len(lst) > 180:
                del lst[0:len(lst)-180]

    if last_time is None:
        last_time = datetime.datetime.now(datetime.timezone.utc)

    print(f"Initial cache populated with {count} readings (last 3 hours)")
    return last_time

def start_cache_refresher(cursor, device_data, lock, last_time_holder):
    """
    Every 30 seconds, fetch new rows since last_time_holder[0],
    append to device_data, trim to 180 entries, and log in PST.
    """
    pst = pytz.timezone("America/Los_Angeles")

    def refresher():
        while True:
            with lock:
                cursor.execute("""
                    SELECT payload, time
                    FROM "KitchenDevices_virtual"
                    WHERE time > %s
                    ORDER BY time ASC;
                """, (last_time_holder[0],))
                rows = cursor.fetchall()

                new_count = 0
                for raw_payload, ts in rows:
                    payload = raw_payload if isinstance(raw_payload, dict) else json.loads(raw_payload)
                    uid = payload.get("asset_uid")

                    # skip unknown devices
                    if uid not in device_data:
                        continue

                    # search for sensors in device, add values to cache
                    for sensor in device_data[uid]:
                        if sensor in payload:
                            try:
                                val = float(payload[sensor])
                            except (TypeError, ValueError):
                                continue
                            lst = device_data[uid][sensor]
                            lst.append((ts, val))
                            new_count += 1

                            # remove old data when cache reaches length of 180
                            if len(lst) > 180:
                                del lst[0]

                    last_time_holder[0] = ts
                
                # display the time the cache is updated
                now_pst = datetime.datetime.now(pst)
                ts_str = now_pst.strftime("%Y-%m-%d %I:%M %p %Z")
                print(f"[{ts_str}] Cache refresher ran: {new_count} new readings added")

            time.sleep(30)
    
    # starting separate thread for referesher to run in background
    thread = threading.Thread(target=refresher, daemon=True)
    thread.start()

def handle_query(query, device_data, name_map):
    """
    Process user queries against the in-memory device_data cache,
    using name_map to translate board_uids to human names.
    """
    q = query.lower()
    pst = pytz.timezone("America/Los_Angeles")
    VOLTAGE = 120.0
    WH_CONVERSION = 1000.0

    # 1) Average moisture over last 3 hours, with PST range
    if "average moisture" in q:
        readings = []
        for sensors in device_data.values():

            # append all readings (timestamp, current) after matching the sensor "moisture meter"
            for name, ents in sensors.items():
                if "moisture meter" in name.lower():
                    readings.extend(ents)
        if readings:

            # computing average 
            values = [val for ts, val in readings]
            avg = sum(values) / len(values)

            # converting timestamp into PST
            times = [ts for ts, _ in readings]
            earliest = min(times).astimezone(pst)
            latest   = max(times).astimezone(pst)
            fmt = "%Y-%m-%d %I:%M %p %Z"
            
            return (
                f"Average moisture from {earliest.strftime(fmt)} "
                f"to {latest.strftime(fmt)}: {avg:.2f}% RH"
            )
        return "No moisture data available."

    # 2) Estimated total water used over last 1 hour (gallons) with PST range
    elif "water consumption" in q:
        readings = []
        for sensors in device_data.values():

            # append all readings (timestamp, current) after matching the sensor "yf-s201"
            for name, ents in sensors.items():
                if "yf-s201" in name.lower() or "water" in name.lower():
                    readings.extend(ents)

        if readings:
            # sort, take freshest 60 (1h)
            readings.sort(key=lambda x: x[0])
            recent = readings[-60:]
            values = [val for ts, val in recent]
            avg_lpm = sum(values) / len(values)
            total_liters = avg_lpm * 5
            total_gallons = total_liters * 0.264172

            # convert timestamp into PST
            times = [ts for ts, _ in recent]
            earliest = min(times).astimezone(pst)
            latest   = max(times).astimezone(pst)
            fmt = "%Y-%m-%d %I:%M %p %Z"

            return (
                f"Water used from {earliest.strftime(fmt)} "
                f"to {latest.strftime(fmt)}: {total_gallons:.2f} gallons"
            )
        return "No water consumption data available."

    # 3) Highest electricity consumption in kWh over last 3h, with PST range
    elif "consumed more electricity" in q:
        energy_usage = {}
        ranges = {}
        factor = VOLTAGE / (60.0 * WH_CONVERSION)

        # searching through device data
        for uid, sensors in device_data.items():
            entries = []

            # append all readings (timestamp, current) after matching the sensor "acs712"
            for name, ents in sensors.items():
                if "acs712" in name.lower():
                    entries.extend(ents)

            # skip device if no "acs712" readings found
            if not entries:
                continue

            # computing total amps, energy usage, time, and ranges
            total_amps = sum(val for ts, val in entries)
            energy_usage[uid] = total_amps * factor
            times = [ts for ts, _ in entries]
            ranges[uid] = (min(times), max(times))

        if energy_usage:

            # finding max energy usage
            top_uid = max(energy_usage, key=energy_usage.get)
            top_name = name_map.get(top_uid, top_uid)
            e = energy_usage[top_uid]
            start, end = ranges[top_uid]

            # convert timestamps into PST
            earliest = start.astimezone(pst)
            latest   = end.astimezone(pst)
            fmt = "%Y-%m-%d %I:%M %p %Z"

            return (
                f"{top_name} consumed most electricity ({e:.2f} kWh) "
                f"from {earliest.strftime(fmt)} to {latest.strftime(fmt)}"
            )
        return "No electricity data available."

    # fallback help
    return (
        "Sorry, I can't process that query.\n"
        "Please try one of:\n"
        "1. What is the average moisture inside my kitchen fridge in the past three hours?\n"
        "2. What is the average water consumption per cycle in my smart dishwasher?\n"
        "3. Which device consumed more electricity among my three IoT devices?"
    )

def main():
    conn = connect_db()
    cursor = conn.cursor()

    # initializing metadata 
    sensor_map, name_map = load_metadata(cursor)
    device_data = {
        uid: {sensor: [] for sensor in sensors}
        for uid, sensors in sensor_map.items()
    }
    
    # initialzing initial cache
    last_time = populate_initial_cache(cursor, device_data)

    # running cache refresher 
    lock = threading.Lock()
    last_time_holder = [last_time]
    start_cache_refresher(cursor, device_data, lock, last_time_holder)

    # prompting user for IP and PORT
    ip = input("Enter IP address: ")
    port = int(input("Enter port number: "))
    print()

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind((ip, port))
    server_sock.listen(1)
    print(f"Server listening on {ip}:{port}")

    try:
        client_sock, addr = server_sock.accept()
        print(f"Connected to {addr}")
        while True:
            data = client_sock.recv(8192)
            if not data:
                print("Client disconnected; shutting down.")
                break
            text = data.decode("utf-8")
            print(f"Received query: {text}")
            with lock:
                response = handle_query(text, device_data, name_map)
            client_sock.send(response.encode("utf-8"))
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        client_sock.close()
        server_sock.close()
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
