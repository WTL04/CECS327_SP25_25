import socket
import os
import json
import threading
import time
import datetime
import pytz
from dotenv import load_dotenv
from collections import defaultdict
import psycopg2

def connect_db():
    load_dotenv()
    conn = psycopg2.connect(os.getenv("DATABASE"))
    print("Database connected!")
    return conn

def load_metadata(cursor):
    cursor.execute('''
        SELECT "customAttributes"
        FROM "KitchenDevices_metadata";
    ''')
    mapping = {}
    for (raw_meta,) in cursor.fetchall():
        rec = raw_meta if isinstance(raw_meta, dict) else json.loads(raw_meta)
        for board in rec.get("children", []):
            board_uid = board.get("assetUid")
            sensors = board.get("customAttributes", {}).get("children", [])
            names = [
                s["customAttributes"]["name"]
                for s in sensors
                if s.get("customAttributes", {}).get("type") == "SENSOR"
            ]
            if board_uid and names:
                mapping[board_uid] = names
    print(f"Loaded metadata for {len(mapping)} boards/devices")
    return mapping

def populate_initial_cache(cursor, device_data):
    cursor.execute("""
        SELECT payload, time
        FROM "KitchenDevices_virtual"
        WHERE time > NOW() - INTERVAL '3 hours'
        ORDER BY time ASC;
    """)
    rows = cursor.fetchall()
    last_time = None

    count = 0
    for raw_payload, ts in rows:
        payload = raw_payload if isinstance(raw_payload, dict) else json.loads(raw_payload)
        uid = payload.get("asset_uid")
        if uid not in device_data:
            continue
        for sensor in device_data[uid]:
            if sensor in payload:
                try:
                    val = float(payload[sensor])
                except (TypeError, ValueError):
                    continue
                device_data[uid][sensor].append(val)
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
    Background thread: every 30s, pull new rows since last_time,
    update device_data, and print PST‐timestamped status.
    """
    PST = pytz.timezone("America/Los_Angeles")

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
                    if uid not in device_data:
                        continue
                    for sensor in device_data[uid]:
                        if sensor in payload:
                            try:
                                val = float(payload[sensor])
                            except (TypeError, ValueError):
                                continue
                            lst = device_data[uid][sensor]
                            lst.append(val)
                            new_count += 1
                            if len(lst) > 180:
                                del lst[0]
                    last_time_holder[0] = ts

                # PST‐timestamped log
                now_pst = datetime.datetime.now(PST)
                ts_str  = now_pst.strftime("%Y-%m-%d %I:%M:%S %p %Z")
                print(f"[{ts_str}] Cache refresher ran: {new_count} new readings added")

            time.sleep(30)

    thread = threading.Thread(target=refresher, daemon=True)
    thread.start()

def handle_query(query, device_data):
    """
    Process user queries against the in-memory device_data cache.
    """
    q = query.lower()
    VOLTAGE = 120.0  # volts
    MINUTES_PER_HOUR = 60.0
    WH_CONVERSION = 1000.0  # W to kW

    # 1) Average moisture
    if "average moisture" in q:
        readings = []
        for sensors in device_data.values():
            for name, vals in sensors.items():
                if "moisture meter" in name.lower():
                    readings.extend(vals)
        if readings:
            avg = sum(readings) / len(readings)
            return f"Average moisture (last 3 h): {avg:.2f}% RH"
        return "No moisture data available."

    # 2) Average water consumption
    elif "water consumption" in q:
        LITERS_TO_GALLONS = 0.264172
        readings = []
        for sensors in device_data.values():
            for name, vals in sensors.items():
                # match your dishwasher flow sensor(s)
                if "yf-s201" in name.lower() or "water" in name.lower():
                    # take only the last 120 readings (one per minute)
                    recent = vals[-120:]
                    readings.extend(recent)

        if readings:
            total_liters = sum(readings)
            total_gallons = total_liters * LITERS_TO_GALLONS
            return f"Total water used in last 2 hours: {total_gallons:.2f} gallons"
        else:
            return "No water consumption data available."


    # 3) Which device consumed more electricity? → report in kWh
    elif "consumed more electricity" in q:
        energy_usage = {}
        # Each reading is amps over a 1-minute slice. Energy per reading (kWh):
        #   Wh = V * I * (1/60)   →  kWh = Wh / 1000
        # So kWh per reading = V * I / (60 * 1000) = V * I / 60000
        CONVERSION_FACTOR = VOLTAGE / (MINUTES_PER_HOUR * WH_CONVERSION)  # 120/(60*1000)=0.002

        for uid, sensors in device_data.items():
            total_amps = sum(
                sum(vals)
                for name, vals in sensors.items()
                if "acs712" in name.lower()
            )
            # total energy (kWh) over all those readings:
            energy_kwh = total_amps * CONVERSION_FACTOR
            energy_usage[uid] = energy_kwh

        if energy_usage:
            top = max(energy_usage, key=energy_usage.get)
            return (f"Device with highest electricity consumption: {top} "
                    f"({energy_usage[top]:.2f} kWh over last 3 h)")
        return "No electricity data available."

    # fallback help text
    else:
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

    metadata_map = load_metadata(cursor)
    device_data = {
        uid: {sensor: [] for sensor in sensors}
        for uid, sensors in metadata_map.items()
    }

    last_time = populate_initial_cache(cursor, device_data)

    data_lock = threading.Lock()
    last_time_holder = [last_time]
    start_cache_refresher(cursor, device_data, data_lock, last_time_holder)

    ip = input("Enter IP address: ")
    port = int(input("Enter port number: "))
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

            with data_lock:
                response = handle_query(text, device_data)

            client_sock.send(response.encode("utf-8"))

    except KeyboardInterrupt:
        print("\nKeyboard interrupt; exiting.")
    finally:
        client_sock.close()
        server_sock.close()
        cursor.close()
        conn.close()
        print("Server shut down.")

if __name__ == "__main__":
    main()
