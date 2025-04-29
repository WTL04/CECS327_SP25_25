import socket
import os
import json
from dotenv import load_dotenv
import psycopg2

def connectDB():
    load_dotenv()
    db_url = os.getenv("DATABASE")
    conn = psycopg2.connect(db_url)
    print("Database connected!")
    return conn

def handle_query(data, cursor):
    query = data.lower()
    print(f"Handling query: {query}")

    if "average moisture" in query:
        cursor.execute("""
            SELECT payload
            FROM "KitchenDevices_virtual"
            WHERE topic = 'KitchenDevices'
            AND payload::json->>'board_name' = 'Fridge 1 Board'
            AND time > NOW() - INTERVAL '3 hours';
        """)
        rows = cursor.fetchall()
        moistures = []

        for row in rows:
            try:
                payload = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                value = payload.get("Moisture Meter - Fridge 1")
                if value:
                    moistures.append(float(value))
            except Exception as e:
                print(f"Moisture parse error: {e}")

        if moistures:
            avg = sum(moistures) / len(moistures)
            return f"Average moisture in the last 3 hours: {avg:.2f}% RH"
        else:
            return "No recent moisture data found."

    elif "average water consumption" in query:
        cursor.execute("""
            SELECT payload
            FROM "KitchenDevices_virtual"
            WHERE topic = 'KitchenDevices'
            AND payload::json->>'board_name' = 'Dishwasher Board';
        """)
        rows = cursor.fetchall()
        waters = []

        for row in rows:
            try:
                payload = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                value = payload.get("YF-S201 - Dishwasher")
                if value:
                    waters.append(float(value))
            except Exception as e:
                print(f"Water parse error: {e}")

        if waters:
            avg = sum(waters) / len(waters)
            return f"Average water consumption per cycle: {avg:.2f} gallons"
        else:
            return "No water consumption data found."

    elif "consumed more electricity" in query:
        cursor.execute("""
            SELECT payload
            FROM "KitchenDevices_virtual"
            WHERE topic = 'KitchenDevices';
        """)
        rows = cursor.fetchall()
        usage_map = {"Fridge 1": 0.0, "Fridge 2": 0.0, "Dishwasher": 0.0}

        for row in rows:
            try:
                payload = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                board = payload.get("board_name", "")
                if "Fridge 1" in board:
                    val = payload.get("ACS712 - Fridge 1")
                    if val:
                        usage_map["Fridge 1"] += float(val)
                elif "Fridge 2" in board:
                    val = payload.get("ACS712 - Fridge 2")
                    if val:
                        usage_map["Fridge 2"] += float(val)
                elif "Dishwasher" in board:
                    val = payload.get("ACS712 - Dishwasher")
                    if val:
                        usage_map["Dishwasher"] += float(val)
            except Exception as e:
                print(f"Electricity parse error: {e}")

        top_device = max(usage_map, key=usage_map.get)
        top_usage = usage_map[top_device]
        return f"Device with highest electricity usage: {top_device} ({top_usage:.2f} amps)"

    else:
        return (
            "Sorry, this query cannot be processed.\n"
            "Please try one of the following:\n"
            "1. What is the average moisture inside my kitchen fridge in the past three hours?\n"
            "2. What is the average water consumption per cycle in my smart dishwasher?\n"
            "3. Which device consumed more electricity among my three IoT devices?"
        )

def main():
    conn = connectDB()
    cursor = conn.cursor()

    ip = input("Enter IP address: ")
    port = int(input("Enter port number: "))

    TCPSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    TCPSocket.bind((ip, port))
    TCPSocket.listen(5)
    print(f"Server listening on {ip}:{port}")

    try:
        while True:
            incomingSocket, incomingAddress = TCPSocket.accept()
            print(f"Connected to {incomingAddress}")
            try:
                while True:
                    data = incomingSocket.recv(8192).decode()
                    if not data:
                        break
                    print(f"Received query: {data}")
                    response = handle_query(data, cursor)
                    incomingSocket.send(response.encode("utf-8"))
            except Exception as e:
                print(f"Connection error: {e}")
            finally:
                incomingSocket.close()
                print(f"{incomingAddress} disconnected.")
    except KeyboardInterrupt:
        print("\nServer shutting down...")
    finally:
        TCPSocket.close()
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
