import socket
import os
from dotenv import load_dotenv
import psycopg2 
import json


# loading in .env variable
load_dotenv()
db_url = os.getenv("DATABASE")
print("Database connected!")

# connect to cursor for db commands
conn = psycopg2.connect(db_url)
cursor = conn.cursor()

def init_database():
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

    # Step 2: Fetch payloads from KitchenDevices_virtual
    cursor.execute("""
        SELECT payload
        FROM "KitchenDevices_virtual"
        LIMIT 100;  -- or more depending on your dataset
    """)
    rows = cursor.fetchall()

    # Step 3: Parse each payload and insert into sensor_data
    for row in rows:
        payload_json = row[0]

        # If stored as a string, convert to dict
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

def test():
    init_database();


def main():
    # ask the user for IP address and port number
    ip = input("Enter IP address: ")
    port = int(input("Enter port number: "))
    
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
                    data = incomingSocket.recv(8192).decode()
                    if not data:
                        break  # Exit loop if client disconnects

                    # ADD NEW CODE HERE


                    
                    # sending back to client
                    incomingSocket.send(newData.encode("utf-8"))
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
    #main()
    test()
