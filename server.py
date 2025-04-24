import socket
import os
from dotenv import load_dotenv
import psycopg2 


def connectDB():
    # loading in .env variable
    load_dotenv()
    database_url = os.getenv("DATABASE")
    print("Database connected!")

def test():
    load_dotenv()
    db_url = os.getenv("DATABASE")

    # connect to db using cursor
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    # create table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS playing_with_neon (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value REAL
        );
        """
    )
    
    # insert data
    cursor.execute("""
        INSERT INTO playing_with_neon(name, value)
        SELECT LEFT(md5(i::TEXT), 10), random()
        FROM generate_series(1, 10) s(i);
    """)

    # commit changes
    conn.commit()

    # query and fetch data
    cursor.execute("SELECT * FROM playing_with_neon;")
    rows = cursor.fetchall()

    # print results
    for row in rows:
        print(row)

    cursor.close()
    conn.close()


def main():
    # connect to database 
    connectDB()

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
