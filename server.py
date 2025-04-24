import socket
import os
from dotenv import load_dotenv


def connectDB():
    database_url = os.getenv("DATABASE")


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
    main()
