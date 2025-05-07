import socket

# Map menu numbers to full question text
query_map = {
    "1": "What is the average moisture inside my kitchen fridge in the past three hours?",
    "2": "What is the average water consumption per cycle in my smart dishwasher?",
    "3": "Which device consumed more electricity among my three IoT devices (two refrigerators and a dishwasher)?"
}

if __name__ == "__main__":
    host = input("Please enter the Host IP Address: ")
    port = int(input("Please enter the Server's Port Number: "))

    # establish socket connection with server
    TCPSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    TCPSocket.connect((host, port))

    try:
        while True:
            print("1. What is the average moisture inside my kitchen fridge in the past three hours?")
            print("2. What is the average water consumption per cycle in my smart dishwasher?")
            print("3. Which device consumed more electricity among my three IoT devices (two refrigerators and a dishwasher)?")
            print("0. Exit")

            choice = input("Please select a query (1, 2, 3, or 0): ")

            if choice == "0":
                print("Exiting client.")
                break

            if choice not in query_map:
                print("Sorry, this query cannot be processed. Please try one of (1, 2, or 3).")
                continue

            # 1) Lookup the full English question
            full_query = query_map[choice]

            # 2) Send the full question text to the server
            TCPSocket.send(full_query.encode("utf-8"))

            # 3) Receive and print the server's response
            server_response = TCPSocket.recv(8192).decode("utf-8")
            print("-----------------------------------------")
            print(f"Server Response: {server_response}")
            print("-----------------------------------------")

    finally:
        # ensure the socket is closed so the server sees EOF
        TCPSocket.close()
