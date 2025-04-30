import socket

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

    while True:
        message = input("Please select a query (1, 2, or 3): ")
        print("1. What is the average moisture inside my kitchen fridge in the past three hours?")
        print("2. What is the average water consumption per cycle in my smart dishwasher?")
        print("3. Which device consumed more electricity among my three IoT devices (two refrigerators and a dishwasher)?")
        
        if message not in query_map:
            print("Sorry, this query cannot be processed. Please try one of the following (1, 2, or 3)")
            continue

        # send query number to server, wait for server to respond and print
        TCPSocket.send(bytearray(str(message), encoding="utf-8"))

        # get server responese and decode it for readability
        serverResponse = TCPSocket.recv(8192)
        decodedResponse = serverResponse.decode("utf-8")

        print(f"Server Response: {decodedResponse}")


