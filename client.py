import socket

VALID_QUERIES = [
    "What is the average moisture inside my kitchen fridge in the past three hours?",
    "What is the average water consumption per cycle in my smart dishwasher?",
    "Which device consumed more electricity among my three IoT devices?"
]

def main():
    server_ip = input("Enter server IP: ")
    server_port = int(input("Enter server port: "))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((server_ip, server_port))
        print("Connected to server.")

        while True:
            print("\nChoose a query:")
            for i, q in enumerate(VALID_QUERIES, 1):
                print(f"{i}. {q}")
            print("0. Exit")

            choice = input("Enter your choice (0-3): ")

            if choice == "0":
                print("Exiting client.")
                break

            if choice in {"1", "2", "3"}:
                query = VALID_QUERIES[int(choice) - 1]
                client_socket.send(query.encode("utf-8"))
                response = client_socket.recv(8192).decode()
                print(f"Server response:\n{response}")
            else:
                print("Invalid option. Please try again.")

if __name__ == "__main__":
    main()
