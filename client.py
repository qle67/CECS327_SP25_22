import ipaddress
import os
import socket
from threading import Thread

class SocketListener(Thread):
    def run(self):
        while True:
            host: str = input("Please enter IP address of the server: ")        #Ask IP address of server
            try:
                ipaddress.ip_address(host)
                break
            except ValueError:
                print("Invalid IP address!")

        while True:
            port: str = input("Please enter port number of the server: ")       #Ask port of server
            try:
                if port.isnumeric() and 1 <= int(port) <= 65535:
                    break
                else:
                    print("Invalid port number!")
            except ValueError:
                print("Invalid port number!")

        address: str = host + ":" + port
        client_socket: socket = socket.socket(socket.AF_INET)       #Create client socket
        client_socket.connect((host, int(port)))                    #Connect client socket to the server
        print("Connected to address:", address)

        #Display the prompt
        message: str = input("Please enter number for one of following commands or enter 'q' to exit: \n 1. What is the average moisture inside my kitchen fridge in the past three hours?\n 2. What is the average water consumption per cycle in my smart dishwasher?\n 3. Which device consumed more electricity among my three IoT devices (two refrigerators and a dishwasher)?\n")
        while message != 'q':
            if message == '1' or message == '2' or message == '3':
                client_socket.send(message.encode("utf-8"))
                print(f"Sent message: '{message}' to address: {address}")
                data: bytes = client_socket.recv(1024)
                print(f"Received message from address {address}: '{data.decode('utf-8')}'")
                message: str = input("Please enter number for one of following commands or enter 'q' to exit: \n 1. What is the average moisture inside my kitchen fridge in the past three hours?\n 2. What is the average water consumption per cycle in my smart dishwasher?\n 3. Which device consumed more electricity among my three IoT devices (two refrigerators and a dishwasher)?\n")
                continue

            #Reject any other input with a user-friendly message
            message = input("Sorry, this query cannot be processed. Please try one of the following or enter 'q' to exit: \n 1. What is the average moisture inside my kitchen fridge in the past three hours?\n 2. What is the average water consumption per cycle in my smart dishwasher?\n 3. Which device consumed more electricity among my three IoT devices (two refrigerators and a dishwasher)?\n")
        client_socket.close()


def main():
    pid = os.getpid()
    client = SocketListener()
    try:
        print("Client started. Press Ctrl-C to abort...")
        client.start()
        client.join()
    except KeyboardInterrupt:
        print("Running client is interrupted by user. Exiting...")
        os.kill(pid, 9)


main()
