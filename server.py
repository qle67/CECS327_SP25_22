import json
import math
import os
import socket
from datetime import datetime, timezone, timedelta
from threading import Thread
from time import sleep

import psycopg2

connection = None
cursor = None
server_socket = None
client_socket = None

class SocketListener(Thread):
    def run(self):
        global connection, cursor, client_socket
        while True:
            port: str = input("Please enter port number to listen on:") #Ask user for port number to listen
            try:
                if port.isnumeric() and 1 <= int(port) <= 65535:    #Check whether port is valid
                    break
                else:
                    print("Invalid port number!")
            except ValueError:
                print("Invalid port number!")
        server_socket: socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)   #Create a socket to start listening
        server_socket.bind(("0.0.0.0", int(port)))  #Bind the server to the port

        try:
            connection = psycopg2.connect(
                dbname="neondb",
                user="neondb_owner",
                password="npg_c7l4JkshBbRA",
                host="ep-twilight-bird-a680p6mk-pooler.us-west-2.aws.neon.tech",  # e.g., "localhost" or an IP address
                port="5432"  # default is 5432
            )   #connect server to database NeonDB
            cursor = connection.cursor()    #cursor is used to execute database command
            print("Connected to NeonDB")

            server_socket.listen()      #Sever listens for client
            print("Listening on port:", port)
            server_socket.settimeout(1)

            while True:
                address = None
                try:
                    if not client_socket:
                        client_socket, (address, _) = server_socket.accept()    #create client socket to communicate with the client.
                    data = None
                    try:
                        data: bytes = client_socket.recv(1024)      #receive message from client
                    except OSError:
                        pass
                    if not data:    #if connection fails, recreate client socket
                        client_socket.close()
                        client_socket, (address, _) = server_socket.accept()
                        continue

                    message: str = data.decode("utf-8")     #Receive the message from client
                    print(f"Received message: '{message}' from address {address}")

                    if message == '1':
                        last_3_hours = datetime.now(timezone.utc) - timedelta(hours=3)      #Calculate last the three hours timestamp
                        timestamp_string = last_3_hours.strftime("%Y-%m-%d %H:%M:%S")
                        cursor.execute(f"SELECT payload FROM \"Dataniz_virtual\" WHERE time > '{timestamp_string}' AND payload::text LIKE '%Fridge1_Arduino_Due%';")    #Select payload(sensor reading) from database of fridge1 of past 3 hours
                        record = cursor.fetchall()      #Execute the command
                        message = record.__str__()      #Return record in array
                        message = message[1:-2]
                        items = message.split('),')     #split the array
                        moisture = 0
                        for item in items:
                            item = item.strip().replace("'", '"')
                            item = item[1:-1]
                            json_data = json.loads(item)
                            try:
                                moisture += float(json_data["Moisture_Meter_Fridge1"])  # Extract moisture sensor reading and add to sum
                            except Exception:
                                pass
                        average_moisture = moisture / len(items)        #Calculate the average of moisture percentage
                        message = f"Average moisture inside your kitchen fridge in the past three hours is {average_moisture} RH%!"
                        client_socket.send(message.encode("utf-8"))         #Send back answer to the client
                        print(f"Sent message: '{message}' back to address {address} on port {port}")
                    elif message == '2':
                        cursor.execute(f"SELECT payload FROM \"Dataniz_virtual\" WHERE payload::text LIKE '%Dishwasher_RaspberryPi4%';")   #Select payload(sensor reading) from database of dishwasher without calculating the time
                        record = cursor.fetchall()
                        message = record.__str__()
                        message = message[1:-2]
                        items = message.split('),')
                        water_consumption = 0
                        for item in items:
                            item = item.strip().replace("'", '"')
                            item = item[1:-1]
                            json_data = json.loads(item)
                            try:
                                water_consumption += float(json_data["WaterConsumptionSensor"])
                            except Exception:
                                pass
                        average_water_consumption = water_consumption / len(items)
                        message = f"Average water consumption per cycle in your smart dishwasher is {average_water_consumption} gallons/min!"
                        client_socket.send(message.encode("utf-8"))
                        print(f"Sent message: '{message}' back to address {address} on port {port}")
                    elif message == '3':
                        cursor.execute(f"SELECT payload FROM \"Dataniz_virtual\" WHERE payload::text LIKE '%Fridge1_Arduino_Due%';")   #Select payload(sensor reading) from database of fridge1
                        record = cursor.fetchall()
                        message = record.__str__()
                        message = message[1:-2]
                        items = message.split('),')
                        fridge1_power = 0
                        min_time = math.inf
                        max_time = -math.inf
                        for item in items:
                            item = item.strip().replace("'", '"')
                            item = item[1:-1]
                            json_data = json.loads(item)
                            fridge1_resistance = float(json_data["Thermistor_Fridge1"])     #Extract thermistor sensor reading
                            fridge1_current = float(json_data["Ammeter_Fridge1"])           #Extract Ammeter sensor reading
                            fridge1_power += pow(fridge1_current, 2) * fridge1_resistance   #P = I^2 * R
                            min_time = min(min_time, int(json_data["timestamp"]))
                            max_time = max(max_time, int(json_data["timestamp"]))
                        elapsed_time = (max_time - min_time) / 3600                             #Calculate hour
                        fridge1_power = (fridge1_power / len(items) / 1000) * elapsed_time      #Calculate power in kWh

                        cursor.execute(
                            f"SELECT payload FROM \"Dataniz_virtual\" WHERE payload::text LIKE '%Fridge2_Arduino_Due%';")      #Select payload(sensor reading) from database of fridge2
                        record = cursor.fetchall()
                        message = record.__str__()
                        message = message[1:-2]
                        items = message.split('),')
                        fridge2_power = 0
                        min_time = math.inf
                        max_time = -math.inf
                        for item in items:
                            item = item.strip().replace("'", '"')
                            item = item[1:-1]
                            json_data = json.loads(item)
                            fridge2_resistance = float(json_data["Thermistor_Fridge2"])
                            fridge2_current = float(json_data["Ammeter_Fridge2"])
                            fridge2_power += pow(fridge2_current, 2) * fridge2_resistance
                            min_time = min(min_time, int(json_data["timestamp"]))
                            max_time = max(max_time, int(json_data["timestamp"]))
                        elapsed_time = (max_time - min_time) / 3600
                        fridge2_power = (fridge2_power / len(items) / 1000) * elapsed_time

                        cursor.execute(
                            f"SELECT payload FROM \"Dataniz_virtual\" WHERE payload::text LIKE '%Dishwasher_RaspberryPi4%';")      #Select payload(sensor reading) from database of dishwasher
                        record = cursor.fetchall()
                        message = record.__str__()
                        message = message[1:-2]
                        items = message.split('),')
                        dishwasher_power = 0
                        min_time = math.inf
                        max_time = -math.inf
                        for item in items:
                            item = item.strip().replace("'", '"')
                            item = item[1:-1]
                            json_data = json.loads(item)
                            dishwasher_water_flow = float(json_data["WaterConsumptionSensor"])
                            dishwasher_current = float(json_data["Ammeter_Dishwasher"])
                            dishwasher_resistance = 100 / dishwasher_water_flow                 #R = 100 / water flow
                            dishwasher_power += pow(dishwasher_current, 2) * dishwasher_resistance
                            min_time = min(min_time, int(json_data["timestamp"]))
                            max_time = max(max_time, int(json_data["timestamp"]))
                        elapsed_time = (max_time - min_time) / 3600
                        dishwasher_power = (dishwasher_power / len(items) / 1000) * elapsed_time

                        device = ""
                        power = 0
                        if fridge1_power > fridge2_power and fridge1_power > dishwasher_power:
                            device = "Smart_Refrigerator"
                            power = fridge1_power
                        elif fridge2_power > fridge1_power and fridge2_power > dishwasher_power:
                            device = "Smart_Refrigerator2"
                            power = fridge2_power
                        else:
                            device = "Smart_Dishwasher"
                            power = dishwasher_power
                        message = f"Your '{device}' consumed more electricity among your three IoT devices (two refrigerators and a dishwasher) at {power} kWh!"
                        client_socket.send(message.encode("utf-8"))
                        print(f"Sent message: '{message}' back to address {address} on port {port}")
                except socket.timeout:
                    continue
                except KeyboardInterrupt:
                    break
            if client_socket:
                client_socket.close()
        except psycopg2.Error as error:
            print("Error connecting to NeonDB:", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Connection closed.")



def main():
    global connection, cursor, client_socket
    pid = os.getpid()
    server = SocketListener()
    try:
        print("Server started. Press Ctrl-C to abort...")
        server.start()
        sleep(3600)
    except KeyboardInterrupt:
        print("Running server is interrupted by user. Exiting...")
        if client_socket:
            client_socket.close()
        if server_socket:
            server_socket.close()
            server_socket.shutdown(server_socket.SHUT_RDWR)
        os.kill(pid, 9)
    finally:
        if client_socket:
            client_socket.close()
        if server_socket:
            server_socket.close()
            server_socket.shutdown(server_socket.SHUT_RDWR)
        if connection and cursor:
            cursor.close()
            connection.close()
            print("Connection closed.")


main()
