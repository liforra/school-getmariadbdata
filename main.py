#!/usr/bin/python
import mariadb
import sys
import json
import logging
import datetime, time
import paho.mqtt.client as mqtt
#from paho import client as client

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)



def connect_mqtt():
    port = 1883
    keep_alive = 60
    ip = "172.20.10.4"
    
    logging.info(f'Connecting to MQTT Server at {ip}:{str(port)} with {str(keep_alive)} Seconds KeepAlive.')
    client = mqtt.Client()
    client.connect(ip, port, keep_alive)
    return client


#Nachricht wird  empfangen


def connect_db():
    try:
        logging.info('Connecting to the Database.')
        conn = mariadb.connect(
            user="root",
            password="Passwort123",
            host="172.20.10.4",
            port=3306,
            database="main",
        )
        logging.info('Successfully Connected')
        return conn
    except mariadb.Error as e:
        logging.error(f"Connection Failed: {e}")
        return e



def write(data:str, conn:object): 
    logging.info(f'Converting into Data into JSON: {data}')
    cur = conn.cursor()
    try:
        data = json.loads(data)
    except json.decoder.JSONDecodeError:
        logging.warning(f'Recieved Non-JSON Data. Ignoring... {data}')
        return 1
    timestamp = datetime.datetime.fromtimestamp(int(data["timestamp"]), tz=datetime.timezone.utc).replace(tzinfo=None)
    logging.info(f'Sending Data into Database: {data}')
    try:
        cur.execute(
            "INSERT INTO Sensor_Daten (Wert, Uhrzeit, Sensor_ID) VALUES (?, ?, ?)",
            (float(data["value"]), timestamp, int(data["id"])),
        )
    except mariadb.IntegrityError as e:
        if str(e).startswith('Duplicate'):
            logging.warning(f'Duplicate Time-Entry... Not Writing to DB')
            return 2
        else:
            logging.warning(f'Integrity Error from Database {e}... Not Writing to DB')
            return 3
    conn.commit()

def on_message(client, userdata, msg):
    message = msg.payload.decode()
    logging.info(f"Recieved Message: {message} on topic: {msg.topic}")
    write(message, connect_db())


#connection = connect_db()
client = connect_mqtt()
client.on_message = on_message
logging.info("Subscribing to MQTT Topic")
client.subscribe("test")
logging.debug("Starting Loop")
client.loop_forever()
#write('{"id":"4","value":"4563","timestamp":"2352652"}', connection)