#!/usr/local/bin/python3
import sys

import paho.mqtt.client as mqtt
import time

def on_connect(client, userdata, flags, rc):
    print("CALLBACK: connected OK with result code " + str(rc))


def on_disconnect(client, userdata, mid):
    print("CALLBACK: disconnected OK")


def on_message(client, userdata, message):
    print("CALLBACK: Received message '" + str(message.payload) + "' on topic '" + message.topic + "' with QoS " + str(message.qos))


def on_publish(client, userdata, mid):
    print("CALLBACK: published OK")


def on_subscribe(client, userdata, mid, granted_qos):
    print("CALLBACK: subscribed OK: " + str(granted_qos))


def on_log(client, userdata, level, buf):
    print("LOG: " + buf)


client = mqtt.Client(client_id="python_invalid")
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_publish = on_publish
client.on_subscribe = on_subscribe
client.on_log = on_log
client.on_message = on_message
client.connect("localhost", 1883, 60)
client.subscribe("topic", qos=1)
client.loop_forever()

# sys.exit(1)
