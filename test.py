#!/usr/bin/python3
import time

import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, rc):
    print("CALLBACK: connected OK with result code " + str(rc))
    # client.subscribe("topic", qos = 1)
    # client.subscribe("/something/wildcard/+", 0)
    # client.subscribe("/something/else", 1)
    # client.subscribe([("/qos", 0), ("/something/else", 1)])

    client.publish(topic = "topic", payload = "test.py_qos0", qos = 0)
    client.publish(topic = "topic", payload = "test.py_qos1", qos = 1)
    # client.publish("/something/else", "abc", 1)
    # client.publish("/something/wildcard/else", "abc", 1)
    # client.publish("/something/wildcard/else/multi", "abc", 1)
    # client.publish("/something/else", "abc", 1, True)
    # client.disconnect()


def on_disconnect(client, userdata, mid):
    print("CALLBACK: disconnected OK")


def on_message(client, userdata, message):
    print("CALLBACK: Received message '" + str(message.payload) + "' on topic '" + message.topic + "' with QoS " + str(
        message.qos))


def on_publish(client, userdata, mid):
    print("CALLBACK: published OK")


def on_subscribe(client, userdata, mid, granted_qos):
    print("CALLBACK: subscribed OK: " + str(granted_qos))


def on_log(client, userdata, level, buf):
    print("LOG: " + buf)


def close(client):
    if client._ssl:
        client._ssl.close()
        client._ssl = None
        client._sock = None
    elif client._sock:
        client._sock.close()
        client._sock = None
    if client._sockpairR:
        client._sockpairR.close()
    if client._sockpairW:
        client._sockpairW.close()


client = mqtt.Client(client_id="python")
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_publish = on_publish
client.on_subscribe = on_subscribe
client.on_log = on_log
client.on_message = on_message
# client.username_pw_set("username", "password")
# client.will_set("/will/topic", "will message", 0)
client.connect("localhost", 1883, 60)
client.loop_forever()
