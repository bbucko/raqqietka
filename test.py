import paho.mqtt.client as mqtt
import time


def on_connect(client, userdata, flags, rc):
    print("CALLBACK: connected OK with result code " + str(rc))


def on_disconnect(client, userdata, mid):
    print("CALLBACK: disconnected OK")


def on_publish(client, userdata, mid):
    print("CALLBACK: published OK")


def on_subscribe(client, userdata, mid, granted_qos):
    print("CALLBACK: subscribed OK: " + str(granted_qos))


def on_log(client, userdata, level, buf):
    print("LOG: " + buf)


def send_and_sleep():
    client.loop()
    client.loop()
    client.loop()
    time.sleep(1)


client = mqtt.Client(client_id="abc")
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_publish = on_publish
client.on_subscribe = on_subscribe
client.on_log = on_log
client.username_pw_set("username", "password")
client.will_set("/will/topic", "will message", 0)

client.connect("localhost", 1883, 60)
send_and_sleep()

client.subscribe("/something", 0)
send_and_sleep()

client.subscribe("/something/else", 1)
send_and_sleep()

client.subscribe([("/qos", 0), ("/something/else", 1)])
send_and_sleep()

client.publish("/something", "abc", 0)
send_and_sleep()

client.publish("/something/else", "abc", 1)
send_and_sleep()

client.publish("/something/else", "abc", 1, True)
send_and_sleep()

client.loop_forever()

# client.disconnect()
# send_and_sleep()
