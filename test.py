import paho.mqtt.client as mqtt


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))


client = mqtt.Client(client_id="abc")
client.on_connect = on_connect
client.connect("localhost", 1883, 60)
client.loop_forever()
