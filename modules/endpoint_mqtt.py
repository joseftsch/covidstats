"""
submodule for inserting data into MQTT
"""
from datetime import datetime
import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, rc):
    """
    function to connect to mqtt
    """
    if rc != 0:
        print("MQTT connection status: " + str(rc) + str(client) + str(userdata) + str(flags))

def insert_mqtt(config,covid_data):
    """
    insert covid-19 data into mqtt
    """
    client = mqtt.Client()
    client.on_connect = on_connect

    now = datetime.now()
    dt_string = now.strftime("%Y/%m/%d %H:%M:%S")

    mqtthost = config['mqtt']['mqtthost']
    mqttport = int(config['mqtt']['mqttport'])
    mqttkeepalive = int(config['mqtt']['mqttkeepalive'])
    retain = bool(config['mqtt']['retain'])
    qos = int(config['mqtt']['qos'])

    try:
        client.connect(mqtthost, mqttport, mqttkeepalive)
    except Exception as e:
        print("MQTT connection not possible")
        raise SystemExit(e)

    client.loop_start()

    for id, info in covid_data.items():
        for key in info:
            client.publish(config['mqtt']['mqttpath']+str(covid_data[id]['Bezirk'])+"/"+key, info[key], qos=qos, retain=retain)
        client.publish(config['mqtt']['mqttpath']+str(covid_data[id]['Bezirk'])+"/"+"updated", dt_string, qos=qos, retain=retain)
