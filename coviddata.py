import requests, csv, configparser
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print("MQTT connection status: " + str(rc))

def print_row(row):
    print(row["Bezirk"],":",row["Anzahl"],":",row["Timestamp"])

def insert_mqtt(config,row):
    client = mqtt.Client()
    client.on_connect = on_connect

    try:
        client.connect(config['mqtt']['mqtthost'], int(config['mqtt']['mqttport']), int(config['mqtt']['mqttkeepalive']))
    except Exception as e:
        print("MQTT connection not possible")
        raise SystemExit(e)

    client.loop_start()

    client.publish(config['mqtt']['mqttpath']+str(row["Bezirk"]), row["Anzahl"])

def insert_influxdb(config,row):
    print("calling insert_influxdb")
    try:
        client = InfluxDBClient(host=config['influxdb']['influxdbhost'], port=config['influxdb']['influxdbport'], username=config['influxdb']['influxdbuser'], password=config['influxdb']['influxdbpassword'])
    except Exception as e:
        print("InfluxDB connection not possible")
        raise SystemExit(e)
    client.switch_database(config['influxdb']['influxdbdb'])

def main():
    config = configparser.ConfigParser()
    config.sections()
    config.read('coviddata.ini')

    url = config['opendata']['csvurl']
    bezirke = config['opendata']['bezirke']

    with requests.Session() as s:
        
        try:
            download = s.get(url)
            decoded_content = download.content.decode('utf-8')
        except requests.exceptions.RequestException as e:
            print("Download of CSV file failed")
            raise SystemExit(e)
        
        try:
            csv_reader = csv.DictReader(decoded_content.splitlines(), delimiter=';')
        except csv.Error as e:
            raise SystemExit(e)

        for row in csv_reader:
            if row["Bezirk"] in bezirke:
                if config['debug']['debug'] == 'yes':
                    print_row(row)
                if config['mqtt']['usemqtt'] == 'yes':
                    insert_mqtt(config,row)
                if config['influxdb']['useinfluxdb'] == 'yes':
                    insert_influxdb(config,row)

if __name__ == "__main__": 
	main()
