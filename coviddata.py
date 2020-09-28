import requests, csv, configparser
import paho.mqtt.client as mqtt

def validate_config(config):
    if not 'opendata' or not 'covid' or not 'mqtt' in config:
        print('opendata or covid or mqtt section missing from config file - exit')
        exit(-1)
    
    if not str(config['covid']['bezirke']) or not str(config['opendata']['csvurl']):
        print('Configuration for Bezirke or CSVURL is not correct or missing - exit')
        exit(-1)

def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print("MQTT connection status: " + str(rc))

def insert_mqtt(config,row):
    client = mqtt.Client()
    client.on_connect = on_connect

    try:
        client.connect(config['mqtt']['mqtthost'], int(config['mqtt']['mqttport']), int(config['mqtt']['mqttkeepalive']))
    except Exception as e:
        print("MQTT connection not possible")
        raise SystemExit(e)

    client.loop_start()

    client.publish("health/covid/anzahl/"+str(row["Bezirk"]), row["Anzahl"])

def insert_influxdb(config,row):
    print("insert_influxdb, tbd")

def print_std(row):
    print(row["Bezirk"],":",row["Anzahl"],":",row["Timestamp"])

def main():
    config = configparser.ConfigParser()
    config.sections()
    config.read('coviddata.ini')

    validate_config(config)

    url = config['opendata']['csvurl']
    bezirke = config['covid']['bezirke']

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
                print_std(row)
                insert_mqtt(config,row)
                #insert_influxdb(config,row)

if __name__ == "__main__": 
	main()
