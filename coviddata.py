import requests, csv, configparser
import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, rc):
    print("MQTT connection status: " + str(rc))

def validate_config(config):
    if not 'opendata' or not 'covid' or not 'mqtt' in config:
        print('opendata or covid or mqtt section missing from config file - exit')
        exit(-1)
    
    if not str(config['covid']['bezirke']) or not str(config['opendata']['csvurl']):
        print('Configuration for Bezirke or CSVURL is not correct or missing - exit')
        exit(-1)

def main():
    config = configparser.ConfigParser()
    config.sections()
    config.read('coviddata.ini')

    validate_config(config)

    url = config['opendata']['csvurl']
    bezirke = config['covid']['bezirke']

    client = mqtt.Client()
    client.on_connect = on_connect

    try:
        client.connect(config['mqtt']['mqtthost'], int(config['mqtt']['mqttport']), int(config['mqtt']['mqttkeepalive']))
    except Exception as e:
        print("MQTT connection not possible")
        raise SystemExit(e)

    client.loop_start()

    with requests.Session() as s:
        
        try:
            download = s.get(url)
            decoded_content = download.content.decode('utf-8')
        except requests.exceptions.RequestException as e:
            print("Download of CSV file failed")
            raise SystemExit(e)
        
        csv_reader = csv.DictReader(decoded_content.splitlines(), delimiter=';')
        for row in csv_reader:
            #print(row["Bezirk"],":",row["Anzahl"])
            if row["Bezirk"] in bezirke:
                client.publish("health/covid/anzahl/"+str(row["Bezirk"]), row["Anzahl"])
                print(row["Bezirk"],":",row["Anzahl"],":",row["Timestamp"])

if __name__ == "__main__": 
	main()