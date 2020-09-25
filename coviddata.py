import requests, csv, configparser
import paho.mqtt.client as mqtt

config = configparser.ConfigParser()
config.sections()
config.read('coviddata.ini')

url = config['opendata']['csvurl']
bezirke = config['covid']['bezirke']

def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print("Connected not successfull - error code is " + str(rc))

client = mqtt.Client()
client.on_connect = on_connect

client.connect(config['mqtt']['mqtthost'], int(config['mqtt']['mqttport']), int(config['mqtt']['mqttkeepalive']))
client.loop_start()

def main():
    with requests.Session() as s:
        download = s.get(url)

        decoded_content = download.content.decode('utf-8')

        csv_reader = csv.DictReader(decoded_content.splitlines(), delimiter=';')
        for row in csv_reader:
            #print(row["Bezirk"],":",row["Anzahl"])
            if row["Bezirk"] in bezirke:
                client.publish("health/covid/anzahl/"+str(row["Bezirk"]), row["Anzahl"])
                print(row["Bezirk"],":",row["Anzahl"],":",row["Timestamp"])

 
if __name__ == "__main__": 
	main()