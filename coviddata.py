"""
Downloding and parsing COVID-19 data from https://www.data.gv.at/
"""
import csv
import configparser
import requests
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import modules.debug_logging as debug_logging
import datetime

def on_connect(client, userdata, flags, rc):
    """
    function to connect to mqtt
    """
    if rc != 0:
        print("MQTT connection status: " + str(rc) + str(client) + str(userdata) + str(flags))

def insert_mqtt(config,row):
    """
    insert covid-19 data into mqtt
    """
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
    """
    insert covid-19 data into influxdb
    """
    #converting timestamp (as in csv) to milliseconds to insert into influxdb
    date_time_obj = datetime.datetime.strptime(row["Timestamp"], '%Y-%m-%dT%H:%M:%S').strftime('%s.%f')
    date_time_obj_in_ns = int(float(date_time_obj)*1000*1000*1000)

    data = []
    data.append("{measurement},type=cases {district}={cases} {timestamp}"
                    .format(measurement="covid",
                    district=row["Bezirk"],
                    cases=row["Anzahl"],
                    timestamp=date_time_obj_in_ns,
                    ))
    try:
        client = InfluxDBClient(host=config['influxdb']['influxdbhost'], port=config['influxdb']['influxdbport'], username=config['influxdb']['influxdbuser'], password=config['influxdb']['influxdbpassword'])
    except Exception as e:
        print("InfluxDB connection not possible")
        raise SystemExit(e)
    client.write_points(data, database=config['influxdb']['influxdbdb'], protocol='line')

def main():
    """
    main function
    """
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
                    debug_logging.print_row(row)
                if config['mqtt']['usemqtt'] == 'yes':
                    insert_mqtt(config,row)
                if config['influxdb']['useinfluxdb'] == 'yes':
                    insert_influxdb(config,row)

if __name__ == "__main__":
    main()
