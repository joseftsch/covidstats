"""
Downloding and parsing COVID-19 data from https://www.data.gv.at/
"""
import csv
import configparser
import os
import zipfile
import json
import sys
import requests
import modules.debug as debug
import modules.endpoint_mqtt as endpoint_mqtt
import modules.endpoint_influxdb as endpoint_influxdb

def download_and_read(dir,zipurl):
    """
    Downloding and parsing COVID-19 data from https://www.data.gv.at/
    """
    try:
        resp = requests.get(zipurl)
        with open("data.zip", "wb") as file:
            file.write(resp.content)
    except requests.exceptions.RequestException as e:
        print("Download of AGES zip file failed")
        raise SystemExit(e)

    with zipfile.ZipFile("data.zip", 'r') as zipObj:
        zipObj.extract('CovidFaelle_GKZ.csv', dir)

    os.remove("data.zip")

def parse_faelle_csv(dir,filename,bezirke):
    """
    function to read and parse CSV file
    """
    covid_data = {}
    i = 0
    with open(dir+"/"+filename, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            if row["Bezirk"] in bezirke:
                i += 1
                covid_data[row["Bezirk"]] = {}
                covid_data[row["Bezirk"]]['Bezirk'] = row["Bezirk"]
                covid_data[row["Bezirk"]]['Einwohner'] = row["AnzEinwohner"]
                covid_data[row["Bezirk"]]['Faelle'] = row["Anzahl"]
                covid_data[row["Bezirk"]]['AnzahlTot'] = row["AnzahlTot"]
                covid_data[row["Bezirk"]]['AnzahlFaelle7Tage'] = row["AnzahlFaelle7Tage"]
                covid_data[row["Bezirk"]]['GKZ'] = row["GKZ"]
    if i != len(bezirke):
        print("Not all districts are returned from AGES in CVS")
        sys.exit('Not all districts are returned from AGES in CVS')

    return covid_data

def cleanup(datafolder):
    """
    function to cleanup data dir
    """
    os.remove(datafolder+"/"+"CovidFaelle_GKZ.csv")

def main():
    """
    main function
    """
    debug.stdout("covidstats application startup ...")
    config = configparser.ConfigParser()
    config.sections()
    config.read('coviddata.ini')

    zipurl = config['ages']['ages_zip_url']
    bezirke = json.loads(config['ages']['bezirke'])
    datafolder = config['ages']['data_folder']

    #download and get csv data
    download_and_read(datafolder,zipurl)

    # parse downloaded file
    covid_data = parse_faelle_csv(datafolder,"CovidFaelle_GKZ.csv",bezirke)

    if config['debug']['debug'] == 'yes':
        debug.debug(covid_data)
    if config['mqtt']['usemqtt'] == 'yes':
        endpoint_mqtt.insert_mqtt(config,covid_data)
    if config['influxdb']['useinfluxdb'] == 'yes':
        endpoint_influxdb.insert_influxdb(config,covid_data)

    #cleanup
    cleanup(datafolder)

    debug.stdout("covidstats application shutdown ...")

if __name__ == "__main__":
    main()
