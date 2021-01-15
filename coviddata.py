"""
Downloding and parsing COVID-19 data from https://www.data.gv.at/
"""
import configparser
import json
import modules.debug as debug
import modules.endpoint_mqtt as endpoint_mqtt
import modules.endpoint_influxdb as endpoint_influxdb
from modules.utils import download_and_read, parse_faelle_csv, parse_faelle_timeline_csv, cleanup

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
    bundeslaender = json.loads(config['ages']['bundeslaender'])
    datafolder = config['ages']['data_folder']
    zipf = config['ages']['zipf']
    csvf = json.loads(config['ages']['csvf'])

    #download and get csv data
    processflag = download_and_read(datafolder,zipurl,zipf,csvf)

    for name, status in processflag.items():
        if status:
            print("We need to process "+name+" as this is a new file."+str(processflag))
            print("Start parsing file: "+name+" now")
            if name == 'CovidFaelle_GKZ.csv':
                covid_data = parse_faelle_csv(datafolder,name,bezirke)
            if name == 'CovidFaelle_Timeline.csv':
                covid_data = parse_faelle_timeline_csv(datafolder,name,bundeslaender)

            if config['debug']['debug'] == 'yes':
                debug.debug(covid_data)
            if config['mqtt']['usemqtt'] == 'yes':
                endpoint_mqtt.insert_mqtt(config,covid_data)
            if config['influxdb']['useinfluxdb'] == 'yes':
                endpoint_influxdb.insert_influxdb(config,covid_data)

        else:
            print("No need to parse "+name+". Hashes match, I have already seen this file. Status of flag: "+str(processflag))

    #cleanup
    cleanup(datafolder,csvf)

    debug.stdout("covidstats application shutdown ...")

if __name__ == "__main__":
    main()
