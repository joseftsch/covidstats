"""
Downloding and parsing COVID-19 data from https://www.data.gv.at/
"""
import configparser
import json
import modules.debug as debug
import modules.endpoint_mqtt as endpoint_mqtt
import modules.endpoint_influxdb as endpoint_influxdb
from modules.utils import download_and_read, parse_faelle_csv, parse_faelle_timeline_csv, cleanup, og_download, parse_vac_timeline_eimpfpass_csv, notification
def main():
    """
    main function
    """
    debug.stdout("covidstats application startup ...")
    config = configparser.ConfigParser()
    config.sections()
    config.read('coviddata.ini')

    # log
    logname = config['log']['name']
    logfolder = config['log']['log_folder']

    #AGES Data
    zipurl = config['ages']['ages_zip_url']
    bezirke = json.loads(config['ages']['bezirke'])
    bundeslaender = json.loads(config['ages']['bundeslaender'])
    datafolder = config['ages']['data_folder']
    zipf = config['ages']['zipf']
    csvf = json.loads(config['ages']['csvf'])

    #Opendata Data
    og_base_url = config['opendata']['od_base_url']
    og_csv_files = json.loads(config['opendata']['og_csv_files'])
    og_data_folder = config['opendata']['og_data_folder']

    #download and get csv data - AGES
    ages_processflag = download_and_read(datafolder,zipurl,zipf,csvf)

    #download and get csv data - OpenData
    og_processflag = og_download(og_base_url,og_csv_files,og_data_folder)

    #process ages csv data
    for name, status in ages_processflag.items():
        if status:
            print("We need to process "+name+" as this is a new file."+str(ages_processflag))
            print("Start parsing file: "+name+" now")
            notification(config,"covidstats: Parsing file: "+name)
            if name == 'CovidFaelle_GKZ.csv':
                covid_data = parse_faelle_csv(datafolder,name,bezirke)
            if name == 'CovidFaelle_Timeline.csv':
                covid_data = parse_faelle_timeline_csv(datafolder,name,bundeslaender)

            if config['debug']['debug'] == 'yes':
                debug.debug(covid_data)
            if config['mqtt']['usemqtt'] == 'yes':
                endpoint_mqtt.insert_mqtt(config,covid_data,'cases')
            if config['influxdb']['useinfluxdb'] == 'yes':
                endpoint_influxdb.insert_influxdb(config,covid_data,'cases')
        else:
            print("No need to parse "+name+". Hashes match, I have already seen this file. Status of flag: "+str(ages_processflag))

    #process opendata csv data
    for name, status in og_processflag.items():
        if status:
            print("We need to process "+name+" as this is a new file."+str(og_processflag))
            print("Start parsing file: "+name+" now")
            if name == 'timeline-eimpfpass.csv':
                covid_data = parse_vac_timeline_eimpfpass_csv(og_data_folder,name,bundeslaender)
            if config['debug']['debug'] == 'yes':
                debug.debug(covid_data)
            if config['mqtt']['usemqtt'] == 'yes':
                endpoint_mqtt.insert_mqtt(config,covid_data,'vac')
            if config['influxdb']['useinfluxdb'] == 'yes':
                endpoint_influxdb.insert_influxdb(config,covid_data,'vac')
            notification(config,"covidstats: Parsing file: "+name)
        else:
            print("No need to parse "+name+". Hashes match, I have already seen this file. Status of flag: "+str(og_processflag))

    #cleanup
    cleanup(datafolder)
    cleanup(og_data_folder)

    debug.stdout("covidstats application shutdown ...")

if __name__ == "__main__":
    main()
