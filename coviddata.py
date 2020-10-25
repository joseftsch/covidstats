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
from filehash import FileHash
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

    #call checkhash function
    processflag = checkhash(dir,"CovidFaelle_GKZ.csv","hashes.sha512")
    os.remove("data.zip")

    return processflag

def checkhash(dir,file,hashfile):
    """
    Check hash of CovidFaelle_GKZ.csv file. Only do stuff with it if it has changed
    """
    sha512hasher = FileHash('sha512')
    hash = sha512hasher.hash_file(dir + "/" + file)
    if os.path.isfile(dir+"/"+hashfile):
        # file with hash value is present, compare hashes
        print("Hashfile present")
        checksums = dict(sha512hasher.verify_checksums(dir+"/"+hashfile))
        for x, y in checksums.items():
            if x == dir+"/"+file:
                if y:
                    #print("Hashes match, I have already seen this file")
                    process = False
                else:
                    print("Hashes do not match ... we need to process this file; updating hashfile as well")
                    writehashfile(dir,file,hashfile,hash)
                    process = True
    else:
        print("Hashfile not present, creating it ...")
        writehashfile(dir,file,hashfile,hash)
        process = True
    return process

def writehashfile(dir,file,hashfile,hash):
    """
    write hash value of covid csv file into file
    """
    with open(dir+"/"+hashfile, 'w') as hash_file:
        hash_file.write(hash+' '+dir+"/"+file)
        hash_file.close()

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
    processflag = download_and_read(datafolder,zipurl)

    #check status of returned processflag if we continue operation or not
    if processflag:
        print("Continue operation as this is a new file to process. Status of flag: "+str(processflag))

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
    else:
        print("Stop operation - Hashes match, I have already seen this file. Status of flag: "+str(processflag))


    debug.stdout("covidstats application shutdown ...")

if __name__ == "__main__":
    main()
