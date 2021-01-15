"""
File containing functions used in covidstats project
"""
import zipfile
import os
import sys
import csv
from datetime import date, timedelta
from filehash import FileHash
import requests

def download_and_read(dir,zipurl,zipf,csvf):
    """
    Downloding and parsing COVID-19 data from https://www.data.gv.at/
    """
    try:
        resp = requests.get(zipurl, timeout=5)
        with open(zipf, "wb") as file:
            file.write(resp.content)
    except requests.exceptions.RequestException as e:
        print("Download of AGES zip file failed")
        raise SystemExit(e)

    with zipfile.ZipFile(zipf, 'r') as zipObj:
        for f in csvf:
            zipObj.extract(f, dir)

    #call checkhash function
    processflag = checkhash(dir,csvf)
    os.remove(zipf)

    return processflag

def checkhash(dir,csvf):
    """
    Check hash of file. Only do stuff with it if it has changed
    """
    sha512hasher = FileHash('sha512')
    process = {}
    for f in csvf:
        hashfile = f+".sha512"
        hashvalue = sha512hasher.hash_file(dir + "/" + f)
        if os.path.isfile(dir+"/"+hashfile):
            # file with hash value is present, compare hashes
            print("Hashfile ("+hashfile+") present")
            checksums = dict(sha512hasher.verify_checksums(dir+"/"+hashfile))
            for x, y in checksums.items():
                if x == dir+"/"+f:
                    if y:
                        #print("Hashes match, I have already seen this file")
                        flag = False
                    else:
                        print("Hashes do not match ... we need to process this file; updating hashfile as well")
                        writehashfile(dir,f,hashfile,hashvalue)
                        flag = True
        else:
            print("Hashfile not present, creating it ...")
            writehashfile(dir,f,hashfile,hashvalue)
            flag = True
        process[f] = flag
    return process

def writehashfile(dir,file,hashfile,hashvalue):
    """
    write hash value of covid csv file into file
    """
    with open(dir+"/"+hashfile, 'w') as hash_file:
        hash_file.write(hashvalue+' '+dir+"/"+file)
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

def parse_faelle_timeline_csv(dir,filename,bundeslaender):
    """
    function to read and parse CSV file
    """
    covid_data = {}
    yesterday = date.today() - timedelta(days=1)
    yesterday = yesterday.strftime('%d.%m.%Y 00:00:00')
    i = 0
    with open(dir+"/"+filename, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            if row["Time"] == yesterday:
                if row["Bundesland"] in bundeslaender:
                    i += 1
                    covid_data[row["Bundesland"]] = {}
                    covid_data[row["Bundesland"]]['Bundesland'] = row["Bundesland"]
                    covid_data[row["Bundesland"]]['BundeslandID'] = row["BundeslandID"]
                    covid_data[row["Bundesland"]]['AnzEinwohner'] = row["AnzEinwohner"]
                    covid_data[row["Bundesland"]]['AnzahlFaelle'] = row["AnzahlFaelle"]
                    covid_data[row["Bundesland"]]['AnzahlFaelleSum'] = row["AnzahlFaelleSum"]
                    covid_data[row["Bundesland"]]['AnzahlFaelle7Tage'] = row["AnzahlFaelle7Tage"]
                    covid_data[row["Bundesland"]]['SiebenTageInzidenzFaelle'] = row["SiebenTageInzidenzFaelle"]
                    covid_data[row["Bundesland"]]['AnzahlTotTaeglich'] = row["AnzahlTotTaeglich"]
                    covid_data[row["Bundesland"]]['AnzahlTotSum'] = row["AnzahlTotSum"]
                    covid_data[row["Bundesland"]]['AnzahlGeheiltTaeglich'] = row["AnzahlGeheiltTaeglich"]
                    covid_data[row["Bundesland"]]['AnzahlGeheiltSum'] = row["AnzahlGeheiltSum"]
                    covid_data[row["Bundesland"]]['Time'] = row["Time"]
    if i != len(bundeslaender):
        print("For date "+str(yesterday)+" we found "+str(i)+" records in provided CSV and not "+str(len(bundeslaender)))
        sys.exit("For date "+str(yesterday)+" we found "+str(i)+" records in provided CSV and not "+str(len(bundeslaender)))

    return covid_data

def cleanup(datafolder,csvf):
    """
    function to cleanup data dir
    """
    for f in csvf:
        os.remove(datafolder+"/"+f)
