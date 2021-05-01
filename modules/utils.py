"""
File containing functions used in covidstats project
"""
import zipfile
import os
import sys
import csv
import glob
from datetime import date, timedelta
import re
from filehash import FileHash
import requests

def og_download(og_base_url,og_csv_files,og_data_folder):
    """
    Downloding and parsing vaccination data from https://info.gesundheitsministerium.gv.at/
    """
    #create data directory if not exists
    if not os.path.exists(og_data_folder):
        os.makedirs(og_data_folder)

    for cfile in og_csv_files:
        try:
            r = requests.get(og_base_url+cfile, timeout=4)
            with open('{}/{}'.format(og_data_folder,cfile), 'wb') as f:
                f.write(r.content)
        except requests.exceptions.RequestException as e:
            print("Download of "+str(cfile)+" from gesundheitsministerium failed")
            raise SystemExit(e)

    processflag = checkhash(og_data_folder,og_csv_files)

    return processflag

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

def parse_vac_timeline_eimpfpass_csv(og_data_folder,name,bundeslaender,day=False):
    """
    function to read and parse CSV file for vaccine data - bundeslaender
    """
    covid_data = {}
    if day:
        today = date.today()
        today = today.strftime(str(day))
    else:
        today = date.today() - timedelta(days=1)
        today = today.strftime('%Y-%m-%d')
    print("Using the following date for parse_vac_timeline_eimpfpass_csv:"+ str(today))
    regex = r'(\d+-\d+-\d+)\w(\d+\:\d+\:\d+)'
    i = 0
    with open(og_data_folder+"/"+name, newline='', encoding='ISO-8859-1') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            if today in row["Datum"]:
                if row["Name"] in bundeslaender:
                    i += 1
                    covid_data[row["Name"]] = {}
                    covid_data[row["Name"]]['BundeslandID'] = row["BundeslandID"]
                    covid_data[row["Name"]]['Name'] = row["Name"]
                    covid_data[row["Name"]]['Bevölkerung'] = row["Bevölkerung"]
                    covid_data[row["Name"]]['EingetrageneImpfungen'] = row["EingetrageneImpfungen"]
                    covid_data[row["Name"]]['Teilgeimpfte'] = row["Teilgeimpfte"]
                    covid_data[row["Name"]]['Vollimmunisierte'] = row["Vollimmunisierte"]
                    covid_data[row["Name"]]['Gu25_1'] = str(int(row["Gruppe_<25_M_1"])+int(row["Gruppe_<25_W_1"])+int(row["Gruppe_<25_D_1"]))
                    covid_data[row["Name"]]['Gu25_2'] = str(int(row["Gruppe_<25_M_2"])+int(row["Gruppe_<25_W_2"])+int(row["Gruppe_<25_D_2"]))
                    covid_data[row["Name"]]['G25-34_1'] = str(int(row["Gruppe_25-34_M_1"])+int(row["Gruppe_25-34_W_1"])+int(row["Gruppe_25-34_D_1"]))
                    covid_data[row["Name"]]['G25-34_2'] = str(int(row["Gruppe_25-34_M_2"])+int(row["Gruppe_25-34_W_2"])+int(row["Gruppe_25-34_D_2"]))
                    covid_data[row["Name"]]['G35-44_1'] = str(int(row["Gruppe_35-44_M_1"])+int(row["Gruppe_35-44_W_1"])+int(row["Gruppe_35-44_D_1"]))
                    covid_data[row["Name"]]['G35-44_2'] = str(int(row["Gruppe_35-44_M_2"])+int(row["Gruppe_35-44_W_2"])+int(row["Gruppe_35-44_D_2"]))
                    covid_data[row["Name"]]['G45-54_1'] = str(int(row["Gruppe_45-54_M_1"])+int(row["Gruppe_45-54_W_1"])+int(row["Gruppe_45-54_D_1"]))
                    covid_data[row["Name"]]['G45-54_2'] = str(int(row["Gruppe_45-54_M_2"])+int(row["Gruppe_45-54_W_2"])+int(row["Gruppe_45-54_D_2"]))
                    covid_data[row["Name"]]['G55-64_1'] = str(int(row["Gruppe_55-64_M_1"])+int(row["Gruppe_55-64_W_1"])+int(row["Gruppe_55-64_D_1"]))
                    covid_data[row["Name"]]['G55-64_2'] = str(int(row["Gruppe_55-64_M_2"])+int(row["Gruppe_55-64_W_2"])+int(row["Gruppe_55-64_D_2"]))
                    covid_data[row["Name"]]['G65-74_1'] = str(int(row["Gruppe_65-74_M_1"])+int(row["Gruppe_65-74_W_1"])+int(row["Gruppe_65-74_D_1"]))
                    covid_data[row["Name"]]['G65-74_2'] = str(int(row["Gruppe_65-74_M_2"])+int(row["Gruppe_65-74_W_2"])+int(row["Gruppe_65-74_D_2"]))
                    covid_data[row["Name"]]['G75-84_1'] = str(int(row["Gruppe_75-84_M_1"])+int(row["Gruppe_75-84_W_1"])+int(row["Gruppe_75-84_D_1"]))
                    covid_data[row["Name"]]['G75-84_2'] = str(int(row["Gruppe_75-84_M_2"])+int(row["Gruppe_75-84_W_2"])+int(row["Gruppe_75-84_D_2"]))
                    covid_data[row["Name"]]['Gg84_1'] = str(int(row["Gruppe_>84_M_1"])+int(row["Gruppe_>84_W_1"])+int(row["Gruppe_>84_D_1"]))
                    covid_data[row["Name"]]['Gg84_2'] = str(int(row["Gruppe_>84_M_2"])+int(row["Gruppe_>84_W_2"])+int(row["Gruppe_>84_D_2"]))
                    covid_data[row["Name"]]['EingetrageneImpfungenBioNTechPfizer_1'] = row["EingetrageneImpfungenBioNTechPfizer_1"]
                    covid_data[row["Name"]]['EingetrageneImpfungenModerna_1'] = row["EingetrageneImpfungenModerna_1"]
                    covid_data[row["Name"]]['EingetrageneImpfungenAstraZeneca_1'] = row["EingetrageneImpfungenAstraZeneca_1"]
                    covid_data[row["Name"]]['EingetrageneImpfungenBioNTechPfizer_2'] = row["EingetrageneImpfungenBioNTechPfizer_2"]
                    covid_data[row["Name"]]['EingetrageneImpfungenModerna_2'] = row["EingetrageneImpfungenModerna_2"]
                    covid_data[row["Name"]]['EingetrageneImpfungenAstraZeneca_2'] = row["EingetrageneImpfungenAstraZeneca_2"]
                    covid_data[row["Name"]]['EingetrageneImpfungenBioNTechPfizer_G'] = str(int(row["EingetrageneImpfungenBioNTechPfizer_1"])+int(row["EingetrageneImpfungenBioNTechPfizer_2"]))
                    covid_data[row["Name"]]['EingetrageneImpfungenModerna_G'] = str(int(row["EingetrageneImpfungenModerna_1"])+int(row["EingetrageneImpfungenModerna_2"]))
                    covid_data[row["Name"]]['EingetrageneImpfungenAstraZeneca_G'] = str(int(row["EingetrageneImpfungenAstraZeneca_1"])+int(row["EingetrageneImpfungenAstraZeneca_2"]))
                    covid_data[row["Name"]]['EingetrageneImpfungenJanssen'] = row["EingetrageneImpfungenJanssen"]
                    m = re.search(regex, row["Datum"])
                    covid_data[row["Name"]]['Timestamp'] = m.group(1)+" "+m.group(2)
                    covid_data[row["Name"]]['Datum'] = m.group(1)
                    covid_data[row["Name"]]['Datum08'] = str(m.group(1))+" 08:00:00"
    if i != len(bundeslaender):
        print("We found "+str(i)+" records in provided CSV and not "+str(len(bundeslaender)))
        sys.exit("We found "+str(i)+" records in provided CSV and not "+str(len(bundeslaender)))
    return covid_data

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

def cleanup(datafolder):
    """
    function to cleanup data dir
    """
    pattern=glob.glob(datafolder+'/*.csv', recursive=True)
    for f in pattern:
        try:
            os.remove(f)
        except OSError:
            pass

def notification(config,msg):
    """
    function to send notification
    """
    if config['notification']['notification_enabled'] == 'yes':
        url = config['notification']['notification_url']
        recipient = config['notification']['notification_recipient']
        sender = config['notification']['notification_sender']
        headers = { 'Content-Type': 'application/json'}
        payload="{\"message\": \""+msg+"\", \"number\": \""+sender+"\", \"recipients\": [\""+recipient+"\"]}"
        try:
            r = requests.post(url, headers=headers, data=payload)
        except requests.exceptions.RequestException as e:
            print("Error during sending of Signal notification: {}".format(str(e)))
