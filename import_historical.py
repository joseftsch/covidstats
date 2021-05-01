"""
This script allows to import historical covid data
"""
import csv
import configparser
import argparse
import os
import sys
import json
from datetime import datetime, timedelta
import modules.debug as debug
import modules.endpoint_influxdb as endpoint_influxdb

def parse_faelle_timeline_csv(datafolder,filename,bundeslaender,importdate):
    """
    function to read and parse CSV file
    """
    covid_data = {}
    i = 0

    with open(datafolder+"/"+filename, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            #print(str(importdate.strftime("%d.%m.%Y 00:00:00")))
            if row["Time"] == str(importdate.strftime("%d.%m.%Y 00:00:00")):
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
    if i == 0:
        print("Nothing was found in "+filename+" for date "+str(importdate))
        sys.exit('Not all Bundeslaender found in file')

    if i != len(bundeslaender):
        print("We did not find data for each district in the file - doing no import at all")
        sys.exit('We did not find data for each district in the file - doing no import at all')

    return covid_data

def main():
    """
    main function; import historical data
    """
    debug.stdout("Startup import historical covidstats to influxdb ...")
    config = configparser.ConfigParser()
    config.sections()
    config.read('coviddata.ini')

    parser = argparse.ArgumentParser(description='Options for historical data import',allow_abbrev=False)
    datafolder = config['ages']['data_folder']
    bundeslaender = json.loads(config['ages']['bundeslaender'])
    filename = "CovidFaelle_Timeline.csv"

    # add opts
    parser.add_argument('--from-date', help="Date to start import from (Format: dd.mm.yyyy) - this date is inclusive", default=False, action='store')
    parser.add_argument('--to-date', help="Date used to import to (Format: dd.mm.yyyy) - this date is inclusive", default=False, action='store')

    parser.print_help()

    args = parser.parse_args()

    if not args.from_date:
        parser.error("Option --from-date needs to be set")

    if not args.to_date:
        parser.error("Option --to-date needs to be set")

    try:
        fromdate = datetime.strptime(args.from_date,"%d.%m.%Y")
        todate = datetime.strptime(args.to_date,"%d.%m.%Y")
    except ValueError as err:
        sys.exit(err)

    delta = todate - fromdate

    if os.path.isfile(datafolder+"/"+filename):
        print("Start processing file "+filename)
        print("I will import data for districts "+str(bundeslaender)+" between "+str(fromdate)+" and "+str(todate))

        for i in range(delta.days + 1):
            day = fromdate + timedelta(days=i)
            print("Parsing "+filename+" now with date "+str(day))
            covid_data = parse_faelle_timeline_csv(datafolder,filename,bundeslaender,day)

            if config['debug']['debug'] == 'yes':
                debug.debug(covid_data)
            endpoint_influxdb.insert_influxdb(config,covid_data,'cases')

    else:
        sys.exit(filename+" does not exist in data folder - EXIT!")

    debug.stdout("Shutdown import historical covidstats to influxdb ...")

if __name__ == "__main__":
    main()
