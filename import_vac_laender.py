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
from modules.utils import parse_vac_timeline_csv


def main():
    """
    main function; import historical data
    """
    debug.stdout("Startup import historical vaccination data to influxdb ...")
    config = configparser.ConfigParser()
    config.sections()
    config.read('coviddata.ini')

    parser = argparse.ArgumentParser(description='Options for historical vaccination data import',allow_abbrev=False)
    datafolder = config['opendata']['og_data_folder']
    bundeslaender = json.loads(config['ages']['bundeslaender'])
    filename = "timeline.csv"

    # add opts
    parser.add_argument('--from-date', help="Date to start import from (Format: yyyy.mm.dd) - this date is inclusive", default=False, action='store')
    parser.add_argument('--to-date', help="Date used to import to (Format: yyyy.mm.dd) - this date is inclusive", default=False, action='store')

    args = parser.parse_args()

    if not args.from_date:
        parser.error("Option --from-date needs to be set")

    if not args.to_date:
        parser.error("Option --to-date needs to be set")

    try:
        fromdate = datetime.strptime(args.from_date,"%Y-%m-%d").date()
        todate = datetime.strptime(args.to_date,"%Y-%m-%d").date()
    except ValueError as err:
        sys.exit(err)

    delta = todate - fromdate

    if os.path.isfile(datafolder+"/"+filename):
        print("Start processing file "+filename)
        print("I will import data for districts "+str(bundeslaender)+" between "+str(fromdate)+" and "+str(todate))

        for i in range(delta.days + 1):
            day = fromdate + timedelta(days=i)
            print("Parsing "+filename+" now with date "+str(day))
            covid_data = parse_vac_timeline_csv(datafolder,filename,bundeslaender,day)

            if config['debug']['debug'] == 'yes':
                debug.debug(covid_data)
            endpoint_influxdb.insert_influxdb(config,covid_data,'vac')

    else:
        sys.exit(filename+" does not exist in data folder - EXIT!")

    debug.stdout("Shutdown import historical vaccination data to influxdb ...")

if __name__ == "__main__":
    main()
