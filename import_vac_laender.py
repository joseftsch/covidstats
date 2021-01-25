"""
This script allows to import historical vaccination data from laender.csv provided by opendata
"""
import configparser
import argparse
import json
import os
import sys
import modules.debug as debug
import modules.endpoint_mqtt as endpoint_mqtt
import modules.endpoint_influxdb as endpoint_influxdb
from modules.utils import download_and_read, parse_faelle_csv, parse_faelle_timeline_csv, cleanup, og_download, parse_vac_laender_csv, notification
import pprint

def main():
    """
    main function; import historical vaccination data
    """
    pp = pprint.PrettyPrinter(indent=4)
    debug.stdout("Startup import historical vaccination data to influxdb ...")
    config = configparser.ConfigParser()
    config.sections()
    config.read('coviddata.ini')

    og_data_folder = config['opendata']['og_data_folder']
    bundeslaender = json.loads(config['ages']['bundeslaender'])

    # add opts
    parser = argparse.ArgumentParser(description='Options for historical vaccination data import',allow_abbrev=False)
    parser.add_argument('--filename', help="File to read import data from", default=False, required=True, action='store')
    parser.print_help()
    args = parser.parse_args()

    filename = args.filename

    if os.path.isfile(og_data_folder+"/"+filename):
        print("Start processing file "+filename)
        covid_data = parse_vac_laender_csv(og_data_folder,filename,bundeslaender)
        endpoint_influxdb.insert_influxdb(config,covid_data,'vac')
        pp.pprint(covid_data)
    else:
        sys.exit(filename+" does not exist in data folder - EXIT!")

    debug.stdout("Shutdown import historical vaccination covidstats to influxdb ...")

if __name__ == "__main__":
    main()
