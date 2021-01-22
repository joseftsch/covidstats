"""
This script allows to import historical vaccination data from laender.csv provided by opendata
"""
import configparser
import argparse
import modules.debug as debug
import modules.endpoint_mqtt as endpoint_mqtt
import modules.endpoint_influxdb as endpoint_influxdb
from modules.utils import download_and_read, parse_faelle_csv, parse_faelle_timeline_csv, cleanup, og_download, parse_vac_laender_csv, notification

def main():
    """
    main function; import historical vaccination data
    """
    debug.stdout("Startup import historical vaccination data to influxdb ...")
    config = configparser.ConfigParser()
    config.sections()
    config.read('coviddata.ini')

    parser = argparse.ArgumentParser(description='Options for historical vaccination data import',allow_abbrev=False)
    datafolder = config['ogdata']['data_folder']
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
            endpoint_influxdb.insert_influxdb(config,covid_data)

    else:
        sys.exit(filename+" does not exist in data folder - EXIT!")

    debug.stdout("Shutdown import historical covidstats to influxdb ...")

if __name__ == "__main__":
    main()
