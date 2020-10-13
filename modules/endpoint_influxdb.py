"""
submodule for inserting covid data into influxdb
"""
from datetime import datetime
from influxdb import InfluxDBClient

def insert_influxdb(config,covid_data):
    """
    insert covid-19 data into influxdb
    """
    data = []

    now = datetime.now()
    dt_string = now.strftime("%s.%f")
    date_time_obj_in_ns = int(float(dt_string)*1000*1000*1000)

    for id, info in covid_data.items():
        data.append("{measurement},district={district} cases={cases},AnzahlFaelle7Tage={AnzahlFaelle7Tage},AnzahlTot={AnzahlTot},Einwohner={Einwohner},gkz={gkz} {timestamp}"
            .format(measurement="covid",
            district=covid_data[id]['Bezirk'],
            cases=covid_data[id]['Faelle'],
            AnzahlFaelle7Tage=covid_data[id]['AnzahlFaelle7Tage'],
            AnzahlTot=covid_data[id]['AnzahlTot'],
            Einwohner=covid_data[id]['Einwohner'],
            gkz=covid_data[id]['GKZ'],
            timestamp=date_time_obj_in_ns,
            ))

    try:
        client = InfluxDBClient(host=config['influxdb']['influxdbhost'], port=config['influxdb']['influxdbport'], username=config['influxdb']['influxdbuser'], password=config['influxdb']['influxdbpassword'])
    except Exception as e:
        print("InfluxDB connection not possible")
        raise SystemExit(e)
    client.write_points(data, database=config['influxdb']['influxdbdb'], protocol='line')
