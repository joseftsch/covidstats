"""
submodule for inserting covid data into influxdb
"""
from datetime import datetime
import json
import pytz
from influxdb import InfluxDBClient

def insert_influxdb(config,covid_data,flag):
    """
    insert covid-19 data into influxdb
    """
    data = []
    tz = pytz.timezone('Europe/Vienna')
    now = datetime.now(tz=tz)
    dt_string = now.strftime("%s.%f")
    date_time_obj_in_ns = int(float(dt_string)*1000*1000*1000)
    bezirke = json.loads(config['ages']['bezirke'])
    bundeslaender = json.loads(config['ages']['bundeslaender'])
    vienna = pytz.timezone('Europe/Vienna')

    if flag == 'cases':
        for id, _ in covid_data.items():
            if id in bezirke:
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
            if id in bundeslaender:
                local_dt = datetime.strptime(covid_data[id]['Time'], '%d.%m.%Y 00:00:00').replace(tzinfo=pytz.timezone('Europe/Vienna')).astimezone(vienna).strftime("%s.%f")
                time_in_ns = int(float(local_dt)*1000*1000*1000)
                data.append("{measurement},Bundesland={Bundesland} BundeslandID={BundeslandID},AnzEinwohner={AnzEinwohner},AnzahlFaelle={AnzahlFaelle},AnzahlFaelleSum={AnzahlFaelleSum},AnzahlFaelle7Tage={AnzahlFaelle7Tage},SiebenTageInzidenzFaelle={SiebenTageInzidenzFaelle},AnzahlTotTaeglich={AnzahlTotTaeglich},AnzahlTotSum={AnzahlTotSum},AnzahlGeheiltTaeglich={AnzahlGeheiltTaeglich},AnzahlGeheiltSum={AnzahlGeheiltSum} {timestamp}"
                    .format(measurement="covid_bundesland",
                    Bundesland=covid_data[id]['Bundesland'],
                    BundeslandID=covid_data[id]['BundeslandID'],
                    AnzEinwohner=covid_data[id]['AnzEinwohner'],
                    AnzahlFaelle=covid_data[id]['AnzahlFaelle'],
                    AnzahlFaelleSum=covid_data[id]['AnzahlFaelleSum'],
                    AnzahlFaelle7Tage=covid_data[id]['AnzahlFaelle7Tage'],
                    SiebenTageInzidenzFaelle=covid_data[id]['SiebenTageInzidenzFaelle'].replace(",", "."),
                    AnzahlTotTaeglich=covid_data[id]['AnzahlTotTaeglich'],
                    AnzahlTotSum=covid_data[id]['AnzahlTotSum'],
                    AnzahlGeheiltTaeglich=covid_data[id]['AnzahlGeheiltTaeglich'],
                    AnzahlGeheiltSum=covid_data[id]['AnzahlGeheiltSum'],
                    timestamp=time_in_ns,
                    ))
    elif flag == 'vac':
        for id, _ in covid_data.items():
            if id in bundeslaender:
                local_dt = datetime.strptime(covid_data[id]['Datum'], '%Y-%m-%d %H:00:00').replace(tzinfo=pytz.timezone('Europe/Vienna')).astimezone(vienna).strftime("%s.%f")
                time_in_ns = int(float(local_dt)*1000*1000*1000)
                data.append("{measurement},Bundesland={Bundesland} Auslieferungen={Auslieferungen},AuslieferungenPro100={AuslieferungenPro100},Bestellungen={Bestellungen},BestellungenPro100={BestellungenPro100},Bevölkerung={Bevölkerung},BundeslandID={BundeslandID},EingetrageneImpfungen={EingetrageneImpfungen},EingetrageneImpfungenPro100={EingetrageneImpfungenPro100},Teilgeimpfte={Teilgeimpfte},TeilgeimpftePro100={TeilgeimpftePro100},Vollimmunisierte={Vollimmunisierte},VollimmunisiertePro100={VollimmunisiertePro100} {timestamp}"
                    .format(measurement="vaccination",
                    Bundesland=covid_data[id]['Bundesland'],
                    Auslieferungen=covid_data[id]['Auslieferungen'],
                    AuslieferungenPro100=covid_data[id]['AuslieferungenPro100'].replace(",", "."),
                    Bestellungen=covid_data[id]['Bestellungen'],
                    BestellungenPro100=covid_data[id]['BestellungenPro100'].replace(",", "."),
                    Bevölkerung=covid_data[id]['Bevölkerung'],
                    BundeslandID=covid_data[id]['BundeslandID'],
                    EingetrageneImpfungen=covid_data[id]['EingetrageneImpfungen'],
                    EingetrageneImpfungenPro100=covid_data[id]['EingetrageneImpfungenPro100'].replace(",", "."),
                    Teilgeimpfte=covid_data[id]['Teilgeimpfte'],
                    TeilgeimpftePro100=covid_data[id]['TeilgeimpftePro100'].replace(",", "."),
                    Vollimmunisierte=covid_data[id]['Vollimmunisierte'],
                    VollimmunisiertePro100=covid_data[id]['VollimmunisiertePro100'].replace(",", "."),
                    timestamp=time_in_ns,
                    ))
    else:
        print("no data for influxdb")

    try:
        client = InfluxDBClient(host=config['influxdb']['influxdbhost'], port=config['influxdb']['influxdbport'], username=config['influxdb']['influxdbuser'], password=config['influxdb']['influxdbpassword'])
    except Exception as e:
        print("InfluxDB connection not possible")
        raise SystemExit(e)
    client.write_points(data, database=config['influxdb']['influxdbdb'], protocol='line')
