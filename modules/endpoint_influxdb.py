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
                #local_dt = datetime.strptime(covid_data[id]['Datum'], '%Y-%m-%d %H:%M:%S').astimezone(pytz.timezone('Europe/Vienna')).strftime("%s.%f")
                #time_in_ns = int(float(local_dt)*1000*1000*1000)
                dfromfile = datetime.strptime(covid_data[id]['Datum08'], '%Y-%m-%d %H:%M:%S')
                time_in_ns = int(float(pytz.timezone("Europe/Vienna").localize(dfromfile).strftime("%s.%f"))*1000*1000*1000)
                data.append("{measurement},Bundesland={Bundesland} Bevoelkerung={Bevoelkerung},BundeslandID={BundeslandID},Teilgeimpfte={Teilgeimpfte},\
Vollimmunisierte={Vollimmunisierte},\
EingetrageneImpfungen={EingetrageneImpfungen},\
EingetrageneImpfungenAstraZeneca_1={EingetrageneImpfungenAstraZeneca_1},\
EingetrageneImpfungenAstraZeneca_2={EingetrageneImpfungenAstraZeneca_2},\
EingetrageneImpfungenBioNTechPfizer_1={EingetrageneImpfungenBioNTechPfizer_1},\
EingetrageneImpfungenBioNTechPfizer_2={EingetrageneImpfungenBioNTechPfizer_2},\
EingetrageneImpfungenModerna_1={EingetrageneImpfungenModerna_1},\
EingetrageneImpfungenModerna_2={EingetrageneImpfungenModerna_2},\
EingetrageneImpfungenAstraZeneca_G={EingetrageneImpfungenAstraZeneca_G},\
EingetrageneImpfungenBioNTechPfizer_G={EingetrageneImpfungenBioNTechPfizer_G},\
EingetrageneImpfungenModerna_G={EingetrageneImpfungenModerna_G},\
EingetrageneImpfungenJanssen={EingetrageneImpfungenJanssen},\
ImpfstoffNichtZugeordnet_1={ImpfstoffNichtZugeordnet_1},\
ImpfstoffNichtZugeordnet_2={ImpfstoffNichtZugeordnet_2},\
ImpfstoffNichtZugeordnet_G={ImpfstoffNichtZugeordnet_G},\
G25_34_1={G25_34_1},\
G25_34_2={G25_34_2},\
G35_44_1={G35_44_1},\
G35_44_2={G35_44_2},\
G45_54_1={G45_54_1},\
G45_54_2={G45_54_2},\
G55_64_1={G55_64_1},\
G55_64_2={G55_64_2},\
G65_74_1={G65_74_1},\
G65_74_2={G65_74_2},\
G75_84_1={G75_84_1},\
G75_84_2={G75_84_2},\
Gg84_2={Gg84_2},\
Gg84_1={Gg84_1},\
Gu15_1={Gu15_1},\
Gu15_2={Gu15_2},\
G15_24_1={G15_24_1},\
G15_24_2={G15_24_2} {timestamp}"
                    .format(measurement="vaccination",
                    Bundesland=covid_data[id]['Name'],
                    Bevoelkerung=covid_data[id]['Bevölkerung'],
                    BundeslandID=covid_data[id]['BundeslandID'],
                    Teilgeimpfte=covid_data[id]['Teilgeimpfte'],
                    Vollimmunisierte=covid_data[id]['Vollimmunisierte'],
                    EingetrageneImpfungen=covid_data[id]['EingetrageneImpfungen'],
                    EingetrageneImpfungenAstraZeneca_1=covid_data[id]['EingetrageneImpfungenAstraZeneca_1'],
                    EingetrageneImpfungenAstraZeneca_2=covid_data[id]['EingetrageneImpfungenAstraZeneca_2'],
                    EingetrageneImpfungenBioNTechPfizer_1=covid_data[id]['EingetrageneImpfungenBioNTechPfizer_1'],
                    EingetrageneImpfungenBioNTechPfizer_2=covid_data[id]['EingetrageneImpfungenBioNTechPfizer_2'],
                    EingetrageneImpfungenModerna_1=covid_data[id]['EingetrageneImpfungenModerna_1'],
                    EingetrageneImpfungenModerna_2=covid_data[id]['EingetrageneImpfungenModerna_2'],
                    EingetrageneImpfungenAstraZeneca_G=covid_data[id]['EingetrageneImpfungenAstraZeneca_G'],
                    EingetrageneImpfungenBioNTechPfizer_G=covid_data[id]['EingetrageneImpfungenBioNTechPfizer_G'],
                    EingetrageneImpfungenModerna_G=covid_data[id]['EingetrageneImpfungenModerna_G'],
                    EingetrageneImpfungenJanssen=covid_data[id]['EingetrageneImpfungenJanssen'],
                    ImpfstoffNichtZugeordnet_1=covid_data[id]['ImpfstoffNichtZugeordnet_1'],
                    ImpfstoffNichtZugeordnet_2=covid_data[id]['ImpfstoffNichtZugeordnet_2'],
                    ImpfstoffNichtZugeordnet_G=covid_data[id]['ImpfstoffNichtZugeordnet_G'],
                    G25_34_1=covid_data[id]['G25-34_1'],
                    G25_34_2=covid_data[id]['G25-34_2'],
                    G35_44_1=covid_data[id]['G35-44_1'],
                    G35_44_2=covid_data[id]['G35-44_2'],
                    G45_54_1=covid_data[id]['G45-54_1'],
                    G45_54_2=covid_data[id]['G45-54_2'],
                    G55_64_1=covid_data[id]['G55-64_1'],
                    G55_64_2=covid_data[id]['G55-64_2'],
                    G65_74_1=covid_data[id]['G65-74_1'],
                    G65_74_2=covid_data[id]['G65-74_2'],
                    G75_84_1=covid_data[id]['G75-84_1'],
                    G75_84_2=covid_data[id]['G75-84_2'],
                    Gg84_2=covid_data[id]['Gg84_2'],
                    Gg84_1=covid_data[id]['Gg84_1'],
                    Gu15_1=covid_data[id]['Gu15_1'],
                    Gu15_2=covid_data[id]['Gu15_2'],
                    G15_24_1=covid_data[id]['G15-24_1'],
                    G15_24_2=covid_data[id]['G15-24_2'],
                    timestamp=time_in_ns,
                    ))
    else:
        print("no data for influxdb")

    try:
        client = InfluxDBClient(host=config['influxdb']['influxdbhost'], port=config['influxdb']['influxdbport'], username=config['influxdb']['influxdbuser'], password=config['influxdb']['influxdbpassword'])
    except Exception as e:
        print("InfluxDB connection not possible")
        raise SystemExit(e)
    print("writing following data to influxdb: "+str(data))
    client.write_points(data, database=config['influxdb']['influxdbdb'], protocol='line')
