# covidstats
> Parsing the virus ...
## Summary
This script is intended to download and parse the COVID-19 data prepared by AGES (https://covid19-dashboard.ages.at/).
Data is updated on regaluar basis (once a day between 13:00 - 14:00) and can be downloaded in compressed format from AGES website.

They distribute different CSV files focusing on differen aspects. This tool only considers `CovidFaelle_GKZ.csv` and `CovidFaelle_Timeline.csv` as of now.

Values for the given districts are extracted and published to different endpoints for further processing.
## Installation & Prerequisite
* `git clone git@github.com:joseftsch/covidstats.git`
* `cd covidstats`
* `pipenv install --dev`

Please make sure you have Python 3.8, pip and pipenv installed and up to date. All other Python modules required for this application will be installed by pipenv.
## Configuration
All configuration is done in the `coviddata.ini` file.<br>There is currently no validation of values/sections in place so please keep them all in `coviddata.ini` file.
Endpoints are enabled/disabled in the corresponding section with e.g "usemqtt = yes"
## Currently supported endpoints
* mqtt
* influxdb
* debug/stdout

(see comments in `coviddata.ini` for details on settings)
## Docker support
If you dont want to deal with Python there is also a experimental Dockerfile so that you can build your own Docker image.
* `docker build -t covidstats .`
* `docker run -v /opt/docker/covidstats/data:/app/data -v /opt/docker/covidstats/ogdata:/app/ogdata --rm --name covidstats covidstats`

**Attention:** There is currently no way to pass the configuration (or parts of it) as variables to the Docker container. Make sure you edit `coviddata.ini` befor you build the image.

## Import historical data
If you want to import historical data from `CovidFaelle_Timeline.csv` to influxdb you can use `import_historical.py`. You need to specify `--from-date` and `--to-date` as options (all other configuration values are read from `coviddata.ini`).

Please note that both `--from-date` and `--to-date` are inclusive.

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=joseftsch_covidstats&metric=alert_status)](https://sonarcloud.io/dashboard?id=joseftsch_covidstats)
![Pylint check](https://github.com/joseftsch/covidstats/workflows/Pylint%20check/badge.svg)
