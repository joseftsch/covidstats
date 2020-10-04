# covidstats
> Parsing the virus ...
## Summary
This script is intended to download and parse the "number of COVID-19 cases" CSV file (Published by the Austrian government on https://www.data.gv.at/covid-19/). Values for the given districts are extracted and published to different endpoints for further processing.
## Installation
* `git clone git@github.com:joseftsch/covidstats.git`
* `cd covidstats`
* `pipenv install`
## Configuration
All configuration is done in the `coviddata.ini` file.<br>There is currently no validation of values/sections in place so please keep them all in `coviddata.ini` file.
Endpoints are enabled/disabled in the corresponding section with e.g "usemqtt = yes"
## Currently supported endpoints
* mqtt
* influxdb
* debug/stdout

(see comments in `coviddata.ini` for details on settings)

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=joseftsch_covidstats&metric=alert_status)](https://sonarcloud.io/dashboard?id=joseftsch_covidstats)
