#!/usr/bin/env bash

#NB! Never automate that action

mvn com.mycila:license-maven-plugin:format -Dlicense.header=./Commons/license.inc -pl Commons -pl Columnar -pl DateTime -pl DistWrapper -pl Geohashing -pl Math -pl Populations -pl Proximity -pl RESTWrapper -pl SimpleFilters -pl TaskWrapper
