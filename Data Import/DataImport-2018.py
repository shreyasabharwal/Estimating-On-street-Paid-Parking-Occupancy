
# coding: utf-8

#!/usr/bin/env python

# make sure to install these packages before running:
# pip install sodapy

import pandas as pd
from sodapy import Socrata

import pyodbc
import psycopg2

import pandas.io.sql as pdsql

from sqlalchemy import create_engine


# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
client = Socrata("data.seattle.gov", None)

conn = psycopg2.connect(host="rds-trafficjam.cpze98eqmmci.us-east-2.rds.amazonaws.com",
                        database="dbtrafficjam", user="trafficjam", password="anss!!596")

cur = conn.cursor()


# ## 2018 Paid Parking Occupancy Data

def getLat(loc):
    lat = loc['coordinates'][0]
    return lat


def getLon(loc):
    lon = loc['coordinates'][1]
    return lon


for i in range(0, 11):
    count = i*100000
    query = ("https://data.seattle.gov/resource/6yaw-2m8q.json?$limit=100000&$offset="+str(count) +
             "&paidparkingarea=Belltown"
             "&paidparkingsubarea=North"
             "&$where=date_extract_mm(occupancydatetime)%20in%20(0,30)"
             "&$order=:id"
             "&$select=:*,*")
    raw_data = pd.read_json(query)
    raw_data['lat'] = raw_data.location.apply(getLat)
    raw_data['lon'] = raw_data.location.apply(getLon)
    raw_data.drop(['location'], axis=1, inplace=True)
    raw_data['index'] = i
    raw_data = raw_data[['index', ':id', 'blockfacename', 'lat', 'lon', 'occupancydatetime', 'paidoccupancy',
                         'paidparkingarea', 'paidparkingsubarea', 'parkingcategory',
                         'parkingspacecount', 'parkingtimelimitcategory', 'sideofstreet',
                         'sourceelementkey']]
    for col in raw_data.columns:
        if col != 'index':
            raw_data[col].fillna(" ", inplace=True)
    #tuples = [tuple(map(str,x)) for x in raw_data.values]
    tuples = [tuple(x) for x in raw_data.values]
    dataText = ','.join(cur.mogrify(
        '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', row).decode('utf-8') for row in tuples)
    cur.execute(
        'insert into paid_parking_2018_belltown_north_30 values ' + dataText)
    conn.commit()
    print(count, flush=True)
