
# coding: utf-8

# In[1]:


#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
from sodapy import Socrata

import pyodbc
import psycopg2

import pandas.io.sql as pdsql

from sqlalchemy import create_engine


# In[2]:


# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
client = Socrata("data.seattle.gov", None)

# Example authenticated client (needed for non-public datasets):
# client = Socrata(data.seattle.gov,
#                  MyAppToken,
#                  userame="user@example.com",
#                  password="AFakePassword")


# In[5]:


def import_to_sql(tablename,results):
    results_df = pd.DataFrame.from_records(results)
    conn = psycopg2.connect(host="rds-trafficjam.cpze98eqmmci.us-east-2.rds.amazonaws.com",
	                    database="dbtrafficjam", user="trafficjam", password="anss!!596")
    
    engine = create_engine('postgresql://trafficjam:anss!!596@rds-trafficjam.cpze98eqmmci.us-east-2.rds.amazonaws.com:5432/dbtrafficjam')
    
    results_df.to_sql(tablename, con=engine, if_exists='fail', index=True, index_label=None, chunksize=None, dtype=None)


# In[2]:


conn = psycopg2.connect(host="rds-trafficjam.cpze98eqmmci.us-east-2.rds.amazonaws.com",
	                    database="dbtrafficjam", user="trafficjam", password="anss!!596")

cur = conn.cursor()


# ## Annual Parking Study Data

# In[151]:



## need to change parameters - where clause

results = client.get("fdax-a9ur", limit=1000000, where="study_year>=2017")
#results = client.get("fdax-a9ur", limit=2000)


# In[152]:


# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)


# In[153]:


results_df.shape


# In[154]:


results_df.columns


# In[155]:


for col in results_df.columns:
    #if col!='bmw_dn':
    results_df[col].fillna(" ", inplace = True)


# In[158]:


#tuples = [tuple(map(str, x)) for x in results_df.values]
tuples = [tuple(x) for x in results_df.values]

'''tuples=[]
for x in results_df.values:
    tuples.append(x)'''


# In[159]:


len(tuples)


# In[160]:


dataText = ','.join(cur.mogrify('(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', row).decode('utf-8') for row in tuples)
#datatext = ','.join(row for row in tuples)


# In[161]:


dataText[1:100]


# In[162]:


cur.execute('insert into annual_parking values ' + dataText)
conn.commit()


# In[ ]:


'''engine = create_engine('postgresql://trafficjam:anss!!596@rds-trafficjam.cpze98eqmmci.us-east-2.rds.amazonaws.com:5432/dbtrafficjam')

results_df.to_sql('annual_parking', con=engine, if_exists='replace', index=True, index_label=None, chunksize=None, dtype=None)'''


# ## City of Seattle Wage data

# In[ ]:


results = client.get("2khk-5ukd", limit=2000)
import_to_sql('cos_wage',results)


# In[3]:


## Query for count (Browser)
#https://data.seattle.gov/resource/6yaw-2m8q.json?$select=count(*)&paidparkingarea=Belltown&paidparkingsubarea=North&$where=date_extract_mm(occupancydatetime)%20in%20(0,15,30,45)

