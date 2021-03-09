# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 18:22:29 2021

@author: manub
"""

# Importing required packages

import re
import os
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt 
# %matplotlib inline
from IPython import get_ipython
get_ipython().run_line_magic('matplotlib', 'inline')
import seaborn as sns
from IPython.core.display import display, HTML
from IPython.display import HTML
import json
import sys
# sys.path.insert(0,'..')
import folium
from matplotlib.colors import Normalize, rgb2hex
import pymongo
from pymongo import MongoClient, GEO2D

# Import data

total_crime = pd.read_csv('Map_of_Police_Department_Incidents.csv')
print(total_crime.shape)

total_crime.head(10)
total_crime.tail(10)

d_crime = total_crime.head(600000)

# Reduce dataset
#d_crime = total_crime.sample(frac = 0.4)
print(d_crime.shape)

del total_crime

d_crime.dtypes
d_crime.index
d_crime.columns
d_crime.values

# Data cleaning. Transform Data from string to date type and delta date
date = pd.to_datetime(d_crime['Date'])

print(date.min())
print(date.max())

# Days in delta time format
t_delta = (date-date.min()).astype('timedelta64[D]')
d_crime['days'] = t_delta
d_crime.head(1)

cat = 'PdDistrict'
cat = 'Category'
l = d_crime.groupby(cat).size()

l.sort_values(inplace=True)

fig=plt.figure(figsize=(10,5))
plt.yticks(fontsize=8)
l.plot(kind='bar',fontsize=12,color='b', )
plt.xlabel('')
plt.ylabel('Number of reports',fontsize=10)


# Plotting bargraph
def plotdat(data,cat):
    l=data.groupby(cat).size()
    l.sort_values(inplace=True)
    fig=plt.figure(figsize=(10,5))
    plt.yticks(fontsize=8)
    l.plot(kind='bar',fontsize=12,color='b', )
    plt.xlabel('')
    plt.ylabel('Number of reports',fontsize=10)


plotdat(d_crime,'PdDistrict')
plotdat(d_crime,'Category')
plotdat(d_crime,'DayOfWeek')
plotdat(d_crime,'Descript')

l=d_crime.groupby('Descript').size()
l.sort_values()
print(l.shape)

hoods_per_type = d_crime.groupby('Descript').PdDistrict.value_counts(sort=True)

t=hoods_per_type.unstack().fillna(0)

hood_sum=t.sum(axis=0)
hood_sum.sort_values(ascending=False)
t=t[hood_sum.index]

crime_sum=t.sum(axis=1)
crime_sum.sort_values(ascending=False)

# Heatmap and hierarchical clustering
def types_districts(d_crime,per):

    # Group by crime type and district
    hoods_per_type=d_crime.groupby('Descript').PdDistrict.value_counts(sort=True)
    t=hoods_per_type.unstack().fillna(0)

    # Sort by hood sum
    hood_sum=t.sum(axis=0)
    hood_sum.sort_values(ascending=False)
    t=t[hood_sum.index]

    # Filter by crime per district
    crime_sum=t.sum(axis=1)
    crime_sum.sort_values(ascending=False)

    # Large number, so let's slice the data.
    p=np.percentile(crime_sum,per)
    ix=crime_sum[crime_sum>p]
    t=t.loc[ix.index]
    return t

t=types_districts(d_crime,98)

sns.clustermap(t,cmap="mako", robust=True)
sns.clustermap(t,standard_scale=1,cmap="mako", robust=True)

sns.clustermap(t,z_score=0,cmap="viridis", robust=True)

###

#print('Mongo version', pymongo._version_)
client = MongoClient('mongodb://dbatest:dPMd%23Vg%25lBpb@localhost:27017/test?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false')
db = client.test
collection= db.crimesf

#Import data into the database. First, transform to JSON records
records = json.loads(d_crime.to_json(orient='records'))

collection.delete_many({})
collection.insert_many(records)

#Check if we can access the data from the MongoDB.
cursor = collection.find().sort('Category',pymongo.ASCENDING).limit(30)

for doc in cursor:
    print(doc)

# stablish a pipeline to select all rows matching attribute "Category" = "DRUG/NARCOTIC"
pipeline = [
        {"$match": {"Category":"DRUG/NARCOTIC"}},
]

#Query the collection with the pipeline filter.
aggResult = collection.aggregate(pipeline)
df2 = pd.DataFrame(list(aggResult))
df2.head()

# Organize incidents' descriptions versus Districts where they were detected
def types_districts(d_crime,per):

    # Group by crime type and district
    hoods_per_type=d_crime.groupby('Descript').PdDistrict.value_counts(sort=True)
    t=hoods_per_type.unstack().fillna(0)

    # Sort by hood sum
    hood_sum=t.sum(axis=0)
    hood_sum.sort_values(ascending=False)
    t=t[hood_sum.index]

    # Filter by crime per district
    crime_sum=t.sum(axis=1)
    crime_sum.sort_values(ascending=False)

    # Large number, so let's slice the data.
    p=np.percentile(crime_sum,per)
    ix=crime_sum[crime_sum>p]
    t=t.loc[ix.index]
    return t

# Filter outliers up to 75 percentile

t=types_districts(df2,75)
sns.clustermap(t,standard_scale=1)
sns.clustermap(t,standard_scale=0, annot=True)

# Bin crime by 30 day window. That is, obtain new colum with corresponding months
df2['Month']=np.floor(df2['days']/30) # Approximate month (30 day window)

# Default
district='All'

def timeseries(dat,per):
''' Category grouped by month '''

# Group by crime type and district
cat_per_time=dat.groupby('Month').Descript.value_counts(sort=True)
t=cat_per_time.unstack().fillna(0)

# Filter by crime per district
crime_sum=t.sum(axis=0)
crime_sum.sort_values()

# Large number, so let's slice the data.
p=np.percentile(crime_sum,per)
ix=crime_sum[crime_sum>p]
t=t[ix.index]

return t

t_all=timeseries(df2,0)

#Find inciden's descriptions related to word patter "BARBITUATES"
pat = re.compile(r'BARBITUATES', re.I)

pipeline = [
{"$match": {"Category":"DRUG/NARCOTIC" , 'Descript': {'$regex': pat}}},
]

aggResult = collection.aggregate(pipeline)
df3 = pd.DataFrame(list(aggResult))
df3.head()

barbituates = df3.groupby('Descript').size()
s = pd.Series(barbituates)
print(s)
s = s[s != 1]
barituate_features = list(s.index)
print(barituate_features)

#Find inciden's descriptions related to word patter "BARBITUATES"
pat = re.compile(r'BARBITUATES', re.I)

pipeline = [
{"$match": {"Category":"DRUG/NARCOTIC" , 'Descript': {'$regex': pat}}},
]

aggResult = collection.aggregate(pipeline)
df3 = pd.DataFrame(list(aggResult))
df3.head()

barbituates = df3.groupby('Descript').size()
s = pd.Series(barbituates)
print(s)
s = s[s != 1]
barituate_features = list(s.index)
print(barituate_features)


#Let's generate a function to constructu subsets of descriptions according to patterns: COCAINE, MARIJUANA, METHADONE, etc.
def descriptionsAccordingToPattern(pattern):
pat = re.compile(pattern, re.I)

pipeline = [
{"$match": {"Category":"DRUG/NARCOTIC" , 'Descript': {'$regex': pat}}},
]

aggResult = collection.aggregate(pipeline)
df3 = pd.DataFrame(list(aggResult))
drug = df3.groupby('Descript').size()
s = pd.Series(drug)
s = s[s != 1] # filter those descriptions with value less equal 1
features = list(s.index)

return features

coke_features = descriptionsAccordingToPattern('COCAINE')

weed_features = descriptionsAccordingToPattern('MARIJUANA')
metadone_features = descriptionsAccordingToPattern('METHADONE')
hallu_features = descriptionsAccordingToPattern('HALLUCINOGENIC')
opium_features = descriptionsAccordingToPattern('OPIUM')
opiates_features = descriptionsAccordingToPattern('OPIATES')
meth_features = descriptionsAccordingToPattern('AMPHETAMINE')
heroin_features = descriptionsAccordingToPattern('HEROIN')
crack_features = descriptionsAccordingToPattern('BASE/ROCK')
