#!/usr/bin/env python
# coding: utf-8

# In[44]:


import csv
import itertools
import sys
import statsmodels as sm
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as sf


# In[2]:


sc = SparkContext()
spark = SparkSession(sc)


# #--------------------------------------------
# #Part 1
# #Total Number of parking Violations 2015-2019
# #--------------------------------------------

# In[34]:


#Create Street DF in Spark
#Modify so only relevant columns are here
#Need to modify county code so it can be matched with centerlines data
df_s = spark.read.csv('hdfs:///tmp/bdm/nyc_parking_violations', header = True,inferSchema = 'true')

#Drop null data
df_s = df_s.na.drop(subset = ['House Number','Issue Date','Street Name','Violation County'])

#Split Compound House numbers into 2 columns
df_s = df_s.withColumn('HouseN1',sf.split(df_s['House Number'],"\-")[0].cast(IntegerType()))
df_s = df_s.withColumn('HouseN2',sf.split(df_s['House Number'],"\-")[1].cast(IntegerType()))
df_s = df_s.na.drop(subset = ['HouseN1'])

#Select Relevant Data
df_s = df_s.select(sf.substring(df_s['Issue Date'],7,10).alias('Year'),                   df_s['HouseN1'],df_s['HouseN2'],                  df_s['Street Name'].alias('Street_Name'),                  sf.when((df_s['Violation County']=='MAN')|(df_s['Violation County']=='MH')|(df_s['Violation County']=='MN')|(df_s['Violation County']=='NEWY')|(df_s['Violation County']=='NEW Y')|(df_s['Violation County']=='NY'),1)                  .otherwise(sf.when((df_s['Violation County']=='BRONX')|(df_s['Violation County']=='BX'),2)                  .otherwise(sf.when((df_s['Violation County']=='BK')|(df_s['Violation County']=='K')|(df_s['Violation County']=='KING')|(df_s['Violation County']=='KINGS'),3)                  .otherwise(sf.when((df_s['Violation County']=='Q')|(df_s['Violation County']=='QN')|(df_s['Violation County']=='QNS')|(df_s['Violation County']=='QU')|(df_s['Violation County']=='QUEEN'),4)                  .otherwise(sf.when((df_s['Violation County']=='R')|(df_s['Violation County']=='RICHMOND')|(df_s['Violation County']=='SI'),5))))).alias('Boro'))
df_s = df_s.cache()

#Subset 1: NonCompound House Numbers
df_s1 = df_s.where(df_s['HouseN2'].isNull())
df_s1 = df_s1.drop(df_s1['HouseN2'])
df_s1 = df_s1.filter(df_s['Boro'].isNotNull())
df_s1 = df_s1.cache()

#Subset 2: Compound House Numbers
df_s2 = df_s.where(df_s['HouseN2'].isNotNull())
df_s2 = df_s2.filter(df_s['Boro'].isNotNull())
df_s2 = df_s2.cache()


# In[55]:


#Modify Centerline data
df_c = spark.read.csv('hdfs:///tmp/bdm/nyc_cscl.csv',header = True,inferSchema = 'true')

#Convert PHYSICALID to correct data form
df_c =df_c.withColumn('PHYSICALID',sf.regexp_replace(df_c['PHYSICALID'],",","").cast(IntegerType()))

#Following line used as precursor to running 2 join functions. Eliminates duplicates
df_c = df_c.withColumn('FULL_STREE',sf.when(df_c['FULL_STREE']==df_c['ST_NAME'],'Medwin123').otherwise(df_c['FULL_STREE']))

#Modify Low & High Columns for compounds
df_c = df_c.withColumn('L_LOW_HN1',sf.split(df_c['L_LOW_HN'],"\-")[0].cast(IntegerType()))
df_c = df_c.withColumn('L_LOW_HN2',sf.split(df_c['L_LOW_HN'],"\-")[1].cast(IntegerType()))
df_c = df_c.withColumn('L_HIGH_HN1',sf.split(df_c['L_HIGH_HN'],"\-")[0].cast(IntegerType()))
df_c = df_c.withColumn('L_HIGH_HN2',sf.split(df_c['L_HIGH_HN'],"\-")[1].cast(IntegerType()))

df_c = df_c.withColumn('R_LOW_HN1',sf.split(df_c['R_LOW_HN'],"\-")[0].cast(IntegerType()))
df_c = df_c.withColumn('R_LOW_HN2',sf.split(df_c['R_LOW_HN'],"\-")[1].cast(IntegerType()))
df_c = df_c.withColumn('R_HIGH_HN1',sf.split(df_c['R_HIGH_HN'],"\-")[0].cast(IntegerType()))
df_c = df_c.withColumn('R_HIGH_HN2',sf.split(df_c['R_HIGH_HN'],"\-")[1].cast(IntegerType()))


#Select Relevant Data
df_c = df_c.select(df_c['PHYSICALID'],df_c['FULL_STREE'],df_c['ST_NAME'],df_c['BOROCODE'],                   df_c['L_LOW_HN1'],df_c['L_LOW_HN2'],df_c['L_HIGH_HN1'],df_c['L_HIGH_HN2'],                   df_c['R_LOW_HN1'],df_c['R_LOW_HN2'],df_c['R_HIGH_HN1'],df_c['R_HIGH_HN2'])

df_c = df_c.cache()

#Subset 1: Noncompound House Numbers
drop_list = ['L_LOW_HN2','L_HIGH_HN2','R_LOW_HN2','R_HIGH_HN2']
df_c1 = df_c.where(df_c['L_LOW_HN2'].isNull())
df_c1 = df_c1.drop(*drop_list)
df_c1 = df_c1.cache()

#Subset 2: Compound House Numbers
df_c2 = df_c.where(df_c['L_LOW_HN2'].isNotNull())
df_c2 = df_c2.cache()


# In[57]:


#Create conditions of join (Borough, Street, Street Segment)

#Noncompounds
#First join condition - 'Street_Name' == 'FULL_STREE'
conda1 = [(df_s1['Boro'] == df_c1['BOROCODE']),        (sf.lower(df_s1['Street_Name'])) == sf.lower(df_c1['FULL_STREE']),         ((df_s1['HouseN1']%2==1)&(df_s1['HouseN1']>df_c1['L_LOW_HN1'])&(df_s1['HouseN1']<=df_c1['L_HIGH_HN1']))          |((df_s1['HouseN1']%2==0)&(df_s1['HouseN1']>df_c1['R_LOW_HN1'])&(df_s1['HouseN1']<=df_c1['R_HIGH_HN1']))]

#Second join condition - 'Street_Name == 'ST_NAME'
condb1 = [(df_s1['Boro'] == df_c1['BOROCODE']),        (sf.lower(df_s1['Street_Name'])) == sf.lower(df_c1['ST_NAME']),         ((df_s1['HouseN1']%2==1)&(df_s1['HouseN1']>df_c1['L_LOW_HN1'])&(df_s1['HouseN1']<=df_c1['L_HIGH_HN1']))          |((df_s1['HouseN1']%2==0)&(df_s1['HouseN1']>df_c1['R_LOW_HN1'])&(df_s1['HouseN1']<=df_c1['R_HIGH_HN1']))]


# In[58]:


#Compounds
#First join condition - 'Street_Name' == 'FULL_STREE'
conda2 = [(df_s2['Boro'] == df_c2['BOROCODE']),        (sf.lower(df_s2['Street_Name'])) == sf.lower(df_c2['FULL_STREE']),         (((df_s2['HouseN1']>=df_c2['L_LOW_HN1'])&(df_s2['HouseN1']<=df_c2['L_HIGH_HN1']))          |((df_s2['HouseN1']>=df_c2['L_LOW_HN1'])&(df_s2['HouseN1']<=df_c2['L_HIGH_HN1']))          &((df_s2['HouseN2']%2==1)&(df_s2['HouseN2']>df_c2['L_LOW_HN2'])&(df_s2['HouseN2']<=df_c2['L_HIGH_HN2'])          |((df_s2['HouseN2']%2==0)&(df_s2['HouseN2']>df_c2['R_LOW_HN2'])&(df_s2['HouseN2']<=df_c2['R_HIGH_HN2']))))]

#Second join condition - 'Street_Name == 'ST_NAME'
condb2 = [(df_s2['Boro'] == df_c2['BOROCODE']),        (sf.lower(df_s2['Street_Name'])) == sf.lower(df_c2['ST_NAME']),         (((df_s2['HouseN1']>=df_c2['L_LOW_HN1'])&(df_s2['HouseN1']<=df_c2['L_HIGH_HN1']))          |((df_s2['HouseN1']>=df_c2['L_LOW_HN1'])&(df_s2['HouseN1']<=df_c2['L_HIGH_HN1']))          &((df_s2['HouseN2']%2==1)&(df_s2['HouseN2']>df_c2['L_LOW_HN2'])&(df_s2['HouseN2']<=df_c2['L_HIGH_HN2'])          |((df_s2['HouseN2']%2==0)&(df_s2['HouseN2']>df_c2['R_LOW_HN2'])&(df_s2['HouseN2']<=df_c2['R_HIGH_HN2']))))]


# In[59]:


#Join data based on Borough, Street, Street Segment

#Noncompounds
df_j1 = df_s1.join(sf.broadcast(df_c1),conda1,how="outer")
df_j1 = df_j1.na.drop(subset = ['PHYSICALID'])

df_j2 = df_s1.join(sf.broadcast(df_c1),condb1,how="outer")
df_j2 = df_j2.na.drop(subset = ['PHYSICALID'])

df_n = df_j1.union(df_j2)
df_n = df_n.select(df_n['PHYSICALID'],df_n['YEAR'])


# In[92]:


#Compounds
df_j3 = df_s2.join(sf.broadcast(df_c2),conda2,how="outer")
df_j3 = df_j3.na.drop(subset = ['PHYSICALID'])

df_j4 = df_s2.join(sf.broadcast(df_c2),condb2,how="outer")
df_j4 = df_j4.na.drop(subset = ['PHYSICALID'])

df_c = df_j3.union(df_j4)
df_c = df_c.select(df_c['PHYSICALID'],df_c['YEAR'])


# In[95]:


#Union Everything & Aggregate
df_f = df_n.union(df_c)
df_f = df_f.groupby('PHYSICALID').pivot('YEAR').count()
df_f = df_f.orderBy('PHYSICALID')
df_f = df_f.drop(df_f['null'])


# #--------------------------------------------
# #Part 2
# #Rate of Change over 2015-2019 (OLS)
# #--------------------------------------------

# In[ ]:


X = [2015,2016,2017,2018,2019]


# In[ ]:


#--------------------------------------------
#Part 3
#Write output file
#--------------------------------------------


# In[76]:


df_f.repartition(1).write.csv('/home/mc7627/tmp/bdm/output.csv')


# In[8]:


sc.stop()

