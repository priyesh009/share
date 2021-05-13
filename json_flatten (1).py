#!/usr/bin/env python
# coding: utf-8

# In[10]:


import pandas as pd
from flatten_json import flatten


# In[11]:


import findspark
findspark.init()


# In[12]:


from pyspark.sql import SparkSession


# In[22]:


spark = SparkSession.builder.appName('demo').getOrCreate()
sc = spark.sparkContext


# In[23]:


df=spark.read.json('jsonflatten.json')


# In[24]:


df


# In[25]:


unflat_json = {'user' :
               {'Rachel':
                {'UserID':1717171717,
                'Email': 'rachel1999@gmail.com', 
                'friends': ['John', 'Jeremy', 'Emily']
                }
               }
              }


# In[26]:


flat_json = flatten(unflat_json)
  
print(flat_json)


# In[27]:


rdd = sc.parallelize([flat_json])


# In[28]:


df = spark.read.json(rdd)

df.show()


# In[ ]:




