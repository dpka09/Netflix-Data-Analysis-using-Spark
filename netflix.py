#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:


from pyspark import SparkContext
sc=SparkContext()


# In[3]:


import json


# In[4]:


rdd=sc.textFile("/home/deepika/Downloads/netflix_titles.json")


# In[5]:


from pyspark.storagelevel import StorageLevel


# In[6]:


rdd.persist(StorageLevel.DISK_ONLY)


# In[7]:


data= rdd.map(lambda x: json.loads(x))


# In[8]:


data.take(1)


# In[9]:


x=data.filter(lambda x:x['release_year']=='2008')


# In[10]:


x.take(1)


# In[11]:


x.count()


# In[12]:


x.collect()


# In[ ]:




