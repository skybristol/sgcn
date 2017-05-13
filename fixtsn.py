
# coding: utf-8

# In[14]:

import requests,configparser,re
from IPython.display import display


# In[4]:

# Get API keys and any other config details from a file that is external to the code.
config = configparser.RawConfigParser()
config.read_file(open(r'../config/stuff.py'))


# In[5]:

# Build base URL with API key using input from the external config.
def getBaseURL():
    gc2APIKey = config.get('apiKeys','apiKey_GC2_BCB').replace('"','')
    apiBaseURL = "https://gc2.mapcentia.com/api/v1/sql/bcb?key="+gc2APIKey
    return apiBaseURL


# In[33]:

r = requests.get(getBaseURL()+"&q=SELECT gid, itis FROM tir.tir2 WHERE itis->'itisMatchMethod' NOT LIKE 'NotMatched%' AND exist(itis, 'discoveredTSN')").json()
for feature in r["features"]:
    print (feature["properties"]["gid"], requests.get(getBaseURL()+"&q=UPDATE tir.tir2 SET itis = '"+feature["properties"]["itis"].replace('"discoveredTSN"','"tsn"').replace("'","''")+"' WHERE gid = "+str(feature["properties"]["gid"])).json())
    
    


# In[ ]:



