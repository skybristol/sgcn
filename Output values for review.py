
# coding: utf-8

# In[2]:

import requests,configparser


# In[3]:

# Get API keys and any other config details from a file that is external to the code.
config = configparser.RawConfigParser()
config.read_file(open(r'../config/stuff.py'))


# In[4]:

# Build base URL with API key using input from the external config.
def getBaseURL():
    gc2APIKey = config.get('apiKeys','apiKey_GC2_BCB').replace('"','')
    apiBaseURL = "https://gc2.mapcentia.com/api/v1/sql/bcb?key="+gc2APIKey
    return apiBaseURL


# In[5]:

uniqueSpecies = requests.get(getBaseURL()+"&q=SELECT scientificname_submitted, commonname_submitted, sgcn_year, sgcn_state FROM sgcn.sgcn ORDER BY sgcn_state, sgcn_year, scientificname_submitted").json()


# In[20]:

sgcnData = []

for feature in uniqueSpecies["features"]:
    sgcnData.append(feature["properties"])

print (sgcnData)


# In[ ]:



