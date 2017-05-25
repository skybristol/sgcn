
# coding: utf-8

# In[1]:

import requests,configparser
from IPython.display import display


# In[2]:

# Get API keys and any other config details from a file that is external to the code.
config = configparser.RawConfigParser()
config.read_file(open(r'../config/stuff.py'))


# In[3]:

# Build base URL with API key using input from the external config.
def getBaseURL():
    gc2APIKey = config.get('apiKeys','apiKey_GC2_BCB').replace('"','')
    apiBaseURL = "https://gc2.mapcentia.com/api/v1/sql/bcb?key="+gc2APIKey
    return apiBaseURL


# In[9]:

r = requests.get(getBaseURL()+"&q=SELECT scientificname_submitted, taxonomicgroup_submitted FROM sgcn.sgcn WHERE taxonomicgroup_submitted <> '' ORDER BY taxonomicgroup_submitted").json()

for feature in r["features"]:
    tg = '"taxonomicgroup_submitted"=>"'+feature["properties"]["taxonomicgroup_submitted"]+'"'
    display (requests.get(getBaseURL()+"&q=UPDATE tir.tir2 SET registration = registration || '"+tg+"' :: hstore WHERE registration->'SGCN_ScientificName_Submitted' = '"+feature["properties"]["scientificname_submitted"]+"' AND NOT EXIST(registration, 'taxonomicgroup_submitted')").json())
    
    


# In[ ]:



