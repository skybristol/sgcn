
# coding: utf-8

# This notebook updates the registration info in the TIR for SGCN records to include the submitted taxonomic group. This property needs to be added to the original processing script as well. I did this so that we can put a step into the process that builds out new values we want to apply for taxonomic grouping to species that are aligned with taxonomic authorities.

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


# In[14]:

notFilled = requests.get(getBaseURL()+"&q=SELECT t.gid, s.taxonomicgroup_submitted FROM tir.tir2 t JOIN sgcn.sgcn s ON s.scientificname_submitted = t.registration->'SGCN_ScientificName_Submitted' WHERE s.taxonomicgroup_submitted <> '' AND not exist (t.registration,'taxonomicgroup_submitted')").json()

for feature in notFilled["features"]:
    tg = '"taxonomicgroup_submitted"=>"'+feature["properties"]["taxonomicgroup_submitted"]+'"'
    q = "UPDATE tir.tir2 SET registration = registration || '"+tg+"' :: hstore WHERE gid = "+str(feature["properties"]["gid"])
    print (q)
    print (requests.get(getBaseURL()+"&q="+q).json())


# In[ ]:



