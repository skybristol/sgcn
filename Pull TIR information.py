
# coding: utf-8

# In[5]:

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


# In[32]:

sgcnTIR = requests.get(getBaseURL()+"&q=SELECT itis->'discoveredTSN' AS discoveredtsn, itis->'acceptedTSN' AS acceptedtsn, itis->'nameWOInd' AS scientificname_accepted, itis->'rank' AS rank, itis->'vernacular:English' AS commonname_accepted, registration->'SGCN_ScientificName_Submitted' AS scientificname_submitted FROM tir.tir2 WHERE itis->'itisMatchMethod' NOT LIKE 'NotMatched%' AND registration->'SGCN_ScientificName_Submitted' IN (SELECT scientificname_submitted FROM sgcn.sgcn WHERE scientificname_accepted IS NULL) ORDER BY registration->'SGCN_ScientificName_Submitted'").json()



# In[48]:

for feature in sgcnTIR["features"]:
    tsn = "http://services.itis.gov/?q=tsn:"+str(feature["properties"]["discoveredtsn"])
    if type(feature["properties"]["acceptedtsn"]) is str and feature["properties"]["acceptedtsn"] != feature["properties"]["discoveredtsn"]:
        tsn = "http://services.itis.gov/?q=tsn:"+str(feature["properties"]["acceptedtsn"])

    if type(feature["properties"]["commonname_accepted"]) is str:
        commonname = feature["properties"]["commonname_accepted"].replace("'","''")
    else:
        commonname = ""
    
    scientificname_accepted = feature["properties"]["scientificname_accepted"].replace("'","''")
    scientificname_submitted = feature["properties"]["scientificname_submitted"].replace("'","''")
    
    print (requests.get(getBaseURL()+"&q=UPDATE sgcn.sgcn SET taxonomicauthorityid_accepted='"+tsn+"', scientificname_accepted='"+scientificname_accepted+"', commonname_accepted='"+commonname+"', taxonomicauthority_rank='"+feature["properties"]["rank"]+"' WHERE scientificname_submitted='"+scientificname_submitted+"'").json())
    
    


# In[ ]:



