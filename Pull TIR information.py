
# coding: utf-8

# In[17]:

import requests,configparser,re
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


# In[20]:

sgcnTIR = requests.get(getBaseURL()+"&q=SELECT itis->'discoveredTSN' AS discoveredtsn, itis->'acceptedTSN' AS acceptedtsn, itis->'nameWInd' AS namewind, itis->'rank' AS rank, itis->'vernacular:English' AS vernacularenglish, registration->'SGCN_ScientificName_Submitted' AS scientificname_submitted, itis FROM tir.tir2 WHERE itis->'itisMatchMethod' NOT LIKE 'NotMatched%' AND registration->'SGCN_ScientificName_Submitted' NOT IN (SELECT scientificname_submitted FROM sgcn.sgcn WHERE taxonomicauthorityid IS NOT NULL) ORDER BY registration->'SGCN_ScientificName_Submitted'").json()



# In[26]:

for feature in sgcnTIR["features"]:

    thisRecord = {}
    thisRecord["scientificname"] = feature["properties"]["namewind"].replace("'","''")
    thisRecord["tsn"] = "http://services.itis.gov/?q=tsn:"+str(feature["properties"]["discoveredtsn"])
    thisRecord["rank"] = feature["properties"]["rank"]
    thisRecord["scientificname_submitted"] = feature["properties"]["scientificname_submitted"].replace("'","''")
    thisRecord["commonname"] = ""

    if type(feature["properties"]["acceptedtsn"]) is str and feature["properties"]["acceptedtsn"] != feature["properties"]["discoveredtsn"]:
        thisRecord["tsn"] = "http://services.itis.gov/?q=tsn:"+str(feature["properties"]["acceptedtsn"])
        m = re.search('\"'+thisRecord["rank"]+'\"\=\>\"(.+?)\"', feature["properties"]["itis"])
        if m:
            thisRecord["scientificname"] = m.group(1)

    if type(feature["properties"]["vernacularenglish"]) is str:
        thisRecord["commonname"] = feature["properties"]["vernacularenglish"].replace("'","''")
    else:
        try:
            r = requests.get(getBaseURL()+"&q=SELECT commonname_submitted FROM sgcn.sgcn WHERE scientificname_submitted = '"+thisRecord["scientificname_submitted"]+"' LIMIT 1").json()
            thisRecord["commonname"] = r["features"][0]["properties"]["commonname_submitted"].replace("'","''")
        except:
            pass
    
    print (requests.get(getBaseURL()+"&q=UPDATE sgcn.sgcn SET taxonomicauthorityid='"+thisRecord["tsn"]+"', scientificname_display='"+thisRecord["scientificname"]+"', commonname_display='"+thisRecord["commonname"]+"', taxonomicauthorityrank='"+thisRecord["rank"]+"' WHERE scientificname_submitted='"+thisRecord["scientificname_submitted"]+"'").json())
    
    

