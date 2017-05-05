
# coding: utf-8

# In[1]:

import requests,datetime,configparser
from IPython.display import display


# In[2]:

# Get API keys and any other config details from a file that is external to the code.
config = configparser.RawConfigParser()
config.read_file(open(r'../config/stuff.py'))

dt = datetime.datetime.utcnow().isoformat()


# In[3]:

# Build base URL with API key using input from the external config.
def getBaseURL():
    gc2APIKey = config.get('apiKeys','apiKey_GC2_BCB').replace('"','')
    apiBaseURL = "https://gc2.mapcentia.com/api/v1/sql/bcb?key="+gc2APIKey
    return apiBaseURL


# In[4]:

# Basic function to insert registration info pairs into TIR
def idsToTIR(recordInfoPairs):
    # Build query string
    insertSQL = "INSERT INTO tir.tir2 (registration) VALUES ('"+recordInfoPairs+"')"
    # Execute query
    response = requests.get(getBaseURL()+"&q="+insertSQL).json()
    return response


# In[8]:

speciesQ = "SELECT DISTINCT scientificname_submitted FROM sgcn.sgcn     WHERE scientificname_submitted <> ''     AND scientificname_submitted NOT IN     (SELECT registration -> 'SGCN_ScientificName_Submitted' AS scientificname_submitted FROM tir.tir2)     ORDER BY scientificname_submitted"
speciesR = requests.get(getBaseURL()+"&q="+speciesQ).json()


# In[9]:

numProcessed = 0
for sgcnRecord in speciesR['features']:
    recordInfoPairs = '"registrationDate" => "'+dt+'"'
    recordInfoPairs = recordInfoPairs+',"SGCN_ScientificName_Submitted"=>"'+sgcnRecord['properties']['scientificname_submitted'].replace("\'","''")+'"'
    try:
        print (idsToTIR(recordInfoPairs))
        numProcessed = numProcessed + 1
    except:
        print ("Problem with: "+recordInfoPairs)

print ("Number Unique Names Processed: "+str(numProcessed))


# In[ ]:



