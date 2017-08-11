
# coding: utf-8

# This process registeres unique species names from the SGCN source data into the Taxonomic Information Registry. The process is all based on pulling unique species names that are then examined via TIR processes to find matches with taxonomic authorities. Those decisions on taxonomic matching are used to create a nationally synthesized list of taxa that states have listed as Species of Greatest Conservation Need.
# 
# Registration consists of a set of key/value pairs that are inserted into the registration property of the TIR table. An hstore column in PostgreSQL of key/value pairs is used in order to accommodate different registration vectors having varying attributes. Every registration has the following:
# * source - Logical name specifying the source of the registration ("SGCN" in this case)
# * registrationDate - Date/time stamp of the registration
# 
# Most TIR registrations will have a "scientificname" property containing the name string used as a primary identifier. Some TIR registrations will have other identifiers that come from source material.
# 
# SGCN registrations include a list of common names and taxonomic groups supplied by the state and pulled together with an array_agg function and a DISTINCT operator to create a list of unique values in a string. These values can then be reasoned on in TIR processing. The code to register names in the TIR from the SGCN table could operate at any time there are new names showing up in the SGCN, but we might miss some of the aggregated common names when new state data is processed. To deal with this, we could set up a process to periodically check the SGCN records for new instances of a given name and reaggregate common names and taxonomic groups.

# In[1]:

import requests
import json
from datetime import datetime
from IPython.display import display
from bis import tir
from bis import bis
from bis2 import gc2


# In[2]:

# Set up the actions/targets for this particular instance
thisRun = {}
thisRun["instance"] = "DataDistillery"
thisRun["db"] = "BCB"
thisRun["baseURL"] = gc2.sqlAPI(thisRun["instance"],thisRun["db"])
thisRun["commitToDB"] = True
thisRun["totalRecordsToProcess"] = 1000
thisRun["totalRecordsProcessed"] = 0

numberWithoutTIRData = 1

while numberWithoutTIRData == 1 and thisRun["totalRecordsProcessed"] < thisRun["totalRecordsToProcess"]:

    q_recordToSearch = "SELECT scientificname_submitted scientificname         FROM sgcn.sgcn         WHERE scientificname_submitted NOT IN         (SELECT registration->>'scientificname' AS scientificname FROM tir.tir WHERE registration->>'source' = 'SGCN')         GROUP BY scientificname_submitted         LIMIT 1"
    recordToSearch = requests.get(thisRun["baseURL"]+"&q="+q_recordToSearch).json()
    
    numberWithoutTIRData = len(recordToSearch["features"])
    
    if numberWithoutTIRData == 1:
        thisRegistration = {}
        thisRegistration["source"] = "SGCN"
        thisRegistration["registrationDate"] = datetime.utcnow().isoformat()
        thisRegistration["taxonomicLookupProperty"] = "scientificname"
        thisRegistration["followTaxonomy"] = True

        tirRecord = recordToSearch["features"][0]
        thisRegistration["scientificname"] = tirRecord['properties']['scientificname'].replace("\'","''")
        tirRecord = recordToSearch["features"][0]
    
        display (thisRegistration)
        if thisRun["commitToDB"]:
            print (tir.tirRegistration(gc2.sqlAPI("DataDistillery","BCB"),json.dumps(thisRegistration)))
        thisRun["totalRecordsProcessed"] = thisRun["totalRecordsProcessed"] + 1


# ### Final Check
# 
# Check that the total number of SGCN registrations in the TIR match the total unique number of names in the SGCN table.

# In[3]:

q_uniqueSGCNNames = "SELECT COUNT(*) AS num FROM (SELECT DISTINCT scientificname_submitted FROM sgcn.sgcn) AS temp"
r_uniqueSGCNNames = requests.get(gc2.sqlAPI("DataDistillery","BCB")+"&q="+q_uniqueSGCNNames).json()
print ("Total number distinct SGCN scientific names: "+str(r_uniqueSGCNNames["features"][0]["properties"]["num"]))

q_tirRegisteredSGCNNames = "SELECT COUNT(*) AS num FROM tir.tir WHERE registration->>'source' = 'SGCN'"
r_tirRegisteredSGCNNames = requests.get(gc2.sqlAPI("DataDistillery","BCB")+"&q="+q_tirRegisteredSGCNNames).json()
print ("Total number SGCN scientific names in TIR: "+str(r_tirRegisteredSGCNNames["features"][0]["properties"]["num"]))


# In[ ]:



