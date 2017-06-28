
# coding: utf-8

# This notebook builds out a set of SGCN-specific annotations in the TIR based on configuration files housed on the SGCN source repository item in ScienceBase. It aligns taxonomic groups with a logical set of higher taxonomy names, setting all others to "other" if not found in the config file. It then uses a cached list of the original species names identified for the 2005 SWAP exercise to flag taxa that should be included in that list. We use the preferred taxonomic group in the national and state lists for display and filtering, and we use the hard list of 2005 species to flag them to the "National List" for consistency when our current process of checking taxonomic authorities (ITIS and WoRMS) does not turn up the names.
# 
# Note that this entire script needs to run in sequence. Because we are coming into this from the standpoint of external config files, it is much more efficient to use targeted SQL statements to update a whole set of records at a time in the TIR as opposed to looping every TIR record and pulling information from the config files.

# In[12]:

import requests
from IPython.display import display
import pandas as pd
from bis2 import gc2


# In[16]:

# Set up the actions/targets for this particular instance
thisRun = {}
thisRun["instance"] = "DataDistillery"
thisRun["db"] = "BCB"
thisRun["baseURL"] = gc2.sqlAPI(thisRun["instance"],thisRun["db"])
thisRun["commitToDB"] = True
thisRun["resetSGCN"] = False


# In[17]:

# Reset SGCN annotation in TIR
if thisRun["resetSGCN"]:
    print (requests.get(gc2.sqlAPI("DataDistillery","BCB")+"&q=UPDATE tir.tir SET sgcn = NULL").json())


# In[18]:

# Retrieve information from stored files on the SGCN base repository item
sb_sgcnCollectionItem = requests.get("https://www.sciencebase.gov/catalog/item/56d720ece4b015c306f442d5?format=json&fields=files").json()

for file in sb_sgcnCollectionItem["files"]:
    if file["title"] == "Configuration:Taxonomic Group Mappings":
        tgMappings = pd.read_table(file["url"], sep=",", encoding="utf-8")
    elif file["title"] == "Original 2005 SWAP National List for reference":
        swap2005 = pd.read_table(file["url"])


# In[19]:

# Insert the tax group name we want to use for any cases where the corresponding names are found from submitted data
if thisRun["resetSGCN"]:
    for index, row in tgMappings.iterrows():
        providedName = str(row["ProvidedName"])
        preferredName = str(row["PreferredName"])
        preferredNamePair = '"taxonomicgroup"=>"'+preferredName+'"'
        print (providedName, preferredName)
        q_updateGroups = "UPDATE tir.tir SET sgcn = '"+preferredNamePair+"' WHERE registration->'taxonomicgroups' LIKE '%"+providedName+"%' OR registration->'taxonomicgroups' LIKE '%"+preferredName+"%'"
        r = requests.get(gc2.sqlAPI("DataDistillery","BCB")+"&q="+q_updateGroups).json()

        # Deal with really stupid problem with "Ec" and "Ce" are throwing a fit with PostgreSQL
        # Strip first character from the provided name and try the query again
        while "message" in r.keys():
            providedName = providedName[1:]
            print (providedName, preferredName)
            q_updateGroups = "UPDATE tir.tir SET sgcn = '"+preferredNamePair+"' WHERE registration->'taxonomicgroups' LIKE '%"+providedName+"%' OR registration->'taxonomicgroups' LIKE '%"+preferredName+"%'"
            r =  requests.get(gc2.sqlAPI("DataDistillery","BCB")+"&q="+q_updateGroups).json()


# In[20]:

# Add "other" as the taxonomic group for anything left over
if thisRun["resetSGCN"]:
    otherGroupPair = '"taxonomicgroup"=>"other"'
    q_updateOther = "UPDATE tir.tir SET sgcn = '"+otherGroupPair+"' WHERE sgcn IS NULL"
    r = requests.get(gc2.sqlAPI("DataDistillery","BCB")+"&q="+q_updateOther).json()
    print ("Other", r["affected_rows"])


# In[24]:

thisRun["totalRecordsToProcess"] = 500
thisRun["totalRecordsProcessed"] = 0

numberWithoutTIRData = 1

while numberWithoutTIRData == 1 and thisRun["totalRecordsProcessed"] <= thisRun["totalRecordsToProcess"]:

    q_recordToSearch = "SELECT id,         registration->'scientificname' AS scientificname         FROM tir.tir         WHERE NOT exist(sgcn, 'swap2005')         LIMIT 1"
    recordToSearch  = requests.get(thisRun["baseURL"]+"&q="+q_recordToSearch).json()

    numberWithoutTIRData = len(recordToSearch["features"])
    
    if numberWithoutTIRData == 1:
        tirRecord = recordToSearch["features"][0]
    
        thisRecord = {}
        thisRecord["id"] = tirRecord["properties"]["id"]
        thisRecord["scientificname_tir"] = tirRecord["properties"]["scientificname"]
        thisRecord["kv_swap2005"] = '"swap2005"=>"false"'
        
        if any(swap2005.scientificname == thisRecord["scientificname_tir"]):
            thisRecord["kv_swap2005"] = '"swap2005"=>"true"'

        display (thisRecord)
        if thisRun["commitToDB"]:
            q_updateSWAP2005 = "UPDATE tir.tir                 SET sgcn = sgcn || '"+thisRecord["kv_swap2005"]+"' :: hstore                 WHERE id = "+str(thisRecord["id"])
            print (requests.get(gc2.sqlAPI("DataDistillery","BCB")+"&q="+q_updateSWAP2005).json())
            
        thisRun["totalRecordsProcessed"] = thisRun["totalRecordsProcessed"] + 1


# In[ ]:



