
# coding: utf-8

# This notebook provides code that processes the [SGCN/SWAP repository in ScienceBase](https://www.sciencebase.gov/catalog/item/56d720ece4b015c306f442d5) into the SGCN database in the GC2 instance we are experimenting with. It works through all of the items in the repo, grabs the source data file, checks the source data file record count against the current database, wipes the current database for that state/year if there's a problem, inserts all records from the source file, and verifies that the new record count matches.
# 
# ### Updates (5/30/2017)
# * I fixed a dumb problem where "taxonomy category" was not getting picked up and processed as "taxonomic_group".
# * I added in a process to check for a list of states to reprocess explicitly in addition to those whose record count does not match source data. This enabled me to clean up an issue where some of the states didn't get their taxonomic group processed properly.
# * I changed the method of detecting the file to operate on and set it up to look first for an update file. If no processable file is found, the script breaks out of the ScienceBase source item and moves on to the next one.
# 
# I use jupyter nbconvert to output this notebook to a python script and run it in its own environment to fully process the repository.

# In[1]:

import requests,io
from IPython.display import display
import pandas as pd
from datetime import datetime

from bis import bis
from bis import sgcn
from bis2 import gc2


# ### Set parameters for the process being run
# 
# A number of parameters are needed for the particular process being run through this script. We build a local dictionary to reference, mostly from functions in the BIS-specific modules.

# In[7]:

thisRun = {}
thisRun["instance"] = "DataDistillery"
thisRun["db"] = "BCB"
thisRun["baseURL"] = gc2.sqlAPI(thisRun["instance"],thisRun["db"])
thisRun["extraCriteria"] = ""
thisRun["reprocessList"] = []
thisRun["numberItemsProcessed"] = 0
thisRun["numberRecordsInserted"] = 0
thisRun["totalRecordsInFiles"] = 0
thisRun["commitItems"] = True


# ### Get data for processing
# 
# The following section runs a query against ScienceBase for all of the items in the SWAP (SGCN) collection. We grab files, tags, and dates because we need to work against all three of those parts of the item structure.
# 
# * Tags - gives us the state name that supplied the list
# * Dates - gives us the year of the SGCN report via the "Collected" date
# * Files - gives us the file URL to access for processing (either an update or the original file)
# 
# Note: This process might need to change if we end up with more than 100 items in the source repository. It might also change when we get to the point of submitting data along the DataDistillery messaging system.

# In[8]:

# Query ScienceBase for all SGCN source items
sbQ = "https://www.sciencebase.gov/catalog/items?parentId=56d720ece4b015c306f442d5&format=json&fields=files,tags,dates&max=100&sort=lastUpdated&order=desc"+thisRun["extraCriteria"]
sbR = requests.get(sbQ).json()


# ### Run the ScienceBase items through the process
# 
# This section uses a number of functions from the tir and sgcn modules to process each item returned in the ScienceBase query. There is probably some other stuff here that could be broken out into more generalized functions.

# In[9]:

# Loop through the repository items and sync data to SGCN database
for item in sbR["items"]:
    # Create an iterative structure to contain processed attributes for a given state and year
    thisItem = {}
    
    # Set the source id as the ScienceBase Item URI/URL to reference from all final records
    thisItem["sourceid"] = item["link"]["url"]
    
    # Extract the state name from the place tag
    # NOTE: This is brittle and needs more work. An item could have many tags, and I just shortcutted to this point based on how Abby has managed these items so far.
    try:
        thisItem["sgcn_state"] = item['tags'][0]['name']
    except:
        display (thisItem)
        break
    
    # Extract the year for this item from the dates collection based on the "Collected" date type
    # Break after we find the date collected that we want to use
    for date in item["dates"]:
        if date["type"] == "Collected":
            thisItem["sgcn_year"] = date["dateString"]
            break
    
    # Stop this item if we did not get a date
    if "sgcn_year" not in list(thisItem.keys()):
        break
    else:
        thisItem["stateYear"] = thisItem["sgcn_state"]+"/"+str(thisItem["sgcn_year"])
    
    # Retrieve the current record count and the last inserted date in the SGCN database for the state and year
    thisItem["startingRecordCount"] = sgcn.getCurrentRecordCount(thisRun["baseURL"],thisItem["sgcn_state"],thisItem["sgcn_year"])
    thisItem["lastDate"]  = datetime.strptime(sgcn.getDateInserted(thisRun["baseURL"],thisItem["sgcn_state"],thisItem["sgcn_year"]), "%Y-%m-%d")

    # Extract the file we need to process from the files structure
    # First look for an update file and process that one. Otherwise get the original file.
    processFile = next((file for file in item["files"] if file["title"] == "Process File"), None)

    if processFile is not None:
        thisItem["sourceFileName"] = processFile["name"]
        thisItem["sourceFileURL"] = processFile["url"]
        thisItem["processFileUploaded"] = datetime.strptime(processFile["dateUploaded"].split("T")[0], "%Y-%m-%d")
    else:
        print ("Problem getting file prepared: "+thisItem["sourceid"])
        break   

    # Retrieve the file into a dataframe for processing
    # The read_table method with explicit tab separator seems to be pretty reliable and robust directly from ScienceBase file URLs, but this may have to be reexamined in future if it fails
    stateData = pd.read_table(thisItem["sourceFileURL"],sep="\t")
    thisItem["sourceRecordCount"] = len(stateData.index)
    thisRun["totalRecordsInFiles"] = thisRun["totalRecordsInFiles"] + thisItem["sourceRecordCount"]
    
    # Determine if we should run this item's file through
    # Check 1: Process File is newer than the last time the item was processed
    # Check 2: Record count in the integrated database does not match the record count in the file
    # Check 3: State and year for this item are in the explicit reprocessList
    thisItem["conditionsMet"] = []
    if thisItem["processFileUploaded"] > thisItem["lastDate"]:
        thisItem["conditionsMet"].append("Newer File in Repository")
    
    if thisItem["startingRecordCount"] != thisItem["sourceRecordCount"]:
        thisItem["conditionsMet"].append("Record Number in Database Does Not Match Record Number in Source File")
    
    if thisItem["stateYear"] in thisRun["reprocessList"]:
        thisItem["conditionsMet"].append(thisItem["stateYear"]+" in Reprocess List")
    display (thisItem)
    if len(thisItem["conditionsMet"]) > 0:
        thisItem["actionLog"] = []
        
        # Clear out the database for reprocessing
        thisItem["actionLog"].append((sgcn.clearStateYear(thisRun["baseURL"],thisItem["sgcn_state"],thisItem["sgcn_year"])))
        thisItem["actionLog"].append("Cleared data for: "+thisItem["stateYear"])
        
        # Set column names to lower case and strip white space to deal with lingering pesky human problems
        stateData.columns = map(str.lower, stateData.columns)
        stateData.columns = map(str.strip, stateData.columns)
        
        # Create a list of duplicate index values in the Scientific Names so that we keep everything but make them all unique
        scientificNames = stateData["scientific name"]
        duplicateNamesList = stateData[scientificNames.isin(scientificNames[scientificNames.duplicated()])].index.tolist()
        duplicateCount = 0
        
        thisRecord = {}
    
        # Loop the dataframe, fix text values, and load to SGCN database
        # NaN values from the dataframe are nulls, show up as float type, and need to get set to a blank string
        for index, row in stateData.iterrows():
            if type(row['scientific name']) is float:
                thisRecord["scientificname_submitted"] = ""
            else:
                thisRecord["scientificname_submitted"] = row["scientific name"]

            if index in duplicateNamesList:
                duplicateCount = duplicateCount + 1
                if thisRecord["scientificname_submitted"] == "":
                    thisRecord["scientificname_submitted"] = thisRecord["sgcn_state"]+" unnamed species "+str(duplicateCount)
                else:
                    thisRecord["scientificname_submitted"] = thisRecord["scientificname_submitted"]+" "+str(duplicateCount)
            
            # Do basic cleaning on the name string in order to get it into the database
            thisRecord["scientificname_submitted"] = bis.stringCleaning(thisRecord["scientificname_submitted"])

            if type(row['common name']) is float:
                thisRecord["commonname_submitted"] = ""
            else:
                thisRecord["commonname_submitted"] = bis.stringCleaning(row['common name'])

            thisRecord["taxonomicgroup_submitted"] = ""
            if 'taxonomy group' in stateData.columns:
                thisRecord["taxonomicgroup_submitted"] = bis.stringCleaning(row['taxonomy group'])
            elif 'taxonomic category' in stateData.columns:
                thisRecord["taxonomicgroup_submitted"] = bis.stringCleaning(row['taxonomic category'])

            thisRecord["firstyear"] = False
            if '2005 swap' in stateData.columns:
                if row['2005 swap'] in ["N","n","No","no"]:
                    thisRecord["firstyear"] = True
            
            # Add in repository item metadata
            thisRecord["sourceid"] = thisItem["sourceid"]
            thisRecord["sourcefilename"] = thisItem["sourceFileName"]
            thisRecord["sourcefileurl"] = thisItem["sourceFileURL"]
            thisRecord["sgcn_state"] = thisItem["sgcn_state"]
            thisRecord["sgcn_year"] = thisItem["sgcn_year"]
            
            # Insert the record
            print (sgcn.insertSGCNData(thisRun["baseURL"],thisRecord))
            thisRun["numberRecordsInserted"] = thisRun["numberRecordsInserted"] + 1
        
        # Check total record count after inserting new data to make sure the numbers line up
        thisItem["finalRecordCount"] = sgcn.getCurrentRecordCount(thisRun["baseURL"],thisItem["sgcn_state"],thisItem["sgcn_year"])
        if thisItem["sourceRecordCount"] != thisItem["finalRecordCount"]:
            print ("Something went wrong with "+thisItem["stateYear"],thisItem["sourceid"])
        else:
            display (thisItem)
        
        thisRun["numberItemsProcessed"] = thisRun["numberItemsProcessed"] + 1

thisRun.pop("baseURL")
display (thisRun)


# In[ ]:



