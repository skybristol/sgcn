
# coding: utf-8

# This notebook provides code that processes the [SGCN/SWAP repository in ScienceBase](https://www.sciencebase.gov/catalog/item/56d720ece4b015c306f442d5) into the SGCN database in the GC2 instance we are experimenting with. It works through all of the items in the repo, grabs the source data file, checks the source data file record count against the current database, wipes the current database for that state/year if there's a problem, inserts all records from the source file, and verifies that the new record count matches.
# 
# I use jupyter nbconvert to output this notebook to a python script and run it in its own environment to fully process the repository.

# In[16]:

import requests,io,configparser
from IPython.display import display
import pandas as pd


# In[17]:

# Get API keys and any other config details from a file that is external to the code.
config = configparser.RawConfigParser()
config.read_file(open(r'../config/stuff.py'))


# In[18]:

# Build base URL with API key using input from the external config.
def getBaseURL():
    gc2APIKey = config.get('apiKeys','apiKey_GC2_BCB').replace('"','')
    apiBaseURL = "https://gc2.mapcentia.com/api/v1/sql/bcb?key="+gc2APIKey
    return apiBaseURL


# In[19]:

def getCurrentRecordCount(sgcn_state,sgcn_year):
    r = requests.get(getBaseURL()+"&q=SELECT COUNT(*) AS sumstateyear FROM sgcn.sgcn WHERE sgcn_year="+str(sgcn_year)+" AND sgcn_state='"+sgcn_state+"'").json()
    return r["features"][0]["properties"]["sumstateyear"]


# In[20]:

def clearStateYear(sgcn_state,sgcn_year):
    return requests.get(getBaseURL()+"&q=DELETE FROM sgcn.sgcn WHERE sgcn_year="+str(sgcn_year)+" AND sgcn_state='"+sgcn_state+"'").json()


# In[21]:

def insertSGCNData(record):
    q = "INSERT INTO sgcn.sgcn (sourceid,sourcefilename,sourcefileurl,sgcn_state,sgcn_year,scientificname_submitted,commonname_submitted,taxonomicgroup_submitted,firstyear)         VALUES         ('"+record["sourceid"]+"','"+record["sourcefilename"]+"','"+record["sourcefileurl"]+"','"+record["sgcn_state"]+"',"+str(record["sgcn_year"])+",'"+record["scientificname_submitted"]+"','"+record["commonname_submitted"]+"','"+record["taxonomicgroup_submitted"]+"',"+str(record["firstyear"])+")"
    return requests.get(getBaseURL()+"&q="+q).json()


# In[25]:

def stringCleaning(string):
    # These are things found in the name strings that cause problems sending the data over the GC2 API to the database
    # It might be faster to do this with regex
    string = string.replace("'","''")
    string = string.replace("--","")
    string = string.replace("&","and")
    string = string.replace('"',"''")
    string = string.replace(";",",")
    string = string.replace("#","no.")
    return string


# In[26]:

# Query ScienceBase for all SGCN source items
sbQ = "https://www.sciencebase.gov/catalog/items?parentId=56d720ece4b015c306f442d5&format=json&fields=files,tags,dates&max=100"
sbR = requests.get(sbQ).json()


totalRecordsInFiles = 0

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
    
    # Retrieve the current record count in the SGCN database for the state and year
    thisItem["startingrecordcount"] = getCurrentRecordCount(thisItem["sgcn_state"],thisItem["sgcn_year"])
    
    # Extract the file we need to process from the files structure
    # Break after we find that file - assumes there is only one original file to process
    # Still need to deal with the update files
    for file in item["files"]:
        if file["title"] == "Process ready version of original file":
            thisItem["sourcefilename"] = file["name"]
            thisItem["sourcefileurl"] = file["url"]
            break
            
    # Retrieve the file into a dataframe for processing
    # The read_table method with explicit tab separator seems to be pretty reliable and robust directly from ScienceBase file URLs, but this may have to be reexamined in future if it fails
    stateData = pd.read_table(thisItem["sourcefileurl"],sep='\t')
    totalRecordsThisFile = len(stateData.index)
    totalRecordsInFiles = totalRecordsInFiles + totalRecordsThisFile

    # Check the total columns in the source data against the starting record count from the database
    if thisItem["startingrecordcount"] != totalRecordsThisFile:
        # If the number of source records does not match the current database, clear out the database for reprocessing
        print (clearStateYear(thisItem["sgcn_state"],thisItem["sgcn_year"]))
        print ("Cleared data for: "+thisItem["sgcn_state"]+" - "+str(thisItem["sgcn_year"]))
    
        # Store the source record count for reference
        thisItem["sourcerecordcount"] = len(stateData.index)
    
        # Set column names to lower case to deal with pesky human problems
        stateData.columns = map(str.lower, stateData.columns)
        
        thisRecord = {}
    
        # Loop the dataframe, fix text values, and load to SGCN database
        # NaN values from the dataframe are nulls, show up as float type, and need to get set to a blank string
        for index, row in stateData.iterrows():
            if type(row['scientific name']) is float:
                thisRecord["scientificname_submitted"] = ""
            else:
                thisRecord["scientificname_submitted"] = stringCleaning(row['scientific name'])

            if type(row['common name']) is float:
                thisRecord["commonname_submitted"] = ""
            else:
                thisRecord["commonname_submitted"] = stringCleaning(row['common name'])

            thisRecord["taxonomicgroup_submitted"] = ""
            if 'taxonomy group' in stateData.columns:
                thisRecord["taxonomicgroup_submitted"] = row['taxonomy group']
            elif 'taxonomic group' in stateData.columns:
                thisRecord["taxonomicgroup_submitted"] = row['taxonomic group']

            thisRecord["firstyear"] = False
            if '2005 swap' in stateData.columns:
                if row['2005 swap'] in ["N","n","No","no"]:
                    thisRecord["firstyear"] = True
            
            # Add in repository item metadata
            thisRecord["sourceid"] = thisItem["sourceid"]
            thisRecord["sourcefilename"] = thisItem["sourcefilename"]
            thisRecord["sourcefileurl"] = thisItem["sourcefileurl"]
            thisRecord["sgcn_state"] = thisItem["sgcn_state"]
            thisRecord["sgcn_year"] = thisItem["sgcn_year"]
            
            # Insert the record
            print(insertSGCNData(thisRecord))
        
        # Check total record count after inserting new data to make sure the numbers line up
        if thisItem["sourcerecordcount"] != getCurrentRecordCount(thisItem["sgcn_state"],thisItem["sgcn_year"]):
            print ("Something went wrong with "+thisItem["sgcn_state"],thisItem["sgcn_year"],thisItem["sourceid"])
        else:
            print (thisItem["sgcn_state"],thisItem["sgcn_year"])
            print ("Source record count: "+str(thisItem["sourcerecordcount"]))
            print ("Database record count: "+str(getCurrentRecordCount(thisItem["sgcn_state"],thisItem["sgcn_year"])))
    else:
        print ("Record Numbers Matched: "+thisItem["sgcn_state"]+" - "+str(thisItem["sgcn_year"]))
