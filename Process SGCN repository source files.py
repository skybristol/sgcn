
# coding: utf-8

# This notebook provides code that processes the [SGCN/SWAP repository in ScienceBase](https://www.sciencebase.gov/catalog/item/56d720ece4b015c306f442d5) into the SGCN database in the GC2 instance we are experimenting with. It works through all of the items in the repo, grabs the source data file, checks the source data file record count against the current database, wipes the current database for that state/year if there's a problem, inserts all records from the source file, and verifies that the new record count matches.
# 
# I use jupyter nbconvert to output this notebook to a python script and run it in its own environment to fully process the repository.

# In[1]:

import requests,io,configparser
from IPython.display import display
import pandas as pd


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


# In[4]:

def getCurrentRecordCount(sgcn_state,sgcn_year):
    r = requests.get(getBaseURL()+"&q=SELECT COUNT(*) AS sumstateyear FROM sgcn.sgcn WHERE sgcn_year="+str(sgcn_year)+" AND sgcn_state='"+sgcn_state+"'").json()
    return r["features"][0]["properties"]["sumstateyear"]


# In[5]:

def clearStateYear(sgcn_state,sgcn_year):
    return requests.get(getBaseURL()+"&q=DELETE FROM sgcn.sgcn WHERE sgcn_year="+str(sgcn_year)+" AND sgcn_state='"+sgcn_state+"'").json()


# In[6]:

def insertSGCNData(record):
    q = "INSERT INTO sgcn.sgcn (sourceid,sourcefilename,sourcefileurl,sgcn_state,sgcn_year,scientificname_submitted,commonname_submitted,taxonomicgroup_submitted,firstyear)         VALUES         ('"+record["sourceid"]+"','"+record["sourcefilename"]+"','"+record["sourcefileurl"]+"','"+record["sgcn_state"]+"',"+str(record["sgcn_year"])+",'"+record["scientificname_submitted"]+"','"+record["commonname_submitted"]+"','"+record["taxonomicgroup_submitted"]+"',"+str(record["firstyear"])+")"
    return requests.get(getBaseURL()+"&q="+q).json()


# In[7]:

def stringCleaning(text):
    import re

    # Specify replacements
    replacements = {}
    replacements["'"] = "''"
    replacements["--"] = ""
    replacements["&"] = "and"
    replacements['"'] = "''"
    replacements[";"] = ","
    replacements["#"] = "no."
    
    # Compile the expressions
    regex = re.compile("(%s)" % "|".join(map(re.escape, replacements.keys())))

    # Strip the text
    text = text.strip()

    # Process replacements
    return regex.sub(lambda mo: replacements[mo.string[mo.start():mo.end()]], text)


# In[22]:

# Query ScienceBase for all SGCN source items
sbQ = "https://www.sciencebase.gov/catalog/items?parentId=56d720ece4b015c306f442d5&format=json&fields=files,tags,dates&max=100"
sbR = requests.get(sbQ).json()


# In[ ]:

# Set a list of states and years to reprocess
reprocessList = ["District of Columbia/2005","Georgia/2005","Guam/2005","Hawaii/2005","Idaho/2005","Illinois/2005","Massachusetts/2005","Mississippi/2005","Nevada/2005","New Mexico/2005","North Carolina/2005","Ohio/2005","Oklahoma/2005","Puerto Rico/2005","South Carolina/2005","Tennessee/2005","Texas/2005","Vermont/2005","Washington/2005","Wisconsin/2005","Wyoming/2005"]

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
    # Also check to see if the state/year are explicitly in a list to reprocess for this run
    if thisItem["startingrecordcount"] != totalRecordsThisFile or thisItem["sgcn_state"]+"/"+str(thisItem["sgcn_year"]) in reprocessList:
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
                thisRecord["taxonomicgroup_submitted"] = stringCleaning(row['taxonomy group'])
            elif 'taxonomic category' in stateData.columns:
                thisRecord["taxonomicgroup_submitted"] = stringCleaning(row['taxonomic category'])

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
    
    


# In[ ]:



