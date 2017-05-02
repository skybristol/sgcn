
# coding: utf-8

import requests,io,configparser
from IPython.display import display
import pandas as pd

# Get API keys and any other config details from a file that is external to the code.
config = configparser.RawConfigParser()
config.read_file(open(r'../config/stuff.py'))

# Build base URL with API key using input from the external config.
def getBaseURL():
    gc2APIKey = config.get('apiKeys','apiKey_GC2_BCB').replace('"','')
    apiBaseURL = "https://gc2.mapcentia.com/api/v1/sql/bcb?key="+gc2APIKey
    return apiBaseURL

# Query ScienceBase for the 2005 states, returning the files structure along with tags (where we get state name)
sbQ = "https://www.sciencebase.gov/catalog/items?q=2015&parentId=56d720ece4b015c306f442d5&format=json&fields=files,tags&max=100"
sbR = requests.get(sbQ).json()

# Run all the data through and process out the records from each source file
totalRecords = 0
sgcn_year = 2015

for item in sbR['items']:
    sgcn_state = item['tags'][0]['name']
    sourceid = "https://www.sciencebase.gov/catalog/item/"+item['id']
    for file in item['files']:
        if file['name'][-9:] == '_2015.txt':
            stateList = requests.get(file['url']).content
            try:
                stateListPD = pd.read_csv(io.StringIO(stateList.decode('utf-8')))
            except:
                pass

            try:
                stateListPD = pd.read_csv(io.StringIO(stateList.decode('utf-8')), sep='\t')
            except:
                pass

            try:
                stateListPD = pd.read_csv(io.StringIO(stateList.decode('iso-8859-1')), sep='\t')
            except:
                pass

    for ir in stateListPD.itertuples():
        if type(ir[1]) is float:
            scientificname_submitted = ""
        else:
            scientificname_submitted = ir[1].replace("'","''")
        
        
        if type(ir[2]) is float:
            commonname_submitted = ""
        else:
            commonname_submitted = ir[2].replace("'","''")
        
        taxonomicgroup_submitted = ir[3]
        
        if ir[4] in ["N","n","No","no"]:
            firstyear = True
        else:
            firstyear = False
        
        q = "INSERT INTO sgcn.sgcn (sourceid,sgcn_year,sgcn_state,scientificname_submitted,commonname_submitted,taxonomicgroup_submitted,firstyear) VALUES ('"+sourceid+"',"+str(sgcn_year)+",'"+sgcn_state+"','"+scientificname_submitted+"','"+commonname_submitted+"','"+taxonomicgroup_submitted+"',"+str(firstyear)+")"
        r = requests.get(getBaseURL()+"&q="+q).json()
        display (r)
        totalRecords = totalRecords+1


print ("Total Records Processed: "+str(totalRecords))            
