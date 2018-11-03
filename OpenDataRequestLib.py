"""Identify, List & Retrieve open data sets. 

Usage:
    from OpenDataRequestLib import Socrata

Args:
    None.
"""
#-----------------------------------------------------------------------------
from urllib.request import urlopen
from urllib.parse import urlencode
from urllib.request import Request
import logging
import codecs
import json
import requests
import os

log = logging.getLogger("OpenDataRequestLib")
#-----------------------------------------------------------------------------
def GetSocrataDomainList(url):
    """call the Socrata api and request the list of data domains

    Param:
        url: override the internally set socrata api URL

    Return:
        domainList[]
    """
    if url == None:
        url = "https://api.us.socrata.com/api/catalog/v1/domains"
    
    log.info("[GetSocrataDomainList]")
    payload = RetrievePayload(url)
    payload_json = ConvertPayloadToJson(payload)
    domainList = []

    for result_item in payload_json['results']:
        set_count = 0
        domain_url = "none"
        adomain_dict = result_item

        for k, v in adomain_dict.items():
            if k == 'domain':
                domain_url = v
            elif k == 'thing':
                domain_url = v
            else:
                set_count = v
            
        if set_count > 0:
            domainList.append((domain_url, set_count))

    return domainList
#-----------------------------------------------------------------------------
def GetSocrataDatasetList(domainList, domainListIndex):
    """call the Socrata api and request the list of domain datasets

    Param:
        domainList:      the Socrata list of data domains
        domainListIndex: the domain index from ShowSocrataDomains() output

    Return:
        datasetList[]
    """
    if domainListIndex == None:
        domainListIndex = 0
    else:
        domainListIndex = domainListIndex-1

    log.info("[GetSocrataDatasetList]")
    data_domain = domainList[domainListIndex][0]
    payload = RetrievePayload("https://api.us.socrata.com/api/catalog/v1?domains=" + data_domain)
    payload_json = ConvertPayloadToJson(payload)

    datasetList = []
    for allDatasets in payload_json['results']:
        dict_done = 0
        set_name = "none"
        set_desc = "none"
        set_link = "none"
        set_updated = "none"

        for k, v in allDatasets.items():
            if dict_done == 2:
                datasetList.append((set_name, set_desc, set_link, set_updated))
                dict_done = 0

            if k == 'resource':
                set_name = v['name']
                set_desc = v['description']
                set_updated = v['updatedAt']
                dict_done += 1
            elif k == 'permalink':
                set_link = v + ".json"
                dict_done += 1

    return datasetList
#-----------------------------------------------------------------------------
def GetSocrataDataset(datasetList, datasetListIndex):
    """call the Socrata api and request a specific dataset

    Param:
        datasetList:      the Socrata list of datasets for a domain
        datasetListIndex: the dataset index from ShowSocrataDatasets() output

    Return:
        datasetDetails[]
    """
    if datasetListIndex == None:
        datasetListIndex = 0
    else:
        datasetListIndex = datasetListIndex-1

    return datasetList[datasetListIndex][2]
#-----------------------------------------------------------------------------



#-----------------------------------------------------------------------------
def ShowSocrataDomains(domains):
    """Print to the console, an indexed list of the Socrata data domains

    Param:
        domains: a list of the available data domains
    """
    idx = 0
    for i in domains:
        idx += 1
        print("{index: <{fill1}} {domainurl: <{fill2}} has {sets} datasets".format(index=idx, 
                                    fill1=5, domainurl=i[0], fill2=45, sets=i[1]))
#-----------------------------------------------------------------------------
def ShowSocrataDatasets(datasets):
    """Print to the console, an indexed list of datasets in a Socrata domain

    Param:
        datasets: a list of the available datasets
    """
    idx = 0
    for i in datasets:
        idx += 1
        print("{index: <{fill1}} {dataset: <{fill2}} {desc}".format(index=idx, 
                                    fill1=5, dataset=i[0], fill2=45, desc=i[1]))
#-----------------------------------------------------------------------------



#-----------------------------------------------------------------------------
def ConvertPayloadToJson(payload):
    """Convert a string to a dictionary object containing the JSON structure

    Param:
        payload: the unconverted string of raw data

    Return:
        json_dict: a dictionary object
    """
    log.info("[ConvertPayloadToJson]")
    json_dict = []
    try:
        json_dict = json.loads(payload)
        return json_dict
    except Exception as er:
        print("ERROR: json Conversion error: {}".format(str(er)))
        log.warning("json conversion error = {}".format(str(er)))
    finally:
        return json_dict
#-----------------------------------------------------------------------------
def RetrievePayload(url):
    """HTTP Get the raw data payload from a URL

    Param:
        url: the location of the payload to retrieve

    Return:
        raw_data: a string value
    """
    log.info("[RetrievePayload] - {}".format(url))
    user_agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
    headers = { 'User-Agent' : user_agent }
    req = Request(url, None, headers)
    
    global openDataCharSet

    with urlopen(req) as payload:
        http_info = payload.info()
        openDataCharSet = http_info.get_content_charset()
        raw_data = payload.read().decode(openDataCharSet)
        log.info("payload character set is {}".format(openDataCharSet))
    return raw_data
#-----------------------------------------------------------------------------
def DownloadFile(url, filePath):
    """Download a dataset to a specified file path

    Param:
        url:      the resource locator of a dataset
        filePath: the destination file location to write
    """
    download_file = requests.get(url, stream=True)

    parts = url.split('/')
    file_name = parts[-1]
    localName = os.path.join(filePath, file_name)

    with open(localName, 'wb') as f:
        f.write(download_file.content)
    
    log.info("dataset written to file {}".format(localName))
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------