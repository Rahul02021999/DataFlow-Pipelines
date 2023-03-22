# import argparse
# import logging
from google.cloud import storage
# import apache_beam as beam
import xmltodict
# import xml.etree.ElementTree as ET
# from apache_beam.runners import DataflowRunner, DirectRunner
# from apache_beam.options.pipeline_options import GoogleCloudOptions
# from apache_beam.options.pipeline_options import PipelineOptions
# from apache_beam.options.pipeline_options import StandardOptions
# import time

def parse_into_dict(xmlfile):
    global doc
    with xmlfile.open('r') as ifp:
        doc = xmltodict.parse(ifp.read())
        return doc

#Reading .xml file from gcs bucket

def write_read():
    # The ID of your GCS bucket
    bucket_name = "indspirit"

    # The ID of your new GCS object
    blob_name = "ComplexXml.xml"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    #print("Connection Successful ", blob)
    #print(blob)

    # tree = ET.ElementTree(blob)
    # root = tree.getroot()

    parse_into_dict(blob)

write_read()

############################################################

#Function created to fetch attributes in each element
def Customers():
    #x=doc['Root']['Orders']['Order']
    y=doc['Root']['Customers']['Customer']
    return y
    #print(y)

Customer=Customers()
[print(dict) for dict in Customer]

# Cleaning the data and making it ready to writetobigquery 
def cleanupCustomers(x):
    x['CustomerID'] = x['@CustomerID'] # optional attribute
    del x['@CustomerID']
    return x

#cleanupCustomers(Customers)

#############################################################

def Orders():
    x=doc['Root']['Orders']['Order']
    #y=doc['Root']['Customers']['Customer']
    return x
    #print(x)

Order=Orders()
[print(dict) for dict in Order]

def cleanupOrders(x):
    import copy
    y = copy.deepcopy(x)
    if '@ShippedDate' in x['ShipInfo']: # optional attribute
        y['ShipInfo']['ShippedDate'] = x['ShipInfo']['@ShippedDate']
        del y['ShipInfo']['@ShippedDate']
    #print(y)
    return y

#cleanupOrders(Orders)

##############################################################

def Assets():
    h=doc['Root']['Assets']['asset']
    return h
    #print(x)

Asset=Assets()
[print(dict) for dict in Asset]

def cleanupAssets(x):
    import copy
    y = copy.deepcopy(x)
    if '@ShippedDate' in x['ShipInfo']: # optional attribute
        y['ShipInfo']['ShippedDate'] = x['ShipInfo']['@ShippedDate']
        del y['ShipInfo']['@ShippedDate']
    #print(y)
    return y

table_schema = {
    'fields': [
        {'name' : 'CustomerID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name' : 'EmployeeID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name' : 'OrderDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name' : 'RequiredDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name' : 'ShipInfo', 'type': 'RECORD', 'mode': 'NULLABLE', 'fields': [
            {'name' : 'ShipVia', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'Freight', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipAddress', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipCity', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipRegion', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipPostalCode', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipCountry', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShippedDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]},
    ]
}