from XmlScript.xmlFuncScript import *
import argparse
import logging
# from google.cloud import storage
import apache_beam as beam
# import xmltodict
# #import xml.etree.ElementTree as ET
from apache_beam.runners import DataflowRunner, DirectRunner
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
import time

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from XML into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--tempLocation', required=True, help='Temp Location')
    parser.add_argument('--output',required=True,help=('Specify text file orders.txt or BigQuery table project:dataset.table '))
    parser.add_argument('--output2',required=True,help=('Specify text file orders.txt or BigQuery table project:dataset.table '))
    parser.add_argument('--output3',required=True,help=('Specify text file orders.txt or BigQuery table project:dataset.table '))
    #parser.add_argument('--template_location', required=True, help='Specify Apache Beam Runner')

    opts, pipeline_opts = parser.parse_known_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(pipeline_opts)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).temp_location = opts.tempLocation
    #options.view_as(GoogleCloudOptions).template_location = opts.template_location ###
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('xml-parsing-testing',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    output = opts.output
    output2 = opts.output2
    output3 = opts.output3

    # Create the pipeline
    p = beam.Pipeline(options=options)

    Customers = (p 
             | 'Customers data' >> beam.Create(Customer) #######
             | 'cleaning customers' >> beam.Map(cleanupCustomers)
             #| 'print' >> beam.Map(print))
             | 'Customer data tobq' >> beam.io.WriteToBigQuery(output,
                                       schema='SCHEMA_AUTODETECT',
                                       create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                       write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE #WRITE_TRUNCATE
                                       )
                                       )

    Orders = (p 
             | 'Orders data' >> beam.Create(Order) #######
             #| 'clean2' >> beam.Map(lambda x : cleanupOrders(x))
             | 'cleaning orders' >> beam.Map(cleanupOrders)
             #| 'print2' >> beam.Map(print))
             | 'Orders data tobq' >> beam.io.WriteToBigQuery(output2,
                                       schema=table_schema,
                                       #schema='SCHEMA_AUTODETECT',
                                       create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                       write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE #WRITE_TRUNCATE
                                       )
                                       )

    Assets = (p 
             | 'Asset data' >> beam.Create(Asset) #######
             #| 'clean3' >> beam.Map(lambda x : cleanupAssets(x))
             | 'cleaning assets' >> beam.Map(cleanupAssets)
             #| 'print2' >> beam.Map(print))
             | 'Assets data tobq' >> beam.io.WriteToBigQuery(output3,
                                       schema='SCHEMA_AUTODETECT',
                                       create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                       write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE, #WRITE_TRUNCATE
                                       )
                                       )

    p.run() #running the pipeline

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

# CLI Command --> python3 FinalxmlParsing.py --project version2-2022 --output version2-2022:StoragetoBQ.Customers  --tempLocation gs://indspirit/temp2 --output2 version2-2022:StoragetoBQ.Orders --output3 version2-2022:StoragetoBQ.Assets --runner DataFlowRunner --region us-central1 --setup ./setup.py