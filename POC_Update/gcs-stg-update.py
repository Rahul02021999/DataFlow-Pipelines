import argparse
import time
import logging
import json
import typing
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.sql import SqlTransform
from apache_beam.runners import DataflowRunner, DirectRunner

# ### functions and classes

def c_types_emp(data):
    data['empno'] = int(data['empno'])
    data['ename'] = str(data['ename'])
    data['deptno'] = int(data['deptno'])
    data['sal'] = int(data['sal'])
    return data

# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--staging_location', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--temp_location', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--input_path', required=True, help='Path to events.json')
    #parser.add_argument('--input_path2', required=True, help='Path to events.json')
    parser.add_argument('--stg_table_name', required=True, help='BigQuery table for raw data')
    #parser.add_argument('--wh_table_name', required=True, help='BigQuery table for percentage data')

    opts, pipeline_opts = parser.parse_known_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(pipeline_opts, save_main_session=True)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.staging_location
    options.view_as(GoogleCloudOptions).temp_location = opts.temp_location
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('gcs-to-stg'
                                                                   ,time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner

    input_path = opts.input_path
    stg_table_name = opts.stg_table_name

    # Create the pipeline
    p = beam.Pipeline(options=options)

    stg_table = (p | 'ReadFromGCS' >> beam.io.ReadFromText(input_path,skip_header_lines=1)
              | 'splitingcsv' >> beam.Map(lambda x:x.split(","))
              | 'Format_e' >> beam.Map(lambda x: {"empno": x[0], "ename":x[1], "deptno":x[2],"sal":x[3]})
              | 'DataType_e' >> beam.Map(c_types_emp)
              #| 'printingcsv' >> beam.Map(print)
              )

    (stg_table | 'WriteRawToBQ' >> beam.io.WriteToBigQuery(
           stg_table_name,
           #schema='SCHEMA_AUTODETECT',
           schema='empno:INTEGER,ename:STRING,deptno:INTEGER,sal:INTEGER',
           create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
           write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
          ))

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()

if __name__ == '__main__':
  run()

# CLI Command >> python3 gcs-stg-update.py --project=indspirit --region='europe-west2' --runner=DirectRunner --experiments=use_runner_v2 --input_path=gs://gcs-bq-files/emp.csv --temp_location=gs://gcs-bq-files/temp --staging_location=gs://gcs-bq-files/stgtemp --stg_table_name=indspirit.storagetobq.stg-Emp