# working through the Minimal word count example from the official Apache Beam documentation 

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import os 
from dotenv import load_dotenv
import re

load_dotenv()

# seting options for pipeline 
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = os.getenv('GCP_PROJECT_ID')
google_cloud_options.job_name = os.getenv('JOB_NAME')
google_cloud_options.staging_location = os.getenv('STAGING_BUCKET')
google_cloud_options.temp_location = os.getenv('TEMP_BUCKET')
options.view_as(StandardOptions).runner = os.getenv('RUNNER')

# creating the Pipeline object on which the options will act on with 
p = beam.Pipeline(options=options)

p | beam.io.ReadFromText('gs://dataflow-samples/shakespeare/kinglear.txt') \
| 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x)) \
| beam.combiners.Count.PerElement() \
| beam.MapTuple(lambda word, count: '%s: %s' % (word, count)) \
| beam.io.WriteToText('gs://cm-explore-beam/counts.txt')

result = p.run()