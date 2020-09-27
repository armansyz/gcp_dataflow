from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
import apache_beam as beam
import logging
import json
import argparse
import os
import re


PROJECT = os.environ.get('PROJECT_ID')
TOPIC = "projects/{0}/topics/{1}".format(PROJECT, os.environ.get('TOPIC'))
schema = 'author:STRING, comment:STRING, sentiment:FLOAT'


class AnalyzeComment(beam.DoFn):
    def to_runner_api_parameter(self, unused_context):
        pass

    def process(self, element):
        def language_analysis(text):

            client = language.LanguageServiceClient()

            document = types.Document(
                content=text,
                type=enums.Document.Type.PLAIN_TEXT)

            return client.analyze_sentiment(document=document).document_sentiment

        result = json.loads(element)
        sentiment = language_analysis(result['comment'])
        return [{
            'author': result['author'],
            'comment': re.sub(r'[^a-zA-Z0-9?!.: /]+', ' ', result['comment']),
            'sentiment': float(sentiment.score)
        }]


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_topic")
    parser.add_argument("--output")
    known_args = parser.parse_known_args(argv)
    p = beam.Pipeline(options=PipelineOptions())
    (p
     | 'ReadData' >> beam.io.ReadFromPubSub(topic=TOPIC).with_output_types(bytes)
     | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
     | 'SentimentAnalysis' >> beam.ParDo(AnalyzeComment())
     | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:jiracomments.jira_comments'.format(PROJECT), schema=schema,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
     )
    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logger = logging.getLogger().setLevel(logging.INFO)
    main()
