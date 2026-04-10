#!/usr/bin/env python3
"""
Pipeline 2 (Dataflow): Word Bigram Analysis using Apache Beam on Cloud Dataflow
Reads 20,000 HTML files from GCS, strips HTML tags,
tokenizes text, and finds top 5 most frequent consecutive word bigrams.
"""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from google.cloud import storage
import re
import time
import logging

BUCKET_NAME = "bu-cs528-mahicm13"
PREFIX = "html_files/"
PROJECT = "constant-idiom-485622-f3"
REGION = "us-east1"
TEMP_LOCATION = f"gs://{BUCKET_NAME}/dataflow-temp"
STAGING_LOCATION = f"gs://{BUCKET_NAME}/dataflow-staging"


class GenerateManifestDoFn(beam.DoFn):
    """Lists all HTML files in the GCS bucket and yields their URIs."""
    def process(self, element):
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blobs = bucket.list_blobs(prefix=PREFIX)
        count = 0
        for blob in blobs:
            if blob.name.endswith('.html'):
                yield f"gs://{BUCKET_NAME}/{blob.name}"
                count += 1
                if count >= 5000:
                    break
        logging.info(f"Found {count} files")


class ReadFileAndExtractBigrams(beam.DoFn):
    """Read a GCS file, strip HTML, extract word bigrams."""
    def process(self, file_uri):
        try:
            from google.cloud import storage as gcs
            parts = file_uri.replace("gs://", "").split("/", 1)
            bucket_name = parts[0]
            blob_name = parts[1]

            client = gcs.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            content = blob.download_as_text()

            # Strip HTML tags
            text = re.sub(r'<[^>]+>', ' ', content)
            # Tokenize to lowercase words
            words = re.findall(r'[a-z]+', text.lower())
            # Generate bigrams
            for i in range(len(words) - 1):
                yield (f"{words[i]} {words[i+1]}", 1)
        except Exception as e:
            logging.warning(f"Error reading {file_uri}: {e}")


def run():
    start_time = time.time()

    # Count files first
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix=PREFIX))
    html_blobs = [b for b in blobs if b.name.endswith('.html')][:5000]
    print(f"Found {len(html_blobs)} files, submitting to Dataflow...")

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project=PROJECT,
        region=REGION,
        temp_location=TEMP_LOCATION,
        staging_location=STAGING_LOCATION,
        machine_type='e2-standard-2',
        num_workers=4,
        save_main_session=True,
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        file_uris = (
            p
            | 'CreateSeed' >> beam.Create([None])
            | 'GenerateManifest' >> beam.ParDo(GenerateManifestDoFn())
        )

        top_bigrams = (
            file_uris
            | 'ExtractBigrams' >> beam.ParDo(ReadFileAndExtractBigrams())
            | 'SumBigrams' >> beam.CombinePerKey(sum)
            | 'TopBigrams' >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        )

        top_bigrams | 'PrintBigrams' >> beam.Map(
            lambda x: print(f"\nTop 5 Bigrams:\n{x}")
        )

    elapsed = time.time() - start_time
    print(f"\nDataflow job runtime: {elapsed:.2f} seconds")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
