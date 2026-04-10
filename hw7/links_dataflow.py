#!/usr/bin/env python3
"""
Pipeline 1 (Dataflow): Link Analysis using Apache Beam on Cloud Dataflow
Reads 20,000 HTML files from GCS, extracts links,
and finds top 5 files by incoming and outgoing links.
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
        logging.info(f"Found {count} files")


def extract_links_from_line(line, filename_holder=[None]):
    """
    This function is called per-line by ReadAllFromText.
    We accumulate content and extract links from each file.
    """
    return line


def extract_links_from_content(element):
    """Extract link targets from file content. Element is (None, content_line)."""
    filename, content = element
    targets = re.findall(r'<a\s+HREF="([^"]+)"', content, re.IGNORECASE)
    # Extract just the filename from the full GCS path
    basename = filename.split('/')[-1] if '/' in filename else filename
    return (basename, targets)


class ReadFileAndExtractLinks(beam.DoFn):
    """Read a single GCS file and extract all link targets."""
    def process(self, file_uri):
        try:
            from google.cloud import storage as gcs
            # Parse bucket and blob name from URI
            # gs://bucket-name/path/to/file.html
            parts = file_uri.replace("gs://", "").split("/", 1)
            bucket_name = parts[0]
            blob_name = parts[1]
            filename = blob_name.split("/")[-1]

            client = gcs.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            content = blob.download_as_text()

            targets = re.findall(r'<a\s+HREF="([^"]+)"', content, re.IGNORECASE)
            yield (filename, targets)
        except Exception as e:
            logging.warning(f"Error reading {file_uri}: {e}")


def emit_outgoing(element):
    filename, targets = element
    yield (filename, len(targets))


def emit_incoming(element):
    filename, targets = element
    for target in targets:
        yield (target, 1)


def run():
    start_time = time.time()

    # First, count files
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix=PREFIX))
    html_blobs = [b for b in blobs if b.name.endswith('.html')]
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
        # Generate file URIs from bucket listing
        file_uris = (
            p
            | 'CreateSeed' >> beam.Create([None])
            | 'GenerateManifest' >> beam.ParDo(GenerateManifestDoFn())
        )

        # Read each file and extract links
        parsed = (
            file_uris
            | 'ReadAndExtractLinks' >> beam.ParDo(ReadFileAndExtractLinks())
        )

        # --- Outgoing Links ---
        top_outgoing = (
            parsed
            | 'EmitOutgoing' >> beam.FlatMap(emit_outgoing)
            | 'TopOutgoing' >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        )

        # --- Incoming Links ---
        top_incoming = (
            parsed
            | 'EmitIncoming' >> beam.FlatMap(emit_incoming)
            | 'SumIncoming' >> beam.CombinePerKey(sum)
            | 'TopIncoming' >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        )

        # Print results
        top_outgoing | 'PrintOutgoing' >> beam.Map(
            lambda x: print(f"\nTop 5 by Outgoing Links:\n{x}")
        )
        top_incoming | 'PrintIncoming' >> beam.Map(
            lambda x: print(f"\nTop 5 by Incoming Links:\n{x}")
        )

    elapsed = time.time() - start_time
    print(f"\nDataflow job runtime: {elapsed:.2f} seconds")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
