#!/usr/bin/env python3
"""
Pipeline 1 (Local): Link Analysis using Apache Beam (FnApiRunner)
Reads 20,000 HTML files from local disk, extracts links,
and finds top 5 files by incoming and outgoing links.
"""
import apache_beam as beam
from apache_beam.runners.portability.fn_api_runner import FnApiRunner
import re
import os
import time
import glob

INPUT_DIR = "/tmp/htmlfiles"

def read_and_extract_links(filepath):
    """Read a single HTML file and extract all link targets."""
    try:
        filename = os.path.basename(filepath)
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        targets = re.findall(r'<a\s+HREF="([^"]+)"', content, re.IGNORECASE)
        return (filename, targets)
    except Exception as e:
        return (os.path.basename(filepath), [])

def emit_outgoing(element):
    """Emit (filename, outgoing_count) for each file."""
    filename, targets = element
    yield (filename, len(targets))

def emit_incoming(element):
    """Emit (target, 1) for each link target found."""
    filename, targets = element
    for target in targets:
        yield (target, 1)

def format_results(label, top_list):
    """Print formatted results."""
    print(f"\n{label}:")
    for item in top_list:
        for name, count in item:
            print(f"  {name}: {count} links")

def run():
    start_time = time.time()

    filenames = sorted(glob.glob(os.path.join(INPUT_DIR, "*.html")))
    print(f"Found {len(filenames)} files")

    with beam.Pipeline(runner=FnApiRunner()) as p:
        # Read all files and extract links
        parsed = (
            p
            | 'CreateFileList' >> beam.Create(filenames)
            | 'ReadAndExtractLinks' >> beam.Map(read_and_extract_links)
        )

        # --- Outgoing Links: count links per source file ---
        top_outgoing = (
            parsed
            | 'EmitOutgoing' >> beam.FlatMap(emit_outgoing)
            | 'TopOutgoing' >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        )

        # --- Incoming Links: count how many files link to each target ---
        top_incoming = (
            parsed
            | 'EmitIncoming' >> beam.FlatMap(emit_incoming)
            | 'SumIncoming' >> beam.CombinePerKey(sum)
            | 'TopIncoming' >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        )

        # Print results
        top_outgoing | 'PrintOutgoing' >> beam.Map(
            lambda x: format_results("Top 5 Files by Outgoing Links", [x])
        )
        top_incoming | 'PrintIncoming' >> beam.Map(
            lambda x: format_results("Top 5 Files by Incoming Links", [x])
        )

    elapsed = time.time() - start_time
    print(f"\nTotal pipeline runtime: {elapsed:.2f} seconds")

if __name__ == '__main__':
    run()
