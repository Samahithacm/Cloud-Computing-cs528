#!/usr/bin/env python3
"""
Pipeline 2 (Local): Word Bigram Analysis using Apache Beam (FnApiRunner)
Reads 20,000 HTML files from local disk, strips HTML tags,
tokenizes text, and finds top 5 most frequent consecutive word bigrams.
"""
import apache_beam as beam
from apache_beam.runners.portability.fn_api_runner import FnApiRunner
import re
import os
import time
import glob

INPUT_DIR = "/tmp/htmlfiles"

def read_and_extract_bigrams(filepath):
    """Read a single HTML file, strip tags, extract word bigrams."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        # Strip HTML tags
        text = re.sub(r'<[^>]+>', ' ', content)
        # Tokenize to lowercase words
        words = re.findall(r'[a-z]+', text.lower())
        # Generate bigrams
        for i in range(len(words) - 1):
            yield (f"{words[i]} {words[i+1]}", 1)
    except Exception as e:
        pass

def run():
    start_time = time.time()

    filenames = sorted(glob.glob(os.path.join(INPUT_DIR, "*.html")))
    print(f"Found {len(filenames)} files")

    with beam.Pipeline(runner=FnApiRunner()) as p:
        top_bigrams = (
            p
            | 'CreateFileList' >> beam.Create(filenames)
            | 'ExtractBigrams' >> beam.FlatMap(read_and_extract_bigrams)
            | 'SumBigrams' >> beam.CombinePerKey(sum)
            | 'TopBigrams' >> beam.combiners.Top.Of(5, key=lambda x: x[1])
        )

        def print_bigrams(top_list):
            print("\nTop 5 Word Bigrams:")
            for bigram, count in top_list:
                print(f"  '{bigram}': {count}")

        top_bigrams | 'PrintBigrams' >> beam.Map(print_bigrams)

    elapsed = time.time() - start_time
    print(f"\nTotal pipeline runtime: {elapsed:.2f} seconds")

if __name__ == '__main__':
    run()
