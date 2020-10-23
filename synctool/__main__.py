import argparse
import os
import urllib.parse

from .synctool import sync_directory

parser = argparse.ArgumentParser(description="Copy directory from Google Storage to Amazon S3")

parser.add_argument(
    '-u', '--project', dest='project',
    help='GCP project to be billed for requests.  For requester pays buckets.')
parser.add_argument(
    '-p', '--parallelism', dest='parallelism',
    type=int,
    default=2 * os.cpu_count(),
    help='Number of files to transfer in parallel.  Default is twice the CPU count.')
parser.add_argument('source')
parser.add_argument('destination')

parsed_args = parser.parse_args()

parallelism = parsed_args.parallelism
if parallelism < 1:
    raise ValueError(f"invalid parallelism: must be positive: {parallelism}")

source = parsed_args.source
source_parts = urllib.parse.urlparse(source)
if source_parts.scheme != 'gs':
    raise ValueError(f"invalid source: must have scheme gs: {source_parts.scheme}")
source_bucket = source_parts.netloc
if not source_bucket:
    raise ValueError("invalid source: bucket not specified")
source_path_prefix = source_parts.path
if source_path_prefix.endswith('/'):
    raise ValueError(f"invalid source: must not end in slash: {source}")
if source_path_prefix:
    assert source_path_prefix.startswith('/')
    source_path_prefix = source_path_prefix[1:]

destination = parsed_args.destination
destination_parts = urllib.parse.urlparse(destination)
if destination_parts.scheme != 's3':
    raise ValueError(f"invalid destination: must have scheme s3: {destination_parts.scheme}")
destination_bucket = destination_parts.netloc
if not destination_bucket:
    raise ValueError("invalid destination: bucket not specified")
destination_path_prefix = destination_parts.path
if destination_path_prefix.endswith('/'):
    raise ValueError(f"invalid destination: must not end in slash: {destination}")
if destination_path_prefix:
    assert destination_path_prefix.startswith('/')
    destination_path_prefix = destination_path_prefix[1:]

sync_directory(parsed_args.project, parallelism, source_bucket, source_path_prefix, destination_bucket, destination_path_prefix)
