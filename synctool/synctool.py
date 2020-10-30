import secrets
import logging
import os
import binascii
import functools
import base64
import multiprocessing
import google.cloud.storage
import botocore
import boto3

logging.basicConfig(level=logging.INFO)


def sync_file(
        project, safe,
        source_bucket, source_path_prefix,
        destination_bucket, destination_path_prefix,
        source_key, source_md5_hash):
    log = logging.getLogger(__name__)

    try:
        # don't sync directory files
        if source_key.endswith('/'):
            return

        assert source_key.startswith(source_path_prefix)
        suffix = source_key[len(source_path_prefix):]
        if suffix:
            assert suffix.startswith('/')
            suffix = suffix[1:]

        if destination_path_prefix:
            destination_key = f'{destination_path_prefix}/{suffix}'
        else:
            destination_key = f'{suffix}'

        gcs_client = google.cloud.storage.Client()
        s3_client = boto3.client('s3')

        try:
            destination_resp = s3_client.head_object(Bucket=destination_bucket, Key=destination_key)
            if safe:
                log.warning(f"safe mode, destination already exists, skipping: {destination_key}")
                return
            if source_md5_hash is not None:
                destination_etag = destination_resp['ETag'].strip('"')
                if destination_etag == source_md5_hash:
                    log.info(f"skipping {source_key}: destination exists and md5 hash matches")
                    return
                log.warning(f"destination exists, but md5 hash doesn't match: {source_key} {source_md5_hash} vs {destination_key} {destination_etag}")
            else:
                log.warning("destination already exists, but source has no md5 hash")
        except botocore.exceptions.ClientError as e:
            if e.response['ResponseMetadata']['HTTPStatusCode'] != 404:
                raise

        temporary_file = secrets.token_hex(32)

        bucket = gcs_client.bucket(source_bucket, user_project=project)
        blob = bucket.blob(source_key)

        try:
            log.info(f'download {source_key} to {temporary_file}')
            blob.download_to_filename(temporary_file)

            log.info(f'upload {temporary_file} to {destination_key}')
            s3_client.upload_file(Filename=temporary_file, Bucket=destination_bucket, Key=destination_key)
        finally:
            log.info(f'removing temporary file for {source_key} => {destination_key}: {temporary_file}')
            try:
                os.remove(temporary_file)
            except FileNotFoundError:
                pass
    except Exception:
        log.exception(f'sync_file for {source_key} failed')


def error_callback(source_key, exc):
    log = logging.getLogger(__name__)
    log.error(f'async_apply sync_file for {source_key} called eror callback: {exc}')


def sync_directory(
        project, parallelism, safe,
        source_bucket, source_path_prefix,
        destination_bucket, destination_path_prefix):
    assert not source_path_prefix.endswith('/')
    assert not destination_path_prefix.endswith('/')

    log = logging.getLogger(__name__)

    log.info(f'project {project}')
    log.info(f'parallelism {parallelism}')
    log.info(f'source_bucket {source_bucket}')
    log.info(f'source_path_prefix {source_path_prefix}')
    log.info(f'destination_bucket {destination_bucket}')
    log.info(f'destination_path_prefix {destination_path_prefix}')

    gcs_client = google.cloud.storage.Client()
    from_bucket = gcs_client.bucket(source_bucket, user_project=project)

    with multiprocessing.Pool(processes=parallelism) as pool:
        for blob in from_bucket.list_blobs(prefix=source_path_prefix):
            source_key = blob.name
            source_md5_hash = blob.md5_hash
            if source_md5_hash is not None:
                source_md5_hash = binascii.hexlify(base64.b64decode(source_md5_hash)).decode('ascii')
            log.info(f'syncing {source_key} md5 {source_md5_hash}')
            pool.apply_async(
                sync_file,
                (project, safe, source_bucket, source_path_prefix, destination_bucket, destination_path_prefix, source_key, source_md5_hash),
                error_callback=functools.partial(error_callback, source_key))
        pool.close()
        pool.join()
