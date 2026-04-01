#!/usr/bin/env python3
import os
import hashlib
import io
from datetime import datetime
from typing import Optional, Dict
from minio import Minio
from pymongo import MongoClient, UpdateOne
from tenacity import retry, stop_after_attempt, wait_exponential
import polars as pl
from .html_cleaner import HTMLCleaner
from utils.logging_utils import setup_logging

# Centralized Logging
logger = setup_logging(__name__)


class TransformationPipeline:
    def __init__(self):
        # MinIO
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.minio_secure = os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        self.landing_bucket = os.getenv('MINIO_LANDING_BUCKET', 'landing-zone')
        self.transformed_bucket = os.getenv('MINIO_TRANSFORMED_BUCKET', 'transformed-zone')

        # MongoDB
        self.mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
        self.mongo_db = os.getenv('MONGO_DB', 'workplace_relations')
        self.landing_collection = os.getenv('MONGO_LANDING_COLLECTION', 'landing_documents')
        self.transformed_collection = os.getenv('MONGO_TRANSFORMED_COLLECTION', 'transformed_documents')

        self._init_clients()
        self.html_cleaner = HTMLCleaner()

    def _init_clients(self):
        self.minio_client = Minio(self.minio_endpoint,
                                  access_key=self.minio_access_key,
                                  secret_key=self.minio_secret_key,
                                  secure=self.minio_secure)
        self.mongo_client = MongoClient(self.mongo_uri)
        self.db = self.mongo_client[self.mongo_db]
        self.landing_meta = self.db[self.landing_collection]
        self.transformed_meta = self.db[self.transformed_collection]

        # Indexes for transformed collection
        self.transformed_meta.create_index('identifier', unique=True)
        self.transformed_meta.create_index('original_hash')
        self.transformed_meta.create_index('new_hash')
        self.transformed_meta.create_index('transformed_at')

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def run(self, start_date: str = None, end_date: str = None) -> Dict:
        logger.info(f"Starting transformation pipeline from {start_date} to {end_date}")

        # Build query
        query = {}
        if start_date or end_date:
            date_query = {}
            if start_date:
                date_query['$gte'] = start_date
            if end_date:
                date_query['$lte'] = end_date
            if date_query:
                query['date'] = date_query

        # Fetch metadata from MongoDB and convert to Polars DataFrame for efficient processing
        cursor = self.landing_meta.find(query)
        docs = list(cursor)

        if not docs:
            logger.info("No documents found in date range.")
            return {'total': 0, 'transformed': 0, 'skipped': 0, 'failed': 0}

        # Use Polars to create DataFrame for metadata
        df = pl.DataFrame(docs)
        logger.info(f"Loaded {len(df)} documents into Polars DataFrame")

        stats = {'total': len(df), 'transformed': 0, 'skipped': 0, 'failed': 0}
        bulk_ops = []

        # Process each row
        for row in df.iter_rows(named=True):
            try:
                # _transform_document now returns the update operation instead of performing it
                op_result = self._transform_document(row)
                if op_result:
                    if isinstance(op_result, UpdateOne):
                        bulk_ops.append(op_result)
                        stats['transformed'] += 1
                    else:
                        # returned True for skipped/already transformed
                        stats['skipped'] += 1
                else:
                    stats['skipped'] += 1
            except Exception as e:
                stats['failed'] += 1
                logger.error(f"Failed to transform {row.get('identifier')}: {e}")

        # Execute all database updates in a single N+1-free roundtrip
        if bulk_ops:
            logger.info(f"Executing bulk write for {len(bulk_ops)} metadata updates...")
            self.transformed_meta.bulk_write(bulk_ops)

        logger.info(f"Transformation completed: {stats}")
        return stats

    def _transform_document(self, doc: Dict) -> Optional[UpdateOne]:
        identifier = doc.get('identifier')
        document_type = doc.get('document_type', 'html')
        file_path = doc.get('file_path')

        if document_type not in ['html', 'htm']:
            logger.info(f"Skipping non-HTML document: {identifier} ({document_type})")
            return self._copy_as_is(doc)

        # Download original HTML
        content = self._download_file(self.landing_bucket, file_path)
        if not content:
            logger.error(f"Failed to download file: {file_path}")
            return None

        # Clean HTML
        cleaned_content = self.html_cleaner.extract_content(content, doc.get('source_url'))

        # Calculate new hash
        new_hash = hashlib.sha256(cleaned_content.encode('utf-8')).hexdigest()
        original_hash = doc.get('file_hash')

        # Check if already transformed
        existing = self.transformed_meta.find_one({'identifier': identifier}, {'new_hash': 1, 'version': 1})
        if existing and existing.get('new_hash') == new_hash:
            logger.info(f"Document {identifier} already transformed with same hash, skipping")
            return True  # Signal skip

        # Prepare new filename
        safe_id = "".join(c for c in identifier if c.isalnum() or c in ('-', '_'))
        new_file_name = f"{safe_id}.html"
        partition_date = doc.get('partition_date', 'unknown')
        body = doc.get('body', 'unknown')
        new_object_path = f"{body}/{partition_date}/{new_file_name}"

        # Upload cleaned HTML
        self._upload_file(self.transformed_bucket, new_object_path, cleaned_content, 'text/html')

        # Return a Bulk Op instead of executing immediately
        transformed_doc = {
            'identifier': identifier,
            'title': doc.get('title'),
            'description': doc.get('description'),
            'date': doc.get('date'),
            'body': doc.get('body'),
            'original_link': doc.get('link_to_doc'),
            'original_file_path': file_path,
            'original_hash': original_hash,
            'transformed_file_path': new_object_path,
            'new_hash': new_hash,
            'document_type': document_type,
            'partition_date': partition_date,
            'transformed_at': datetime.now().isoformat(),
            'version': existing.get('version', 0) + 1 if existing else 1
        }

        return UpdateOne({'identifier': identifier}, {'$set': transformed_doc}, upsert=True)

    def _copy_as_is(self, doc: Dict) -> Optional[UpdateOne]:
        identifier = doc.get('identifier')
        file_path = doc.get('file_path')
        document_type = doc.get('document_type')

        content = self._download_file(self.landing_bucket, file_path)
        if not content:
            return None

        safe_id = "".join(c for c in identifier if c.isalnum() or c in ('-', '_'))
        new_file_name = f"{safe_id}.{document_type}"
        partition_date = doc.get('partition_date', 'unknown')
        body = doc.get('body', 'unknown')
        new_object_path = f"{body}/{partition_date}/{new_file_name}"

        content_type = self._get_content_type(document_type)
        self._upload_file(self.transformed_bucket, new_object_path, content, content_type)

        new_hash = hashlib.sha256(content).hexdigest()

        transformed_doc = {
            'identifier': identifier,
            'title': doc.get('title'),
            'description': doc.get('description'),
            'date': doc.get('date'),
            'body': doc.get('body'),
            'original_link': doc.get('link_to_doc'),
            'original_file_path': file_path,
            'original_hash': doc.get('file_hash'),
            'transformed_file_path': new_object_path,
            'new_hash': new_hash,
            'document_type': document_type,
            'partition_date': partition_date,
            'transformed_at': datetime.now().isoformat(),
            'is_copy': True
        }

        return UpdateOne({'identifier': identifier}, {'$set': transformed_doc}, upsert=True)

    def _download_file(self, bucket: str, object_path: str) -> Optional[bytes]:
        try:
            response = self.minio_client.get_object(bucket, object_path)
            content = response.read()
            response.close()
            response.release_conn()
            return content
        except Exception as e:
            logger.error(f"Download failed: {bucket}/{object_path} - {e}")
            return None

    def _upload_file(self, bucket: str, object_path: str, content: bytes, content_type: str):
        try:
            if not self.minio_client.bucket_exists(bucket):
                self.minio_client.make_bucket(bucket)
                logger.info(f"Created bucket: {bucket}")

            # Ensure content is in bytes and wrap in BytesIO for MinIO
            if isinstance(content, str):
                content = content.encode('utf-8')

            data = io.BytesIO(content)
            self.minio_client.put_object(
                bucket, object_path, data, len(content), content_type=content_type
            )
            logger.info(f"Uploaded to {bucket}/{object_path}")
        except Exception as e:
            logger.error(f"Upload failed: {bucket}/{object_path} - {e}")
            raise

    def _get_content_type(self, doc_type: str) -> str:
        mapping = {
            'pdf': 'application/pdf',
            'doc': 'application/msword',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'html': 'text/html',
            'htm': 'text/html'
        }
        return mapping.get(doc_type.lower(), 'application/octet-stream')

    def close(self):
        self.mongo_client.close()


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--start-date', help='Start date YYYY-MM-DD')
    parser.add_argument('--end-date', help='End date YYYY-MM-DD')
    args = parser.parse_args()

    pipeline = TransformationPipeline()
    try:
        pipeline.run(start_date=args.start_date, end_date=args.end_date)
    finally:
        pipeline.close()


if __name__ == '__main__':
    main()