import os
import io
import logging
from minio import Minio
from pymongo import MongoClient, errors

logger = logging.getLogger(__name__)


class MinIOStorage:
    def __init__(self):
        self.endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.secure = os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        self.bucket_name = os.getenv('MINIO_LANDING_BUCKET', 'landing-zone')
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

    def ensure_bucket_exists(self):
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)
            logger.info(f"Created bucket: {self.bucket_name}")

    def upload_file(self, object_path, content, content_length=None, content_type='application/octet-stream'):
        """Upload content to MinIO.

        Args:
            object_path: The object key/path in the bucket.
            content: File-like object (BytesIO) or raw bytes.
            content_length: Length of content in bytes. Required if content is file-like.
            content_type: MIME type of the content.

        Returns:
            etag of the uploaded object.
        """
        # Ensure we have a file-like object
        if isinstance(content, (bytes, bytearray)):
            content_length = len(content)
            content = io.BytesIO(content)
        elif content_length is None:
            # Try to determine length
            pos = content.tell()
            content.seek(0, 2)
            content_length = content.tell()
            content.seek(pos)

        result = self.client.put_object(
            self.bucket_name,
            object_path,
            content,
            content_length,
            content_type=content_type,
        )
        return result.etag

    def download_file(self, object_path):
        try:
            response = self.client.get_object(self.bucket_name, object_path)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except Exception as e:
            logger.error(f"Download failed for {object_path}: {e}")
            return None

    def file_exists(self, object_path):
        try:
            self.client.stat_object(self.bucket_name, object_path)
            return True
        except Exception:
            return False

    def close(self):
        pass  # MinIO client doesn't need explicit close


class MongoDBStorage:
    def __init__(self):
        self.uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
        self.db_name = os.getenv('MONGO_DB', 'workplace_relations')
        self.collection_name = os.getenv('MONGO_LANDING_COLLECTION', 'landing_documents')
        self.client = MongoClient(self.uri)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
        self._create_indexes()

    def _create_indexes(self):
        self.collection.create_index('identifier', unique=True)
        self.collection.create_index('file_hash')
        self.collection.create_index('date')
        self.collection.create_index('partition_date')
        self.collection.create_index('body')

    def find_by_identifier(self, identifier):
        return self.collection.find_one({'identifier': identifier})

    def find_by_date_range(self, start_date, end_date):
        return list(self.collection.find({'date': {'$gte': start_date, '$lte': end_date}}))

    def find_by_partition(self, partition_date):
        return list(self.collection.find({'partition_date': partition_date}))

    def insert_document(self, doc):
        try:
            result = self.collection.insert_one(doc)
            return str(result.inserted_id)
        except errors.DuplicateKeyError:
            logger.warning(f"Duplicate identifier: {doc.get('identifier')}")
            raise

    def update_by_identifier(self, identifier, doc):
        result = self.collection.update_one(
            {'identifier': identifier}, {'$set': doc}, upsert=False
        )
        return result.modified_count > 0

    def close(self):
        self.client.close()