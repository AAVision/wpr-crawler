import io
import hashlib
from datetime import datetime
from scrapy.exceptions import DropItem
from scrapy_project.utils.storage import MinIOStorage, MongoDBStorage
from utils.logging_utils import setup_logging, log_structured

# Centralized Logging
logger = setup_logging(__name__)


class HashCalculationPipeline:
    """Calculate SHA-256 hash of the document content."""

    def process_item(self, item, spider=None):
        content = item.get('document_content')
        if content:
            if isinstance(content, str):
                content = content.encode('utf-8')
            item['file_hash'] = hashlib.sha256(content).hexdigest()
        else:
            item['file_hash'] = None
            log_structured(logger, 'hash_missing', {
                'identifier': item.get('identifier'),
                'reason': 'no document content',
            })
        return item


class MinIOStoragePipeline:
    """Upload document files to MinIO blob storage."""

    def __init__(self):
        self.storage = None

    def open_spider(self, spider=None):
        self.storage = MinIOStorage()
        self.storage.ensure_bucket_exists()

    def process_item(self, item, spider=None):
        content = item.get('document_content')
        if not content:
            log_structured(logger, 'storage_skip', {
                'identifier': item.get('identifier'),
                'reason': 'no content to store',
            })
            return item

        identifier = item.get('identifier', f"doc_{datetime.now().timestamp()}")
        doc_type = item.get('document_type', 'html')
        safe_id = "".join(c for c in identifier if c.isalnum() or c in ('-', '_'))
        file_name = f"{safe_id}.{doc_type}"
        partition_date = item.get('partition_date', datetime.now().strftime('%Y-%m'))
        body = item.get('body', 'unknown')
        # Sanitize body name for path
        safe_body = "".join(c for c in body if c.isalnum() or c in ('-', '_', ' ')).strip().replace(' ', '_')
        object_path = f"{safe_body}/{partition_date}/{file_name}"

        try:
            content_type = self._get_content_type(doc_type)
            if isinstance(content, str):
                content = content.encode('utf-8')

            # MinIO put_object requires a file-like object, not raw bytes
            content_stream = io.BytesIO(content)
            content_length = len(content)

            etag = self.storage.upload_file(
                object_path, content_stream, content_length, content_type=content_type
            )
            item['file_path'] = object_path

            log_structured(logger, 'document_stored', {
                'identifier': identifier,
                'path': object_path,
                'size_bytes': content_length,
                'etag': etag,
                'document_type': doc_type,
            })

            # Remove binary content from item (not needed in MongoDB)
            del item['document_content']
        except Exception as e:
            log_structured(logger, 'storage_failed', {
                'identifier': identifier,
                'path': object_path,
                'error': str(e),
            }, level=setup_logging().error) # Pass error level
            raise DropItem(f"Storage failed for {identifier}: {str(e)}")

        return item

    def _get_content_type(self, doc_type: str) -> str:
        mapping = {
            'pdf': 'application/pdf',
            'doc': 'application/msword',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'html': 'text/html',
            'htm': 'text/html'
        }
        return mapping.get(doc_type.lower(), 'application/octet-stream')

    def close_spider(self, spider=None):
        if self.storage:
            self.storage.close()


class MongoDBPipeline:
    """Store metadata in MongoDB. Handles idempotency via bulk operations."""

    def __init__(self, batch_size=50):
        self.storage = None
        self.items_buffer = []
        self.batch_size = batch_size

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            batch_size=crawler.settings.getint('MONGO_BATCH_SIZE', 50)
        )

    def open_spider(self, spider=None):
        self.storage = MongoDBStorage()

    def process_item(self, item, spider=None):
        if not item.get('file_hash'):
            log_structured(logger, 'metadata_skip', {
                'identifier': item.get('identifier'),
                'reason': 'no file hash',
            })
            return item

        self.items_buffer.append((item, spider))
        if len(self.items_buffer) >= self.batch_size:
            self._flush_buffer()

        return item

    def _flush_buffer(self):
        if not self.items_buffer:
            return

        from pymongo import UpdateOne

        # 1. Extract identifiers for buffered items
        identifiers = [item.get('identifier') for item, _ in self.items_buffer]
        
        # 2. Bulk query for existing documents
        # This replaces N separate find_one() calls with 1 bulk query
        cursor = self.storage.collection.find(
            {'identifier': {'$in': identifiers}},
            {'identifier': 1, 'file_hash': 1, 'version': 1}
        )
        existing_docs = {doc['identifier']: doc for doc in cursor}

        bulk_ops = []
        for item, spider in self.items_buffer:
            identifier = item.get('identifier')
            new_hash = item.get('file_hash')
            doc = item.to_dict()
            existing = existing_docs.get(identifier)

            if existing:
                if existing.get('file_hash') == new_hash:
                    log_structured(logger, 'document_unchanged', {
                        'identifier': identifier,
                        'file_hash': new_hash,
                    })
                    if hasattr(spider, '_stats'):
                        spider._stats['records_skipped_duplicate'] += 1
                    continue # Skip unchanged duplicate
                else:
                    log_structured(logger, 'document_updated', {
                        'identifier': identifier,
                        'old_hash': existing.get('file_hash'),
                        'new_hash': new_hash,
                        'version': existing.get('version', 0) + 1,
                    })
                    doc['updated_at'] = datetime.now().isoformat()
                    doc['version'] = existing.get('version', 0) + 1
                    bulk_ops.append(
                        UpdateOne({'identifier': identifier}, {'$set': doc}, upsert=False)
                    )
            else:
                doc['created_at'] = datetime.now().isoformat()
                doc['version'] = 1
                bulk_ops.append(
                    UpdateOne({'identifier': identifier}, {'$set': doc}, upsert=True)
                )

        # 3. Bulk Write
        if bulk_ops:
            try:
                result = self.storage.collection.bulk_write(bulk_ops, ordered=False)
                log_structured(logger, 'metadata_batch_stored', {
                    'upserted_count': result.upserted_count,
                    'modified_count': result.modified_count,
                    'total_ops': len(bulk_ops),
                })
            except Exception as e:
                logger.error(f"Bulk write failed: {e}")

        self.items_buffer = []

    def close_spider(self, spider=None):
        self._flush_buffer()
        if self.storage:
            self.storage.close()