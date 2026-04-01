import io
import hashlib
import json
import logging
from datetime import datetime
from scrapy.exceptions import DropItem
from scrapy_project.utils.storage import MinIOStorage, MongoDBStorage

logger = logging.getLogger(__name__)


def _log_structured(event: str, data: dict):
    """Emit a structured JSON log entry from pipeline."""
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'component': 'pipeline',
        'event': event,
        **data,
    }
    logger.info(json.dumps(log_entry))


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
            _log_structured('hash_missing', {
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
            _log_structured('storage_skip', {
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

            _log_structured('document_stored', {
                'identifier': identifier,
                'path': object_path,
                'size_bytes': content_length,
                'etag': etag,
                'document_type': doc_type,
            })

            # Remove binary content from item (not needed in MongoDB)
            del item['document_content']
        except Exception as e:
            _log_structured('storage_failed', {
                'identifier': identifier,
                'path': object_path,
                'error': str(e),
            })
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
    """Store metadata in MongoDB. Handles idempotency via file_hash deduplication."""

    def __init__(self):
        self.storage = None

    def open_spider(self, spider=None):
        self.storage = MongoDBStorage()

    def process_item(self, item, spider=None):
        if not item.get('file_hash'):
            _log_structured('metadata_skip', {
                'identifier': item.get('identifier'),
                'reason': 'no file hash',
            })
            return item

        doc = item.to_dict()
        existing = self.storage.find_by_identifier(item.get('identifier'))

        if existing:
            if existing.get('file_hash') == item.get('file_hash'):
                _log_structured('document_unchanged', {
                    'identifier': item.get('identifier'),
                    'file_hash': item.get('file_hash'),
                })
                # Update spider stats for structured summary
                if hasattr(spider, '_stats'):
                    spider._stats['records_skipped_duplicate'] += 1
                raise DropItem(f"Duplicate unchanged document: {item.get('identifier')}")
            else:
                _log_structured('document_updated', {
                    'identifier': item.get('identifier'),
                    'old_hash': existing.get('file_hash'),
                    'new_hash': item.get('file_hash'),
                    'version': existing.get('version', 0) + 1,
                })
                doc['updated_at'] = datetime.now().isoformat()
                doc['version'] = existing.get('version', 0) + 1
                self.storage.update_by_identifier(item.get('identifier'), doc)
        else:
            doc['created_at'] = datetime.now().isoformat()
            doc['version'] = 1
            doc_id = self.storage.insert_document(doc)
            _log_structured('metadata_stored', {
                'identifier': item.get('identifier'),
                'mongo_id': doc_id,
                'body': item.get('body'),
                'partition_date': item.get('partition_date'),
            })

        return item

    def close_spider(self, spider=None):
        if self.storage:
            self.storage.close()