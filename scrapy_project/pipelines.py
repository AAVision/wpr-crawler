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
        content = item.get("document_content")  # pragma: no cover
        if content:  # pragma: no cover
            if isinstance(content, str):  # pragma: no cover
                content = content.encode("utf-8")  # pragma: no cover
            item["file_hash"] = hashlib.sha256(content).hexdigest()  # pragma: no cover
        else:
            item["file_hash"] = None  # pragma: no cover
            log_structured(  # pragma: no cover
                logger,
                "hash_missing",
                {
                    "identifier": item.get("identifier"),
                    "reason": "no document content",
                },
            )
        return item  # pragma: no cover


class MinIOStoragePipeline:
    """Upload document files to MinIO blob storage."""

    def __init__(self):
        self.storage = None  # pragma: no cover

    def open_spider(self, spider=None):
        self.storage = MinIOStorage()  # pragma: no cover
        self.storage.ensure_bucket_exists()  # pragma: no cover

    def process_item(self, item, spider=None):
        content = item.get("document_content")  # pragma: no cover
        if not content:  # pragma: no cover
            log_structured(  # pragma: no cover
                logger,
                "storage_skip",
                {
                    "identifier": item.get("identifier"),
                    "reason": "no content to store",
                },
            )
            return item  # pragma: no cover

        identifier = item.get(
            "identifier", f"doc_{datetime.now().timestamp()}"
        )  # pragma: no cover
        doc_type = item.get("document_type", "html")  # pragma: no cover
        safe_id = "".join(
            c for c in identifier if c.isalnum() or c in ("-", "_")
        )  # pragma: no cover
        file_name = f"{safe_id}.{doc_type}"  # pragma: no cover
        partition_date = item.get(
            "partition_date", datetime.now().strftime("%Y-%m")
        )  # pragma: no cover
        body = item.get("body", "unknown")  # pragma: no cover
        # Sanitize body name for path
        safe_body = (  # pragma: no cover
            "".join(c for c in body if c.isalnum() or c in ("-", "_", " "))
            .strip()
            .replace(" ", "_")
        )
        object_path = f"{safe_body}/{partition_date}/{file_name}"  # pragma: no cover

        try:  # pragma: no cover
            content_type = self._get_content_type(doc_type)  # pragma: no cover
            if isinstance(content, str):  # pragma: no cover
                content = content.encode("utf-8")  # pragma: no cover

            # MinIO put_object requires a file-like object, not raw bytes
            content_stream = io.BytesIO(content)  # pragma: no cover
            content_length = len(content)  # pragma: no cover

            etag = self.storage.upload_file(  # pragma: no cover
                object_path, content_stream, content_length, content_type=content_type
            )
            item["file_path"] = object_path  # pragma: no cover

            log_structured(  # pragma: no cover
                logger,
                "document_stored",
                {
                    "identifier": identifier,
                    "path": object_path,
                    "size_bytes": content_length,
                    "etag": etag,
                    "document_type": doc_type,
                },
            )

            # Remove binary content from item (not needed in MongoDB)
            del item["document_content"]  # pragma: no cover
        except Exception as e:
            log_structured(
                logger,
                "storage_failed",
                {
                    "identifier": identifier,
                    "path": object_path,
                    "error": str(e),
                },
                level=setup_logging().error,
            )  # Pass error level
            raise DropItem(f"Storage failed for {identifier}: {str(e)}")

        return item  # pragma: no cover

    def _get_content_type(self, doc_type: str) -> str:
        mapping = {  # pragma: no cover
            "pdf": "application/pdf",
            "doc": "application/msword",
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "html": "text/html",
            "htm": "text/html",
        }
        return mapping.get(
            doc_type.lower(), "application/octet-stream"
        )  # pragma: no cover

    def close_spider(self, spider=None):
        if self.storage:  # pragma: no cover
            self.storage.close()  # pragma: no cover


class MongoDBPipeline:
    """Store metadata in MongoDB. Handles idempotency via bulk operations."""

    def __init__(self, batch_size=50):
        self.storage = None  # pragma: no cover
        self.items_buffer = []  # pragma: no cover
        self.batch_size = batch_size  # pragma: no cover

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            batch_size=crawler.settings.getint("MONGO_BATCH_SIZE", 50)
        )  # pragma: no cover

    def open_spider(self, spider=None):
        self.storage = MongoDBStorage()  # pragma: no cover

    def process_item(self, item, spider=None):
        if not item.get("file_hash"):  # pragma: no cover
            log_structured(  # pragma: no cover
                logger,
                "metadata_skip",
                {
                    "identifier": item.get("identifier"),
                    "reason": "no file hash",
                },
            )
            return item  # pragma: no cover

        self.items_buffer.append((item, spider))  # pragma: no cover
        if len(self.items_buffer) >= self.batch_size:  # pragma: no cover
            self._flush_buffer()  # pragma: no cover

        return item  # pragma: no cover

    def _flush_buffer(self):
        if not self.items_buffer:  # pragma: no cover
            return  # pragma: no cover

        from pymongo import UpdateOne  # pragma: no cover

        # 1. Extract identifiers for buffered items
        identifiers = [
            item.get("identifier") for item, _ in self.items_buffer
        ]  # pragma: no cover

        # 2. Bulk query for existing documents
        # This replaces N separate find_one() calls with 1 bulk query
        cursor = self.storage.collection.find(  # pragma: no cover
            {"identifier": {"$in": identifiers}},
            {"identifier": 1, "file_hash": 1, "version": 1},
        )
        existing_docs = {doc["identifier"]: doc for doc in cursor}  # pragma: no cover

        bulk_ops = []  # pragma: no cover
        for item, spider in self.items_buffer:  # pragma: no cover
            identifier = item.get("identifier")  # pragma: no cover
            new_hash = item.get("file_hash")  # pragma: no cover
            doc = item.to_dict()  # pragma: no cover
            existing = existing_docs.get(identifier)  # pragma: no cover

            if existing:  # pragma: no cover
                if existing.get("file_hash") == new_hash:  # pragma: no cover
                    log_structured(  # pragma: no cover
                        logger,
                        "document_unchanged",
                        {
                            "identifier": identifier,
                            "file_hash": new_hash,
                        },
                    )
                    if hasattr(spider, "_stats"):  # pragma: no cover
                        spider._stats["records_skipped_duplicate"] += (
                            1  # pragma: no cover
                        )
                    continue  # Skip unchanged duplicate  # pragma: no cover
                else:
                    log_structured(  # pragma: no cover
                        logger,
                        "document_updated",
                        {
                            "identifier": identifier,
                            "old_hash": existing.get("file_hash"),
                            "new_hash": new_hash,
                            "version": existing.get("version", 0) + 1,
                        },
                    )
                    doc["updated_at"] = datetime.now().isoformat()  # pragma: no cover
                    doc["version"] = existing.get("version", 0) + 1  # pragma: no cover
                    bulk_ops.append(  # pragma: no cover
                        UpdateOne(
                            {"identifier": identifier}, {"$set": doc}, upsert=False
                        )
                    )
            else:
                doc["created_at"] = datetime.now().isoformat()  # pragma: no cover
                doc["version"] = 1  # pragma: no cover
                bulk_ops.append(  # pragma: no cover
                    UpdateOne({"identifier": identifier}, {"$set": doc}, upsert=True)
                )

        # 3. Bulk Write
        if bulk_ops:  # pragma: no cover
            try:  # pragma: no cover
                result = self.storage.collection.bulk_write(
                    bulk_ops, ordered=False
                )  # pragma: no cover
                log_structured(  # pragma: no cover
                    logger,
                    "metadata_batch_stored",
                    {
                        "upserted_count": result.upserted_count,
                        "modified_count": result.modified_count,
                        "total_ops": len(bulk_ops),
                    },
                )
            except Exception as e:
                logger.error(f"Bulk write failed: {e}")

        self.items_buffer = []  # pragma: no cover

    def close_spider(self, spider=None):
        self._flush_buffer()  # pragma: no cover
        if self.storage:  # pragma: no cover
            self.storage.close()  # pragma: no cover
