WorkplaceRelationsSpider(start_date, end_date, body_id, body_name, partition_date)
│
│  __init__():  Stores args, formats dates to DD/MM/YYYY, creates _stats dict
│
▼
start_requests()
│  Builds URL: https://workplacerelations.ie/en/search/?decisions=1&body=15376&from=1/1/2025&to=1/2/2025
│  Yields 1 scrapy.Request with meta={'impersonate': 'chrome124'}
│
▼
parse_search_results(response)          ◄──── called for EACH page of results
│  1. Extracts total count from "Shows 1 to 10 of 2395 results"
│  2. Finds all <a class="btn-primary"> ("View Page" buttons)
│  3. For each result:
│     ├── Extracts identifier from <a title="ADJ-00055934">
│     ├── Extracts date via regex (DD/MM/YYYY)
│     ├── Extracts description (party names)
│     └── Yields scrapy.Request → parse_decision_detail()
│  4. Looks for <a class="next"> (pagination)
│     └── If found → yields Request back to parse_search_results()  ◄─── LOOP
│
▼
parse_decision_detail(response)         ◄──── called for EACH case page
│  e.g. /en/cases/2026/march/adj-00055934.html
│  1. Extracts: identifier, title, description, date
│  2. Checks for PDF/DOC download links
│  3. If PDF/DOC found:
│     └── Yields Request → save_document()
│  4. If HTML only (most cases):
│     └── Sets item['document_content'] = response.body
│         Yields DecisionItem ──────────────────────────┐
│                                                       │
save_document(response)                                 │
│  (only for PDF/DOC downloads)                         │
│  Sets item['document_content'] = response.body        │
│  Yields DecisionItem ────────────────────────────────┐│
│                                                      ││
▼▼ ════════════════════════════════════════════════════ ▼▼
                    ITEM PIPELINES
                (run in order for each item)

Pipeline 1: HashCalculationPipeline (priority 100)
│  item['file_hash'] = SHA256(document_content)
▼
Pipeline 2: MinIOStoragePipeline (priority 200)
│  Uploads document_content to MinIO as:
│  {body}/{partition_date}/{identifier}.{html|pdf|doc}
│  item['file_path'] = "Workplace_Relations_Commission/2025-01/ADJ-00055934.html"
│  Deletes document_content from item (no longer needed)
▼
Pipeline 3: MongoDBPipeline (priority 300)
│  Checks MongoDB for existing record by identifier:
│  ├── Not found → INSERT new document
│  ├── Found + same hash → DROP (idempotent, no duplicate)
│  └── Found + different hash → UPDATE (version++)
▼
closed(reason)
│  Emits JSON summary: records_found, records_scraped, failures, etc.
