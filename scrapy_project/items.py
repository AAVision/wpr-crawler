import scrapy


class DecisionItem(scrapy.Item):
    identifier = scrapy.Field()
    title = scrapy.Field()
    description = scrapy.Field()
    date = scrapy.Field()
    body = scrapy.Field()
    link_to_doc = scrapy.Field()
    partition_date = scrapy.Field()
    file_path = scrapy.Field()
    file_hash = scrapy.Field()
    source_url = scrapy.Field()
    scraped_at = scrapy.Field()
    document_type = scrapy.Field()
    document_content = scrapy.Field()

    def to_dict(self):
        """Convert to dict, excluding binary document_content."""
        d = dict(self)  # pragma: no cover
        d.pop("document_content", None)  # pragma: no cover
        return d  # pragma: no cover
