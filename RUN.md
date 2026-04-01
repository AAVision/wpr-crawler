subprocess.run(["scrapy", "crawl", "wr_spider"], cwd=scrapy_project/, env={PYTHONPATH: project_root})
                    │
                    ▼
        Scrapy CLI finds scrapy.cfg in cwd
                    │
                    ▼
        scrapy.cfg says: settings = scrapy_project.settings
                    │
                    ▼
        settings.py has: SPIDER_MODULES = ['scrapy_project.spiders']
                    │
                    ▼
        Scrapy imports scrapy_project.spiders.wr_spider
        (works because PYTHONPATH includes the project root)
                    │
                    ▼
        Finds WorkplaceRelationsSpider with name='wr_spider' → runs it
