from unittest.mock import MagicMock, patch, mock_open
from scrapy_project.spiders.wr_spider import WorkplaceRelationsSpider


def test_spider_init():
    spider = WorkplaceRelationsSpider(start_date="2025-01-01", end_date="2025-01-31")
    assert spider.start_date == "2025-01-01"


@patch("scrapy_project.spiders.wr_spider.os.path.exists")
def test_spider_start_requests_with_url_list(mock_exists):
    mock_exists.return_value = True
    spider = WorkplaceRelationsSpider(url_list_file="/tmp/dummy.json")

    with patch("builtins.open", mock_open(read_data='["http://test.com"]')):
        requests = list(spider.start_requests())
        assert len(requests) == 1
        assert requests[0].url == "http://test.com"


def test_spider_parse():
    spider = WorkplaceRelationsSpider()
    mock_response = MagicMock()
    mock_response.css.return_value.get.return_value = "http://test.com/next"

    # Just basic invocation test
    _ = spider.parse_search_results(mock_response)
    # the exact behavior depends on html structure
