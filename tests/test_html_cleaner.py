from bs4 import BeautifulSoup
from transformer.html_cleaner import HTMLCleaner


def test_find_main_content():
    cleaner = HTMLCleaner()
    html = "<html><body><div id='content'>" + "A" * 300 + "</div></body></html>"
    soup = BeautifulSoup(html, "html.parser")
    content = cleaner._find_main_content(soup)
    assert "A" * 300 in content.get_text()


def test_find_main_content_fallback():
    cleaner = HTMLCleaner()
    html = "<html><body><div>" + "B" * 400 + "</div></body></html>"
    soup = BeautifulSoup(html, "html.parser")
    content = cleaner._find_main_content(soup)
    assert "B" * 400 in content.get_text()


def test_extract_content_empty():
    cleaner = HTMLCleaner()
    cleaned = cleaner.extract_content("")
    assert cleaned == "None" or cleaned == "" or cleaned is None


def test_extract_content_basic():
    cleaner = HTMLCleaner()
    html = (
        "<html><body><script>foo</script><div id='content'>"
        + "C" * 300
        + "</div></body></html>"
    )
    cleaned = cleaner.extract_content(html)
    assert "C" * 300 in cleaned
    assert "foo" not in cleaned


def test_extract_content_exception():
    cleaner = HTMLCleaner()
    # pass bad data to trigger exception gracefully
    cleaned = cleaner.extract_content(None)
    assert cleaned is None
