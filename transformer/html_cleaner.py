from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)


class HTMLCleaner:
    def __init__(self):
        self.navigation_selectors = [
            "nav",
            ".navbar",
            ".navigation",
            ".menu",
            ".sidebar",
            ".header",
            "header",
            ".footer",
            "footer",
            ".breadcrumb",
            ".pagination",
            ".share-buttons",
            ".social-share",
            ".related-links",
            ".comments",
            "aside",
            ".advertisement",
            ".ads",
            ".popup",
            "#nav",
            "#sidebar",
            "#footer",
            "#header",
        ]

    def extract_content(self, html_content: str, url: str = None) -> str:
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            for element in soup(["script", "style", "noscript", "meta", "link"]):
                element.decompose()
            for selector in self.navigation_selectors:
                for element in soup.select(selector):
                    element.decompose()  # pragma: no cover
            main_content = self._find_main_content(soup)
            if main_content:
                self._clean_empty_tags(main_content)
                cleaned = str(main_content)
            else:
                body = soup.find("body")
                cleaned = str(body) if body else str(soup)
            return cleaned
        except Exception as e:
            logger.error(f"Error cleaning HTML: {e}")
            return html_content

    def _find_main_content(self, soup):
        content_selectors = [
            "main",
            "article",
            ".content",
            ".main-content",
            ".decision-content",
            ".document-content",
            "#content",
            ".post-content",
            ".entry-content",
            ".page-content",
        ]
        for selector in content_selectors:
            content = soup.select_one(selector)
            if content and len(content.get_text(strip=True)) > 200:
                return content
        max_len = 0
        main_div = None
        for div in soup.find_all("div"):
            text_len = len(div.get_text(strip=True))
            if text_len > max_len and text_len > 300:
                max_len = text_len
                main_div = div
        return main_div if main_div else soup.find("body")

    def _clean_empty_tags(self, element):
        for tag in element.find_all():
            if not tag.get_text(strip=True) and not tag.find_all():
                tag.decompose()
            elif (
                tag.name in ["span", "div"]
                and len(tag.get_text(strip=True)) < 10
                and not tag.find_all()
            ):
                tag.unwrap()  # pragma: no cover
