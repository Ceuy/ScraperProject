"""
scraper.py
Stage 1: scrape source homepages → collect article URLs → fetch article text → save CSV
"""

import requests
from bs4 import BeautifulSoup
import csv
import os
import re
import concurrent.futures
from datetime import datetime
from urllib.parse import urlparse, urljoin

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

SOURCES = {
    "bbc": {
        "label": "BBC News",
        "pages": [
            "https://www.bbc.com/news",
            "https://www.bbc.com/news/world",
            "https://www.bbc.com/news/business",
            "https://www.bbc.com/news/technology",
            "https://www.bbc.com/news/science-environment",
        ],
        "base": "https://www.bbc.com",
        "color": "#bb1919",
        "bg": "#fbeaea",
        "text_color": "#7a1010",
    },
    "reuters": {
        "label": "Reuters",
        "pages": [
            "https://www.reuters.com",
            "https://www.reuters.com/world",
            "https://www.reuters.com/business",
            "https://www.reuters.com/technology",
            "https://www.reuters.com/science",
        ],
        "base": "https://www.reuters.com",
        "color": "#ff8000",
        "bg": "#fff3e6",
        "text_color": "#7a3d00",
    },
    "guardian": {
        "label": "The Guardian",
        "pages": [
            "https://www.theguardian.com/international",
            "https://www.theguardian.com/world",
            "https://www.theguardian.com/politics",
            "https://www.theguardian.com/business",
            "https://www.theguardian.com/technology",
        ],
        "base": "https://www.theguardian.com",
        "color": "#005689",
        "bg": "#e6f0f8",
        "text_color": "#003d63",
    },
    "ap": {
        "label": "AP News",
        "pages": [
            "https://apnews.com",
            "https://apnews.com/world-news",
            "https://apnews.com/politics",
            "https://apnews.com/business",
            "https://apnews.com/science",
        ],
        "base": "https://apnews.com",
        "color": "#c41e3a",
        "bg": "#fbeaed",
        "text_color": "#7a1225",
    },
    "aljazeera": {
        "label": "Al Jazeera",
        "pages": [
            "https://www.aljazeera.com",
            "https://www.aljazeera.com/news",
            "https://www.aljazeera.com/economy",
            "https://www.aljazeera.com/politics",
        ],
        "base": "https://www.aljazeera.com",
        "color": "#8b0000",
        "bg": "#fbeaea",
        "text_color": "#5a0000",
    },
    "nyt": {
        "label": "NY Times",
        "pages": [
            "https://www.nytimes.com",
            "https://www.nytimes.com/section/world",
            "https://www.nytimes.com/section/politics",
            "https://www.nytimes.com/section/technology",
            "https://www.nytimes.com/section/science",
        ],
        "base": "https://www.nytimes.com",
        "color": "#222222",
        "bg": "#f0f0f0",
        "text_color": "#222222",
    },
    "dw": {
        "label": "DW News",
        "pages": [
            "https://www.dw.com/en/top-stories/s-9097",
            "https://www.dw.com/en/world/s-1429",
            "https://www.dw.com/en/europe/s-1433",
            "https://www.dw.com/en/business/s-1431",
        ],
        "base": "https://www.dw.com",
        "color": "#0060a9",
        "bg": "#e6f0f8",
        "text_color": "#003d6b",
    },
    "france24": {
        "label": "France 24",
        "pages": [
            "https://www.france24.com/en",
            "https://www.france24.com/en/europe",
            "https://www.france24.com/en/economy",
        ],
        "base": "https://www.france24.com",
        "color": "#c8002b",
        "bg": "#fbeaed",
        "text_color": "#7a001a",
    },
    "euronews": {
        "label": "Euronews",
        "pages": [
            "https://www.euronews.com/news/europe",
            "https://www.euronews.com/news/world",
            "https://www.euronews.com/business",
        ],
        "base": "https://www.euronews.com",
        "color": "#00437a",
        "bg": "#e6f0f8",
        "text_color": "#002d52",
    },
    "politico": {
        "label": "Politico",
        "pages": [
            "https://www.politico.eu/news",
            "https://www.politico.com/news",
        ],
        "base": "https://www.politico.com",
        "color": "#d0021b",
        "bg": "#fbeaed",
        "text_color": "#7a0010",
    },
}

CSV_PATH = "data/articles.csv"
CSV_FIELDS = ["source_id", "source_label", "date", "title", "summary", "url",
              "source_color", "source_bg", "source_text_color"]

# --- Junk filtering ---

_JUNK_RE = re.compile(
    r"^(top stories|breaking news|latest news|more stories|watch live|"
    r"what to (watch|read|know)|sign in|subscribe|newsletters?|"
    r"most (read|viewed|popular)|editors? picks?|recommended|"
    r"live updates?|full coverage|read more|see more|load more|"
    r"skip to|advertisement|sponsored|follow us|cookie|"
    r"comments?|share|print|listen)[\s:–|-]?",
    re.IGNORECASE,
)
_BRAND_SEP = re.compile(r"\s+[-|–]\s+.{3,}$")
_SOURCE_NAMES = {
    "bbc", "bbc news", "reuters", "the guardian", "guardian", "ap news",
    "associated press", "new york times", "nytimes", "al jazeera", "aljazeera",
    "dw", "deutsche welle", "france 24", "euronews", "politico",
}


def is_junk_title(title):
    t = title.strip()
    if len(t) < 25:
        return True
    if _JUNK_RE.match(t):
        return True
    if _BRAND_SEP.search(t):
        base = _BRAND_SEP.sub("", t).strip().lower()
        if base in _SOURCE_NAMES:
            return True
    lower = t.lower()
    if any(lower.startswith(name) for name in _SOURCE_NAMES):
        return True
    return False


def is_article_url(url, base):
    """Heuristic: a real article URL has a meaningful path depth."""
    try:
        p = urlparse(url)
        path = p.path.rstrip("/")
        if not path:
            return False
        parts = [x for x in path.split("/") if x]
        # Need at least 2 path segments, or 1 segment with digits (article ID)
        if len(parts) >= 2:
            return True
        if len(parts) == 1 and any(c.isdigit() for c in parts[0]) and len(parts[0]) > 6:
            return True
        return False
    except Exception:
        return False


# --- Article text extraction ---

_NOISE_TAGS = {"script", "style", "nav", "header", "footer", "aside",
               "form", "button", "figure", "figcaption", "iframe", "noscript"}


def extract_article_text(soup):
    """Extract the main body text from an article page."""
    # Remove noise elements
    for tag in soup(_NOISE_TAGS):
        tag.decompose()

    # Try common article containers in priority order
    for selector in [
        "article",
        "[class*='article-body']",
        "[class*='article__body']",
        "[class*='story-body']",
        "[class*='post-content']",
        "[class*='entry-content']",
        "main",
    ]:
        container = soup.select_one(selector)
        if container:
            paras = container.find_all("p")
            text = " ".join(p.get_text(" ", strip=True) for p in paras)
            if len(text) > 150:
                return text

    # Fallback: all <p> tags
    paras = soup.find_all("p")
    return " ".join(p.get_text(" ", strip=True) for p in paras)


def clean_text(text):
    text = re.sub(r"\s+", " ", text).strip()
    # Remove cookie/subscription boilerplate often appended
    for marker in ["Sign up for", "Subscribe to", "Already a subscriber",
                   "Create a free account", "We use cookies"]:
        idx = text.find(marker)
        if idx > 200:
            text = text[:idx].strip()
    return text


def summarize(text, max_words=200):
    """Return the first max_words words of cleaned text."""
    words = clean_text(text).split()
    return " ".join(words[:max_words])


def extract_date(soup):
    """Try to find a publication date from meta tags or time elements."""
    # Meta tags
    for attr in ["article:published_time", "datePublished", "pubdate", "date"]:
        tag = soup.find("meta", property=attr) or soup.find("meta", attrs={"name": attr})
        if tag and tag.get("content"):
            return tag["content"][:10]
    # <time> element
    time_tag = soup.find("time")
    if time_tag:
        dt = time_tag.get("datetime") or time_tag.get_text()
        return dt[:10] if dt else ""
    return datetime.now().strftime("%Y-%m-%d")


# --- Homepage scraping → article URL collection ---

def collect_article_urls(source_id, progress_cb=None):
    """Scrape all section pages for a source, return list of unique article URLs."""
    src = SOURCES[source_id]
    base = src["base"]
    seen_urls = set()
    seen_titles = set()
    candidates = []  # (url, title)

    for page_url in src["pages"]:
        try:
            r = requests.get(page_url, headers=HEADERS, timeout=12)
            soup = BeautifulSoup(r.text, "html.parser")

            for tag in soup.find_all(["h1", "h2", "h3", "h4"]):
                title = re.sub(r"\s+", " ", tag.get_text()).strip()
                if is_junk_title(title) or title in seen_titles:
                    continue

                a = tag.find("a") or tag.find_parent("a")
                if not a or not a.get("href"):
                    continue

                href = a["href"]
                if href.startswith("/"):
                    href = base + href
                elif not href.startswith("http"):
                    continue

                if href in seen_urls:
                    continue
                if not is_article_url(href, base):
                    continue
                # Only keep URLs from the same domain
                if urlparse(href).netloc not in urlparse(base).netloc and \
                   urlparse(base).netloc not in urlparse(href).netloc:
                    continue

                seen_urls.add(href)
                seen_titles.add(title)
                candidates.append({"url": href, "title": title})

        except Exception as e:
            print(f"  [{source_id}] page error ({page_url}): {e}")

    if progress_cb:
        progress_cb(source_id, "collected", len(candidates))
    return candidates


# --- Article fetching ---

def fetch_article(candidate, source_id):
    """Fetch a single article and return a row dict."""
    src = SOURCES[source_id]
    try:
        r = requests.get(candidate["url"], headers=HEADERS, timeout=12)
        soup = BeautifulSoup(r.text, "html.parser")

        # Prefer page title over scraped heading (more reliable)
        page_title = soup.find("title")
        title = candidate["title"]
        if page_title:
            pt = re.sub(r"\s+", " ", page_title.get_text()).strip()
            # Strip site name suffix e.g. " - BBC News"
            pt = re.sub(r"\s*[-|–]\s*(BBC|Reuters|Guardian|AP|Al Jazeera|DW|France 24|Euronews|Politico|NYT|NY Times).*$", "", pt, flags=re.IGNORECASE).strip()
            if len(pt) > 20:
                title = pt

        body = extract_article_text(soup)
        summary = summarize(body, max_words=200)
        date = extract_date(soup)

        if len(summary) < 50:
            return None  # Not enough content — probably a JS-rendered page

        return {
            "source_id": source_id,
            "source_label": src["label"],
            "date": date or datetime.now().strftime("%Y-%m-%d"),
            "title": title,
            "summary": summary,
            "url": candidate["url"],
            "source_color": src["color"],
            "source_bg": src["bg"],
            "source_text_color": src["text_color"],
        }
    except Exception as e:
        print(f"  [{source_id}] article error ({candidate['url'][:60]}): {e}")
        return None


# --- Main entry point ---

def run_scrape(source_ids, max_per_source=25, progress_cb=None, data_dir="data"):
    """
    Full pipeline: collect URLs → fetch articles → save CSV.
    Returns (path_to_csv, total_articles, stats_by_source).
    """
    os.makedirs(data_dir, exist_ok=True)

    all_articles = []
    stats = {}

    # Phase 1: collect article URLs — limit concurrency for free tier RAM
    if progress_cb:
        progress_cb("all", "collecting", 0)

    url_map = {}  # source_id -> [candidates]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(collect_article_urls, sid, progress_cb): sid for sid in source_ids}
        for future in concurrent.futures.as_completed(futures):
            sid = futures[future]
            candidates = future.result()[:max_per_source]
            url_map[sid] = candidates
            if progress_cb:
                progress_cb(sid, "urls_ready", len(candidates))

    # Phase 2: fetch article text — throttled to avoid OOM
    if progress_cb:
        progress_cb("all", "fetching", sum(len(v) for v in url_map.values()))

    fetch_tasks = [(c, sid) for sid, candidates in url_map.items() for c in candidates]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(fetch_article, c, sid): (c, sid) for c, sid in fetch_tasks}
        done = 0
        for future in concurrent.futures.as_completed(futures):
            row = future.result()
            done += 1
            if row:
                all_articles.append(row)
            if progress_cb:
                progress_cb("all", "fetched", done)

    # Phase 3: write CSV
    date_str = datetime.now().strftime("%Y-%m-%d")
    csv_path = os.path.join(data_dir, f"articles_{date_str}.csv")

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(all_articles)

    # Stats per source
    for sid in source_ids:
        stats[sid] = sum(1 for a in all_articles if a["source_id"] == sid)

    if progress_cb:
        progress_cb("all", "done", len(all_articles))

    return csv_path, len(all_articles), stats


def list_csv_files(data_dir="data"):
    """Return available CSV files sorted newest first."""
    os.makedirs(data_dir, exist_ok=True)
    files = [f for f in os.listdir(data_dir) if f.startswith("articles_") and f.endswith(".csv")]
    files.sort(reverse=True)
    return files
