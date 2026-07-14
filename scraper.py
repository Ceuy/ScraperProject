"""
scraper.py
Stage 1: scrape source homepages → collect article URLs → fetch article text → save CSV
"""

import argparse
import csv
import logging
import os
import re
import threading
import time
import concurrent.futures
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Per-domain polite delay (seconds between consecutive requests to the same host)
DOMAIN_DELAY = 1.5
FETCH_MAX_RETRIES = 2
FETCH_RETRY_BACKOFF = 2.0
MIN_SUMMARY_CHARS = 50

SOURCES = {
    "bbc": {
        "label": "BBC News",
        "pages": [
            "https://www.bbc.com/news",
            "https://www.bbc.com/news/world",
            "https://www.bbc.com/news/business",
            "https://www.bbc.com/news/technology",
            "https://www.bbc.com/news/science_and_environment",
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
            "https://www.aljazeera.com/tag/politics/",
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
            "https://www.dw.com/en/middle-east/s-14207",
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
            "https://www.euronews.com/tag/world-news",
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

CSV_FIELDS = [
    "source_id", "source_label", "date", "title", "summary", "url",
    "source_color", "source_bg", "source_text_color",
]

# --- Per-domain rate limiting ---

_domain_locks: dict[str, threading.Lock] = {}
_domain_last_request: dict[str, float] = {}
_rate_limit_lock = threading.Lock()


def _get_domain_lock(domain: str) -> threading.Lock:
    with _rate_limit_lock:
        if domain not in _domain_locks:
            _domain_locks[domain] = threading.Lock()
        return _domain_locks[domain]


def polite_get(url: str, timeout: int = 12) -> requests.Response:
    """Fetch a URL with per-domain spacing to avoid hammering a single host."""
    domain = urlparse(url).netloc
    lock = _get_domain_lock(domain)
    with lock:
        elapsed = time.monotonic() - _domain_last_request.get(domain, 0.0)
        if elapsed < DOMAIN_DELAY:
            time.sleep(DOMAIN_DELAY - elapsed)
        response = requests.get(url, headers=HEADERS, timeout=timeout)
        _domain_last_request[domain] = time.monotonic()
        return response


def classify_http_error(code: int) -> str:
    """Map a status code to a human-readable reason bucket."""
    if code in (401, 403, 406, 429):
        return "likely blocking scrapers"
    if code in (404, 410):
        return "URL moved or removed"
    return "server error"


# --- Structured fetch results ---

@dataclass
class FetchResult:
    status: str  # ok | js_rendered | fetch_failed | http_error | timeout
    url: str
    source_id: str
    row: Optional[dict] = None
    error: Optional[str] = None
    attempts: int = 1
    http_status: Optional[int] = None

    def to_error_dict(self) -> dict:
        return {
            "url": self.url,
            "source_id": self.source_id,
            "status": self.status,
            "error": self.error,
            "attempts": self.attempts,
        }


@dataclass
class SourceStats:
    urls_collected: int = 0
    articles_saved: int = 0
    dropped_js_rendered: int = 0
    dropped_fetch_failed: int = 0
    page_errors: int = 0
    errors: list = field(default_factory=list)

    @property
    def dropped_total(self) -> int:
        return self.dropped_js_rendered + self.dropped_fetch_failed

    @property
    def js_rendered_pct(self) -> float:
        if self.urls_collected == 0:
            return 0.0
        return round(100.0 * self.dropped_js_rendered / self.urls_collected, 1)

    def to_dict(self) -> dict:
        return {
            "urls_collected": self.urls_collected,
            "articles_saved": self.articles_saved,
            "dropped_total": self.dropped_total,
            "dropped_js_rendered": self.dropped_js_rendered,
            "dropped_fetch_failed": self.dropped_fetch_failed,
            "page_errors": self.page_errors,
            "js_rendered_pct": self.js_rendered_pct,
            "errors": self.errors[:20],
        }


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


def is_junk_title(title: str) -> bool:
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


def is_article_url(url: str, base: str) -> bool:
    """Heuristic: a real article URL has a meaningful path depth."""
    try:
        path = urlparse(url).path.rstrip("/")
        if not path:
            return False
        parts = [x for x in path.split("/") if x]
        if len(parts) >= 2:
            return True
        if len(parts) == 1 and any(c.isdigit() for c in parts[0]) and len(parts[0]) > 6:
            return True
        return False
    except Exception:
        return False


# --- Article text extraction ---

_NOISE_TAGS = {
    "script", "style", "nav", "header", "footer", "aside",
    "form", "button", "figure", "figcaption", "iframe", "noscript",
}


def extract_article_text(soup: BeautifulSoup) -> str:
    """Extract the main body text from an article page."""
    for tag in soup(_NOISE_TAGS):
        tag.decompose()

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

    paras = soup.find_all("p")
    return " ".join(p.get_text(" ", strip=True) for p in paras)


def clean_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    for marker in [
        "Sign up for", "Subscribe to", "Already a subscriber",
        "Create a free account", "We use cookies",
    ]:
        idx = text.find(marker)
        if idx > 200:
            text = text[:idx].strip()
    return text


def summarize(text: str, max_words: int = 200) -> str:
    """Return the first max_words words of cleaned text."""
    words = clean_text(text).split()
    return " ".join(words[:max_words])


def extract_date(soup: BeautifulSoup) -> str:
    """Try to find a publication date from meta tags or time elements."""
    for attr in ["article:published_time", "datePublished", "pubdate", "date"]:
        tag = soup.find("meta", property=attr) or soup.find("meta", attrs={"name": attr})
        if tag and tag.get("content"):
            return tag["content"][:10]
    time_tag = soup.find("time")
    if time_tag:
        dt = time_tag.get("datetime") or time_tag.get_text()
        return dt[:10] if dt else ""
    return datetime.now().strftime("%Y-%m-%d")


# --- Homepage scraping → article URL collection ---

def collect_article_urls(
    source_id: str,
    progress_cb: Optional[Callable] = None,
    stats: Optional[SourceStats] = None,
) -> list[dict]:
    """Scrape all section pages for a source, return list of unique article URLs."""
    src = SOURCES[source_id]
    base = src["base"]
    seen_urls: set[str] = set()
    seen_titles: set[str] = set()
    candidates: list[dict] = []

    base_netloc = urlparse(base).netloc

    for page_url in src["pages"]:
        try:
            page_netloc = urlparse(page_url).netloc
            page_origin = f"{urlparse(page_url).scheme}://{page_netloc}"
            response = polite_get(page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            for tag in soup.find_all(["h1", "h2", "h3", "h4"]):
                title = re.sub(r"\s+", " ", tag.get_text()).strip()
                if is_junk_title(title) or title in seen_titles:
                    continue

                anchor = tag.find("a") or tag.find_parent("a")
                if not anchor or not anchor.get("href"):
                    continue

                href = anchor["href"]
                if href.startswith("/"):
                    # Resolve relative to the page we scraped it from, not the
                    # source's global base — some sources (e.g. Politico) serve
                    # multiple regional domains under one source_id.
                    href = urljoin(page_origin, href)
                elif not href.startswith("http"):
                    continue

                if href in seen_urls:
                    continue
                if not is_article_url(href, base):
                    continue
                href_netloc = urlparse(href).netloc
                same_as_base = href_netloc == base_netloc or base_netloc in href_netloc
                same_as_page = href_netloc == page_netloc or page_netloc in href_netloc
                if not same_as_base and not same_as_page:
                    continue

                seen_urls.add(href)
                seen_titles.add(title)
                candidates.append({"url": href, "title": title})

        except Exception as exc:
            reason = None
            if isinstance(exc, requests.HTTPError) and exc.response is not None:
                reason = classify_http_error(exc.response.status_code)
            elif isinstance(exc, requests.Timeout):
                reason = "request timed out"
            error_msg = f"{exc} ({reason})" if reason else str(exc)
            logger.warning("[%s] page error (%s): %s", source_id, page_url, error_msg)
            if stats is not None:
                stats.page_errors += 1
                stats.errors.append({
                    "stage": "collect",
                    "page_url": page_url,
                    "error": error_msg,
                })

    if progress_cb:
        progress_cb(source_id, "collected", len(candidates))
    return candidates


# --- Article fetching ---

_TITLE_SUFFIX_RE = re.compile(
    r"\s*[-|–]\s*(BBC|Reuters|Guardian|AP|Al Jazeera|DW|France 24|Euronews|Politico|NYT|NY Times).*$",
    re.IGNORECASE,
)


def fetch_article(candidate: dict, source_id: str) -> FetchResult:
    """Fetch a single article and return a structured result."""
    src = SOURCES[source_id]
    url = candidate["url"]

    try:
        response = polite_get(url)
        if response.status_code >= 400:
            code = response.status_code
            reason = classify_http_error(code)
            return FetchResult(
                status="http_error",
                url=url,
                source_id=source_id,
                error=f"HTTP {code} ({reason})",
                http_status=code,
            )

        soup = BeautifulSoup(response.text, "html.parser")

        page_title = soup.find("title")
        title = candidate["title"]
        if page_title:
            pt = re.sub(r"\s+", " ", page_title.get_text()).strip()
            pt = _TITLE_SUFFIX_RE.sub("", pt).strip()
            if len(pt) > 20:
                title = pt

        body = extract_article_text(soup)
        summary = summarize(body, max_words=200)
        date = extract_date(soup)

        if len(summary) < MIN_SUMMARY_CHARS:
            return FetchResult(
                status="js_rendered",
                url=url,
                source_id=source_id,
                error=f"Summary too short ({len(summary)} chars) — likely JS-rendered or paywalled",
            )

        return FetchResult(
            status="ok",
            url=url,
            source_id=source_id,
            row={
                "source_id": source_id,
                "source_label": src["label"],
                "date": date or datetime.now().strftime("%Y-%m-%d"),
                "title": title,
                "summary": summary,
                "url": url,
                "source_color": src["color"],
                "source_bg": src["bg"],
                "source_text_color": src["text_color"],
            },
        )

    except requests.Timeout:
        return FetchResult(
            status="timeout",
            url=url,
            source_id=source_id,
            error="Request timed out",
        )
    except requests.RequestException as exc:
        return FetchResult(
            status="fetch_failed",
            url=url,
            source_id=source_id,
            error=str(exc),
        )
    except Exception as exc:
        return FetchResult(
            status="fetch_failed",
            url=url,
            source_id=source_id,
            error=f"Unexpected error: {exc}",
        )


def fetch_article_with_retry(candidate: dict, source_id: str) -> FetchResult:
    """Retry transient failures (timeouts, network errors) with exponential backoff."""
    last_result: Optional[FetchResult] = None

    for attempt in range(1, FETCH_MAX_RETRIES + 2):
        result = fetch_article(candidate, source_id)
        result.attempts = attempt
        last_result = result

        if result.status == "ok":
            return result
        if result.status == "js_rendered":
            return result
        if result.status == "http_error":
            # 4xx (other than 429) won't succeed on retry — the URL moved,
            # is gone, or the source is actively blocking us. 5xx/429 are
            # transient, so give those a retry.
            is_transient = result.http_status == 429 or (result.http_status or 0) >= 500
            if not is_transient:
                return result
        if attempt <= FETCH_MAX_RETRIES:
            time.sleep(FETCH_RETRY_BACKOFF ** (attempt - 1))

    return last_result  # type: ignore[return-value]


# --- CSV helpers ---

def csv_path_for_date(data_dir: str, date_str: Optional[str] = None) -> str:
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
    return os.path.join(data_dir, f"articles_{date_str}.csv")


def count_articles_in_csv(csv_path: str) -> int:
    with open(csv_path, encoding="utf-8") as f:
        return sum(1 for _ in csv.DictReader(f))


def stats_from_csv(csv_path: str, source_ids: list[str]) -> dict[str, SourceStats]:
    """Rebuild per-source article counts from an existing CSV."""
    counts: dict[str, int] = {sid: 0 for sid in source_ids}
    with open(csv_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sid = row.get("source_id", "")
            if sid in counts:
                counts[sid] += 1
    return {
        sid: SourceStats(urls_collected=counts[sid], articles_saved=counts[sid])
        for sid in source_ids
    }


def log_scrape_summary(stats: dict[str, SourceStats], total: int, cached: bool = False) -> None:
    """Print a per-source summary after each run."""
    prefix = "Using cached CSV" if cached else "Scrape complete"
    logger.info("%s — %d articles saved", prefix, total)
    print(f"\n{'=' * 60}")
    print(f"  {prefix}: {total} articles saved")
    print(f"{'=' * 60}")
    print(f"  {'Source':<14} {'Collected':>10} {'Saved':>7} {'Dropped':>8} {'JS-miss%':>9}")
    print(f"  {'-' * 52}")

    for sid, s in stats.items():
        label = SOURCES.get(sid, {}).get("label", sid)[:14]
        print(
            f"  {label:<14} {s.urls_collected:>10} {s.articles_saved:>7} "
            f"{s.dropped_total:>8} {s.js_rendered_pct:>8.1f}%"
        )

    dropped_js = sum(s.dropped_js_rendered for s in stats.values())
    dropped_fetch = sum(s.dropped_fetch_failed for s in stats.values())
    if dropped_js or dropped_fetch:
        print(f"\n  Drop breakdown: {dropped_js} JS-rendered/paywall, {dropped_fetch} fetch errors")
    print()


# --- Main entry point ---

def run_scrape(
    source_ids: list[str],
    max_per_source: int = 25,
    progress_cb: Optional[Callable] = None,
    data_dir: str = "data",
    dry_run: bool = False,
    force: bool = False,
) -> tuple[str, int, dict[str, SourceStats], bool]:
    """
    Full pipeline: collect URLs → fetch articles → save CSV.

    Returns (path_to_csv, total_articles, stats_by_source, was_cached).
    Skips scraping when today's CSV already exists unless force=True.
    """
    os.makedirs(data_dir, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    csv_path = csv_path_for_date(data_dir, date_str)

    if not force and not dry_run and os.path.exists(csv_path):
        total = count_articles_in_csv(csv_path)
        stats = stats_from_csv(csv_path, source_ids)
        log_scrape_summary(stats, total, cached=True)
        if progress_cb:
            progress_cb("all", "cached", total)
        return csv_path, total, stats, True

    stats: dict[str, SourceStats] = {sid: SourceStats() for sid in source_ids}
    all_articles: list[dict] = []

    if progress_cb:
        progress_cb("all", "collecting", 0)

    url_map: dict[str, list[dict]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(collect_article_urls, sid, progress_cb, stats[sid]): sid
            for sid in source_ids
        }
        for future in concurrent.futures.as_completed(futures):
            sid = futures[future]
            candidates = future.result()[:max_per_source]
            url_map[sid] = candidates
            stats[sid].urls_collected = len(candidates)
            if progress_cb:
                progress_cb(sid, "urls_ready", len(candidates))

    if dry_run:
        log_scrape_summary(stats, 0, cached=False)
        print("  Dry run — URLs collected, no article text fetched.\n")
        return csv_path, 0, stats, False

    if progress_cb:
        progress_cb("all", "fetching", sum(len(v) for v in url_map.values()))

    fetch_tasks = [(c, sid) for sid, candidates in url_map.items() for c in candidates]

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(fetch_article_with_retry, c, sid): (c, sid)
            for c, sid in fetch_tasks
        }
        done = 0
        for future in concurrent.futures.as_completed(futures):
            result: FetchResult = future.result()
            done += 1
            sid = result.source_id
            src_stats = stats[sid]

            if result.status == "ok" and result.row:
                all_articles.append(result.row)
                src_stats.articles_saved += 1
            elif result.status == "js_rendered":
                src_stats.dropped_js_rendered += 1
                src_stats.errors.append(result.to_error_dict())
            else:
                src_stats.dropped_fetch_failed += 1
                src_stats.errors.append(result.to_error_dict())

            if progress_cb:
                progress_cb("all", "fetched", done)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(all_articles)

    total = len(all_articles)
    log_scrape_summary(stats, total, cached=False)

    if progress_cb:
        progress_cb("all", "done", total)

    return csv_path, total, stats, False


def list_csv_files(data_dir: str = "data") -> list[str]:
    """Return available CSV files sorted newest first."""
    os.makedirs(data_dir, exist_ok=True)
    files = [
        f for f in os.listdir(data_dir)
        if f.startswith("articles_") and f.endswith(".csv")
    ]
    files.sort(reverse=True)
    return files


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape news sources and save articles to CSV.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Collect article URLs only; do not fetch article text or write CSV",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-scrape even if today's CSV already exists",
    )
    parser.add_argument(
        "--sources",
        nargs="*",
        default=list(SOURCES.keys()),
        choices=list(SOURCES.keys()),
        metavar="SOURCE",
        help="Sources to scrape (default: all)",
    )
    parser.add_argument(
        "--max-per-source",
        type=int,
        default=25,
        help="Maximum articles to fetch per source (default: 25)",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory for CSV output (default: data)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    args = _parse_args()
    csv_path, total, stats, cached = run_scrape(
        source_ids=args.sources,
        max_per_source=args.max_per_source,
        data_dir=args.data_dir,
        dry_run=args.dry_run,
        force=args.force,
    )
    if not args.dry_run:
        tag = "cached" if cached else "written"
        print(f"CSV {tag}: {csv_path} ({total} articles)")
