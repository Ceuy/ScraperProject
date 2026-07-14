# Newscraper

A Python news aggregation pipeline that scrapes headlines from 10 international outlets, clusters articles covering the same story, and labels each cluster with an AI-generated topic and category — served through a Flask web UI.

## What it does

1. **Scrape** — Collects article URLs from source homepages (BBC, Reuters, Guardian, AP, Al Jazeera, NYT, DW, France 24, Euronews, Politico), fetches full article text, and saves structured rows to a dated CSV.
2. **Analyse** — Groups articles by text similarity and named-entity overlap, then labels each cluster using Groq (Llama 3.1 8B) in production or local Ollama during development.
3. **Display** — A Flask frontend lets you trigger scrapes, run analysis, and browse the top story clusters ranked by how many outlets covered them.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  scraper.py │ ──► │ analyser.py │ ──► │   app.py    │
│  (Stage 1)  │     │  (Stage 2)  │     │  (Stage 3)  │
│             │     │             │     │             │
│ BeautifulSoup│    │ Clustering  │     │ Flask + SSE │
│ → CSV       │     │ + Groq/Ollama│    │ → Browser   │
└─────────────┘     └─────────────┘     └─────────────┘
       │                                       │
       └──────────── data/articles_*.csv ─────┘
```

| File | Role |
|------|------|
| `scraper.py` | Scrape sources, rate-limit requests, write `data/articles_YYYY-MM-DD.csv` |
| `analyser.py` | Cluster articles, label with AI, return ranked story groups |
| `app.py` | REST/SSE API and web UI |
| `templates/index.html` | Single-page frontend |

## Requirements

- Python 3.11+
- [Groq API key](https://console.groq.com/) for cloud labelling (recommended for deployment)
- [Ollama](https://ollama.com/) with the `mistral` model (optional local fallback)

## Setup

```bash
git clone <your-repo-url>
cd ScraperProject
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
mkdir -p data
```

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GROQ_API_KEY` | Production | Groq API key for cluster labelling. Without it, the analyser falls back to local Ollama at `http://localhost:11434`. |
| `PORT` | Render only | Set automatically by Render. |

Create a `.env` file locally (not committed):

```env
GROQ_API_KEY=gsk_...
```

## Running locally

### Web UI (full pipeline)

```bash
python app.py
```

Open [http://localhost:5000](http://localhost:5000), select sources, click **Scrape**, then **Analyse**.

### Scraper CLI

```bash
# Full scrape (all 10 sources)
python scraper.py

# Dry run — collect URLs only, no article fetches
python scraper.py --dry-run

# Specific sources, limit articles per source
python scraper.py --sources bbc reuters guardian --max-per-source 15

# Force re-scrape even if today's CSV already exists
python scraper.py --force
```

If `data/articles_YYYY-MM-DD.csv` already exists for today, the scraper skips the run unless `--force` is passed. This avoids redundant requests during development.

### Analyser (standalone)

```python
from analyser import analyse
groups = analyse("data/articles_2026-06-20.csv", top_n=10)
for g in groups:
    print(g["topic"], g["category"], g["source_count"], "outlets")
```

## Scraper behaviour

- **Rate limiting** — Per-domain delays (1.5 s between requests to the same host) with reduced concurrency (3 workers for URL collection, 4 for article fetches).
- **Retries** — Transient network/timeout failures are retried up to 2 times with exponential backoff.
- **Structured errors** — Failed fetches are tracked per source (`js_rendered`, `fetch_failed`, `http_error`, `timeout`) and logged in a summary table after each run.
- **Stats summary** — After every run, a per-source table shows URLs collected, articles saved, drops, and % likely JS-rendered pages missed.

## Deployment on Render

The project includes a [`render.yaml`](render.yaml) Blueprint for one-click deployment.

### Render setup

1. Connect your GitHub repo in the [Render Dashboard](https://dashboard.render.com/).
2. Apply the Blueprint or create a **Web Service** manually:
   - **Runtime:** Python 3.11
   - **Build command:** `pip install -r requirements.txt`
   - **Start command:** `gunicorn app:app --workers 2 --worker-class gevent --timeout 300 --bind 0.0.0.0:$PORT`
3. Add a **Persistent Disk** mounted at `/data` (1 GB is enough for CSV storage).
4. Set `GROQ_API_KEY` in the service environment variables.

The app auto-detects Render's `/data` mount and stores CSVs there so they survive redeploys.

### Notes

- Scraping from Render's shared IPs can trigger blocks on some news sites. For portfolio demos, pre-scrape locally and upload CSVs, or scrape a subset of sources.
- Long scrape/analyse jobs use Server-Sent Events (SSE); the 300 s Gunicorn timeout accommodates this.
- Free-tier instances sleep after inactivity; first request may be slow.

## Known issues

- **France 24 actively blocks scrapers.** Every page returns HTTP 403 from an Imperva/Incapsula-style "Access denied" WAF page, not a simple User-Agent check — confirmed with full browser-like headers. Header/UA changes won't fix this; the source is currently expected to yield 0 articles.
- **News sites periodically restructure URLs.** BBC, Al Jazeera, DW, and Euronews have all changed section paths at least once (see `SOURCES` in `scraper.py`); expect occasional 404s that need a URL update.
- **Memory headroom on Render's Free/Starter plans is tight.** Per-domain rate limiting plus 10 sources × up to 25 articles each can approach the RAM ceiling on constrained plans. If you see `SIGKILL`/OOM in Render logs, move to a plan with more memory (Standard or higher) or lower `max_per_source`.

## Project structure

```
ScraperProject/
├── app.py              # Flask server
├── scraper.py          # Stage 1: scraping
├── analyser.py         # Stage 2: clustering + AI labels
├── requirements.txt
├── render.yaml         # Render Blueprint
├── templates/
│   └── index.html      # Web UI
└── data/               # Generated CSVs (gitignored)
```

## License

MIT — use freely for portfolio and learning purposes.
