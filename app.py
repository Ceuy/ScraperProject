"""
app.py — Flask web frontend for the news aggregation pipeline
"""

import json
import logging
import os
import queue
import threading
import traceback

from flask import Flask, Response, jsonify, render_template, request, stream_with_context
from flask_cors import CORS

from analyser import analyse
from scraper import SOURCES, list_csv_files, run_scrape

logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# On Render the persistent disk is mounted at /data. Locally uses ./data
DATA_DIR = "/data" if os.path.isdir("/data") else os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

_latest_results = {"groups": [], "csv": "", "total_articles": 0, "stats": {}}
_results_lock = threading.Lock()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/sources")
def get_sources():
    return jsonify({
        sid: {
            "label": s["label"],
            "color": s["color"],
            "bg": s["bg"],
            "text_color": s["text_color"],
            "pages": len(s["pages"]),
        }
        for sid, s in SOURCES.items()
    })


@app.route("/api/csvfiles")
def csv_files():
    return jsonify({"files": list_csv_files(DATA_DIR)})


@app.route("/api/scrape", methods=["POST"])
def scrape():
    data = request.get_json() or {}
    source_ids = [s for s in data.get("sources", list(SOURCES.keys())) if s in SOURCES]
    max_per = int(data.get("max_per_source", 25))
    force = bool(data.get("force", False))
    q = queue.Queue()

    def progress_cb(source_id, stage, value):
        q.put({"source": source_id, "stage": stage, "value": value})

    def run():
        try:
            csv_path, total, stats, cached = run_scrape(
                source_ids,
                max_per_source=max_per,
                progress_cb=progress_cb,
                data_dir=DATA_DIR,
                force=force,
            )
            q.put({
                "stage": "complete",
                "csv": os.path.basename(csv_path),
                "total": total,
                "cached": cached,
                "stats": {sid: s.to_dict() for sid, s in stats.items()},
            })
        except Exception as exc:
            logger.exception("Scrape failed")
            q.put({"stage": "error", "message": str(exc)})

    threading.Thread(target=run, daemon=True).start()

    def generate():
        while True:
            try:
                msg = q.get(timeout=90)
                yield f"data: {json.dumps(msg)}\n\n"
                if msg.get("stage") in ("complete", "error"):
                    break
            except queue.Empty:
                yield 'data: {"stage":"heartbeat"}\n\n'

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/analyse", methods=["POST"])
def analyse_route():
    data = request.get_json() or {}
    csv_file = data.get("csv", "")
    top_n = int(data.get("top_n", 10))

    if not csv_file:
        files = list_csv_files(DATA_DIR)
        if not files:
            return jsonify({"error": "No CSV files found. Run a scrape first."}), 400
        csv_file = files[0]

    csv_path = os.path.join(DATA_DIR, csv_file)
    if not os.path.exists(csv_path):
        return jsonify({"error": f"File not found: {csv_file}"}), 404

    q = queue.Queue()

    def progress_cb(stage, value):
        q.put({"stage": stage, "value": value})

    def run():
        try:
            groups = analyse(csv_path, top_n=top_n, progress_cb=progress_cb)
            serialised = []
            for g in groups:
                serialised.append({
                    "topic": g["topic"],
                    "category": g["category"],
                    "count": g["count"],
                    "source_count": g["source_count"],
                    "sources_covered": g["sources_covered"],
                    "articles": [
                        {
                            "title": a["title"],
                            "url": a["url"],
                            "source_id": a["source_id"],
                            "source_label": a["source_label"],
                            "date": a["date"],
                            "summary": a["summary"][:200],
                            "source_color": a["source_color"],
                            "source_bg": a["source_bg"],
                            "source_text_color": a["source_text_color"],
                        }
                        for a in g["articles"]
                    ],
                })
            with _results_lock:
                _latest_results["groups"] = serialised
                _latest_results["csv"] = csv_file
                _latest_results["total_articles"] = sum(g["count"] for g in serialised)
            q.put({"stage": "complete", "groups": serialised})
        except Exception as exc:
            traceback.print_exc()
            logger.exception("Analysis failed")
            q.put({"stage": "error", "message": str(exc)})

    threading.Thread(target=run, daemon=True).start()

    def generate():
        while True:
            try:
                msg = q.get(timeout=120)
                yield f"data: {json.dumps(msg)}\n\n"
                if msg.get("stage") in ("complete", "error"):
                    break
            except queue.Empty:
                yield 'data: {"stage":"heartbeat"}\n\n'

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/results/latest")
def latest_results():
    with _results_lock:
        return jsonify(_latest_results)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("\n  Newscraper running at http://localhost:5000\n")
    app.run(debug=True, port=5000, threaded=True)
