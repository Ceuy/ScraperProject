"""
analyser.py
Stage 2: read CSV → cluster articles by story using text similarity → label with Mistral → rank top N
"""

import csv
import re
import os
import requests
import concurrent.futures
from collections import Counter

# --- AI backend: Groq (cloud) or Ollama (local fallback) ---
# Groq is used when GROQ_API_KEY env var is set (production/Render)
# Falls back to local Ollama if not set (local development)

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_MODEL = "mixtral-8x7b-32768"   # correct Groq-hosted Mistral model name
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "mistral"

VALID_CATEGORIES = {
    "Politics", "Business", "Technology", "Climate",
    "Health", "Science", "Sports", "Entertainment", "World"
}

CATEGORY_KEYWORDS = {
    "Politics": [
        "election","vote","president","prime minister","parliament","congress","senate",
        "democrat","republican","government","minister","policy","trump","biden",
        "zelensky","putin","orban","macron","referendum","coalition","opposition",
        "diplomatic","sanctions","treaty","summit","nato","un","european union",
    ],
    "Business": [
        "stock","market","economy","trade","inflation","gdp","bank","dollar","euro",
        "oil","gas","price","invest","billion","million","profit","loss","merger",
        "tariff","export","import","recession","fed","interest rate","earnings",
        "revenue","shares","crypto","bitcoin","imf","world bank","fiscal",
    ],
    "Technology": [
        "ai","artificial intelligence","tech","apple","google","microsoft","meta",
        "amazon","software","cyber","hack","robot","chip","semiconductor","startup",
        "algorithm","machine learning","openai","nvidia","tesla","spacex","satellite",
        "rocket","internet","5g","quantum","computer","data breach","deepfake",
    ],
    "Climate": [
        "climate","carbon","emission","flood","wildfire","drought","environment",
        "warming","fossil fuel","renewable","solar","wind energy","greenhouse",
        "deforestation","ocean","glacier","extreme weather","hurricane","tornado",
        "net zero","cop","paris agreement","pollution","biodiversity",
    ],
    "Health": [
        "hospital","vaccine","disease","cancer","health","virus","drug","medical",
        "patient","pandemic","outbreak","mental health","nhs","treatment","surgery",
        "fda","pharmaceutical","obesity","diabetes","epidemic","mortality","who",
        "clinical trial","antibiotic","mpox","measles",
    ],
    "Science": [
        "research","study","discover","planet","species","dna","genome","fossil",
        "physics","chemistry","biology","astronomy","nasa","esa","telescope",
        "mars","moon","asteroid","quantum","nuclear fusion","archaeological",
        "evolution","neuroscience","experiment","findings",
    ],
    "Sports": [
        "cup","league","championship","olympic","athlete","match","football","soccer",
        "basketball","tennis","cricket","rugby","baseball","golf","formula","f1",
        "nfl","nba","premier league","champions league","wimbledon","medal",
        "tournament","transfer","coach","stadium","world cup","playoff",
    ],
    "Entertainment": [
        "film","movie","music","celebrity","award","oscar","grammy","bafta","emmy",
        "actor","actress","singer","album","box office","streaming","netflix","disney",
        "hollywood","concert","festival","billboard","tv","series","trailer",
    ],
}

STOPWORDS = {
    "the","a","an","in","of","to","and","or","is","are","was","were","for",
    "on","at","with","by","from","that","this","as","it","its","be","been",
    "have","has","had","will","would","could","should","may","says","say",
    "after","over","into","up","out","about","new","more","than","but","not",
    "he","she","they","his","her","their","we","us","i","you","amid","also",
    "which","who","when","where","how","what","been","just","than","then",
    "its","also","into","over","before","after","during","within","between",
}


# --- Text processing ---

def tokenize(text):
    words = re.sub(r"[^a-z0-9 ]", "", text.lower()).split()
    return set(w for w in words if w not in STOPWORDS and len(w) > 2)


def extract_entities(text):
    tokens = re.findall(r"\b[A-Z][a-zA-Z]{2,}\b", text)
    generic = {
        "The","This","That","There","These","Those","After","Before","During",
        "While","When","How","Why","What","Says","Said","New","Top","Big",
        "Last","First","More","According","Report","Reuters","Guardian","Times",
    }
    return set(t for t in tokens if t not in generic)


def similarity(text1, text2):
    """
    Combined similarity using:
    - Jaccard on summary tokens (broad topic overlap)
    - Named entity shared boost (same story signal)
    - Title token overlap bonus
    """
    # Token similarity on full summary text
    w1, w2 = tokenize(text1), tokenize(text2)
    if not w1 or not w2:
        return 0.0
    jaccard = len(w1 & w2) / len(w1 | w2)

    # Entity boost — shared proper nouns strongly indicate same story
    e1 = extract_entities(text1)
    e2 = extract_entities(text2)
    shared = e1 & e2
    entity_boost = min(len(shared) * 0.20, 0.55) if shared else 0.0

    # Penalise if no entities shared at all and jaccard is borderline
    if not shared and jaccard < 0.15:
        return 0.0

    return min(jaccard + entity_boost, 1.0)


def smart_categorize(texts):
    """Score all category keywords against combined text, return best match."""
    combined = " ".join(texts).lower()
    scores = {cat: 0 for cat in CATEGORY_KEYWORDS}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in combined:
                scores[cat] += 2 if " " in kw else 1
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "World"


# --- Clustering ---

def cluster_articles(articles, threshold=0.28):
    """
    Greedy clustering on summary text similarity.
    Each article is compared to the representative of existing groups.
    Threshold tuned for summary-level text (higher than headline-only).
    """
    # Build combined text per article for similarity comparison
    def article_text(a):
        return a["title"] + " " + a["summary"]

    groups = []  # list of lists of article indices

    for i, article in enumerate(articles):
        placed = False
        for group in groups:
            rep_idx = group[0]
            rep_text = article_text(articles[rep_idx])
            if similarity(article_text(article), rep_text) >= threshold:
                group.append(i)
                placed = True
                break
        if not placed:
            groups.append([i])

    return groups


def merge_small_groups(groups, articles, threshold=0.22):
    """
    Second pass: try to merge singleton/small groups that are semantically close
    but didn't cross the main threshold. Helps catch synonym-heavy headlines.
    """
    changed = True
    while changed:
        changed = False
        merged = []
        used = set()
        for i, g1 in enumerate(groups):
            if i in used:
                continue
            combined = list(g1)
            rep1 = articles[g1[0]]["title"] + " " + articles[g1[0]]["summary"]
            for j, g2 in enumerate(groups):
                if j <= i or j in used:
                    continue
                rep2 = articles[g2[0]]["title"] + " " + articles[g2[0]]["summary"]
                if similarity(rep1, rep2) >= threshold:
                    combined.extend(g2)
                    used.add(j)
                    changed = True
            merged.append(combined)
            used.add(i)
        groups = merged
    return groups


# --- AI labelling ---

def _call_groq(prompt):
    """Call Groq's OpenAI-compatible API."""
    res = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": GROQ_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.0,
            "max_tokens": 60,
        },
        timeout=30,
    )
    res.raise_for_status()
    return res.json()["choices"][0]["message"]["content"].strip()


def _call_ollama(prompt):
    """Call local Ollama instance."""
    res = requests.post(
        OLLAMA_URL,
        json={
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.0, "num_predict": 60},
        },
        timeout=45,
    )
    res.raise_for_status()
    return res.json().get("response", "").strip()


def label_cluster_mistral(titles_and_summaries, cluster_idx):
    """
    Ask the AI to label a single story cluster.
    Uses Groq if GROQ_API_KEY is set, otherwise falls back to local Ollama.
    """
    snippets = []
    for title, summary in titles_and_summaries[:4]:
        first_sent = summary.split(".")[0][:120] if summary else ""
        snippets.append(f"- {title}: {first_sent}")
    content = "\n".join(snippets)

    prompt = f"""You are a news editor. The following articles from different outlets all cover the same story.

{content}

Provide:
1. A short neutral topic label for this story (4-7 words, no quotes, no punctuation at end)
2. The single most accurate category from this exact list:
   Politics, Business, Technology, Climate, Health, Science, Sports, Entertainment, World

Reply with ONLY these two lines:
TOPIC: <label>
CATEGORY: <category>"""

    try:
        if GROQ_API_KEY:
            raw = _call_groq(prompt)
        else:
            raw = _call_ollama(prompt)

        topic, category = None, None
        for line in raw.splitlines():
            line = line.strip()
            if line.upper().startswith("TOPIC:"):
                topic = line.split(":", 1)[1].strip().strip('"').strip("'")
            elif line.upper().startswith("CATEGORY:"):
                cat_raw = line.split(":", 1)[1].strip()
                for valid in VALID_CATEGORIES:
                    if valid.lower() in cat_raw.lower():
                        category = valid
                        break
        return cluster_idx, topic, category
    except Exception as e:
        print(f"  [Mistral] label error cluster {cluster_idx}: {e}")
        return cluster_idx, None, None


def merge_duplicate_topics(story_groups):
    """Post-process: collapse groups with near-identical topic labels."""
    merged = []
    used = set()
    for i, g in enumerate(story_groups):
        if i in used:
            continue
        combined = dict(g)
        combined["articles"] = list(g["articles"])
        ti = g["topic"].lower()
        wi = set(ti.split())
        vague_i = any(v in ti for v in ("other", "miscellaneous", "various", "world news", "news stories"))
        for j, g2 in enumerate(story_groups):
            if j <= i or j in used:
                continue
            tj = g2["topic"].lower()
            wj = set(tj.split())
            overlap = len(wi & wj) / max(len(wi | wj), 1)
            vague_j = any(v in tj for v in ("other", "miscellaneous", "various", "world news", "news stories"))
            if overlap >= 0.65 or (vague_i and vague_j):
                combined["articles"].extend(g2["articles"])
                used.add(j)
        combined["count"] = len(combined["articles"])
        # Recompute category from merged article texts
        all_texts = [a["title"] + " " + a["summary"] for a in combined["articles"]]
        if combined.get("category") == "World" and len(all_texts) > 1:
            combined["category"] = smart_categorize(all_texts)
        merged.append(combined)
        used.add(i)
    return merged


# --- Main entry point ---

def analyse(csv_path, top_n=10, progress_cb=None):
    """
    Full analysis pipeline:
    1. Read CSV
    2. Cluster by text similarity
    3. Label each cluster with Mistral (parallel)
    4. Rank by count
    5. Return top_n story groups with all article links
    """
    # Step 1: read CSV
    articles = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            articles.append(row)

    if not articles:
        return []

    if progress_cb:
        progress_cb("cluster", len(articles))

    # Step 2: cluster
    raw_groups = cluster_articles(articles, threshold=0.28)
    raw_groups = merge_small_groups(raw_groups, articles, threshold=0.22)

    if progress_cb:
        progress_cb("label", len(raw_groups))

    # Step 3: label with Mistral in parallel
    story_groups = [None] * len(raw_groups)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        futures = {}
        for idx, group in enumerate(raw_groups):
            pairs = [(articles[i]["title"], articles[i]["summary"]) for i in group]
            futures[ex.submit(label_cluster_mistral, pairs, idx)] = idx

        for future in concurrent.futures.as_completed(futures):
            idx, topic, category = future.result()
            group = raw_groups[idx]
            group_articles = [articles[i] for i in group]
            all_texts = [a["title"] + " " + a["summary"] for a in group_articles]

            # Python category as cross-check
            py_cat = smart_categorize(all_texts)
            chosen_cat = category if category in VALID_CATEGORIES else py_cat
            if py_cat != "World" and chosen_cat == "World":
                chosen_cat = py_cat

            # Fallback topic
            if not topic or len(topic) < 4 or topic.lower() in ("n/a", "none", "unknown", "untitled"):
                longest = max(group_articles, key=lambda a: len(a["title"]))
                topic = longest["title"][:60] + ("…" if len(longest["title"]) > 60 else "")

            # Count unique sources covering this story
            sources_covered = list({a["source_id"] for a in group_articles})

            story_groups[idx] = {
                "topic": topic,
                "category": chosen_cat,
                "articles": group_articles,
                "count": len(group_articles),
                "source_count": len(sources_covered),
                "sources_covered": sources_covered,
            }

            if progress_cb:
                progress_cb("labelled", idx)

    # Step 4: post-process
    story_groups = [g for g in story_groups if g is not None]
    story_groups = merge_duplicate_topics(story_groups)

    # Sort: primarily by number of sources covering the story (breadth),
    # secondarily by total article count (volume)
    story_groups.sort(key=lambda g: (g["source_count"], g["count"]), reverse=True)

    if progress_cb:
        progress_cb("done", len(story_groups))

    return story_groups[:top_n] if top_n else story_groups
