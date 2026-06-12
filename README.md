# Super Scraper

Intelligent job crawler for company career sites + a live monitoring dashboard.

For each company in your leads CSV it walks a cheapest-first ladder: find the
career page → detect the ATS → pull jobs from the ATS API → fall back to
structured data (JSON-LD / embedded JSON) → heuristic links → AI extraction →
optional headless browser. Crash-safe and **resumable** — re-run the same
command to continue where it stopped.

---

## 1. Setup

```bash
# from the project folder
python3 -m venv venv
source venv/bin/activate

pip install httpx pandas beautifulsoup4 lxml openai fastapi uvicorn
# optional — only if you use --browser:
pip install playwright && playwright install chromium
```

### API key (for the AI extraction stage)

Create a `.env` file in this folder — it loads automatically, no need to export:

```bash
echo 'OPENAI_API_KEY=sk-your-key' > .env
```

(There's a `.env.example` template you can copy: `cp .env.example .env`.)

---

## 2. Run the scraper

Your CSV needs a **Company Name** column and a **Website** column.

```bash
# Basic run
python super_scraper.py --csv Leads.csv --out out

# Trial run — only the first 50 companies
python super_scraper.py --csv Leads.csv --out out --limit 50

# Faster — more companies in parallel
python super_scraper.py --csv Leads.csv --out out --concurrency 40

# Full power — AI extraction (OpenAI) + per-job descriptions
python super_scraper.py --csv Leads.csv --out out --concurrency 40 --details

# Add headless-browser rendering for JavaScript-heavy career boards
python super_scraper.py --csv Leads.csv --out out --concurrency 40 --details --browser
```

**Resume:** if it stops or you Ctrl+C, just run the same command again — it skips
companies already recorded in `out/state.json`.

### Scraper flags

| Flag | Default | What it does |
|------|---------|--------------|
| `--csv PATH` | *(required)* | Leads CSV (Company Name + Website columns) |
| `--out DIR` | `out` | Output directory |
| `--limit N` | `0` (all) | Only crawl the first N companies (trial run) |
| `--concurrency N` | `30` | Companies crawled in parallel |
| `--details` | off | Fetch each job's page to fill in descriptions the ATS API omits (slower) |
| `--browser` | off | Render JS-heavy pages with Playwright (needs `playwright install chromium`) |
| `--ai-provider` | `auto` | `auto` \| `openai` \| `anthropic` \| `off` |
| `--ai-model` | auto | Override the model, e.g. `gpt-4o`, `gpt-4o-mini` |

### Outputs (in `--out`)

| File | Contents |
|------|----------|
| `jobs.jsonl` | Every job as it's found (append-only stream, all runs) |
| `jobs.csv` | Deduped final table (written when the run finishes) |
| `report.csv` | Per-company status: method used, ATS detected, job count, errors |
| `state.json` | Resume checkpoint — re-run the same command to continue |

---

## 3. Run the dashboard

A live web UI showing progress, per-company status, the jobs table, failure
breakdown, and ATS/method charts. **Read-only** — runs alongside the scraper
without interfering.

```bash
# point it at the same output dir; pass the CSV so it knows the total count
python dashboard.py --out out --csv Leads.csv
```

Opens automatically at **http://localhost:8050** and refreshes every 3 seconds.

```bash
# custom port
python dashboard.py --out out --csv Leads.csv --port 8060
```

### Dashboard flags

| Flag | Default | What it does |
|------|---------|--------------|
| `--out DIR` | `out` | Scraper output directory to read |
| `--csv PATH` | *(none)* | Original leads CSV (shows the total company count) |
| `--port N` | `8050` | Port to serve on |

---

## 4. Typical workflow

```bash
# Terminal 1 — start the crawl
source venv/bin/activate
python super_scraper.py --csv Leads.csv --out out --concurrency 40 --details

# Terminal 2 — watch it live
source venv/bin/activate
python dashboard.py --out out --csv Leads.csv
```

---

## Notes

- **AI stage** uses OpenAI by default (auto-detects `OPENAI_API_KEY` from `.env`).
  Startup prints `AI extraction: ON — openai / gpt-4o-mini` when active.
- **Geo-blocked sites** (some Gulf government/medical domains) only respond from
  a regional IP — a Middle East VPN can recover those; no code change helps.
- **Some career pages reuse a multinational parent's global ATS board**, which
  returns worldwide jobs (not just regional). Check `report.csv` if one company
  shows an unusually large job count.
