# Super Scraper — What's New

**From the Taleer Scraper to the Super Scraper: faster, smarter, and dramatically simpler.**

This document summarises the major upgrades between our previous scraping system
(the Taleer Scraper) and the new **Super Scraper** engine that now powers job
extraction across your company list.

---

## TL;DR — The Big Wins

| | Taleer Scraper (old) | **Super Scraper (new)** |
|---|---|---|
| **Processing model** | Sequential — one company at a time, with a 2-second delay between each | **Fully async — 30–40 companies crawled in parallel** (configurable) |
| **Throughput** | A 1,866-company batch took hours | **A 5,000+ company run finishes in roughly 1.5–2 hours** |
| **Infrastructure** | Required a running MongoDB server, 14 collections, complex setup | **Zero database. Zero servers. One Python file — runs anywhere** |
| **ATS coverage** | HTML selector scraping; Workday / Oracle HCM near-zero success | **11 direct ATS API adapters** — including Workday and Oracle, previously unscrapable |
| **AI extraction** | Optional, discovery-only (finding URLs) | **Built into the extraction ladder** — AI reads the page and extracts the jobs themselves |
| **Crash recovery** | Batch/resume logic spread across MongoDB + JSON mirror (could drift) | **Single checkpoint file — re-run the same command, it continues exactly where it stopped** |
| **Monitoring** | Dashboard coupled to MongoDB | **Live read-only dashboard** — progress, per-company status, job table, failure breakdown, auto-refresh every 3 s |
| **Output** | 6 CSV buckets, overwritten each run | **Streaming `jobs.jsonl` (never loses data) + deduped `jobs.csv` + per-company `report.csv`** |

---

## 1. Speed: From Sequential to Massively Parallel

The old scraper processed companies **one at a time**, with a polite 2-second
pause between each. Every slow website held up the entire queue.

The Super Scraper is built on a fully asynchronous engine (`httpx` + `asyncio`)
that crawls **30–40 companies simultaneously**. A slow or unresponsive site no
longer blocks anything — the other 39 keep working. Per-company timeouts ensure
no single website can stall a run.

In a recent production run, the engine processed **5,081 companies** — nearly
3× the size of the old 1,866-company list — in a single session.

```bash
# one command, full power
python super_scraper.py --csv Leads.csv --out out --concurrency 40 --details
```

---

## 2. Power: A Cheapest-First Extraction Ladder

Instead of "fetch the page and hope the parser works", every company now walks
a **6-stage ladder**, always trying the cheapest, most reliable method first
and only escalating when needed:

```
1. Career-page discovery   → find the careers page from the homepage
2. ATS detection           → 18 platform fingerprints (URL + HTML)
3. ATS API extraction      → pull jobs straight from the ATS's own JSON API
4. Structured data         → JSON-LD JobPosting + embedded JSON blobs
5. Heuristic link analysis → job links found directly in the HTML
6. AI extraction           → an LLM reads the page and extracts the jobs
   (+ optional)            → headless Chromium for JavaScript-only boards
```

### Direct ATS API integration — the game changer

The old scraper detected ATS platforms but then tried to scrape their HTML —
which failed for JavaScript-heavy platforms (Workday and Oracle HCM were
documented as *"near-zero success rate"*).

The Super Scraper talks to the ATS **APIs directly** for 11 platforms:

> Greenhouse · Lever · Ashby · Workable · SmartRecruiters · Recruitee ·
> BambooHR · Breezy · Personio · **Workday** · **Oracle Recruiting Cloud**

API responses are immune to CAPTCHAs, page redesigns, and JavaScript rendering
— they return clean, structured job data every time. Workday and Oracle in
particular are widely used by large Gulf enterprises, so this directly unlocks
companies that were previously written off.

Seven more platforms (Taleo, SuccessFactors, iCIMS, Jobvite, Teamtailor,
JazzHR, Zoho Recruit) are fingerprinted and routed to the AI/browser stages.

### AI that extracts, not just discovers

In the old system, AI was only used (optionally) to *find* career page URLs.
In the Super Scraper, AI is a first-class **extraction stage**: when a career
page doesn't match any known pattern, the LLM reads the page content and
returns the structured job list itself. In the latest run, the AI stage alone
recovered jobs from **200+ companies** that every conventional method missed.

---

## 3. Efficiency: Radically Simpler to Run and Maintain

| | Old | New |
|---|---|---|
| Codebase | ~20+ source modules across orchestrator, engine, discoverer, parser, healer, exporter, storage, UI | **One engine file + one dashboard file (~1,700 lines total)** |
| Dependencies | MongoDB, PyMongo, Pydantic, structlog, flashtext, Jinja2, HTMX, … | **httpx, pandas, BeautifulSoup — plus optional OpenAI / Playwright** |
| Setup time | Install + run MongoDB, create indexes, configure 14 env vars | **`pip install`, drop in an API key, run** |
| State | MongoDB + a JSON mirror that could drift apart | **One `state.json` checkpoint — single source of truth** |

Less infrastructure means fewer failure points, instant onboarding for any
machine, and no database to host, back up, or keep in sync.

### Crash-safe and resumable by design

Every job is appended to `jobs.jsonl` the moment it's found — even a power cut
mid-run loses nothing. If a run stops for any reason, **re-running the exact
same command resumes from the checkpoint**, skipping everything already done.

---

## 4. Visibility: Know Exactly What's Happening, Live

A built-in monitoring dashboard runs alongside the scraper (read-only — it
never interferes with the crawl):

```bash
python dashboard.py --out out --csv Leads.csv     # → http://localhost:8050
```

- Live progress bar against the full company list, refreshing every 3 seconds
- Per-company status: career page found, ATS detected, method used, job count
- Searchable jobs table as results stream in
- Failure breakdown by cause — and the causes are now **diagnostic**, not generic:
  geo-blocked sites, dead DNS (bad lead data), HTTP 403 bot-blocks, and timeouts
  are each identified separately, so we know which failures are fixable
  (e.g. via a regional VPN) and which leads are simply invalid.

---

## 5. Honest, Actionable Reporting

Each run produces three artifacts in the output folder:

| File | What it gives you |
|---|---|
| `jobs.csv` | The final deduplicated job table — title, location, apply URL, description, source |
| `jobs.jsonl` | Every job as a stream record, across all runs — nothing is ever overwritten |
| `report.csv` | One row per company: career page URL, ATS detected, extraction method, job count, exact failure reason, and time taken |

From the latest 5,081-company production run: **2,101 career pages discovered
and 4,757 jobs extracted across 674 companies**, with every non-producing
company carrying a precise, classified reason — so the lead list itself can be
cleaned and improved over time.

---

## Summary

The Super Scraper takes everything the Taleer pipeline learned about MENA
career sites and rebuilds it as a modern, parallel, API-first engine:

- **Faster** — 30–40× more parallel than the old sequential loop
- **More powerful** — direct ATS APIs (including Workday & Oracle) + a true AI extraction stage
- **More efficient** — no database, no servers, one command, fully resumable
- **More transparent** — live dashboard and per-company diagnostic reporting

One command in, clean structured jobs out.
