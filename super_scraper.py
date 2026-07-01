"""
SUPER SCRAPER — Intelligent job crawler for company career sites.

Pipeline per company (escalation ladder — cheapest method first):

  Website ─► 1. CAREER PAGE FINDER   common paths + homepage links + sitemap
             2. ATS DETECTOR         fingerprints 15+ ATS platforms
             3. ATS API ADAPTERS     Greenhouse, Lever, Ashby, Workable,
                                     SmartRecruiters, Recruitee, BambooHR,
                                     Breezy, Personio, Workday, Oracle ORC
             4. JSON-LD EXTRACTOR    schema.org JobPosting structured data
             5. EMBEDDED JSON        __NEXT_DATA__ / initial-state blobs
             6. HEURISTIC LINKS      job-looking anchors on career pages
             7. AI EXTRACTOR         Claude reads the page → structured jobs
             8. BROWSER FALLBACK     Playwright renders JS-heavy sites (optional)

Outputs (crash-safe, resumable):
  out/jobs.jsonl      every job as it is found
  out/jobs.csv        deduped final table
  out/report.csv      per-company status (method used, ATS, count, errors)
  out/state.json      checkpoint — rerun the same command to resume

Usage:
  python super_scraper.py --csv Leads.csv --out out
  python super_scraper.py --csv Leads.csv --out out --limit 50          # trial run
  python super_scraper.py --csv Leads.csv --out out --browser           # add JS rendering
  OPENAI_API_KEY=sk-...    python super_scraper.py --csv Leads.csv --out out   # AI stage via OpenAI
  ANTHROPIC_API_KEY=sk-... python super_scraper.py --csv Leads.csv --out out   # AI stage via Claude
  python super_scraper.py ... --ai-model gpt-5-mini                     # pick a specific model
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import re
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict, field
from html import unescape
from urllib.parse import urljoin, urlparse

import httpx
import pandas as pd
from bs4 import BeautifulSoup

# ============================================================================
# Config
# ============================================================================

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]
UA = _USER_AGENTS[0]

def _headers(attempt: int = 0) -> dict:
    ua = _USER_AGENTS[attempt % len(_USER_AGENTS)]
    return {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }

HEADERS = _headers(0)
TIMEOUT = httpx.Timeout(30.0, connect=15.0)
MAX_HTML = 2_500_000          # ignore bodies bigger than this
AI_TEXT_BUDGET = 14_000       # chars of page text sent to the AI model
DESC_BUDGET = 4_000           # max chars of job description stored per job
COMPANY_TIMEOUT = 120         # hard wall-clock cap per company (s) — prevents stalls
AI_CONCURRENCY = 6            # max simultaneous AI calls (independent of --concurrency)
AI_TIMEOUT = 45.0             # per-request timeout for the AI client (s)
AI_DEFAULT_MODELS = {
    "openai": "gpt-4o-mini",                 # cheap + reliable JSON mode; try gpt-5-mini / gpt-4.1 for more brains
    "anthropic": "claude-sonnet-4-20250514",
}

CAREER_WORDS = [
    "career", "careers", "jobs", "job", "vacanc", "join us", "join-us",
    "joinus", "work with us", "work-with-us", "opportunit", "hiring",
    "recruitment", "employment", "open positions", "open-positions",
    "we're hiring", "talent",
    # Arabic (relevant for Gulf companies)
    "وظائف", "التوظيف", "توظيف", "انضم إلينا", "فرص العمل", "الوظائف",
]
CAREER_PATHS = [
    "/careers", "/careers/", "/career", "/jobs", "/jobs/", "/job",
    "/vacancies", "/join-us", "/joinus", "/join", "/work-with-us",
    "/opportunities", "/recruitment", "/employment", "/hiring",
    "/about/careers", "/about-us/careers", "/company/careers",
    "/en/careers", "/en/jobs", "/careers/jobs", "/current-openings",
    "/open-positions", "/people/careers", "/hr/careers",
]
JOB_URL_HINT = re.compile(
    r"/(job|jobs|career|careers|vacanc|position|opening|opportunit|requisition|posting)s?[/\-_?#]",
    re.I,
)
NOISE_TITLES = re.compile(
    r"^(home|about|contact|login|apply now|read more|learn more|view all|"
    r"see all|search|filter|next|previous|back|careers?|jobs?|share|menu)$", re.I)

# ============================================================================
# Data model
# ============================================================================

@dataclass
class Job:
    company: str
    title: str
    url: str = ""
    location: str = ""
    department: str = ""
    employment_type: str = ""
    posted_date: str = ""
    description: str = ""          # short snippet only
    source_method: str = ""        # ats:greenhouse | jsonld | heuristic | ai | browser+...
    ats: str = ""
    career_page: str = ""

@dataclass
class Report:
    company: str
    website: str
    career_page: str = ""
    ats_detected: str = ""
    method: str = ""
    jobs_found: int = 0
    needs_javascript: bool = False
    error: str = ""
    seconds: float = 0.0

# ============================================================================
# HTTP helpers
# ============================================================================

async def fetch(client: httpx.AsyncClient, url: str, *, method="GET",
                json_body=None, retries=2) -> httpx.Response | None:
    if not str(url).startswith(("http://", "https://")):
        return None                      # skip mailto:/tel:/javascript:/# etc.
    for attempt in range(retries + 1):
        try:
            hdrs = _headers(attempt)        # rotate UA on each retry
            if method == "POST":
                r = await client.post(url, json=json_body, headers=hdrs)
            else:
                r = await client.get(url, headers=hdrs)
            if r.status_code in (403, 429, 500, 502, 503, 504) and attempt < retries:
                await asyncio.sleep(2 * (attempt + 1))
                continue
            return r
        except Exception:
            if attempt < retries:
                await asyncio.sleep(1.5 * (attempt + 1))
    return None


async def get_text(client, url) -> tuple[str, str]:
    """Return (final_url, html) or ('','')."""
    r = await fetch(client, url)
    if r is None or r.status_code >= 400:
        return "", ""
    body = r.text[:MAX_HTML]
    return str(r.url), body


def soup_of(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def clean_desc(text, cap: int = DESC_BUDGET) -> str:
    """HTML/whitespace-clean a job description into readable plain text."""
    if not text:
        return ""
    t = str(text)
    t = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", t)
    t = re.sub(r"(?i)<br\s*/?>", "\n", t)
    t = re.sub(r"(?i)</(p|div|li|h[1-6]|tr)>", "\n", t)
    t = re.sub(r"<[^>]+>", " ", t)
    t = unescape(t)
    t = re.sub(r"[ \t\xa0]+", " ", t)
    t = re.sub(r"\n[ \t]*\n+", "\n", t)
    return t.strip()[:cap]


def normalize_site(url: str) -> str:
    url = str(url or "").strip()
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "http://" + url
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def base_variants(base: str) -> list[str]:
    """Ordered scheme/host variants to try (https first, then toggle www).

    Many sites fail on the exact CSV URL (e.g. http://www.x) but answer fine on
    another variant — https instead of http, or with/without the www host.
    """
    p = urlparse(base)
    host = p.netloc
    if not host:
        return [base]
    nowww = host[4:] if host.startswith("www.") else host
    withwww = host if host.startswith("www.") else "www." + host
    hosts, out = [], []
    for h in (host, withwww, nowww):
        if h and h not in hosts:
            hosts.append(h)
    for scheme in ("https", "http"):
        for h in hosts:
            u = f"{scheme}://{h}"
            if u not in out:
                out.append(u)
    return out


def is_fetchable(url: str) -> bool:
    """True only for http(s) URLs — filters mailto:/tel:/javascript:/#/data: links."""
    return url.startswith(("http://", "https://"))


_DNS_EXECUTOR: ThreadPoolExecutor | None = None


def _dns_executor(workers: int = 128) -> ThreadPoolExecutor:
    global _DNS_EXECUTOR
    if _DNS_EXECUTOR is None:
        _DNS_EXECUTOR = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dns")
    return _DNS_EXECUTOR


def _resolve_sync(host: str) -> bool:
    try:
        old = socket.getdefaulttimeout()
        socket.setdefaulttimeout(8.0)   # per-lookup hard cap; prevents OS default 20-30s hangs
        try:
            socket.getaddrinfo(host, None)
            return True
        finally:
            socket.setdefaulttimeout(old)
    except Exception:
        return False


async def host_resolves(host: str, timeout: float = 10.0) -> bool:
    """Async DNS check that can't hang the run. getaddrinfo runs in a dedicated
    pool; if it doesn't finish within `timeout` we ABANDON the thread (it can't be
    killed) and return False, so a slow/dead resolver never blocks a worker slot."""
    if not host:
        return False
    loop = asyncio.get_event_loop()
    fut = loop.run_in_executor(_dns_executor(), _resolve_sync, host)
    done, _ = await asyncio.wait({fut}, timeout=timeout)
    if fut in done:
        try:
            return fut.result()
        except Exception:
            return False
    return False                      # abandon the still-running lookup; don't await it


def _curl_fetch_sync(url: str, timeout: float = 30.0) -> tuple[str, str, int]:
    """Synchronous Chrome-impersonating fetch using curl_cffi.
    Returns (final_url, html, status) or ('', '', 0) on failure.
    Runs in a thread — curl_cffi has no native async API."""
    try:
        from curl_cffi import requests as curl_requests
        r = curl_requests.get(
            url, impersonate="chrome124",
            timeout=timeout, allow_redirects=True,
            headers={"Accept-Language": "en-US,en;q=0.9,ar;q=0.8"},
        )
        if r.status_code < 400 and r.text:
            p = urlparse(str(r.url))
            return f"{p.scheme}://{p.netloc}", r.text[:MAX_HTML], r.status_code
        return "", "", r.status_code
    except Exception:
        return "", "", 0


async def curl_fetch(url: str) -> tuple[str, str, int]:
    """Async wrapper — runs _curl_fetch_sync in the DNS thread pool to avoid blocking."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_dns_executor(), _curl_fetch_sync, url)


async def resolve_home(client, base: str) -> tuple[str, str, int]:
    """Try variants; return (resolved_base, home_html, status).

    status: the HTTP status of the page that loaded; else the last 4xx/5xx seen
    (e.g. 403 = blocked); 0 if a resolvable host never connected (timeout /
    geo-block); -1 if NO variant host resolves in DNS (bad/fabricated domain).
    Variants whose host doesn't resolve are skipped, so dead domains fail fast.
    """
    variants = base_variants(base)
    hosts = []
    for v in variants:
        h = urlparse(v).netloc
        if h not in hosts:
            hosts.append(h)
    resolves = dict(zip(hosts, await asyncio.gather(*(host_resolves(h) for h in hosts))))
    dns_ok = any(resolves.values())
    # DNS pre-check is an optimisation — it can time out under concurrent load even for
    # live domains. If all checks failed, still attempt HTTP directly before giving up.
    if not dns_ok:
        for cand in base_variants(base):
            r = await fetch(client, cand, retries=0)
            if r is not None and r.status_code < 400:
                body = r.text[:MAX_HTML]
                if body:
                    p = urlparse(str(r.url))
                    return f"{p.scheme}://{p.netloc}", body, r.status_code
        return base, "", -1

    best_status = 0
    for cand in variants:
        if dns_ok and not resolves.get(urlparse(cand).netloc):
            continue                      # only skip when DNS check was conclusive
        r = await fetch(client, cand, retries=0)   # variant diversity IS the retry
        if r is None:
            continue
        best_status = r.status_code
        if r.status_code < 400:
            body = r.text[:MAX_HTML]
            if body:
                p = urlparse(str(r.url))
                return f"{p.scheme}://{p.netloc}", body, r.status_code

    # httpx failed — retry with Chrome TLS fingerprint (curl_cffi) for sites that
    # block non-browser TLS handshakes (Cloudflare, Akamai, etc.)
    for cand in variants:
        final_url, body, status = await curl_fetch(cand)
        if body:
            return final_url, body, status
        if status >= 400:
            best_status = status
            break   # got a real HTTP response, no point trying more variants

    return base, "", best_status


# Internal sections that often link to a careers page one level deep.
SECTION_HINTS = (
    "about", "company", "who-we-are", "whoweare", "corporate", "overview",
    "our-", "people", "team", "investor", "media", "press", "contact",
)

# ============================================================================
# Stage 1 — Career page discovery
# ============================================================================

def _career_links(html: str, page_url: str) -> list[str]:
    """Anchors on a page whose text or href smells like careers (http(s) only)."""
    out = []
    for a in soup_of(html).find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().startswith(("mailto:", "tel:", "javascript:", "data:", "sms:", "#")):
            continue
        text = " ".join(a.get_text(" ", strip=True).lower().split())
        if any(w in text or w in href.lower() for w in CAREER_WORDS):
            u = urljoin(page_url, href)
            if is_fetchable(u):
                out.append(u)
    return out


async def find_career_pages(client, base: str) -> tuple[list[str], str, str, int]:
    """Return (career_urls, homepage_html, resolved_base, http_status)."""
    base, home_html, status = await resolve_home(client, base)
    if not home_html:
        return [], "", base, status     # host dead/blocked — don't probe paths on it
    found: list[str] = []
    seen = set()

    def add(u: str):
        u = u.split("#")[0]
        if u and is_fetchable(u) and u not in seen:
            seen.add(u)
            found.append(u)

    # 1a. links on the homepage whose text/href smells like careers
    if home_html:
        for u in _career_links(home_html, base):
            add(u)

    # 1b. common paths (only if homepage gave us nothing)
    if not found:
        candidates = [base + p for p in CAREER_PATHS]
        results = await asyncio.gather(*(fetch(client, u) for u in candidates))
        for u, r in zip(candidates, results):
            if r is not None and r.status_code < 400 and len(r.text) > 500:
                add(str(r.url))

    # 1c. nested: follow a few internal section pages and scan THEM for
    #     career links (handles careers buried one level deep under About/Company)
    if not found and home_html:
        host = urlparse(base).netloc
        nav, seen_nav = [], set()
        for a in soup_of(home_html).find_all("a", href=True):
            href = urljoin(base, a["href"].split("#")[0])
            low = href.lower()
            if (urlparse(href).netloc == host and href not in seen_nav
                    and any(h in low for h in SECTION_HINTS)):
                seen_nav.add(href)
                nav.append(href)
        for fu, html in await asyncio.gather(*(get_text(client, u) for u in nav[:6])):
            if html:
                for u in _career_links(html, fu or base):
                    add(u)

    # 1d. sitemap scan as a last resort
    if not found:
        r = await fetch(client, base + "/sitemap.xml")
        if r is not None and r.status_code < 400:
            for loc in re.findall(r"<loc>(.*?)</loc>", r.text)[:500]:
                if any(w in loc.lower() for w in ("career", "job", "vacanc")):
                    add(loc.strip())

    return found[:4], home_html, base, status

# ============================================================================
# Stage 2 — ATS detection (URL + HTML fingerprints)
# ============================================================================

ATS_FINGERPRINTS: list[tuple[str, re.Pattern]] = [
    ("greenhouse",     re.compile(r"boards(?:-api)?\.greenhouse\.io/(?:v1/boards/)?(?:embed/job_board\?for=)?([\w\-]+)|greenhouse\.io/embed/job_board\?for=([\w\-]+)", re.I)),
    ("lever",          re.compile(r"jobs\.(?:eu\.)?lever\.co/([\w\-]+)", re.I)),
    ("ashby",          re.compile(r"jobs\.ashbyhq\.com/([\w\-%2520 ]+)", re.I)),
    ("workable",       re.compile(r"apply\.workable\.com/(?:api/v\d/accounts/)?([\w\-]+)", re.I)),
    ("smartrecruiters",re.compile(r"(?:careers|jobs)\.smartrecruiters\.com/([\w\-]+)|api\.smartrecruiters\.com/v1/companies/([\w\-]+)", re.I)),
    ("recruitee",      re.compile(r"([\w\-]+)\.recruitee\.com", re.I)),
    ("bamboohr",       re.compile(r"([\w\-]+)\.bamboohr\.com", re.I)),
    ("breezy",         re.compile(r"([\w\-]+)\.breezy\.hr", re.I)),
    ("personio",       re.compile(r"([\w\-]+)\.jobs\.personio\.(?:de|com)", re.I)),
    ("workday",        re.compile(r"([\w\-]+)\.(wd\d+)\.myworkdayjobs\.com(?:/([\w\-]+))?(?:/([\w\-]+))?", re.I)),
    ("oracle_orc",     re.compile(r"([\w\-\.]+\.oraclecloud\.com)/hcmUI/CandidateExperience(?:/[a-z\-]+)?/sites/([\w\-]+)", re.I)),
    ("taleo",          re.compile(r"([\w\-]+)\.taleo\.net", re.I)),
    ("successfactors", re.compile(r"(career\d*)\.successfactors\.(?:com|eu)|jobs\.sap\.com", re.I)),
    ("icims",          re.compile(r"(?:careers?[\-.])?([\w\-]+)\.icims\.com", re.I)),
    ("jobvite",        re.compile(r"jobs\.jobvite\.com/([\w\-]+)", re.I)),
    ("teamtailor",     re.compile(r"([\w\-]+)\.teamtailor\.com", re.I)),
    ("jazzhr",         re.compile(r"([\w\-]+)\.applytojob\.com", re.I)),
    ("zoho",           re.compile(r"([\w\-]+)\.zohorecruit\.com", re.I)),
]

def detect_ats(url: str, html: str = "") -> tuple[str, tuple]:
    """Return (ats_name, regex_groups) from a URL or page source."""
    haystacks = [url or "", html or ""]
    for name, pat in ATS_FINGERPRINTS:
        for hay in haystacks:
            m = pat.search(hay)
            if m:
                return name, m.groups()
    return "", ()

# ============================================================================
# Stage 3 — ATS API adapters (public, key-less endpoints)
# ============================================================================

def _first(groups):  # first non-empty regex group
    return next((g for g in groups if g), "")


async def ats_greenhouse(client, company, g, **_):
    token = _first(g)
    r = await fetch(client, f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true")
    if not r or r.status_code != 200:
        return []
    return [Job(company, j.get("title", ""), j.get("absolute_url", ""),
                (j.get("location") or {}).get("name", ""),
                "; ".join(d.get("name", "") for d in j.get("departments", []) or []),
                posted_date=j.get("updated_at", "")[:10],
                description=clean_desc(j.get("content", "")),
                source_method="ats:greenhouse", ats="greenhouse")
            for j in r.json().get("jobs", [])]


async def ats_lever(client, company, g, **_):
    site = _first(g)
    r = await fetch(client, f"https://api.lever.co/v0/postings/{site}?mode=json")
    if not r or r.status_code != 200:
        return []
    out = []
    for j in r.json():
        cat = j.get("categories") or {}
        parts = [j.get("descriptionPlain") or ""]
        for lst in (j.get("lists") or []):
            parts.append((lst.get("text", "") + "\n" + clean_desc(lst.get("content", ""), 1500)).strip())
        parts.append(j.get("additionalPlain") or "")
        out.append(Job(company, j.get("text", ""), j.get("hostedUrl", ""),
                       cat.get("location", ""), cat.get("team", ""),
                       cat.get("commitment", ""),
                       description=clean_desc("\n".join(p for p in parts if p)),
                       source_method="ats:lever", ats="lever"))
    return out


async def ats_ashby(client, company, g, **_):
    org = _first(g).replace("%2520", " ").replace("%20", " ")
    r = await fetch(client, f"https://api.ashbyhq.com/posting-api/job-board/{org}?includeCompensation=false")
    if not r or r.status_code != 200:
        return []
    return [Job(company, j.get("title", ""), j.get("jobUrl", ""),
                j.get("location", ""), j.get("department", ""),
                j.get("employmentType", ""),
                description=clean_desc(j.get("descriptionPlain") or j.get("description") or ""),
                source_method="ats:ashby", ats="ashby")
            for j in r.json().get("jobs", [])]


async def ats_workable(client, company, g, **_):
    acct = _first(g)
    r = await fetch(client, f"https://apply.workable.com/api/v1/widget/accounts/{acct}")
    if not r or r.status_code != 200:
        return []
    return [Job(company, j.get("title", ""), j.get("url", ""),
                ", ".join(filter(None, [(j.get("city") or ""), (j.get("country") or "")])),
                j.get("department", ""), j.get("employment_type", ""),
                source_method="ats:workable", ats="workable")
            for j in r.json().get("jobs", [])]


async def ats_smartrecruiters(client, company, g, **_):
    comp = _first(g)
    jobs, offset = [], 0
    while True:
        r = await fetch(client, f"https://api.smartrecruiters.com/v1/companies/{comp}/postings?limit=100&offset={offset}")
        if not r or r.status_code != 200:
            break
        data = r.json()
        for j in data.get("content", []):
            loc = j.get("location") or {}
            jobs.append(Job(company, j.get("name", ""),
                            f"https://jobs.smartrecruiters.com/{comp}/{j.get('id','')}",
                            ", ".join(filter(None, [loc.get("city"), loc.get("country")])),
                            (j.get("department") or {}).get("label", ""),
                            (j.get("typeOfEmployment") or {}).get("label", ""),
                            j.get("releasedDate", "")[:10],
                            source_method="ats:smartrecruiters", ats="smartrecruiters"))
        offset += 100
        if offset >= data.get("totalFound", 0):
            break
    return jobs


async def ats_recruitee(client, company, g, **_):
    sub = _first(g)
    r = await fetch(client, f"https://{sub}.recruitee.com/api/offers/")
    if not r or r.status_code != 200:
        return []
    return [Job(company, j.get("title", ""), j.get("careers_url", ""),
                j.get("location", ""), j.get("department", ""),
                j.get("employment_type_code", ""),
                description=clean_desc(" ".join(filter(None, [j.get("description"), j.get("requirements")]))),
                source_method="ats:recruitee", ats="recruitee")
            for j in r.json().get("offers", [])]


async def ats_bamboohr(client, company, g, **_):
    sub = _first(g)
    r = await fetch(client, f"https://{sub}.bamboohr.com/careers/list")
    if not r or r.status_code != 200:
        return []
    try:
        data = r.json()
    except json.JSONDecodeError:
        return []
    out = []
    for j in data.get("result", []):
        loc = j.get("location") or {}
        out.append(Job(company, j.get("jobOpeningName", ""),
                       f"https://{sub}.bamboohr.com/careers/{j.get('id','')}",
                       ", ".join(filter(None, [loc.get("city"), loc.get("state")])),
                       j.get("departmentLabel", ""),
                       j.get("employmentStatusLabel", ""),
                       source_method="ats:bamboohr", ats="bamboohr"))
    return out


async def ats_breezy(client, company, g, **_):
    sub = _first(g)
    r = await fetch(client, f"https://{sub}.breezy.hr/json")
    if not r or r.status_code != 200:
        return []
    try:
        data = r.json()
    except json.JSONDecodeError:
        return []
    return [Job(company, j.get("name", ""), j.get("url", ""),
                (j.get("location") or {}).get("name", ""),
                (j.get("department") or ""),
                (j.get("type") or {}).get("name", ""),
                source_method="ats:breezy", ats="breezy")
            for j in data]


async def ats_personio(client, company, g, **_):
    sub = _first(g)
    r = await fetch(client, f"https://{sub}.jobs.personio.de/xml")
    if not r or r.status_code != 200:
        return []
    jobs = []
    for m in re.finditer(r"<position>(.*?)</position>", r.text, re.S):
        block = m.group(1)
        def tag(t):
            mm = re.search(rf"<{t}>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</{t}>", block, re.S)
            return (mm.group(1).strip() if mm else "")
        jid = tag("id")
        desc = " ".join(re.findall(r"<value>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</value>", block, re.S))
        jobs.append(Job(company, tag("name"),
                        f"https://{sub}.jobs.personio.de/job/{jid}" if jid else "",
                        tag("office"), tag("department"), tag("employmentType"),
                        description=clean_desc(desc),
                        source_method="ats:personio", ats="personio"))
    return jobs


async def ats_workday(client, company, g, *, source_url="", **_):
    tenant, wd = g[0], g[1]
    # site token = last path segment that isn't a locale like en-US
    site = ""
    for seg in reversed(urlparse(source_url).path.split("/")):
        if seg and not re.fullmatch(r"[a-z]{2}-[A-Z]{2}", seg) and seg.lower() not in ("jobs", "job"):
            site = seg
            break
    site = site or (g[3] or g[2] or "External")
    api = f"https://{tenant}.{wd}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs"
    jobs, offset = [], 0
    while offset < 2000:
        r = await fetch(client, api, method="POST",
                        json_body={"appliedFacets": {}, "limit": 20,
                                   "offset": offset, "searchText": ""})
        if not r or r.status_code != 200:
            break
        data = r.json()
        batch = data.get("jobPostings", [])
        for j in batch:
            jobs.append(Job(company, j.get("title", ""),
                            f"https://{tenant}.{wd}.myworkdayjobs.com/{site}{j.get('externalPath','')}",
                            j.get("locationsText", ""),
                            posted_date=j.get("postedOn", ""),
                            source_method="ats:workday", ats="workday"))
        offset += 20
        if offset >= data.get("total", 0) or not batch:
            break
    return jobs


async def ats_oracle_orc(client, company, g, **_):
    host, site = g[0], g[1]
    api = (f"https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
           f"?onlyData=true&finder=findReqs;siteNumber={site},limit=200,offset=0,"
           f"sortBy=POSTING_DATES_DESC")
    r = await fetch(client, api)
    if not r or r.status_code != 200:
        return []
    try:
        items = r.json().get("items", [])
        reqs = items[0].get("requisitionList", []) if items else []
    except (json.JSONDecodeError, AttributeError, IndexError):
        return []
    return [Job(company, j.get("Title", ""),
                f"https://{host}/hcmUI/CandidateExperience/en/sites/{site}/job/{j.get('Id','')}",
                j.get("PrimaryLocation", ""),
                posted_date=str(j.get("PostedDate", ""))[:10],
                source_method="ats:oracle_orc", ats="oracle_orc")
            for j in reqs]


ATS_ADAPTERS = {
    "greenhouse": ats_greenhouse, "lever": ats_lever, "ashby": ats_ashby,
    "workable": ats_workable, "smartrecruiters": ats_smartrecruiters,
    "recruitee": ats_recruitee, "bamboohr": ats_bamboohr, "breezy": ats_breezy,
    "personio": ats_personio, "workday": ats_workday, "oracle_orc": ats_oracle_orc,
    # detected but no public API → fall through to generic/AI/browser:
    # taleo, successfactors, icims, jobvite, teamtailor, jazzhr, zoho
}

# ============================================================================
# Stage 4/5/6 — Generic extraction (JSON-LD, embedded JSON, heuristic links)
# ============================================================================

def extract_jsonld(html: str, page_url: str, company: str) -> list[Job]:
    jobs = []
    for script in soup_of(html).find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, TypeError):
            continue
        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, list):
                stack.extend(node)
            elif isinstance(node, dict):
                if node.get("@type") in ("JobPosting", ["JobPosting"]):
                    loc = node.get("jobLocation") or {}
                    if isinstance(loc, list):
                        loc = loc[0] if loc else {}
                    addr = (loc.get("address") or {}) if isinstance(loc, dict) else {}
                    locality = ", ".join(filter(None, [
                        addr.get("addressLocality"), addr.get("addressCountry")
                    ])) if isinstance(addr, dict) else ""
                    jobs.append(Job(
                        company, str(node.get("title", "")).strip(),
                        urljoin(page_url, node.get("url", "") or page_url),
                        locality,
                        employment_type=str(node.get("employmentType", "")),
                        posted_date=str(node.get("datePosted", ""))[:10],
                        description=clean_desc(node.get("description", "")),
                        source_method="jsonld"))
                else:
                    stack.extend(node.values())
    return jobs


def extract_embedded_json(html: str, page_url: str, company: str) -> list[Job]:
    """Mine __NEXT_DATA__-style blobs for arrays of job-like objects."""
    jobs = []
    for m in re.finditer(
            r'<script[^>]*(?:id="__NEXT_DATA__"|__INITIAL_STATE__\s*=|window\.__NUXT__\s*=)[^>]*>(.*?)</script>',
            html, re.S):
        blob = m.group(1)
        blob = re.sub(r"^[^{\[]*", "", blob).rsplit("}", 1)[0] + "}"
        try:
            data = json.loads(blob)
        except json.JSONDecodeError:
            continue
        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                keys = {k.lower() for k in node}
                if {"title"} <= keys and keys & {"location", "locations", "department", "applyurl", "url", "slug"}:
                    title = str(node.get("title", "")).strip()
                    if title and len(title) < 120:
                        url = node.get("applyUrl") or node.get("url") or node.get("slug") or ""
                        d = node.get("description") or node.get("jobDescription") or node.get("summary") or node.get("content") or ""
                        jobs.append(Job(company, title, urljoin(page_url, str(url)),
                                        str(node.get("location") or node.get("locations") or ""),
                                        str(node.get("department", "")),
                                        description=clean_desc(d) if isinstance(d, str) else "",
                                        source_method="embedded_json"))
                stack.extend(node.values())
            elif isinstance(node, list):
                stack.extend(node)
    return jobs


def extract_heuristic_links(html: str, page_url: str, company: str) -> list[Job]:
    jobs, seen = [], set()
    for a in soup_of(html).find_all("a", href=True):
        href = urljoin(page_url, a["href"].split("#")[0])
        title = " ".join(a.get_text(" ", strip=True).split())
        if not title or len(title) > 110 or NOISE_TITLES.match(title):
            continue
        if not JOB_URL_HINT.search(href):
            continue
        if urlparse(href).path in ("", "/") or href.rstrip("/") == page_url.rstrip("/"):
            continue
        key = (title.lower(), href)
        if key in seen:
            continue
        seen.add(key)
        jobs.append(Job(company, title, href, source_method="heuristic"))
    return jobs

# ============================================================================
# Stage 7 — AI extraction (OpenAI or Anthropic, auto-detected from env keys)
# ============================================================================

class AIExtractor:
    """
    provider: "auto" | "openai" | "anthropic" | "off"
    auto → uses OPENAI_API_KEY if set, else ANTHROPIC_API_KEY, else disabled.
    """

    def __init__(self, provider: str = "auto", model: str = ""):
        self.provider = ""
        self.client = None
        self.model = model
        self.sem = asyncio.Semaphore(AI_CONCURRENCY)   # throttle AI calls globally
        if provider == "off":
            return
        if provider in ("auto", "openai") and os.environ.get("OPENAI_API_KEY"):
            try:
                from openai import AsyncOpenAI
                self.client = AsyncOpenAI(timeout=AI_TIMEOUT, max_retries=2)
                self.provider = "openai"
            except ImportError:
                pass
        if not self.client and provider in ("auto", "anthropic") \
                and os.environ.get("ANTHROPIC_API_KEY"):
            try:
                from anthropic import AsyncAnthropic
                self.client = AsyncAnthropic(timeout=AI_TIMEOUT, max_retries=2)
                self.provider = "anthropic"
            except ImportError:
                pass
        if self.provider:
            self.model = model or AI_DEFAULT_MODELS[self.provider]

    @property
    def enabled(self):
        return self.client is not None

    async def _complete(self, system: str, prompt: str) -> str:
        async with self.sem:                 # cap concurrent AI calls — avoids rate-limit stalls
            if self.provider == "openai":
                kwargs = dict(
                    model=self.model,
                    messages=[{"role": "system", "content": system},
                              {"role": "user", "content": prompt}],
                    max_completion_tokens=4000,
                )
                try:  # JSON mode — supported on gpt-4o/4.1/5 chat families
                    resp = await self.client.chat.completions.create(
                        response_format={"type": "json_object"}, **kwargs)
                except Exception:
                    resp = await self.client.chat.completions.create(**kwargs)
                return resp.choices[0].message.content or ""
            # anthropic
            resp = await self.client.messages.create(
                model=self.model, max_tokens=4000, system=system,
                messages=[{"role": "user", "content": prompt}],
            )
            return "".join(b.text for b in resp.content if b.type == "text")

    async def extract(self, html: str, page_url: str, company: str) -> tuple[list[Job], bool]:
        """Returns (jobs, needs_javascript)."""
        if not self.client:
            return [], False
        soup = soup_of(html)
        for t in soup(["script", "style", "noscript", "svg"]):
            t.decompose()
        text = re.sub(r"\n{2,}", "\n", soup.get_text("\n", strip=True))[:AI_TEXT_BUDGET]
        links = []
        for a in soup.find_all("a", href=True)[:150]:
            lt = " ".join(a.get_text(" ", strip=True).split())[:90]
            if lt:
                links.append(f"{lt} -> {urljoin(page_url, a['href'])}")
        links_block = "\n".join(links[:120])

        prompt = (
            f"Below is the visible text and links of a company career page "
            f"({page_url}) for the company \"{company}\".\n\n"
            f"=== PAGE TEXT ===\n{text}\n\n=== LINKS ===\n{links_block}\n\n"
            "Extract every individual job opening listed. Respond ONLY with JSON:\n"
            '{"jobs":[{"title":"","location":"","department":"",'
            '"employment_type":"","url":"","posted_date":"","description":""}],'
            '"needs_javascript": false}\n'
            "Rules: needs_javascript=true if the page clearly loads jobs via "
            "scripts and none are visible. Use absolute URLs from the links "
            "list when possible. For description, include any role summary, "
            "responsibilities, or requirements visible for that job (empty if "
            "none shown). Do not invent jobs. Empty list is valid."
        )
        try:
            raw = await self._complete(
                "You extract job postings from web pages into strict JSON. No markdown.",
                prompt)
            data = json.loads(re.sub(r"```(json)?", "", raw).strip())
        except Exception:
            return [], False
        jobs = []
        for j in data.get("jobs", []):
            title = str(j.get("title", "")).strip()
            if title:
                jobs.append(Job(company, title,
                                urljoin(page_url, str(j.get("url", "") or page_url)),
                                str(j.get("location", "")), str(j.get("department", "")),
                                str(j.get("employment_type", "")),
                                str(j.get("posted_date", ""))[:10],
                                description=clean_desc(str(j.get("description", ""))),
                                source_method=f"ai:{self.provider}"))
        return jobs, bool(data.get("needs_javascript"))

# ============================================================================
# Stage 8 — Browser fallback (optional)
# ============================================================================

class Browser:
    def __init__(self, enabled: bool):
        self.enabled = enabled
        self._pw = self._browser = None

    async def start(self):
        if not self.enabled:
            return
        try:
            from playwright.async_api import async_playwright
            self._pw = await async_playwright().start()
            self._browser = await self._pw.chromium.launch(headless=True)
        except Exception as e:
            print(f"[browser] disabled ({e})", file=sys.stderr)
            self.enabled = False

    async def render(self, url: str) -> str:
        if not self.enabled or not self._browser:
            return ""
        page = await self._browser.new_page(user_agent=UA)
        try:
            await page.goto(url, timeout=30_000, wait_until="networkidle")
            await page.wait_for_timeout(1500)
            return await page.content()
        except Exception:
            return ""
        finally:
            await page.close()

    async def stop(self):
        if self._browser:
            await self._browser.close()
        if self._pw:
            await self._pw.stop()

# ============================================================================
# Orchestrator — the ladder, per company
# ============================================================================

def dedupe(jobs: list[Job]) -> list[Job]:
    seen, out = set(), []
    for j in jobs:
        key = (j.title.lower().strip(), j.url or j.location.lower())
        if j.title and key not in seen:
            seen.add(key)
            out.append(j)
    return out


async def enrich_descriptions(client, jobs: list[Job], cap: int = 60):
    """Opt-in (--details): fetch each job's own page to fill a missing
    description. Bounded to `cap` jobs/company; works on static job pages
    (JS-rendered boards yield little without --browser)."""
    targets = [j for j in jobs if not j.description and is_fetchable(j.url)][:cap]

    async def one(j: Job):
        _, html = await get_text(client, j.url)
        if not html:
            return
        soup = soup_of(html)
        for t in soup(["script", "style", "noscript", "nav", "header", "footer", "svg", "form"]):
            t.decompose()
        main = soup.find("main") or soup.find(attrs={"role": "main"}) or soup.body or soup
        j.description = clean_desc(main.get_text(" "))

    await asyncio.gather(*(one(j) for j in targets))


async def process_company(client, ai: AIExtractor, browser: Browser,
                          company: str, website: str,
                          fetch_details: bool = False,
                          proxy_client=None) -> tuple[list[Job], Report]:
    t0 = time.time()
    rep = Report(company=company, website=website)
    base = normalize_site(website)
    if not base:
        rep.error = "no website"
        return [], rep

    try:
        career_pages, home_html, base, status = await find_career_pages(client, base)
        if not career_pages and not home_html:
            # Direct failed — retry via proxy for ALL failure modes (DNS, timeout, 4xx)
            if proxy_client is not None:
                career_pages, home_html, base, status = await find_career_pages(proxy_client, base)
                if career_pages or home_html:
                    client = proxy_client   # proxy worked — use for rest of pipeline
            if not career_pages and not home_html:
                if status == -1:
                    rep.error = "Site unreachable (DNS check failed — may be geo-blocked or slow resolver)"
                elif status >= 400:
                    rep.error = f"blocked (HTTP {status})"
                else:
                    rep.error = "connection failed (timeout / geo-blocked?)"
                rep.seconds = round(time.time() - t0, 1)
                return [], rep
        rep.career_page = career_pages[0] if career_pages else base
        pages_to_try = career_pages or [base]

        all_jobs: list[Job] = []
        for page_url in pages_to_try:
            # ATS in the career URL itself?
            ats, groups = detect_ats(page_url)
            page_html = ""
            if not ats or ats not in ATS_ADAPTERS:
                final, page_html = await get_text(client, page_url)
                page_url = final or page_url
                if page_html:
                    ats2, groups2 = detect_ats("", page_html)   # embedded ATS?
                    if ats2:
                        ats, groups = ats2, groups2

            if ats:
                rep.ats_detected = ats
            if ats in ATS_ADAPTERS:
                jobs = await ATS_ADAPTERS[ats](client, company, groups,
                                               source_url=page_url)
                if jobs:
                    rep.method = f"ats:{ats}"
                    all_jobs.extend(jobs)
                    break   # ATS API gives the complete list — done

            if not page_html:
                continue

            # structured data ladder
            jobs = extract_jsonld(page_html, page_url, company)
            if jobs:
                rep.method = rep.method or "jsonld"
            else:
                jobs = extract_embedded_json(page_html, page_url, company)
                if jobs:
                    rep.method = rep.method or "embedded_json"
            if not jobs:
                heur = extract_heuristic_links(page_html, page_url, company)
                # heuristics are noisy — only trust a plausible amount
                if 0 < len(heur) <= 150:
                    jobs = heur
                    rep.method = rep.method or "heuristic"

            # AI pass if still nothing (or to validate a noisy heuristic set)
            needs_js = False
            if not jobs and ai.enabled:
                jobs, needs_js = await ai.extract(page_html, page_url, company)
                if jobs:
                    rep.method = rep.method or "ai"
            rep.needs_javascript = rep.needs_javascript or needs_js

            # browser pass for JS-rendered boards
            if not jobs and browser.enabled:
                rendered = await browser.render(page_url)
                if rendered:
                    jobs = (extract_jsonld(rendered, page_url, company)
                            or extract_embedded_json(rendered, page_url, company)
                            or extract_heuristic_links(rendered, page_url, company))
                    if not jobs and ai.enabled:
                        jobs, _ = await ai.extract(rendered, page_url, company)
                    if jobs:
                        rep.method = rep.method or "browser"
            all_jobs.extend(jobs)

        all_jobs = dedupe(all_jobs)
        if fetch_details:
            await enrich_descriptions(client, all_jobs)
        for j in all_jobs:
            j.career_page = rep.career_page
            j.ats = j.ats or rep.ats_detected
        rep.jobs_found = len(all_jobs)
        return all_jobs, rep
    except Exception as e:                          # never kill the run
        name = type(e).__name__
        msg = str(e).strip()
        # Map low-level network exceptions to readable messages
        if name in ("RemoteProtocolError", "EndOfStream", "ConnectError",
                    "ReadError", "WriteError", "ProtocolError"):
            rep.error = f"connection failed ({name})"
        else:
            rep.error = f"{name}: {msg}"[:200]
        return [], rep
    finally:
        rep.seconds = round(time.time() - t0, 1)

# ============================================================================
# Runner — checkpointed, concurrent
# ============================================================================

class Sink:
    """Crash-safe appenders + resume state."""

    def __init__(self, out_dir: str):
        os.makedirs(out_dir, exist_ok=True)
        self.dir = out_dir
        self.state_path = os.path.join(out_dir, "state.json")
        self.jobs_path = os.path.join(out_dir, "jobs.jsonl")
        self.report_path = os.path.join(out_dir, "report.csv")
        self.done: set[str] = set()
        if os.path.exists(self.state_path):
            self.done = set(json.load(open(self.state_path)).get("done", []))
        self._report_header = os.path.exists(self.report_path)
        self.lock = asyncio.Lock()

    async def write(self, website: str, jobs: list[Job], rep: Report):
        async with self.lock:
            with open(self.jobs_path, "a", encoding="utf-8") as f:
                for j in jobs:
                    f.write(json.dumps(asdict(j), ensure_ascii=False) + "\n")
            with open(self.report_path, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=list(asdict(rep).keys()))
                if not self._report_header:
                    w.writeheader()
                    self._report_header = True
                w.writerow(asdict(rep))
            self.done.add(website)
            json.dump({"done": sorted(self.done)}, open(self.state_path, "w"))

    def finalize(self):
        if not os.path.exists(self.jobs_path):
            return 0
        rows = [json.loads(l) for l in open(self.jobs_path, encoding="utf-8")]
        df = pd.DataFrame(rows).drop_duplicates(subset=["company", "title", "url"])
        df.to_csv(os.path.join(self.dir, "jobs.csv"), index=False)
        return len(df)


async def run(args):
    # Dedicated DNS pool — abandoned slow lookups can't starve real work.
    _dns_executor(max(64, args.concurrency * 3))
    df = pd.read_csv(args.csv)
    col_site = next(c for c in df.columns if "website" in c.lower())
    col_name = next(c for c in df.columns if "company name" in c.lower())
    rows = df[[col_name, col_site]].dropna().drop_duplicates(subset=[col_site])
    if args.limit:
        rows = rows.head(args.limit)

    sink = Sink(args.out)
    todo = [(r[col_name], r[col_site]) for _, r in rows.iterrows()
            if r[col_site] not in sink.done]
    print(f"{len(rows)} companies | {len(rows) - len(todo)} already done | "
          f"{len(todo)} to crawl | concurrency={args.concurrency}")

    ai = AIExtractor(args.ai_provider, args.ai_model)
    print(f"AI extraction: {'ON — ' + ai.provider + ' / ' + ai.model if ai.enabled else 'OFF (set OPENAI_API_KEY or ANTHROPIC_API_KEY to enable)'}")
    browser = Browser(args.browser)
    await browser.start()
    print(f"Browser fallback: {'ON' if browser.enabled else 'OFF'}")

    # Proxy: --proxy flag wins, then env vars (standard convention).
    # Strategy: try direct first; if unreachable, retry through the proxy.
    proxy_url = (args.proxy or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or "").strip()
    print(f"Proxy fallback: {proxy_url or 'none'}")

    sem = asyncio.Semaphore(args.concurrency)
    counter = {"i": 0, "jobs": 0}

    # HTTP/2 support — negotiated via ALPN; fixes 0s failures on sites that reject HTTP/1.1
    import importlib.util
    _h2 = importlib.util.find_spec("h2") is not None
    if not _h2:
        print("tip: pip install h2  — enables HTTP/2, fixes instant connection failures")

    client_kwargs = dict(timeout=TIMEOUT, follow_redirects=True, http2=_h2,
                         limits=httpx.Limits(max_connections=args.concurrency * 2))

    # Open both clients upfront and close manually — nested async-with closes them
    # before all workers finish when a timeout cancels a task.
    client = httpx.AsyncClient(**client_kwargs)
    proxy_client = httpx.AsyncClient(**client_kwargs, proxy=proxy_url) if proxy_url else None
    try:
        async def worker(company, website):
            async with sem:
                try:
                    # asyncio.shield prevents timeout cancellation from corrupting
                    # the shared httpx client's connection pool
                    jobs, rep = await asyncio.wait_for(
                        asyncio.shield(
                            process_company(client, ai, browser, company, website,
                                            fetch_details=args.details,
                                            proxy_client=proxy_client)),
                        timeout=COMPANY_TIMEOUT)
                except asyncio.TimeoutError:
                    jobs, rep = [], Report(company=company, website=website,
                                           error=f"timeout (>{COMPANY_TIMEOUT}s, skipped)")
                await sink.write(website, jobs, rep)
                counter["i"] += 1
                counter["jobs"] += len(jobs)
                status = rep.method or rep.error or "no jobs found"
                print(f"[{counter['i']}/{len(todo)}] {company[:38]:<38} "
                      f"{len(jobs):>4} jobs  {status}")

        await asyncio.gather(*(worker(c, w) for c, w in todo))
    finally:
        await client.aclose()
        if proxy_client:
            await proxy_client.aclose()

    await browser.stop()
    total = sink.finalize()
    print(f"\nDone. {counter['jobs']} jobs this run, {total} unique jobs total."
          f"\n  {args.out}/jobs.csv\n  {args.out}/report.csv")


def load_env():
    """Load KEY=VALUE lines from a .env file into os.environ (zero-dependency).
    Looks in the working dir, then next to this script. Does NOT override a var
    already set in the environment, so an explicit `export` still wins."""
    candidates = [".env", os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")]
    for path in candidates:
        if not os.path.isfile(path):
            continue
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                if line.startswith("export "):
                    line = line[len("export "):]
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
        break        # first .env found wins


def main():
    load_env()
    p = argparse.ArgumentParser(description="Super Scraper — career site job crawler")
    p.add_argument("--csv", required=True, help="Leads CSV with Company Name + Website columns")
    p.add_argument("--out", default="out", help="output directory")
    p.add_argument("--limit", type=int, default=0, help="only first N companies (trial run)")
    p.add_argument("--concurrency", type=int, default=10)
    p.add_argument("--browser", action="store_true",
                   help="enable Playwright rendering for JS-heavy sites")
    p.add_argument("--details", action="store_true",
                   help="fetch each job's page to fill descriptions the ATS API omits (slower)")
    p.add_argument("--ai-provider", choices=["auto", "openai", "anthropic", "off"],
                   default="auto", help="AI extraction backend (default: auto-detect from env keys)")
    p.add_argument("--ai-model", default="",
                   help="override model, e.g. gpt-5-mini, gpt-4.1, claude-sonnet-4-20250514")
    p.add_argument("--proxy", default="",
                   help="HTTP/SOCKS proxy for all requests, e.g. http://user:pass@host:port "
                        "or socks5://host:port. Also read from HTTPS_PROXY / HTTP_PROXY env vars.")
    args = p.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
