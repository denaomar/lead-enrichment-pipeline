"""
Lead Enrichment Pipeline — ACQ Vantage Trial Project
=====================================================
Author: Dena Omar
Description:
    Enriches a list of company websites with structured CRM-ready data.
    For each URL, the pipeline:
      1. Normalises the URL (handles casing, missing schemes, trailing slashes)
      2. Scrapes the page content via Firecrawl (clean markdown output)
      3. Extracts structured fields via OpenAI GPT-4o with a strict taxonomy prompt
      4. Validates the output against allowed values
      5. Writes results to a timestamped CSV ready for CRM import

"""

import os
import re
import json
import time
import logging
import pandas as pd
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
from firecrawl import Firecrawl
from openai import OpenAI
from urllib.parse import urlparse, urlunparse
import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────
# 0. CONFIGURATION
# ─────────────────────────────────────────────

load_dotenv()

FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY")
OPENAI_API_KEY    = os.getenv("OPENAI_API_KEY")

# Wait time between API calls — keeps us within rate limits
# and avoids hammering sites. 1 second is safe.
DELAY_BETWEEN_REQUESTS = 1.0

# Max characters of scraped content to send to the LLM.
# Most homepage content is well under 8k chars. This keeps token cost low
# while giving the model enough context to make confident decisions.
MAX_CONTENT_CHARS = 8000

# Minimum useful content length to consider a scrape successful.
MIN_CONTENT_CHARS = 50

# Lightweight HTML fallback request settings.
REQUEST_TIMEOUT_SECONDS = 15

# Common desktop browser user-agent for the HTML fallback.
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}

# OpenAI model — gpt-4o-mini is fast, cheap, and more than capable here.
# Swap to "gpt-4o" at a later phase if we want higher accuracy on ambiguous sites.
OPENAI_MODEL = "gpt-4o-mini"

# Output file path
TIMESTAMP   = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_PATH = f"enriched_output_{TIMESTAMP}.csv"
LOG_PATH    = f"enrichment_log_{TIMESTAMP}.log"

# ─────────────────────────────────────────────
# 1. TAXONOMY
# ─────────────────────────────────────────────
# These are the ONLY valid values for business_type and industry.
# Fixed taxonomy = consistent, filterable CRM data. This is what makes
# the output useful for cohort analysis.

BUSINESS_TYPES = [
    "Agency",
    "Coaching / Consulting",
    "SaaS / Software",
    "E-commerce",
    "Professional Services",
    "Education / Course",
    "Local / Trade Services",
    "Media / Content",
]

INDUSTRIES = [
    "Real Estate",
    "Health & Wellness",
    "Legal",
    "Finance & Wealth",
    "Home Services",
    "Marketing & Lead Gen",
    "Food & Hospitality",
    "Fitness",
    "Technology",
    "Education",
    "E-commerce / Retail",
    "Business Operations",
]

CONFIDENCE_LEVELS = ["High", "Medium", "Low"]

# ─────────────────────────────────────────────
# 2. LOGGING SETUP
# ─────────────────────────────────────────────
# Logs to both terminal and file so we have a full audit trail.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_PATH),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 3. URL NORMALISER
# ─────────────────────────────────────────────

def normalise_url(raw: str) -> str:
    """
    Converts any messy URL variant into a clean, fetchable https:// URL.

    Handles all patterns found in this dataset:
      "Www.example.com"                        → "https://www.example.com"
      "www.example.com"                        → "https://www.example.com"
      "example.com"                            → "https://example.com"
      "http://example.com"                     → "https://example.com"
      "https://example.com"                    → "https://example.com"
      "https://www.example.com"                → "https://www.example.com"
      "https://www.example.com/path/to/page"   → "https://www.example.com/path/to/page"

    Logic — strip first, then prepend:
      Rather than conditionally detecting every prefix variant, we always
      strip whatever scheme exists (or doesn't), then always prepend https://.
      This is idempotent: running it twice gives the same result.

    Note: domains are case-insensitive by spec, so full lowercasing is safe.
    All paths in this dataset are already lowercase so no info is lost.
    """
    url = raw.strip().lower()

    # Strip any existing scheme: handles https://, http://, and bare //
    url = re.sub(r'^https?://', '', url)
    url = re.sub(r'^//',        '', url)

    # Strip trailing slash for consistency
    # Mid-path slashes (e.g. /gretna, /home) are preserved — those are
    # intentional landing pages that contain more relevant business info
    url = url.rstrip('/')

    # Always prepend clean https://
    url = 'https://' + url

    return url


def test_normalise_url():
    """
    Quick sanity-check — run this before the full pipeline to verify
    all URL patterns in the dataset are handled correctly.
    Call it manually: python -c "from enrichment_pipeline import test_normalise_url; test_normalise_url()"
    """
    cases = [
        # (input,                                          expected_output)
        ("Www.resultsdrivenrei.com",                      "https://www.resultsdrivenrei.com"),
        ("www.brooklynpastalab.com",                      "https://www.brooklynpastalab.com"),
        ("n2o.com",                                       "https://n2o.com"),
        ("Onpointchiro.com",                              "https://onpointchiro.com"),
        ("http://jpcannonlawfirm.com",                    "https://jpcannonlawfirm.com"),
        ("https://drshaunna.com",                         "https://drshaunna.com"),
        ("https://www.augustalawncareservices.com/gretna","https://www.augustalawncareservices.com/gretna"),
        ("https://connect.profitablecoach.net/home",      "https://connect.profitablecoach.net/home"),
        ("https://globaltize.com/",                       "https://globaltize.com"),
        ("CommunityLaunch.com",                           "https://communitylaunch.com"),
        ("Www.donstree.com",                              "https://www.donstree.com"),
        ("Www.swimtechalbury.com",                        "https://www.swimtechalbury.com"),
    ]

    print("\n── URL Normaliser Test ──────────────────────────────────────")
    all_passed = True
    for raw, expected in cases:
        result  = normalise_url(raw)
        passed  = result == expected
        status  = "PASS" if passed else "FAIL"
        if not passed:
            all_passed = False
        print(f"  [{status}]  {raw!r:50s} → {result!r}")
        if not passed:
            print(f"           Expected: {expected!r}")

    print("─────────────────────────────────────────────────────────────")
    print(f"  Result: {'ALL PASSED' if all_passed else 'SOME TESTS FAILED'}\n")
    return all_passed

# ─────────────────────────────────────────────
# 4. FIRECRAWL SCRAPER
# ─────────────────────────────────────────────

# Fallback paths to try when the homepage returns empty content.
# Many business sites have richer info on /about or /services than the homepage.
FALLBACK_PATHS = ["/about", "/about-us", "/services", "/what-we-do"]

def _content_is_useful(text: str) -> bool:
    """Returns True if text is long enough to be worth sending to the LLM."""
    return bool(text and len(text.strip()) >= MIN_CONTENT_CHARS)
 
 
def _root_url(url: str) -> str:
    """
    Returns scheme + netloc only, stripping path/query/fragment.
    Using urlparse is safer than split('/') which breaks on ports.
    Example: https://example.com/landing?ref=1 → https://example.com
    """
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))
 
 
def _with_www_variant(url: str) -> Optional[str]:
    """
    Returns https://www.<domain> if no www. prefix exists, else None.
    Fixes the common DNS misconfiguration where the bare domain has a
    broken SSL cert but www. works fine.
    """
    parsed = urlparse(url)
    if not parsed.netloc or parsed.netloc.startswith("www."):
        return None
    return urlunparse(("https", f"www.{parsed.netloc}",
                       parsed.path, parsed.params, parsed.query, parsed.fragment))

def _html_to_text(html: str) -> str:
    """
    Strips HTML tags and noise elements, returning clean visible text.
    Removes script/style/svg/iframe so JS and CSS don't pollute the output.
    """
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "iframe"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()

def _attempt_requests_fallback(url: str) -> dict:
    """
    Stage 4: direct HTTP GET with a browser User-Agent.
    Bypasses Firecrawl entirely — often succeeds where its engines are blocked.
    verify=False handles broken SSL certs at the requests level.
    """
    try:
        response = requests.get(
            url, headers=DEFAULT_HEADERS,
            timeout=REQUEST_TIMEOUT_SECONDS,
            allow_redirects=True, verify=False,
        )
        if response.status_code in {401, 403}:
            return {"content": "", "status": "blocked",
                    "error": f"Requests blocked: HTTP {response.status_code}"}
        response.raise_for_status()
        text = _html_to_text(response.text)
        if not _content_is_useful(text):
            return {"content": "", "status": "empty",
                    "error": "Requests fallback returned near-empty text"}
        return {"content": text[:MAX_CONTENT_CHARS], "status": "success", "error": ""}
    except requests.exceptions.SSLError as e:
        return {"content": "", "status": "ssl_error", "error": f"Requests SSL error: {e}"}
    except requests.exceptions.HTTPError as e:
        code = getattr(e.response, "status_code", None)
        return {"content": "", "status": "blocked" if code in {401, 403} else "failed",
                "error": f"Requests HTTP error: {e}"}
    except requests.exceptions.RequestException as e:
        return {"content": "", "status": "failed", "error": f"Requests error: {e}"}

def _attempt_scrape(app: Firecrawl, url: str) -> dict:
    """
    Single Firecrawl scrape attempt. Returns "empty" (not "blocked") for
    thin content — this is intentional so Stage 3 subpage fallbacks trigger.
    "blocked" means the site actively rejected the request.
 
    Firecrawl handles:
      - JavaScript-rendered pages (unlike simple requests)
      - Bot detection bypass (within limits)
    """
    try:
        result = app.scrape(
            url,
            formats=["markdown"],
        )
 
        # Firecrawl returns a dict — extract markdown content
        content = ""
        if isinstance(result, dict):
            content = result.get("markdown", "") or result.get("content", "") or ""
        elif hasattr(result, "markdown"):
            content = result.markdown or ""
 
        content = content.strip()
        if not _content_is_useful(content):
            return {"content": "", "status": "empty",
                    "error": "Empty or near-empty content returned"}
        return {"content": content[:MAX_CONTENT_CHARS], "status": "success", "error": ""}
 
    except Exception as e:
        error_msg = str(e)
        error_lower = error_msg.lower()

        if any(k in error_lower for k in ["ssl", "tls", "certificate"]):
            status = "ssl_error"
        elif any(k in error_lower for k in ["403", "blocked", "captcha", "forbidden"]):
            status = "blocked"
        else:
            status = "failed"  # "engines failed" → failed keeps recovery paths open

        return {
            "content": "",
            "status": status,
            "error": error_msg,
        }

def scrape_website(app: Firecrawl, url: str) -> dict:
    """
    4-stage retry strategy. Each stage runs only if all previous ones failed.
 
    Stage 1 — Direct Firecrawl scrape (https://)
    Stage 2 — SSL recovery:
               2a. https://www.<domain>  (DNS misconfiguration fix)
               2b. http://               (broken cert, plain HTTP works)
    Stage 3 — Subpage fallbacks for empty homepages (/about, /services…)
    Stage 4 — Direct requests GET with browser User-Agent (bypasses Firecrawl)
 
    scrape_note records which stage succeeded for the audit trail.
    """
 
    # ── Stage 1: Normal attempt ──────────────────────────────────────
    log.info(f"        Stage 1: scraping {url}")
    result = _attempt_scrape(app, url)
 
    if result["status"] == "success":
        result["scrape_note"] = "stage1_direct"
        result["resolved_url"] = url
        return result
 
    # ── Stage 2: SSL recovery
    if result["status"] == "ssl_error":
        www_url = _with_www_variant(url)
        if www_url:
            log.warning(f"        Stage 2a: www variant {www_url}")
            r2a = _attempt_scrape(app, www_url)
            if r2a["status"] == "success":
                r2a["scrape_note"] = "stage2a_www_fallback"
                r2a["resolved_url"] = www_url
                return r2a
            log.warning(f"        Stage 2a failed ({r2a['status']}): {r2a['error'][:100]}")

        http_url = url.replace("https://", "http://", 1)
        log.warning(f"        Stage 2b: http fallback {http_url}")
        r2b = _attempt_scrape(app, http_url)
 
        if r2b["status"] == "success":
            r2b["scrape_note"] = "stage2b_http_fallback"
            r2b["resolved_url"] = http_url
            return r2b
        log.warning(f"        Stage 2b failed ({r2b['status']}): {r2b['error'][:100]}")
        result = r2b  # carry forward latest error
 
    # ── Stage 3: subpage fallbacks for empty homepages
    if result["status"] == "empty":
        base = _root_url(url)
 
        for path in FALLBACK_PATHS:
            fallback_url = base + path
            log.info(f"        Stage 3: trying fallback path {fallback_url}")
            r3 = _attempt_scrape(app, fallback_url)
 
            if r3["status"] == "success":
                r3["scrape_note"] = f"stage3_fallback_{path.strip('/')}"
                r3["resolved_url"] = fallback_url
                log.info(f"        Stage 3 success on {fallback_url}")
                return r3
 
            time.sleep(0.5)  # small pause between subpage attempts
 
        # All fallback paths also empty/failed
        log.warning(f"        Stage 3 exhausted for {url}")
        result["error"] = "Homepage and all subpages returned empty content"
 
    # Stage 4: direct requests fallback
    log.warning(f"        Stage 4: requests fallback {url}")
    r4 = _attempt_requests_fallback(url)
    if r4["status"] == "success":
        r4["scrape_note"] = "stage4_requests_fallback"
        r4["resolved_url"] = url
        log.info(f"        Stage 4 success: {url}")
        return r4
    log.warning(f"        Stage 4 failed ({r4['status']}): {r4['error'][:100]}")
 
    result["scrape_note"] = "all_stages_failed"
    result["resolved_url"] = url
    return result

# ─────────────────────────────────────────────
# 5. LLM EXTRACTION PROMPT
# ─────────────────────────────────────────────

def build_prompt(url: str, content: str, scrape_status: str) -> str:
    """
    Builds the extraction prompt for the LLM.

    Two modes:
      - If we have scraped content: give the LLM the full page text
      - If scrape failed: ask LLM to infer from the URL alone (url_only mode)
        This is a graceful fallback — better than a blank row in the CRM.

    The prompt enforces:
      - Strict taxonomy (only valid values accepted)
      - Consistent summary format
      - Self-assessed confidence score
      - Always English output regardless of source language
    """

    taxonomy_block = f"""
VALID business_type values (pick exactly one):
{json.dumps(BUSINESS_TYPES, indent=2)}

VALID industry values (pick exactly one):
{json.dumps(INDUSTRIES, indent=2)}

VALID confidence_score values (pick exactly one):
{json.dumps(CONFIDENCE_LEVELS, indent=2)}
"""

    if scrape_status == "success" and content:
        source_block = f"""
WEBSITE URL: {url}

SCRAPED CONTENT (may be in any language — always respond in English):
---
{content}
---
"""
        data_source = "scraped"
    else:
        source_block = f"""
WEBSITE URL: {url}

NOTE: The website could not be scraped. Infer what you can from the URL alone.
Set confidence_score to "Low" unless the URL makes the business very obvious.
"""
        data_source = "url_only"

    prompt = f"""You are a business analyst enriching CRM data for a network of entrepreneurial companies.

Given the website information below, extract structured company data.

{source_block}

{taxonomy_block}

Instructions:
1. company_name: The brand name as it appears on the site. If unavailable, derive from the domain.
2. business_type: Choose the BEST match from the valid values above. No other values allowed.
3. industry: Choose the BEST match from the valid values above. No other values allowed.
4. company_summary: ONE sentence max 25 words. Format: "[Company] helps [audience] [achieve outcome] through [mechanism]."
5. confidence_score: How confident are you in this classification given the available information?
   - High: clear website with obvious business model
   - Medium: some content but ambiguous or limited info
   - Low: scrape failed, or very little useful content
6. data_source: Use "{data_source}" exactly.

Return ONLY a valid JSON object. No markdown, no explanation, no extra text.

Example of correct output:
{{
  "company_name": "Acme Agency",
  "business_type": "Agency",
  "industry": "Marketing & Lead Gen",
  "company_summary": "Acme Agency helps e-commerce brands grow revenue through paid social campaigns.",
  "confidence_score": "High",
  "data_source": "{data_source}"
}}
"""
    return prompt

# ─────────────────────────────────────────────
# 6. LLM EXTRACTOR
# ─────────────────────────────────────────────

def extract_with_llm(client: OpenAI, prompt: str) -> dict:
    """
    Calls OpenAI and parses the structured JSON response.

    Uses response_format=json_object to force valid JSON output —
    this eliminates the need for fragile regex parsing.

    Returns the parsed dict, or an error dict if something goes wrong.
    """
    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a precise data extraction assistant. "
                        "You always return valid JSON and nothing else. "
                        "You never deviate from the taxonomy provided."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},  # Forces valid JSON output
            temperature=0.1,  # Low temperature = consistent, deterministic output
            max_tokens=300,   # Fields are short — 300 tokens is more than enough
        )

        raw = response.choices[0].message.content
        return json.loads(raw)

    except json.JSONDecodeError as e:
        return {"_error": f"JSON parse failed: {e}"}
    except Exception as e:
        return {"_error": f"LLM call failed: {e}"}

# ─────────────────────────────────────────────
# 7. OUTPUT VALIDATOR
# ─────────────────────────────────────────────

def validate_and_clean(raw: dict, original_url: str, normalised_url: str,
                        scrape_status: str, scrape_error: str,
                        #scrape_note: str = "",
                        resolved_url: str = "") -> dict:
    """
    Validates LLM output against our taxonomy and builds the final row dict.

    If the LLM returns a value outside the taxonomy (rare but possible),
    we flag it as "Unknown" rather than silently writing bad data to the CRM.
    This keeps the output trustworthy.
    """
    # Check for upstream LLM errors
    if "_error" in raw:
        return _error_row(original_url, normalised_url, scrape_status,
                          scrape_error, raw["_error"])

    # Validate taxonomy fields — coerce invalid values to "Unknown"
    business_type = raw.get("business_type", "Unknown")
    if business_type not in BUSINESS_TYPES:
        log.warning(f"Invalid business_type '{business_type}' — setting to Unknown")
        business_type = "Unknown"

    industry = raw.get("industry", "Unknown")
    if industry not in INDUSTRIES:
        log.warning(f"Invalid industry '{industry}' — setting to Unknown")
        industry = "Unknown"

    confidence = raw.get("confidence_score", "Low")
    if confidence not in CONFIDENCE_LEVELS:
        confidence = "Low"

    # Clean summary — strip extra whitespace, ensure it ends with a period
    summary = raw.get("company_summary", "").strip()
    if summary and not summary.endswith("."):
        summary += "."

    return {
        "company_name":     raw.get("company_name", "Unknown").strip(),
        "business_type":    business_type,
        "industry":         industry,
        "company_summary":  summary,
        "confidence_score": confidence,
        "data_source":      raw.get("data_source", "unknown"),
        "original_url":     original_url,
        "normalised_url":   normalised_url,
        "resolved_url":     resolved_url or normalised_url,
        "scrape_status":    scrape_status,
        "error_notes":      scrape_error or "",
    }

def _error_row(original_url, normalised_url, scrape_status, scrape_error, llm_error, resolved_url=""):
    """Returns a clearly-flagged error row so no URL silently disappears from output."""
    return {
        "company_name":     "Unknown",
        "business_type":    "Unknown",
        "industry":         "Unknown",
        "company_summary":  "",
        "confidence_score": "Low",
        "data_source":      "error",
        "original_url":     original_url,
        "normalised_url":   normalised_url,
        "resolved_url":     resolved_url or normalised_url,
        "scrape_status":    scrape_status,
        #"scrape_note":      "error",
        "error_notes":      f"Scrape: {scrape_error} | LLM: {llm_error}",
    }

# ─────────────────────────────────────────────
# 8. MAIN PIPELINE
# ─────────────────────────────────────────────

def run_pipeline(input_csv: str):
    """
    Orchestrates the full enrichment pipeline.

    Flow per URL:
      normalise → scrape → prompt → extract → validate → append row

    Results are written to CSV incrementally (after each URL) so you never
    lose progress if the script is interrupted mid-run.
    """
    log.info("=" * 60)
    log.info("ACQ Vantage Lead Enrichment Pipeline")
    log.info(f"Input:  {input_csv}")
    log.info(f"Output: {OUTPUT_PATH}")
    log.info("=" * 60)

    # Initialise API clients
    firecrawl_app = Firecrawl(api_key=FIRECRAWL_API_KEY)
    openai_client = OpenAI(api_key=OPENAI_API_KEY)

    # Load URLs
    df_input = pd.read_csv(input_csv)
    urls = df_input["website"].dropna().tolist()
    total = len(urls)
    log.info(f"Loaded {total} URLs to process\n")

    results = []

    for i, raw_url in enumerate(urls, start=1):
        raw_url = str(raw_url).strip()
        log.info(f"[{i}/{total}] Processing: {raw_url}")

        # Step 1: Normalise URL
        normalised = normalise_url(raw_url)
        log.info(f"        Normalised → {normalised}")

        # Step 2: Scrape
        scrape_result = scrape_website(firecrawl_app, normalised)
        scrape_note   = scrape_result.get("scrape_note", "")
        resolved_url  = scrape_result.get("resolved_url", normalised)
        log.info(f"        Scrape status: {scrape_result['status']} "
                 f"({len(scrape_result['content'])} chars)"
                 + (f" [{scrape_note}]" if scrape_note else "")
                 + (f" resolved: {resolved_url}" if resolved_url != normalised else ""))

        if scrape_result["error"]:
            log.warning(f"        Scrape error: {scrape_result['error'][:120]}")

        # Step 3: Build prompt (adapts based on scrape success/failure)
        prompt = build_prompt(
            url=resolved_url,
            content=scrape_result["content"],
            scrape_status=scrape_result["status"]
        )

        # Step 4: Extract with LLM
        raw_extracted = extract_with_llm(openai_client, prompt)

        # Step 5: Validate and build final row
        row = validate_and_clean(
            raw=raw_extracted,
            original_url=raw_url,
            normalised_url=normalised,
            resolved_url=resolved_url,
            scrape_status=scrape_result["status"],
            scrape_error=scrape_result["error"],
            #scrape_note=scrape_note,
        )

        results.append(row)

        # Log what we got
        log.info(f"        → {row['company_name']} | "
                 f"{row['business_type']} | "
                 f"{row['industry']} | "
                 f"Confidence: {row['confidence_score']}")

        # Write incrementally — protects against interruption mid-run
        pd.DataFrame(results).to_csv(OUTPUT_PATH, index=False)

        # Polite delay between requests
        if i < total:
            time.sleep(DELAY_BETWEEN_REQUESTS)

    # ── Final summary ──
    log.info("\n" + "=" * 60)
    log.info("PIPELINE COMPLETE")
    log.info(f"Total processed:  {total}")

    df_out = pd.DataFrame(results)
    success_count  = len(df_out[df_out["scrape_status"] == "success"])
    blocked_count  = len(df_out[df_out["scrape_status"] == "blocked"])
    failed_count   = len(df_out[df_out["scrape_status"] == "failed"])
    empty_count   = len(df_out[df_out["scrape_status"] == "empty"])
    ssl_error_count   = len(df_out[df_out["scrape_status"] == "ssl_error"])
    high_conf      = len(df_out[df_out["confidence_score"] == "High"])
    med_conf       = len(df_out[df_out["confidence_score"] == "Medium"])
    low_conf       = len(df_out[df_out["confidence_score"] == "Low"])

    log.info(f"Scrape results:   {success_count} success | "
             f"{blocked_count} blocked | {failed_count} failed")
    log.info(f"Confidence:       {high_conf} High | {med_conf} Medium | {low_conf} Low")
    log.info(f"Output saved to:  {OUTPUT_PATH}")
    log.info(f"Log saved to:     {LOG_PATH}")
    log.info("=" * 60)

    return df_out


# ─────────────────────────────────────────────
# 9. ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    # Step 1: Run URL normaliser tests — fast, free, catches issues
    # before spending any API credits
    print("Running pre-flight checks...")
    if not test_normalise_url():
        raise SystemExit("URL normaliser tests failed — fix before running pipeline.")

    # Step 2: Validate API keys are present
    if not FIRECRAWL_API_KEY:
        raise EnvironmentError("FIRECRAWL_API_KEY not found in .env file")
    if not OPENAI_API_KEY:
        raise EnvironmentError("OPENAI_API_KEY not found in .env file")

    print("Pre-flight checks passed. Starting pipeline...\n")
    run_pipeline("trial-project-websites.csv")