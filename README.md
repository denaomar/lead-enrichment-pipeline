# Lead Enrichment Pipeline
### ACQ Vantage — Trial Project
**Author:** Dena Omar

---

## What This Does

Takes a CSV of 48 member company websites and enriches each one with structured, CRM-ready company data using a multi-stage scraping pipeline and an LLM extraction layer.

Each URL produces:

| Field | Description |
|---|---|
| `company_name` | Brand name as it appears on the site |
| `business_type` | How the company delivers value (fixed taxonomy) |
| `industry` | Domain the company operates in (fixed taxonomy) |
| `company_summary` | One sentence: who they help and how |
| `confidence_score` | High / Medium / Low — LLM self-assessment |
| `data_source` | `scraped` or `url_only` |
| `original_url` | Raw input from the CSV |
| `normalised_url` | Cleaned URL that was attempted |
| `resolved_url` | URL that actually served content (may differ after fallbacks) |
| `scrape_status` | `success` / `empty` / `blocked` / `ssl_error` / `failed` |
| `error_notes` | Full error detail for manual review |

---

## Quick Start

**Requirements**
```
Python 3.9+
pip install firecrawl-py openai pandas python-dotenv requests beautifulsoup4
```

**Setup**

Create a `.env` file in the project root:
```
FIRECRAWL_API_KEY=fc-your-key-here
OPENAI_API_KEY=sk-your-key-here
```

**Run**
```bash
python enrichment_pipeline.py
```

Output is written incrementally to `enriched_output_<timestamp>.csv` so progress is never lost if the run is interrupted.

---

## Pipeline Flow

```
CSV input
   │
   ▼
1. URL Normalisation      — clean casing, missing schemes, trailing slashes
   │
   ▼
2. Scraping (4-stage)     — Firecrawl → www variant → http → direct requests
   │
   ▼
3. LLM Extraction         — OpenAI GPT-4o-mini with strict taxonomy prompt
   │
   ▼
4. Validation             — coerce any out-of-taxonomy values to "Unknown"
   │
   ▼
5. CSV output             — written after each URL, timestamped
```

The scraper has four recovery stages that run sequentially, each only if the previous one failed:

1. **Direct Firecrawl scrape** — primary path for most sites
2. **SSL recovery** — tries `https://www.` prefix, then falls back to `http://`
3. **Subpage fallbacks** — tries `/about`, `/about-us`, `/services`, `/what-we-do` when the homepage returns empty content
4. **Direct requests** — bypasses Firecrawl entirely with a browser User-Agent

---

## Cost

| Component | Cost |
|---|---|
| Firecrawl | ~1 credit per URL. 48 URLs ≈ 48–100 credits (retries). Free tier covers 500. |
| OpenAI GPT-4o-mini | ~$0.0002 per URL. 48 URLs ≈ **$0.01 total**. |

Total estimated cost to run the full pipeline: **under $0.15**.

---

## Design Decisions

For the full reasoning behind architecture choices, taxonomy design, column selection, and cost optimisation, see [`PIPELINE_DESIGN.md`](PIPELINE_DESIGN.md).
