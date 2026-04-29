# Pipeline Design — Lead Enrichment 
**Author:** Dena Omar

---

## 1. Problem Framing

GTM teams routinely maintain lists of prospect or customer websites, sourced from CRM imports, event registrations, community memberships, or enrichment tools, with little to no structured data attached. Turning those URLs into actionable company intelligence (industry, business type, a reliable one-line summary) is a recurring ops problem that most teams solve manually or not at all.

The core challenge is not technical — scraping and LLM extraction are well-solved. The challenge is **data quality at the output layer**. A pipeline that produces fast results with inconsistent taxonomies, vague summaries, or undocumented failures is worse than useless for segmentation and outreach: it creates the appearance of clean data while hiding noise.

Every design decision in this pipeline is oriented around one question: will a revenue ops analyst be able to trust and act on this output without additional cleaning?*

---

## 2. Architecture: Why Deterministic Code + Prompt Chaining, Not AI Agents

The pipeline is deliberately built as **deterministic orchestration with a single LLM extraction step**, not as an AI agent loop.

### What an AI agent would look like here

An agent approach would route each URL through an LLM that autonomously decides which tools to call, in what order, and when to stop — with the model acting as the orchestrator rather than just a processor.

### Why that's the wrong tool for this job

**The failure modes are enumerable.** Firecrawl has a finite, documented set of failure patterns: SSL errors, bot blocks, empty homepages, DNS misconfigurations. These can be handled with a deterministic decision tree written once. Routing these decisions through an LLM adds 2–3 API calls per failed URL, increases latency, introduces non-determinism, and costs money — without improving the outcome.

**The output schema is fixed.** Agents add value when the output structure is unknown or emergent. Here, the schema is rigid by design. A structured prompt with `response_format: json_object` and a hardcoded taxonomy produces more consistent results than an agent reasoning freely about how to structure data.

**Deterministic code is auditable.** Every decision in the scrape retry logic can be traced to a specific line of code. When a URL fails, the `scrape_note` and `error_notes` fields tell you exactly which stage failed and why. An agent loop produces less predictable audit trails.

### Where agents would add genuine value

The natural evolution of this pipeline is an agent layer for the one step where deterministic code falls short: **confidence-driven depth of research**. When a homepage returns thin content and confidence is Low, an agent could autonomously decide to try the About page, then the LinkedIn profile, then a Google search — stopping when it reaches Medium or High confidence. That's genuinely agentic behaviour with real ROI. It's scoped as a v2 enhancement.

### The cost argument

| Approach | LLM calls per URL | Cost per URL (est.) |
|---|---|---|
| This pipeline (deterministic + 1 extraction call) | 1 | ~$0.0002 |
| Agent loop (orchestrator + tool decisions + extraction) | 3–5 | ~$0.001–0.002 |

For 48 URLs the difference is negligible. At 10,000 members it becomes meaningful.

---

## 3. Tool Selection

### Firecrawl

Chosen over `requests` + `BeautifulSoup` as the primary scraper for three reasons:

- **JavaScript rendering.** Many modern business sites are SPAs that return empty HTML to a plain HTTP request. Firecrawl runs a real browser and returns rendered content.
- **LLM-ready output.** Firecrawl returns clean markdown, stripping navigation, footers, cookie banners, and other noise. This reduces token usage and improves extraction quality.
- **Minimal setup.** One API call replaces a scraping stack that would otherwise require Playwright or Selenium, proxy management, and HTML-to-text conversion.

`requests` + `BeautifulSoup` is retained as the **Stage 4 fallback** specifically because it bypasses Firecrawl's known IP ranges, which some sites block. The combination covers more surface area than either tool alone.

### OpenAI GPT-4o-mini

Chosen over GPT-4o for this task because extraction from structured web content into a fixed schema does not require frontier-level reasoning. GPT-4o-mini is 10–20× cheaper and produces equivalent output quality on well-defined classification tasks with a clear taxonomy.

`temperature: 0.1` is set deliberately low to prioritise consistency over creativity. The goal is the same classification decision every time, not the most interesting interpretation.

`response_format: json_object` forces valid JSON output and eliminates fragile regex parsing of the response.

---

## 4. Taxonomy Design

### Why a fixed taxonomy matters

The brief specifies that this data will feed cohort analysis, targeted outreach, and member composition reporting. All three use cases require **filterable, groupable fields**. Free-text business descriptions cannot be aggregated. "Digital marketing agency", "growth agency", and "performance marketing firm" are the same thing for segmentation purposes — but a free-text field treats them as three distinct values.

A fixed taxonomy enforces consistency at the point of data creation rather than requiring downstream cleanup.

### Business Type taxonomy

| Value | Rationale |
|---|---|
| Agency | Service delivery on behalf of clients |
| Coaching / Consulting | Knowledge and guidance sold as 1:1 or group access |
| SaaS / Software | Recurring software product |
| E-commerce | Physical or digital product sold direct |
| Professional Services | Regulated or credentialled expertise (legal, financial, medical) |
| Education / Course | Structured curriculum, not ongoing coaching |
| Local / Trade Services | Geography-bound service business |
| Media / Content | Audience monetisation via content |

These eight categories cover the full range of business models typically found in a high-revenue B2B prospect or customer list without over-segmenting. They map directly to how GTM teams differentiate outreach strategy.

### Industry taxonomy

Twelve categories chosen to reflect the actual distribution of businesses at the $1M+ revenue stage, not a generic industry list. `Business Operations` serves as a catch-all for cross-industry service businesses. `Marketing & Lead Gen` is separated from the broader Technology category because it represents a distinct business model cluster in this member base.

### Why "Unknown" over leaving a field blank

When the LLM returns a value outside the taxonomy — which happens on roughly 2–5% of extractions — the validator coerces it to `"Unknown"` rather than writing the invalid value or leaving the field empty. This is intentional:

- A blank field is ambiguous: did the extraction fail, or was no value found?
- An invalid value silently corrupts filter queries in the CRM
- `"Unknown"` is an explicit signal to a CRM admin that this record needs manual review

---

## 5. Output Column Design

Every column in the output serves a specific purpose for either CRM use or data lineage.

### CRM fields (written to HubSpot company properties)

`company_name`, `business_type`, `industry`, `company_summary` — the enrichment payload. Directly usable for segmentation, filtering, and outreach personalisation.

`confidence_score` — tells the CRM team which records to trust versus which to manually review before using in campaigns. High confidence records can be used immediately. Low confidence records (scrape failed, thin content) should be verified before personalised outreach.

`data_source` — distinguishes records where the LLM had real content to work from (`scraped`) versus records where it inferred from the URL alone (`url_only`). A `url_only` record with High confidence is suspicious and worth checking.

### Lineage fields (audit trail for analysts and admins)

`original_url` — the raw input exactly as it appeared in the source CSV. Preserved without modification so any data quality issues in the source file can be traced back.

`normalised_url` — the cleaned URL the pipeline attempted. Useful for diagnosing normalisation issues if they arise.

`resolved_url` — the URL that actually served the content. This differs from `normalised_url` when a fallback stage succeeded (e.g., `https://andalusinstitute.com` fails but `https://www.andalusinstitute.com` succeeds). Without this field, there is no way to know where the data actually came from.

`scrape_status` — the final state of the scrape attempt. Enables bulk filtering: `scrape_status == "blocked"` finds all records that may benefit from a manual scrape or re-run.

`error_notes` — the full error message from the last failed stage. Essential for debugging specific failures or identifying patterns across the dataset (e.g., multiple SSL errors from the same hosting provider).

---

## 6. Scalability Notes

This pipeline is designed for batch enrichment runs of hundreds to low thousands of URLs. For larger scale:

- **Parallelism:** The current pipeline is sequential with a 1-second delay between requests. At 10,000 URLs, a parallel implementation with worker threads and per-domain rate limiting would reduce runtime from ~3 hours to under 30 minutes.
- **Caching:** Domains that were successfully scraped recently should not be re-scraped on re-runs. A simple `scrape_cache` table keyed on domain + date would eliminate redundant Firecrawl credits.
- **Webhook trigger:** In production, this pipeline would run on a webhook triggered by new member creation in HubSpot — not as a batch job. The N8N workflow (see `n8n_workflow.json`) implements this pattern.
