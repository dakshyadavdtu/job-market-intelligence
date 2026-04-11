# Role grouping & company display quality — design note

## Section 1: Diagnosis

**Why raw `role_demand_monthly` is weak for Sheet 1**

- Gold stores **one row per distinct title string** after silver normalization. Postings that differ only by gender tags, reference numbers, language, or punctuation become **separate categories**.
- In real feeds (e.g. Arbeitnow), titles are **high-cardinality**: many strings appear once or twice. Pareto and Top‑N visuals then show **noise** instead of **structure**: the “story” is hidden in long tails of near-unique labels.
- **Evaluation metrics** (concentration, head vs tail) are still valid at the **posting** level, but **semantic** interpretation of “which roles dominate?” is poor when dominance is spread across lexical variants.

**Why rules-based grouping is the right tradeoff**

- **Reproducible:** same SQL + same inputs ⇒ same buckets every run (viva-friendly).
- **Explainable:** each bucket is a **priority-ordered** keyword/regex rule, not a black box.
- **No new infrastructure:** stays in **Athena views** over existing gold; base tables unchanged.
- **Honest limitation:** some titles are **ambiguous**; rules pick **one** bucket deterministically. That is preferable to pretending raw strings are a clean ontology.

---

## Section 2: Grouping strategy

**Text cleaning (deterministic, applied before grouping)**

1. Lowercase, trim.
2. Remove common DE/EN hiring boilerplate: `(m/w/d)`, `(m/f/x)`, “all genders”, etc.
3. Remove `ref.nr`, `ref no`, `job id`, hash-heavy tokens that look like reference codes.
4. Collapse whitespace; trim trailing/leading punctuation noise (`.,;:|/-`).
5. **Do not** strip meaningful technical tokens (e.g. “senior”, “junior”) unless they are pure noise in context.

**Role families (priority order — first match wins)**

Higher-priority rules are **more specific** domains to avoid swallowing titles by a broad “engineer” rule too early.

1. Cybersecurity  
2. Data / analytics / BI / ML  
3. DevOps / SRE / cloud / platform infrastructure  
4. Software / application development (incl. common DE: *Entwickler*, *Softwareingenieur*, FE/BE/full-stack)  
5. Product / program / project / Scrum / Agile delivery  
6. Marketing / content / growth / SEO  
7. Sales / business development / account / Vertrieb  
8. Finance / accounting / controlling  
9. HR / recruiting / people / talent  
10. Design / UX / UI / creative  
11. Legal / compliance (narrow patterns)  
12. Hardware / embedded / firmware / electronics  
13. Customer success / support / helpdesk / Kundenservice  
14. Consulting / Beratung  
15. Operations / office / administration / assistant  
16. **Other / unknown** — no pattern matched  

**Principles**

- **English + German** keywords where common in DACH job ads.
- **`regexp_like` on cleaned text**; case-insensitive patterns via `(?i)` in Trino.
- **Default bucket** for unmatched strings preserves mass without false precision.

**Company improvement**

- **Problem:** Raw legal suffixes (`GmbH`, `AG`, …) split the same employer across labels; **Top‑12 + Other** then inflates **Other**.
- **Approach:** Normalize **display key** (lowercase, trim, strip common legal suffixes), **re-aggregate** `job_count` per `(ingest_month, run_id, cleaned_name)`, then **Top‑15 + Other** (slightly larger N than 12 for readability after collapse).

---

## Section 3: Athena SQL (canonical file)

All new views are defined in:

**`docs/dashboard_implementation/ATHENA_VIEWS_ROLE_AND_COMPANY_QUALITY.sql`**

| View | Purpose |
|------|---------|
| `jmi_analytics.role_title_classified` | Audit: `raw_role` (gold title), `cleaned_role_title`, `normalized_role_group`, `job_count` |
| `jmi_analytics.role_group_demand_monthly` | One row per `(ingest_month, run_id, role_group)` with summed postings |
| `jmi_analytics.role_group_top20` | Top 20 **families** by `job_count` |
| `jmi_analytics.role_group_pareto` | Pareto over **families** (same columns as `role_pareto` but `role_group` instead of `role`) |
| `jmi_analytics.company_top15_other_clean` | Employer key after suffix strip + re-aggregate + Top 15 + Other |

Run this script **after** `ATHENA_VIEWS.sql`. It does **not** modify `jmi_gold` or replace existing `jmi_analytics.role_pareto` / `role_top20` / `company_top12_other`.

---

## Section 4: QuickSight migration mapping

| Current dataset / visual | Action |
|--------------------------|--------|
| **S1-PARETO-ROLE** (`role_pareto`) | Point to **`jmi_analytics.role_group_pareto`**. X-axis: `pareto_rank`. Bar: `job_count`. Line: `cumulative_job_pct`. Tooltip: **`role_group`** (replaces raw `role`). |
| **S1-TABLE-ROLE** (`role_top20`) | Point to **`jmi_analytics.role_group_top20`**. Columns: `pareto_rank`, **`role_group`**, `job_count`. |
| **S1-TREEMAP-COMPANY** (`company_top12_other`) | Prefer **`jmi_analytics.company_top15_other_clean`** for treemap + highlight table. Same field mapping: `company_label`, `job_count`. |
| **sheet1_kpis** | **No change** (still anchored on raw `role_demand_monthly` for canonical totals). Optional later: add separate KPI for “top‑1 **role group** share” — out of scope unless you add a new KPI view. |
| **skill_*, location_*, donut, KPI strip** | **Unchanged** |

**Title / subtitle tweaks (optional but clear)**

- Pareto title: e.g. **“Role families — Pareto coverage”** (was title-level roles).  
- Subtitle: **“Postings grouped by rules-based role family (see design note); not raw job titles.”**  
- Top table: **“Top 20 role families (by postings)”**  
- Company treemap: **“Employer mass (Top 15 + Other, normalized names)”**

### Migration plan (what stays vs switches)

| Sheet 1 element | Stays on old dataset? | Switches to new dataset |
|-----------------|----------------------|-------------------------|
| KPI strip (`sheet1_kpis`) | **Yes** | No |
| Skills donut | **Yes** | No |
| Location treemap + table | **Yes** | No |
| **Pareto combo** | No (recommended) | **`role_group_pareto`** — use field **`role_group`** where you used **`role`** |
| **Top 20 table** | Optional small raw table | **Primary:** **`role_group_top20`** — field **`role_group`** |
| **Company treemap** (and highlight) | Old view still valid | **Recommended:** **`company_top15_other_clean`** (same `company_label` / `job_count` mapping) |

**New QuickSight datasets to create**

- `DS_ROLE_GROUP_PARETO` → `jmi_analytics.role_group_pareto`  
- `DS_ROLE_GROUP_TOP20` → `jmi_analytics.role_group_top20`  
- `DS_COMPANY_TOP15_CLEAN` → `jmi_analytics.company_top15_other_clean`  
- Optional audit: `DS_ROLE_TITLE_CLASSIFIED` → `jmi_analytics.role_title_classified` (table or export for viva)

---

## Section 5: Raw role table — keep or drop?

**Recommendation: keep raw role as a small supporting visual (optional) or drop from main story.**

- **Primary** Pareto + Top‑20 should use **`role_group_*`** so Sheet 1 answers **“what kinds of work?”** not **“what exact string?”**
- **Optional:** keep **one** compact table or scroll-limited visual on **`role_top20` (raw)** for “examples of noisy top strings” — or remove to reduce clutter.  
- **Do not** run two full Paretos side by side (redundant); if keeping raw, use a **small table** only.

---

## Section 6: Risk note

**Semantic accuracy lost**

- Distinct seniority, stack, or niche titles **collapse** into one family (e.g. “Python developer” and “Java developer” both → software family unless you add stack-specific rules).
- Ambiguous titles (e.g. “Consultant” + IT context) may land in **consulting** vs **software** depending on keyword order — **documented and deterministic**.

**Why this is still better for dashboard evaluation**

- **Concentration and Pareto curves** become **interpretable** (“data vs software vs sales”) instead of lexical noise.
- **Viva:** you can show the **rule stack** and the **`role_title_classified`** view for audit (raw → cleaned → group).
