# METRIC_DEFINITIONS.md

Definitions for Sheet 1 evaluation metrics and supporting fields. Aligned with `jmi_analytics.sheet1_kpis` and `ATHENA_VIEWS.sql`.

---

## Total postings

| Item | Detail |
|------|--------|
| **Plain language** | Number of job postings counted in the role-aggregated gold table for the slice — each posting contributes **once** to this total. |
| **Formula** | `SUM(job_count)` over `jmi_gold.role_demand_monthly` for the selected `ingest_month` and `run_id`. |
| **Denominator** | N/A (this **is** the primary universe for role-based shares). |
| **Interpretation** | Anchor for “how many postings” in this monthly gold extract. |
| **Limitations** | Excludes postings dropped before Silver or with empty role after cleaning (per pipeline rules). |
| **Viva line** | “Total postings is the sum of per-role counts in gold; each job maps to one role bucket, so the sum is cardinality-consistent.” |

---

## Located postings

| Item | Detail |
|------|--------|
| **Plain language** | Postings that have a non-empty **normalized** location string in the location gold table. |
| **Formula** | `SUM(job_count)` over `jmi_gold.location_demand_monthly` for the slice. |
| **Denominator** | N/A. |
| **Interpretation** | Sub-universe for **location-only** metrics; compare to total postings to see how much mass is geocodable in source data. |
| **Limitations** | Always **≤ total postings**; gap = postings without usable location after cleaning. |
| **Viva line** | “Located postings sum location buckets; empty locations are dropped in gold, so this is a subset of the role total.” |

---

## Top-3 location share (located mass)

| Item | Detail |
|------|--------|
| **Plain language** | Fraction of **located** postings that fall in the **three largest** location buckets by `job_count`. |
| **Formula** | `(sum of job_count for ranks 1–3 by job_count desc) / (sum of all job_count in location_demand_monthly)` for the same slice. |
| **Denominator** | **Located postings** (sum of location table), **not** total postings. |
| **Interpretation** | High value → located demand is **top-heavy** across a few places; low → more spread (among locations that exist). |
| **Limitations** | If **located postings = 0**, metric is **NULL/undefined**. Not comparable to national geography without geocoding quality proof. |
| **Viva line** | “It measures concentration among postings we could place in a location bucket, not among all postings.” |

---

## Location HHI (Herfindahl–Hirschman Index)

| Item | Detail |
|------|--------|
| **Plain language** | Sum of squared **market shares** of each location bucket, where “market” = located postings only. |
| **Formula** | \( \text{HHI} = \sum_i (s_i)^2 \) with \( s_i = \frac{\text{job\_count}_i}{\sum_j \text{job\_count}_j} \) over all location rows in the slice. |
| **Denominator** | Total **located** postings (same as location table sum). |
| **Interpretation** | Ranges **roughly** from **1/n** (even spread) to **1** (single bucket). Higher → **more concentrated**. |
| **Limitations** | Descriptive only; not an antitrust “market definition.” Sensitive to number of buckets (here, string locations). |
| **Viva line** | “HHI summarizes how unequal the distribution of located postings is across location labels.” |

---

## Company HHI

| Item | Detail |
|------|--------|
| **Plain language** | Same HHI logic as locations, but buckets are **company_name** rows in `company_hiring_monthly`. |
| **Formula** | \( \sum_i (c_i)^2 \) where \( c_i = \frac{\text{job\_count}_i}{\sum_j \text{job\_count}_j} \) over company rows. |
| **Denominator** | Sum of `job_count` in **company** table (≤ total postings if some postings lack company). |
| **Interpretation** | High → postings concentrate under fewer employer strings; low → more fragmented. |
| **Limitations** | Employer **name strings** ≠ legal entities; subsidiaries may appear separately. |
| **Viva line** | “It measures concentration of posting mass across raw employer labels in the feed.” |

---

## Top-1 role share

| Item | Detail |
|------|--------|
| **Plain language** | Share of all postings that fall in the **single most frequent** normalized role/title bucket. |
| **Formula** | `MAX(job_count) / SUM(job_count)` over `role_demand_monthly` for the slice. |
| **Denominator** | **Total postings** (role sum — canonical). |
| **Interpretation** | High → one title dominates; low → titles are more evenly split. |
| **Limitations** | Sensitive to how titles are normalized; long tail may hide many niche strings. |
| **Viva line** | “It’s the dominance of the largest title bucket relative to the full posting count.” |

---

## Share of total (`share_of_total` in `role_pareto`)

| Item | Detail |
|------|--------|
| **Plain language** | Fraction of **all postings** in one role bucket. |
| **Formula** | `job_count / total_jobs` where `total_jobs = SUM(job_count)` over roles for the slice. |
| **Denominator** | Total postings (role sum). |
| **Interpretation** | Per-bar height in **relative** terms; complements absolute `job_count`. |

---

## Cumulative job percent (`cumulative_job_pct` in `role_pareto`)

| Item | Detail |
|------|--------|
| **Plain language** | Running percentage of postings accounted for when roles are taken from **largest** to **smallest** by `job_count`. |
| **Formula** | `100 × (running sum of job_count in that order) / total_jobs`. |
| **Ordering column** | `pareto_rank` (1…R) in `jmi_analytics.role_pareto` — same order as bars and cumulative line. |
| **Denominator** | Total postings. |
| **Interpretation** | Pareto / “head vs tail” curve; must end at **100%** at the last `pareto_rank`. |
| **Limitations** | Ordering is by **frequency**, not alphabet or SOC code. |

---

## Top-N + Other (locations & companies)

| Item | Detail |
|------|--------|
| **Plain language** | Keep the **N** largest buckets by `job_count`; sum all remaining buckets into a single row **`Other`**. |
| **Formula (conceptual)** | Rank by `job_count` desc; label = actual name if `rank ≤ N`, else `'Other'`; then `GROUP BY` label and `SUM(job_count)`. |
| **Denominator** | For **share** calculations inside a view, use sums within the same table after aggregation. |
| **Interpretation** | Reduces visual clutter while preserving **mass** in the tail in one bucket. |
| **Limitations** | **Other** is not a single place or employer — it is an **aggregate long tail**. |

---

## Skill `job_count` (not a “total jobs” metric)

| Item | Detail |
|------|--------|
| **Plain language** | For each skill tag, count of **distinct jobs** (or tag occurrences per gold logic) — **multiple tags per job** possible. |
| **Formula** | Do **not** sum skill `job_count` across skills to infer total jobs. |
| **Interpretation** | Composition of **tags** on postings. |
| **Viva line** | “Skill counts answer ‘how often does this tag appear,’ not ‘how many jobs exist.’” |
