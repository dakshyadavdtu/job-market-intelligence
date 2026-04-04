# SHEET1_COPY_BLOCKS.md

Ready-to-paste copy for Sheet 1. Match block IDs to `DASHBOARD_SPEC.md`.

---

## S1-HDR-TITLE

**Arbeitnow job market snapshot — structural view**

---

## S1-HDR-SUBTITLE

Single-month gold aggregates from a validated pipeline run. This page describes **how posting mass is distributed** across skills, locations, titles, and employers — not trends over time.

---

## S1-METRIC-DEF-BODY

**Total postings** = sum of `job_count` in `role_demand_monthly` (each job has exactly one role row in gold).  
**Located postings** = sum of `job_count` in `location_demand_monthly` (jobs without a location are excluded; this sum is ≤ total postings).  
**Top-3 location share** = share of *located* postings accounted for by the three largest location buckets.  
**Location HHI / Company HHI** = Herfindahl–Hirschman Index over posting shares within that dimension (higher = more concentrated).  
**Top-1 role share** = postings in the largest title bucket ÷ total postings.  
**Skills:** `job_count` per skill is **not additive** to jobs — postings can list multiple tags.

---

## S1-GUARDRAILS-TITLE

**How to interpret this page**

---

## S1-GUARDRAILS-BODY

- This is a **single monthly slice**, not a forecast or longitudinal trend.  
- Concentration metrics describe **this dataset’s internal distribution**, not the entire labor market.  
- **Skills** reflect Arbeitnow tag fields; they are useful for composition, not an exhaustive skills ontology.  
- **Locations** are normalized in the pipeline but may still reflect source quirks.  
- **Roles** are title strings as cleaned in silver — interpret as “posting title concentration,” not official occupational codes.  
- **No causal claims** (e.g., “X causes hiring”); association only.

---

## S1-HOWTO-READ (optional small block below guardrails)

**Reading order:** KPI strip → skill mix → geographic mass → role Pareto and top titles → employer mass. The Pareto line shows **cumulative share of all postings** as titles are added from most to least frequent.

---

## S1-KPI-K1-TITLE

Total postings

---

## S1-KPI-K1-SUB

Canonical count from role-level gold aggregates.

---

## S1-KPI-K2-TITLE

Located postings

---

## S1-KPI-K2-SUB

Postings with a non-empty normalized location.

---

## S1-KPI-K3-TITLE

Top-3 location share

---

## S1-KPI-K3-SUB

Share of *located* postings in the three largest location buckets.

---

## S1-KPI-K4-TITLE

Location HHI

---

## S1-KPI-K4-SUB

Concentration of located postings across locations.

---

## S1-KPI-K5-TITLE

Company HHI

---

## S1-KPI-K5-SUB

Concentration of postings across employer name buckets.

---

## S1-KPI-K6-TITLE

Top-1 role share

---

## S1-KPI-K6-SUB

Largest single title bucket as a share of all postings.

---

## S1-DONUT-SKILLS-TITLE

Skill tag composition

---

## S1-DONUT-SKILLS-SUB

Angle ∝ tag-level posting counts. Jobs may appear under multiple tags — slices do not sum to total postings.

---

## S1-TREEMAP-LOC-TITLE

Geographic mass (Top 15 + Other)

---

## S1-TREEMAP-LOC-SUB

Tile area ∝ located postings. “Other” aggregates the long tail beyond the top 15 locations.

---

## S1-HIGHLIGHT-LOC-TITLE

Location buckets (detail)

---

## S1-HIGHLIGHT-LOC-SUB

Same aggregation as the treemap; use for exact counts and sorting.

---

## S1-PARETO-ROLE-TITLE

Role titles — Pareto coverage

---

## S1-PARETO-ROLE-SUB

Bars: postings per title (rank order). Line: cumulative % of **all** postings. Hover for full title text.

---

## S1-TABLE-ROLE-TITLE

Top 20 role titles

---

## S1-TABLE-ROLE-SUB

Highest posting counts; full title strings on-sheet.

---

## S1-TREEMAP-COMPANY-TITLE

Employer mass (Top 12 + Other)

---

## S1-TREEMAP-COMPANY-SUB

Tile area ∝ postings per employer bucket. “Other” aggregates remaining employers.

---

## S1-INSIGHT-PLACEHOLDERS (optional — fill after QA)

- **Concentration one-liner:** *e.g., “Top-3 location share is [value] — located demand is [concentrated / moderately spread] in this slice.”*  
- **Tail one-liner:** *e.g., “Pareto curve reaches [X]% by top [k] titles — [head-heavy / long-tail] title mix.”*  
*(Delete placeholders if unused.)*
