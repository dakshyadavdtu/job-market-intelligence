# ARCHITECTURE_DIAGRAM_BRIEF.md

Single diagram for Sheet 2 (`S2-ARCH-IMG`). Build in **draw.io**, **Excalidraw**, or **Lucidchart**; export **PNG** (width ≥ 1200px).

---

## Diagram title (for canvas)

**Arbeitnow → AWS data lake → Athena → QuickSight**

---

## Recommended layout

**Left-to-right swimlane** (one row of boxes). Optional **dashed box** around “AWS account” containing everything after API.

---

## Nodes to draw (exact labels inside boxes)

| # | Box label (short) | Longer subtitle (smaller text under title) |
|---|-------------------|---------------------------------------------|
| 1 | **Arbeitnow API** | Source / job postings JSON |
| 2 | **Ingestion (Lambda)** | Fetch, batch, write Bronze |
| 3 | **S3 — Bronze** | Raw JSONL.GZ + manifests |
| 4 | **Transform — Silver (Lambda)** | Curated job Parquet |
| 5 | **S3 — Silver** | `silver/jobs/...` |
| 6 | **Transform — Gold (Lambda)** | Aggregates + summary |
| 7 | **S3 — Gold** | `skill_*`, `role_*`, `location_*`, `company_*`, `pipeline_run_summary` |
| 8 | **Glue Data Catalog** | Tables → Athena |
| 9 | **Amazon Athena** | SQL over Parquet |
| 10 | **Amazon QuickSight** | Dashboards & KPIs |

---

## Arrows (exact flow)

1. **Arbeitnow API** → **Ingestion (Lambda)** — label: `HTTPS / API`  
2. **Ingestion (Lambda)** → **S3 — Bronze** — label: `PUT`  
3. **S3 — Bronze** → **Transform — Silver (Lambda)** — label: `trigger / invoke`  
4. **Transform — Silver (Lambda)** → **S3 — Silver** — label: `Parquet`  
5. **S3 — Silver** → **Transform — Gold (Lambda)** — label: `invoke`  
6. **Transform — Gold (Lambda)** → **S3 — Gold** — label: `Parquet`  
7. **S3 — Gold** → **Glue Data Catalog** — label: `register / partition` (dashed optional)  
8. **Glue Data Catalog** → **Amazon Athena** — label: `external tables`  
9. **Amazon Athena** → **Amazon QuickSight** — label: `dataset / SPICE or DQ`

**Optional small callout** (box or sticky note, does not need arrows to all):  
**“Validation: `pipeline_run_summary` row per run (PASS + row counts)”** — arrow from **Transform — Gold** or **S3 — Gold** to this note.

---

## Grouping (optional)

- **Group A (edge):** Arbeitnow API  
- **Group B (compute):** Ingestion Lambda, Silver Lambda, Gold Lambda  
- **Group C (storage):** S3 Bronze, Silver, Gold (three nested mini-boxes inside one “Amazon S3” container)  
- **Group D (serve & BI):** Glue, Athena, QuickSight  

---

## Caption under diagram (paste under image on Sheet 2)

**Figure:** End-to-end path for this project. Job postings originate in the Arbeitnow API, are ingested into **Bronze** on S3, normalized to **Silver** job-level Parquet, aggregated to **Gold** monthly tables plus a **run summary** for validation, then exposed to **Athena** for SQL and **QuickSight** for analytics. Each stage preserves **run and month partitions** so results are traceable to a **single pipeline execution**.

---

## Narrative (viva, 30 seconds)

“We treat the API as the system boundary, land immutable raw files in Bronze, normalize to a job grain in Silver, aggregate to analytics-ready Parquet in Gold—including a summary row that proves the transform outputs—and then query through Athena into QuickSight. The catalog makes S3 files look like tables without loading them into a traditional warehouse.”
