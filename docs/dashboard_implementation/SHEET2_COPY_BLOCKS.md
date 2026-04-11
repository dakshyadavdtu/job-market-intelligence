# SHEET2_COPY_BLOCKS.md

Ready-to-paste copy for Sheet 2. No market analytics language.

---

## S2-HDR-TITLE

**Platform, pipeline & validation**

---

## S2-HDR-SUBTITLE

How Arbeitnow job data is ingested, stored in a lake, transformed through Bronze → Silver → Gold, queried in Athena, and consumed in QuickSight — with a **recorded validation snapshot** for each run.

---

## S2-LIFECYCLE

**End-to-end flow**

Arbeitnow exposes job postings via an API — that is the **source** and **generation** boundary. Ingestion jobs pull new and updated postings on a controlled schedule or on demand, write **immutable raw captures** to **Amazon S3** under a Bronze layout, and record run metadata so every batch is **re-identifiable**.

Silver transforms **normalize and deduplicate** postings into a curated job-level table (one row per job where possible). Gold transforms **aggregate** that table into **monthly analytics datasets** used for dashboards.

**Amazon Athena** queries Parquet in place (or via the catalog) as the **serving / query** layer. **Amazon QuickSight** connects to Athena as the **analytics and presentation** layer. Together, this implements a minimal but complete **modern data-warehouse pattern** on the AWS data-lake stack.

---

## S2-LAYER-CONTRACT

**Bronze, Silver, Gold — contract for this project**

**Bronze**  
Raw JSON lines (compressed) as landed from the connector, plus manifests and lineage paths. Purpose: **auditability** and **replay**; minimal transformation.

**Silver**  
A **job-level** Parquet dataset: cleaned titles, company, location, skills array, URLs, and stable identifiers. Purpose: **analytic grain = one posting record** with consistent typing and deduplication rules.

**Gold**  
**Monthly aggregates** derived only from Silver: demand by skill, role, location, company, plus a **pipeline run summary** row with validation counts and status. Purpose: **fast dashboard consumption** and **proof of transform outputs** per run.

---

## S2-PROOF-ABOVE-TABLE

**Validation artifact for the selected run**  
The table below is the gold-stage **pipeline_run_summary**: row counts per published dataset and an overall status flag. It ties QuickSight to a **specific** `run_id` and `ingest_month`.

---

## S2-SECURITY

**Security**

- **Least-privilege IAM:** Lambda execution roles limited to required S3 prefixes and Athena/Glue actions; no console-wide admin for batch jobs.  
- **Encryption:** Data at rest in S3 uses **SSE-S3** (or account standard); in transit **TLS** for API and AWS service calls.  
- **Access boundaries:** Dashboard consumers use **QuickSight** entitlements; raw bucket access restricted to pipeline roles — reduces accidental overwrite and data exfiltration risk.  
- **Secrets:** API keys and credentials stored outside the repo (environment/parameter store pattern), not embedded in dashboard assets.

---

## S2-DATA-MGMT

**Data management**

- **Lake layout:** Bronze / Silver / Gold prefixes separate **raw**, **curated**, and **aggregated** data; partitions (e.g., `ingest_month`, `run_id`) support **incremental** processing and **selective** querying.  
- **Formats:** **Parquet** for Silver/Gold for columnar efficiency; compressed JSONL for Bronze.  
- **Catalog:** Tables registered (e.g., **AWS Glue Data Catalog**) so Athena and QuickSight share **one** schema definition.  
- **Naming:** Consistent dataset names (`*_demand_monthly`, `pipeline_run_summary`) make **lineage** and **handover** straightforward.

---

## S2-DATAOPS

**DataOps & reliability**

- **Run identity:** Each batch has a **`run_id`** and **`ingest_month`** so outputs are **idempotent per run** and auditable.  
- **Validation:** Gold writes a **summary row** with expected **row counts** and **PASS/FAIL** semantics for the pipeline stage.  
- **Failure posture:** Ingest or transform failures are contained to a run; prior Parquet partitions remain for comparison (when retained).  
- **Observability:** CloudWatch logs for Lambda; S3 **list/manifest** checks for empty outputs.

---

## S2-ORCHESTRATION

**Orchestration**

- **Triggers:** Ingestion and transforms may be invoked **manually**, on a **schedule** (e.g., EventBridge), or **chained** (ingest → silver → gold) depending on deployment — the dashboard reflects **whatever succeeded** for the selected `run_id`.  
- **Ordering:** Bronze must exist before Silver; Silver before Gold; Athena/QuickSight read **published** Gold paths only after completion.

---

## S2-SWE

**Software engineering**

- **Packaging:** Lambda artifacts bundle application code and dependencies reproducibly (zip or container image, per deployment).  
- **Configuration:** Environment variables for bucket names, region, and downstream function names — **no** hard-coded secrets in source.  
- **Versioning:** Git tracks transform logic; S3 partitions track **data** versions by time and run.  
- **Testing mindset:** Local or dry-run transforms against sample Parquet before promoting to production paths.
