from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

DATA_GOLD_ROOT = Path("data/gold/skill_demand_monthly")
HEALTH_FILE = Path("data/health/latest_ingest.json")

st.set_page_config(page_title="JMI Dashboard", layout="wide")
st.title("Job Market Intelligence (MVP)")

health: dict = {}
if HEALTH_FILE.exists():
    health = json.loads(HEALTH_FILE.read_text(encoding="utf-8"))

parquet_files = sorted(DATA_GOLD_ROOT.glob("ingest_month=*/run_id=*/part-*.parquet"))
if not parquet_files:
    st.info("No Gold dataset yet. Run ingest, silver, and gold transforms.")
    st.stop()

latest = parquet_files[-1]
df = pd.read_parquet(latest)

st.subheader("Overview")
metric1, metric2, metric3 = st.columns(3)
metric1.metric("Gold rows (skills)", int(len(df)))
metric2.metric("Total job mentions", int(df["job_count"].sum()) if "job_count" in df else 0)
metric3.metric("Unique skills", int(df["skill"].nunique()) if "skill" in df else 0)

st.subheader("Freshness and Run Metadata")
meta_col1, meta_col2 = st.columns(2)
with meta_col1:
    st.write(
        {
            "source": health.get("source"),
            "last_run_id": health.get("run_id"),
            "bronze_ingest_date": health.get("bronze_ingest_date"),
            "batch_created_at": health.get("batch_created_at"),
            "bronze_record_count": health.get("record_count"),
        }
    )
with meta_col2:
    st.write(
        {
            "gold_file": str(latest),
            "gold_source": str(df["source"].iloc[0]) if "source" in df.columns and not df.empty else None,
            "gold_bronze_run_id": str(df["bronze_run_id"].iloc[0])
            if "bronze_run_id" in df.columns and not df.empty
            else None,
            "gold_bronze_ingest_date": str(df["bronze_ingest_date"].iloc[0])
            if "bronze_ingest_date" in df.columns and not df.empty
            else None,
        }
    )

st.subheader("Top Skills")
top_n = df.sort_values("job_count", ascending=False).head(20)
st.dataframe(top_n[["skill", "job_count"]], use_container_width=True)
st.bar_chart(top_n.set_index("skill")["job_count"])
