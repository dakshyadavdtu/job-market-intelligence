# EventBridge Scheduler — ingest cadence

- **Schedule name:** `jmi-ingest-10min` (historical name; **not** 10-minute cadence in repo)
- **Expression:** `rate(24 hours)` — see `jmi-ingest-schedule.json`
- **Target:** `jmi-ingest-live` Lambda (`scheduler-target.json`)

Apply or update in **ap-south-1**:

```bash
./infra/aws/eventbridge/apply-ingest-schedule.sh
```

Or one-shot:

```bash
aws scheduler update-schedule \
  --name jmi-ingest-10min \
  --group-name default \
  --region ap-south-1 \
  --schedule-expression "rate(24 hours)" \
  --state ENABLED \
  --flexible-time-window Mode=OFF \
  --target file://infra/aws/eventbridge/scheduler-target.json
```

Verify: `aws scheduler get-schedule --name jmi-ingest-10min --group-name default --region ap-south-1`
