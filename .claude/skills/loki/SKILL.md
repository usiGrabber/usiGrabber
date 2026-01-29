---
name: loki
description: Query and verify logs in Loki/Grafana. Use when debugging log delivery, checking log content, or verifying structured metadata.
allowed-tools: Bash(curl*), Read
---

# Loki Log Verification and Querying

## Configuration

Loki endpoint from `.env`:
```
LOKI_URL=mpws2025br1.cloud.sci.hpi.de:3100
```

## Quick Verification Commands

### Check available labels (stream selectors)
```bash
curl -s "http://mpws2025br1.cloud.sci.hpi.de:3100/loki/api/v1/labels" | jq
```

### Get recent logs
```bash
curl -s "http://mpws2025br1.cloud.sci.hpi.de:3100/loki/api/v1/query_range" \
  --data-urlencode 'query={app=~".+"}' \
  --data-urlencode 'limit=10' | jq '.data.result[0].values'
```

### Query by project_accession (uses logfmt parser)
```bash
curl -s "http://mpws2025br1.cloud.sci.hpi.de:3100/loki/api/v1/query_range" \
  --data-urlencode 'query={app=~".+"} | logfmt | project_accession="PXD000039"' \
  --data-urlencode 'limit=10' | jq '.data.result[0].values'
```

### Query by log level
```bash
curl -s "http://mpws2025br1.cloud.sci.hpi.de:3100/loki/api/v1/query_range" \
  --data-urlencode 'query={app=~".+"} | logfmt | level="ERROR"' \
  --data-urlencode 'limit=10' | jq '.data.result[0].values'
```

### Count logs per project
```bash
curl -s "http://mpws2025br1.cloud.sci.hpi.de:3100/loki/api/v1/query_range" \
  --data-urlencode 'query=count_over_time({app=~".+"} | logfmt | project_accession!="" [1h]) by (project_accession)' \
  | jq '.data.result[] | {project: .metric.project_accession, count: .values[-1][1]}'
```

## LogQL Query Syntax

### Stream Selectors (indexed labels)
```logql
{app="usigrabber"}           # Exact match
{app=~"usi.*"}               # Regex match
{app=~".+"}                  # Any app
{job_id="12345"}             # By job ID
```

### Log Pipeline (parse & filter)
```logql
{app=~".+"} | logfmt                                    # Parse logfmt fields
{app=~".+"} | logfmt | level="ERROR"                    # Filter by level
{app=~".+"} | logfmt | project_accession="PXD000039"    # Filter by project
{app=~".+"} | logfmt | file_id=~".*mzid.*"              # Regex on field
{app=~".+"} | logfmt | level="ERROR" | line_format "{{.msg}}"  # Format output
```

### Combining Filters
```logql
{app=~".+"} | logfmt | project_accession="PXD000039" | level=~"ERROR|WARNING"
```

## Available Fields (after `| logfmt`)

From LokiHandler structured metadata:
- `msg` - The log message
- `level` - Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `logger` - Logger name
- `file` - Source filename
- `line` - Source line number
- `function` - Function name
- `process` - Process ID
- `thread` - Thread ID
- `project_accession` - Current project being processed (from ContextVar)
- `file_id` - Current file being processed (from ContextVar)

## Troubleshooting

### Logs not appearing?
1. Check Loki is reachable: `curl -s "http://mpws2025br1.cloud.sci.hpi.de:3100/ready"`
2. Check labels exist: `curl -s "http://mpws2025br1.cloud.sci.hpi.de:3100/loki/api/v1/labels"`
3. Verify handler is flushing (check stderr for LokiHandler errors)

### Can't filter by field?
- Fields like `project_accession` are in the log line, not labels
- You MUST use `| logfmt` before filtering: `{app=~".+"} | logfmt | project_accession="..."`

### Query returns empty?
- Check time range (default is last hour)
- Add `&start=<unix_nano>&end=<unix_nano>` for custom range
- Verify the field value exists: query without filter first
