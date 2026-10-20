# goevent-creator dashboard

`goevent-creator.json` is a Grafana dashboard for the worker's metrics:
shadow checks (mismatches, refusals compared with the native path, what was
not checked), native results, fallbacks and refusals, verify checks, main's
latency for what the worker forwards, USER_IP, and the worker process.

Prometheus must scrape the worker's metrics listener (`metrics.addr`,
`:9102` in the container):

```yaml
scrape_configs:
  - job_name: goevent-creator
    static_configs:
      - targets: ["av-goevent-creator-1:9102"]
```

Import it in Grafana (Dashboards, New, Import) and pick the Prometheus data
source; the Job variable selects the scrape job.
