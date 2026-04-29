# Performance

Track benchmark deltas for ingestion, transformation and queries. Each entry
should include: scenario, dataset size, hardware, wall-clock, and a pointer
to the commit or PR that introduced the change.

## Reproducing

```bash
make benchmark                                     # default 100k rows, all scenarios
uv run python -m src.benchmarks.run \
    --rows 500000 \
    --scenario partitioning --scenario zorder \
    --output docs/performance.md
```

The runner writes a markdown table to stdout. Pass `--output` to also append
to this file.

## Scenarios

### Partitioning vs flat scan
- **Setup**: 100k synthetic events spread across 28 days. Two Delta tables —
  one partitioned by `event_date`, the other flat.
- **Query**: `count(*) WHERE event_date = '2024-01-15'`.
- **Why it matters**: partition pruning is the cheapest data-skipping
  technique. If partitioned scan isn't materially faster, partition values
  are too coarse (one file per partition) or the column isn't selective.

### Z-Order on `repo_id`
- **Setup**: Same dataset, two copies. One has `OPTIMIZE … ZORDER BY repo_id`
  applied; the other is unoptimized.
- **Query**: `count(*) WHERE repo_id = 42`.
- **Why it matters**: Z-Ordering co-locates rows with similar `repo_id`
  values, enabling Delta's min/max statistics to skip whole files. Expected
  improvement is dataset-dependent — measure before claiming wins.

### Broadcast vs shuffle join
- **Setup**: 100k-row fact joined to a small dim by `repo_id`.
- **Query**: same join, with and without `F.broadcast(dim)`.
- **Why it matters**: when the dim fits in driver memory, broadcasting it
  avoids a shuffle on the fact. AQE often picks this automatically, but
  explicit hints make the intent reviewable in code.

## Results

| Date | Scenario | Variant | Rows | Duration (s) | Notes |
| --- | --- | --- | --- | --- | --- |
| _tba_ | _tba_ | _tba_ | _tba_ | _tba_ | run `make benchmark --output docs/performance.md` |
