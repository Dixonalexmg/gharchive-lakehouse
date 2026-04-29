# LinkedIn insights

Three takeaways from building the GHArchive Lakehouse — written to be
copy-pasted into a LinkedIn post and to hold up under engineer scrutiny.

---

## 1) The cheapest data-skipping technique is the partition column you already have

Delta on `event_date` cut the `WHERE event_date = …` scan dramatically vs the
flat layout — same rows, same Spark, only metadata changed. Most teams reach
for Z-Order or bloom filters before they've measured what plain partition
pruning is doing for them. **Partition first, optimize last.** Layer
Z-Ordering only on the *next* most selective column (here: `repo_id`) and
benchmark before/after — see [Performance](performance.md).

> Lesson: a one-line `partitionBy("event_date")` paid for itself before any
> "performance tuning" PR landed.

---

## 2) `dbt-spark` + `session` adapter beats running a Thrift Server locally

Most dbt-spark tutorials ask you to stand up a Spark Thrift Server, expose
port 10000, and configure ODBC. For a self-contained lakehouse, that's
infrastructure no one wants to debug at 23:47. The **`session` method**
calls `SparkSession.builder.getOrCreate()` in-process — so if you boot a
Delta-aware session *before* invoking `dbtRunner`, dbt reuses it.

```python
spark = get_spark(app_name="gharchive-gold")  # Delta extensions configured
dbtRunner().invoke(["build", "--project-dir", "dbt", "--profiles-dir", "dbt"])
```

`make gold` is now a single Python entrypoint with no Thrift dependency.
Production can still point dbt at a real cluster — only the profile
changes.

> Lesson: prefer the in-process adapter for local + CI. Save the Thrift
> Server for the case it actually pays for itself (multi-tenant BI).

---

## 3) dbt tests and Great Expectations don't overlap — keep both

Teams pick *one* quality framework and call it covered. They're solving
different problems:

| Concern | dbt tests | Great Expectations |
| --- | --- | --- |
| Schema, uniqueness, referential integrity, not-null | ✅ native | ✅ native |
| Column-value distributions, ranges, set membership, drift | manual macros | ✅ native |
| **Where it runs** | inside the dbt graph | as a separate checkpoint |
| **What it gates** | model materialization | the lakehouse layer's contract |

The `fact_events.event_id is unique` invariant is verified by *both*
frameworks in this repo — and that's intentional. They're independent
proofs from different code paths. If one passes and the other fails,
you've found a framework bug instead of a data bug. Both run in CI; both
fail the build.

> Lesson: redundancy in quality checks is a feature, not waste.

---

*Built with PySpark 3.5, Delta Lake 3.x, dbt-core 1.x, Dagster 1.x, Great
Expectations 1.x. Source: [github.com/Dixonalexmg/gharchive-lakehouse](https://github.com/Dixonalexmg/gharchive-lakehouse).*
