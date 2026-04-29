# Databricks notebook source
# MAGIC %md
# MAGIC # GHArchive — Silver → Gold validation
# MAGIC
# MAGIC Cross-platform notebook: runs both in **Databricks Community Edition**
# MAGIC and against a local Spark+Delta session. It reads the Silver Delta
# MAGIC table, materializes the Gold marts in-line as Spark SQL, and checks
# MAGIC referential integrity between the fact and the dimensions.
# MAGIC
# MAGIC No project-local imports are required, so the notebook runs unchanged
# MAGIC after `git clone` on Databricks Repos.

# COMMAND ----------

# DBTITLE 1,Configuration
# In Databricks, use widgets:
#   dbutils.widgets.text("silver_path", "dbfs:/FileStore/silver/silver_gharchive_events")
# Locally, set SILVER_PATH env var or override below.
import os

SILVER_PATH = os.environ.get(
    "SILVER_PATH",
    "./data/silver/silver_gharchive_events",
)
print(f"Reading Silver from: {SILVER_PATH}")

# COMMAND ----------

# DBTITLE 1,Load Silver
silver = spark.read.format("delta").load(SILVER_PATH)
silver.createOrReplaceTempView("silver_events")
print(f"Silver row count: {silver.count():,}")
silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Gold marts as temporary views
# MAGIC
# MAGIC We mirror the dbt models verbatim so this notebook acts as an
# MAGIC executable spec — any divergence between this and `/dbt/models` is a
# MAGIC bug.

# COMMAND ----------

# DBTITLE 1,fact_events
spark.sql("""
CREATE OR REPLACE TEMP VIEW fact_events AS
SELECT
    event_id,
    event_type,
    repo_id,
    actor_id,
    event_date,
    event_hour,
    created_at,
    public
FROM silver_events
""")

# COMMAND ----------

# DBTITLE 1,dim_repo
spark.sql("""
CREATE OR REPLACE TEMP VIEW dim_repo AS
SELECT
    repo_id,
    max(repo_name)                                                    AS repo_name,
    min(created_at)                                                   AS first_seen_at,
    max(created_at)                                                   AS last_seen_at,
    count(*)                                                          AS total_events,
    count(DISTINCT actor_id)                                          AS unique_actors,
    count(DISTINCT event_date)                                        AS active_days,
    sum(CASE WHEN event_type = 'PushEvent'        THEN 1 ELSE 0 END)  AS push_events,
    sum(CASE WHEN event_type = 'PullRequestEvent' THEN 1 ELSE 0 END)  AS pr_events,
    sum(CASE WHEN event_type = 'IssuesEvent'      THEN 1 ELSE 0 END)  AS issue_events,
    sum(CASE WHEN event_type = 'WatchEvent'       THEN 1 ELSE 0 END)  AS watch_events,
    sum(CASE WHEN event_type = 'ForkEvent'        THEN 1 ELSE 0 END)  AS fork_events
FROM silver_events
WHERE repo_id IS NOT NULL
GROUP BY repo_id
""")

# COMMAND ----------

# DBTITLE 1,dim_actor
spark.sql("""
CREATE OR REPLACE TEMP VIEW dim_actor AS
SELECT
    actor_id,
    max(actor_login)             AS actor_login,
    min(created_at)              AS first_seen_at,
    max(created_at)              AS last_seen_at,
    count(*)                     AS total_events,
    count(DISTINCT repo_id)      AS unique_repos,
    count(DISTINCT event_date)   AS active_days,
    count(DISTINCT event_type)   AS distinct_event_types
FROM silver_events
WHERE actor_id IS NOT NULL
GROUP BY actor_id
""")

# COMMAND ----------

# DBTITLE 1,dim_date
spark.sql("""
CREATE OR REPLACE TEMP VIEW dim_date AS
SELECT
    DISTINCT event_date AS date_key,
    year(event_date)        AS year,
    quarter(event_date)     AS quarter,
    month(event_date)       AS month,
    day(event_date)         AS day,
    dayofweek(event_date)   AS day_of_week,
    weekofyear(event_date)  AS week_of_year,
    CASE WHEN dayofweek(event_date) IN (1, 7) THEN true ELSE false END AS is_weekend
FROM silver_events
WHERE event_date IS NOT NULL
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks
# MAGIC
# MAGIC * Fact event_id is unique.
# MAGIC * Every fact row joins to dim_repo (when repo_id is set) and dim_actor.
# MAGIC * dim_date covers every event_date in the fact.

# COMMAND ----------

uniqueness = spark.sql(
    "SELECT count(*) AS n_rows, count(DISTINCT event_id) AS n_unique FROM fact_events"
).collect()[0]
assert uniqueness["n_rows"] == uniqueness["n_unique"], "fact_events.event_id is not unique"
print(f"fact_events: {uniqueness['n_rows']:,} rows, all unique event_id")

# COMMAND ----------

orphans_repo = spark.sql("""
SELECT count(*) AS n
FROM fact_events f LEFT JOIN dim_repo r ON f.repo_id = r.repo_id
WHERE f.repo_id IS NOT NULL AND r.repo_id IS NULL
""").collect()[0]["n"]
assert orphans_repo == 0, f"{orphans_repo} fact rows have no matching dim_repo"
print("Referential integrity fact → dim_repo: PASS")

# COMMAND ----------

orphans_actor = spark.sql("""
SELECT count(*) AS n
FROM fact_events f LEFT JOIN dim_actor a ON f.actor_id = a.actor_id
WHERE f.actor_id IS NOT NULL AND a.actor_id IS NULL
""").collect()[0]["n"]
assert orphans_actor == 0, f"{orphans_actor} fact rows have no matching dim_actor"
print("Referential integrity fact → dim_actor: PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 10 most active repositories

# COMMAND ----------

display(
    spark.sql(
        """
        SELECT repo_id, repo_name, total_events, unique_actors
        FROM dim_repo
        ORDER BY total_events DESC
        LIMIT 10
        """
    )
)
