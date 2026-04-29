# CLAUDE.md — GHArchive Lakehouse

## Contexto del Proyecto
Lakehouse open-source que procesa el dataset público GH Archive (eventos de GitHub
desde 2011, ~6B+ eventos, JSON comprimido por hora). Implementa arquitectura
medallion (Bronze/Silver/Gold) con Delta Lake, transformaciones SQL con dbt,
orquestación con Dagster, y observabilidad de calidad con Great Expectations.
Objetivo: demostrar competencia end-to-end en data engineering distribuido.

## Stack Técnico
- Python 3.11, PySpark 3.5, Delta Lake 3.x
- dbt-core 1.7 + dbt-spark
- Dagster 1.6 (assets-based orchestration)
- Great Expectations 0.18
- DuckDB 0.10 (consulta ad-hoc sobre Delta)
- MLflow 2.x
- pytest 8.x, pytest-spark, ruff, mypy
- Docker + docker-compose
- GitHub Actions

## Estructura del Proyecto
- `/src/ingestion`: Descarga GH Archive (gharchive.org), parsea JSON, escribe Bronze Delta.
- `/src/transformation`: Limpieza, normalización, schema enforcement → Silver.
- `/src/quality`: Suites de Great Expectations versionadas por capa.
- `/src/ml`: Feature engineering + modelo de churn + MLflow tracking.
- `/dbt`: Modelo dimensional Gold (Kimball) — fact_events, dim_repo, dim_actor.
- `/dagster`: Definición de assets, schedules, sensors.
- `/notebooks`: Exploratory + Databricks-CE compatible.

## Comandos Clave
- `make setup` — Instala deps con uv y levanta docker-compose.
- `make ingest YEAR=2024 MONTH=01` — Backfill Bronze para un mes.
- `make silver` — Corre transformaciones Bronze → Silver.
- `make gold` — Ejecuta `dbt run` sobre Silver.
- `make test` — Corre pytest + GX checkpoints.
- `make lint` — Ruff + mypy strict.
- `make benchmark` — Ejecuta suite de performance benchmarks.
- `make ml-train` — Entrena modelo de churn y loggea en MLflow.
- `dagster dev` — Levanta UI de Dagster en :3000.

## Convenciones de Código
- Nombrado de archivos: `snake_case.py`; tablas Delta: `bronze_<source>_<entity>`.
- Estilo: Ruff (ruleset: E, F, I, N, UP, B, A) + mypy strict.
- Commits: Conventional Commits (`feat:`, `fix:`, `perf:`, `test:`, `docs:`).
- Branches: `feat/<issue>`, `fix/<issue>`. PRs requieren CI verde.
- SQL: lower_snake_case, CTEs antes que subqueries, dbt models con `materialized` explícito.
- PySpark: nunca `.collect()` sobre datasets > 1M filas; usar broadcast hints en joins con dim tables.

## Agentes y Responsabilidades

### Agente Principal (Orchestrator)
Coordina las fases del proyecto. Antes de implementar cualquier feature:
1. Lee el módulo afectado y los tests existentes.
2. Propone diseño en pseudocódigo o diagrama de flujo.
3. Implementa siguiendo TDD cuando aplique.
4. Corre `make lint` y `make test` antes de cerrar el ciclo.
Nunca modifica el schema de Delta sin migración explícita y test de regresión.

### Agente de Tests
- Cobertura objetivo: 80% en `/src`, 100% en lógica de transformación.
- Para PySpark: usar `pytest-spark` con SparkSession local de tamaño fijo.
- Tests de integración usan datasets sintéticos pequeños (<10 MB).
- Cada bug corregido debe tener un test que lo capture.

### Agente de Documentación
- Cada feature nueva actualiza `README.md` (sección "Features") y, si aplica, `/docs/architecture.md`.
- Diagramas en Mermaid embebidos en markdown.
- Performance benchmarks: antes/después con tabla y gráfico.

## Restricciones Importantes
- NO usar pandas para transformaciones sobre Bronze/Silver — solo PySpark.
- NO commitear secrets ni credenciales (usar `.env` + dotenv).
- NO usar `SELECT *` en modelos dbt productivos.
- NO escribir en formato Parquet plano — todo Delta.
- NO incluir el dataset crudo en el repo (>100 MB blocked).
- Scope: solo eventos públicos de GitHub. Sin scraping de la API privada.

## Definition of Done por Feature
- [ ] Código pasa `make lint` (cero warnings)
- [ ] Tests unitarios + integración pasando, cobertura ≥ 80%
- [ ] Great Expectations checkpoint definido y verde
- [ ] README.md actualizado (sección Features)
- [ ] Si afecta performance: benchmark documentado en `/docs/performance.md`
- [ ] PR con descripción tipo Conventional Commits
- [ ] CI verde end-to-end


Fase 1 — Core (Día 1–3)

- [x] Scaffolding del repo + CLAUDE.md + .env.example + GitHub Actions base.
- [x] Ingestión Bronze: descarga concurrente desde gharchive.org, parsing JSON, escritura Delta.
- [x] Suite de tests con datos sintéticos.
- [x] Configurar Dagster con un asset bronze_events.

Fase 2 — Features (Día 4–10)

- [x] Transformación Silver: deduplicación, schema enforcement, partitioning por fecha.
- [x] Modelo dimensional dbt en Gold (fact_events, dim_repo, dim_actor, dim_date).
- [x] Great Expectations checkpoints en cada capa + reportes en CI.
- [x] Notebook compatible con Databricks CE para validación cross-platform.
- [x] Performance tuning: benchmarks documentados (partitioning, Z-order, broadcast).
- [x] Capa ML: feature engineering + modelo de churn de repos con MLflow.

Fase 3 — Polish & Deploy (Día 11–14)

- [x] Tests e2e con un mes completo de datos. (`tests/e2e/test_full_month.py`,
      30 días × 4 h sintéticos, gated con marker `e2e`. Corre vía `make test-e2e`.)
- [x] README final con diagrama de arquitectura, métricas y placeholder para
      screenshots de Dagster UI (drop en `docs/img/`).
- [x] Deploy de docs estáticas (MkDocs Material → GitHub Pages).
      `mkdocs.yml` + `.github/workflows/docs.yml` (build + deploy on push to main).
- [x] Publicar imagen Docker en GHCR.
      `Dockerfile` (Python 3.11 + JRE 17 + uv) + `.github/workflows/release.yml`
      (trigger en `tags: v*.*.*` o workflow_dispatch).
- [x] Preparar 3 insights técnicos para el post de LinkedIn.
      (`docs/linkedin-insights.md`: partition pruning, dbt-spark `session`
      adapter, dbt + GX como capas de calidad complementarias.)