# LinkedIn post — draft

> Texto listo para copiar y pegar. Acompañar con 1–3 imágenes (ver
> "Adjuntos" al final). Hashtags al final del cuerpo.

---

## Versión larga (recomendada)

🚀 Acabo de publicar **GHArchive Lakehouse**, un proyecto open-source
end-to-end que procesa los ~6B+ eventos públicos de GitHub (dataset GH
Archive) usando arquitectura medallion sobre Delta Lake.

Lo construí para demostrar competencia real en data engineering moderno:
ingesta concurrente, transformaciones distribuidas, modelado dimensional,
calidad observada en cada capa y un modelo de ML on top — todo orquestado
y testeado end-to-end.

🧱 **Stack**
PySpark 3.5 · Delta Lake 3.x · dbt-spark · Dagster · Great Expectations ·
DuckDB · MLflow · Docker · GitHub Actions

🏗️ **Arquitectura medallion**
Bronze (JSON.gz crudo → Delta particionado por fecha) → Silver (schema
enforcement + dedup) → Gold (modelo Kimball con `fact_events`,
`dim_repo`, `dim_actor`, `dim_date`).

💡 **3 cosas que aprendí construyendo esto**

**1. La técnica de data-skipping más barata es la columna de partición que
ya tienes.** Antes de tocar Z-Order o bloom filters, mide qué te está
dando un simple `partitionBy("event_date")`. En este repo, la diferencia
de plain partition pruning vs flat layout fue inmediata — Z-Order solo
añadió valor sobre la *siguiente* columna más selectiva (`repo_id`).
Particionar primero, optimizar después.

**2. `dbt-spark` con el adapter `session` es muy superior al Thrift Server
para entornos locales/CI.** El tutorial estándar te pide levantar Thrift,
exponer puerto 10000 y configurar ODBC. El método `session` reusa la
`SparkSession` que ya tienes en proceso, así que `make gold` es un único
entrypoint Python sin infraestructura extra. Producción puede seguir
apuntando a un cluster real — solo cambia el profile de dbt.

**3. dbt tests y Great Expectations no se solapan: úsalos juntos.** dbt
cubre invariantes estructurales en el grafo (uniqueness, not-null,
referencial). GX cubre contratos de capa: distribuciones, rangos, drift,
membership. En este repo, la unicidad de `event_id` la verifican AMBOS
frameworks por código independiente — si uno pasa y el otro falla, has
encontrado un bug del framework, no de los datos. Ambos corren en CI;
ambos rompen el build.

🔗 **Repo:** https://github.com/Dixonalexmg/gharchive-lakehouse
📚 **Docs:** https://dixonalexmg.github.io/gharchive-lakehouse/
🐳 **Imagen:** `docker pull ghcr.io/dixonalexmg/gharchive-lakehouse:latest`

Feedback, críticas y PRs son bienvenidos. ¿Qué insight te gustaría que
explorara más a fondo en un próximo post?

#DataEngineering #Lakehouse #DeltaLake #dbt #Dagster #PySpark #OpenSource #MLflow #GreatExpectations

---

## Versión corta (alternativa, ~1300 chars)

🚀 Publiqué **GHArchive Lakehouse** — proyecto open-source que procesa
los ~6B+ eventos públicos de GitHub con arquitectura medallion sobre
Delta Lake.

🧱 **Stack:** PySpark · Delta · dbt · Dagster · Great Expectations ·
MLflow.

3 cosas que aprendí:

▸ La columna de partición que ya tienes es el data-skipping más barato.
  Mide partition pruning antes de tocar Z-Order.

▸ `dbt-spark` con adapter `session` > Thrift Server para local/CI. Cero
  infra extra, mismo `dbt build`.

▸ dbt tests y Great Expectations son complementarios, no excluyentes.
  Verificar la misma invariante por dos caminos = redundancia útil.

🔗 https://github.com/Dixonalexmg/gharchive-lakehouse
📚 https://dixonalexmg.github.io/gharchive-lakehouse/

#DataEngineering #Lakehouse #DeltaLake #dbt #Dagster #PySpark #OpenSource

---

## Adjuntos sugeridos para el post

Subir 1–3 imágenes (LinkedIn favorece posts con visual). En orden de
prioridad:

1. **`docs/img/dagster-assets.png`** — asset graph completo. Es la imagen
   "hero" más impactante: muestra el DAG end-to-end de un vistazo.
2. **Diagrama de arquitectura del README** — captura del Mermaid renderizado
   en GitHub o en el sitio MkDocs (sección "Architecture" del README).
3. **`docs/img/dagster-asset-detail.png`** — vista de detalle de
   `silver_events` con metadata de materialización + estado de particiones.

## Hora y canales

- **Mejor ventana:** martes a jueves, 9–11am o 5–6pm (zona horaria del
  autor) → mejor engagement orgánico.
- **Cross-post:** comparte en grupos de DataTalksClub, Locally Optimistic,
  comunidades hispanohablantes de data.
- **Primeras 2 horas:** responde TODO comentario rápido — el algoritmo
  amplifica posts con engagement temprano.
- **Opcional:** thread cruzado en X/Twitter linkeando al post de LinkedIn
  para tracción extra.
