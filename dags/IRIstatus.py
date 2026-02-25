from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from rdflib import Graph
from rdflib.namespace import OWL, RDF
from rdflib.util import guess_format
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from airflow.sdk import dag, task, Variable

log = logging.getLogger(__name__)

# ---------------------------
# Config
# ---------------------------

IRIS = [
    "https://w3id.org/pmd/co/",
    "http://purls.helmholtz-metadaten.de/mwo/",
    "https://nfdi.fiz-karlsruhe.de/ontology",
    "https://w3id.org/pmd/vto/",
    "https://w3id.org/pmd/tto/tto-full.owl",
    "https://w3id.org/pmd/co/2.0.7",
    "https://w3id.org/pmd/fsp/",
    "https://w3id.org/pmd/log/",
    "https://w3id.org/pmd/hto/hto-base.owl",
    "https://w3id.org/pmd/materials-mechanics-ontology",
]

RDF_MEDIA_TYPES = {
    "application/rdf+xml": "xml",
    "text/turtle": "turtle",
    "application/ld+json": "json-ld",
    "application/n-triples": "nt",
    "application/n-quads": "nquads",
    "text/n3": "n3",
}

ACCEPT_HEADERS = [
    "application/rdf+xml",
    "text/turtle",
    "application/ld+json",
    "application/n-triples",
    "text/n3",
]


# ---------------------------
# Helpers
# ---------------------------

def engine():
    """
      - sqlite:////data/ontology_results.db
    """
    dsn = Variable.get("iri_results_db")
    url = make_url(dsn)

    # Create parent directory only for file-based sqlite DBs
    if url.drivername.startswith("sqlite") and url.database:
        db_file = Path(url.database)
        db_file.parent.mkdir(parents=True, exist_ok=True)
        log.info("Ensured SQLite directory exists: %s", db_file.parent)

    return create_engine(dsn, pool_pre_ping=True)


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


def try_parse_rdf(content: bytes, content_type: str):
    media_type = (content_type or "").split(";")[0].strip()
    rdf_format = RDF_MEDIA_TYPES.get(media_type) or guess_format(media_type)

    if not rdf_format:
        return False, 0, "Unknown format", Graph()

    g = Graph()
    try:
        g.parse(data=content, format=rdf_format)
        return True, len(g), "Parsed", g
    except Exception as e:
        return False, 0, f"Parse error: {e}", g


def get_version_info(graph: Graph) -> tuple[str, str]:
    version_iri = ""
    prior_version = ""

    # Correct ontology discovery pattern
    for s in graph.subjects(RDF.type, OWL.Ontology):
        for v in graph.objects(s, OWL.versionIRI):
            version_iri = str(v)
        for p in graph.objects(s, OWL.priorVersion):
            prior_version = str(p)

    return version_iri, prior_version


def test_iri(iri: str) -> list[dict]:
    results = []

    for accept in ACCEPT_HEADERS:
        headers = {"Accept": accept}

        try:
            response = requests.get(
                iri,
                headers=headers,
                timeout=(10, 30),
                allow_redirects=True,
            )

            status = response.status_code
            final_url = response.url
            content_type = response.headers.get("Content-Type", "Unknown")

            if status == 200:
                parsed, triple_count, message, graph = try_parse_rdf(response.content, content_type)
                curr_version, prior_version = get_version_info(graph)
            else:
                parsed = False
                triple_count = 0
                message = f"HTTP {status}"
                curr_version = ""
                prior_version = ""

        except Exception as e:
            status = "ERROR"
            final_url = ""
            content_type = ""
            parsed = False
            triple_count = 0
            curr_version = ""
            prior_version = ""
            message = str(e)

        results.append(
            {
                "iri": iri,
                "accept_header": accept,
                "http_status": str(status),
                "final_url": final_url,
                "content_type": content_type,
                "parsed_successfully": int(bool(parsed)),
                "current_version": curr_version,
                "previous_version": prior_version,
                "triple_count": int(triple_count),
                "message": str(message)[:50000],  # avoid oversized values
            }
        )

    return results


# ---------------------------
# DAG
# ---------------------------

@dag(
    dag_id="ontology_iri_monitor",
    schedule="0 6 * * *",   # daily 06:00 UTC
    catchup=False,
    tags=["ontology", "rdf", "superset"],
)
def ontology_iri_monitor():

    @task
    def preflight():
        v = Variable.get("iri_results_db")
        log.info("Variable iri_results_db present=%s", v is not None)

        with engine().connect() as cx:
            ok = cx.execute(text("select 1")).scalar()
            log.info("DB OK: select 1 => %s", ok)

        log.info("Configured IRIs count=%d", len(IRIS))
        for iri in IRIS:
            log.info("IRI: %s", iri)

    @task
    def ensure_tables():
        """
        SQLite-compatible schema.
        Keep history in ontology_results, and expose ontology_results_latest view.
        """
        ddl = [
            """
            CREATE TABLE IF NOT EXISTS ontology_results (
              run_ts_utc TEXT NOT NULL,
              iri TEXT NOT NULL,
              accept_header TEXT NOT NULL,
              http_status TEXT,
              final_url TEXT,
              content_type TEXT,
              parsed_successfully INTEGER NOT NULL,
              current_version TEXT,
              previous_version TEXT,
              triple_count INTEGER NOT NULL,
              message TEXT
            );
            """,
            # Optional index for latest queries / filtering in Superset
            """
            CREATE INDEX IF NOT EXISTS idx_ontology_results_run_ts
            ON ontology_results (run_ts_utc);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_ontology_results_iri
            ON ontology_results (iri);
            """,
        ]

        eng = engine()
        with eng.begin() as cx:
            for stmt in ddl:
                cx.execute(text(stmt))

        log.info("Ensured ontology_results table/indexes exist")

    @task
    def run_checks() -> list[dict]:
        now = iso_utc_now()
        all_rows: list[dict] = []

        for i, iri in enumerate(IRIS, start=1):
            log.info("Processing %d/%d: %s", i, len(IRIS), iri)
            rows = test_iri(iri)
            for r in rows:
                r["run_ts_utc"] = now
            all_rows.extend(rows)

        log.info("Generated rows=%d for run_ts_utc=%s", len(all_rows), now)
        return all_rows

    @task
    def write_results(rows: list[dict]):
        if not rows:
            raise RuntimeError("No rows produced by run_checks")

        df = pd.DataFrame(rows)

        eng = engine()
        df.to_sql("ontology_results", eng, if_exists="append", index=False)
        log.info("Appended %d rows to ontology_results", len(df))

    @task
    def ensure_latest_view():
        """
        SQLite-compatible latest view:
        one latest row per (iri, accept_header)
        """
        ddl = [
            """
            DROP VIEW IF EXISTS ontology_results_latest;
            """,
            """
            CREATE VIEW ontology_results_latest AS
            SELECT
              run_ts_utc,
              iri,
              accept_header,
              http_status,
              final_url,
              content_type,
              parsed_successfully,
              current_version,
              previous_version,
              triple_count,
              message
            FROM (
              SELECT
                run_ts_utc,
                iri,
                accept_header,
                http_status,
                final_url,
                content_type,
                parsed_successfully,
                current_version,
                previous_version,
                triple_count,
                message,
                ROW_NUMBER() OVER (
                  PARTITION BY iri, accept_header
                  ORDER BY run_ts_utc DESC
                ) AS rn
              FROM ontology_results
            )
            WHERE rn = 1;
            """,
        ]

        eng = engine()
        with eng.begin() as cx:
            for stmt in ddl:
                cx.execute(text(stmt))

        log.info("Ensured ontology_results_latest view exists")

    @task
    def ensure_summary_views():
        """
        Optional helper views that make Superset charting easier.
        """
        ddl = [
            """
            DROP VIEW IF EXISTS ontology_iri_latest_summary;
            """,
            """
            CREATE VIEW ontology_iri_latest_summary AS
            SELECT
              iri,
              MAX(run_ts_utc) AS run_ts_utc,
              COUNT(*) AS accept_variants_tested,
              SUM(CASE WHEN http_status = '200' THEN 1 ELSE 0 END) AS http_200_count,
              SUM(CASE WHEN parsed_successfully = 1 THEN 1 ELSE 0 END) AS parsed_ok_count,
              MAX(triple_count) AS max_triple_count
            FROM ontology_results_latest
            GROUP BY iri;
            """,
        ]

        eng = engine()
        with eng.begin() as cx:
            for stmt in ddl:
                cx.execute(text(stmt))

        log.info("Ensured ontology_iri_latest_summary view exists")

    p = preflight()
    t = ensure_tables()
    r = run_checks()
    w = write_results(r)
    v1 = ensure_latest_view()
    v2 = ensure_summary_views()

    p >> t >> r >> w >> [v1, v2]


ontology_iri_monitor()