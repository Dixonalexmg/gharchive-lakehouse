"""Smoke tests that verify the package imports cleanly."""

from __future__ import annotations


def test_src_imports() -> None:
    import src  # noqa: F401
    import src.benchmarks  # noqa: F401
    import src.benchmarks.run  # noqa: F401
    import src.ingestion  # noqa: F401
    import src.ingestion.config  # noqa: F401
    import src.ingestion.gharchive  # noqa: F401
    import src.ingestion.run  # noqa: F401
    import src.ingestion.spark  # noqa: F401
    import src.ml  # noqa: F401
    import src.ml.train  # noqa: F401
    import src.quality  # noqa: F401
    import src.quality.run_checkpoints  # noqa: F401
    import src.transformation  # noqa: F401
    import src.transformation.run  # noqa: F401


def test_dagster_definitions_loadable() -> None:
    from dagster_project.definitions import defs

    assert defs is not None
