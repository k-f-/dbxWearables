"""Table Configuration Loader for SDP Pipeline.

Reads DDL metadata from YAML fixture files and provides helper functions
to generate schema DDL strings, expectation dicts, and constraint clauses
for use with dp.create_streaming_table() and related SDP APIs.

Usage in pipeline code:
    from lib.table_config import load_table_config, build_schema_ddl

    cfg = load_table_config("bronze_typed_health_samples")
    schema_ddl = build_schema_ddl(cfg)

    dp.create_streaming_table(
        name=cfg["table"]["name"],
        comment=cfg["table"]["comment"].strip(),
        schema=schema_ddl,
        table_properties=cfg["table"]["properties"],
        cluster_by=cfg["table"]["cluster_by"],
        expect_all_or_drop=cfg["expectations"]["drop"],
        expect_all=cfg["expectations"]["warn"],
    )
"""

import os
import yaml
from typing import Any


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

# Resolve fixtures/ddl/ relative to this file's location.
# This file lives at: src/pipelines/lib/table_config.py
# Fixtures live at:   fixtures/ddl/
# Relative path:      ../../../fixtures/ddl/
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_FIXTURES_DIR = os.path.normpath(os.path.join(_THIS_DIR, "..", "..", "..", "fixtures", "ddl"))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_table_config(table_name: str) -> dict[str, Any]:
    """Load and return the YAML config dict for the given table name.

    Args:
        table_name: The table name (without .yml extension).
                    e.g. "bronze_typed_health_samples"

    Returns:
        Parsed YAML dict with keys: table, columns, constraints, expectations.

    Raises:
        FileNotFoundError: If the YAML fixture file does not exist.
    """
    path = os.path.join(_FIXTURES_DIR, f"{table_name}.yml")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"DDL fixture not found: {path}\n"
            f"Searched in: {_FIXTURES_DIR}\n"
            f"Available files: {os.listdir(_FIXTURES_DIR) if os.path.isdir(_FIXTURES_DIR) else 'DIR NOT FOUND'}"
        )
    with open(path, "r") as f:
        return yaml.safe_load(f)


def build_schema_ddl(cfg: dict[str, Any], *, include_constraints: bool = True) -> str:
    """Build a DDL schema string from the YAML config.

    Generates a CREATE TABLE column specification with:
    - Column names, types, NOT NULL constraints
    - Column COMMENT clauses
    - PRIMARY KEY table constraint (if include_constraints=True)

    Args:
        cfg: Parsed YAML config dict (output of load_table_config).
        include_constraints: Whether to append PK constraint. Default True.

    Returns:
        DDL string suitable for dp.create_streaming_table(schema=...).
        Example: "record_id STRING NOT NULL COMMENT '...', uuid STRING COMMENT '...', ..."
    """
    columns = cfg.get("columns", [])
    constraints_cfg = cfg.get("constraints", {})

    col_defs = []
    for col in columns:
        parts = [col["name"], col["type"]]
        if col.get("nullable") is False:
            parts.append("NOT NULL")
        if col.get("comment"):
            # Escape single quotes in comments
            comment_escaped = col["comment"].replace("'", "\\'")
            parts.append(f"COMMENT '{comment_escaped}'")
        col_defs.append(" ".join(parts))

    # Add PK constraint if defined
    if include_constraints:
        pk = constraints_cfg.get("primary_key")
        if pk and pk.get("columns"):
            pk_name = pk.get("name", f"{cfg['table']['name']}_pk")
            pk_cols = ", ".join(pk["columns"])
            col_defs.append(f"CONSTRAINT {pk_name} PRIMARY KEY ({pk_cols})")

    return ", ".join(col_defs)


def get_table_comment(cfg: dict[str, Any]) -> str:
    """Return the table comment, stripped of leading/trailing whitespace."""
    return cfg.get("table", {}).get("comment", "").strip()


def get_table_properties(cfg: dict[str, Any]) -> dict[str, str]:
    """Return the table_properties dict from config."""
    return cfg.get("table", {}).get("properties", {})


def get_cluster_by(cfg: dict[str, Any]) -> list[str]:
    """Return the cluster_by column list from config."""
    return cfg.get("table", {}).get("cluster_by", [])


def get_expectations_drop(cfg: dict[str, Any]) -> dict[str, str]:
    """Return the expect_all_or_drop dict (hard expectations)."""
    return cfg.get("expectations", {}).get("drop", {})


def get_expectations_warn(cfg: dict[str, Any]) -> dict[str, str]:
    """Return the expect_all dict (soft expectations)."""
    return cfg.get("expectations", {}).get("warn", {})


def get_foreign_keys_ddl(cfg: dict[str, Any], fqn_prefix: str = "") -> list[str]:
    """Generate FK constraint DDL strings for post-creation application.

    Note: FK constraints may not be applicable on pipeline-managed tables
    at creation time. These are provided for documentation and potential
    future use when the platform supports it.

    Args:
        cfg: Parsed YAML config dict.
        fqn_prefix: Fully qualified name prefix (e.g. "catalog.schema.").
                    Prepended to referenced table names.

    Returns:
        List of FK constraint DDL strings (without ALTER TABLE prefix).
    """
    constraints_cfg = cfg.get("constraints", {})
    fks = constraints_cfg.get("foreign_keys", [])
    results = []
    for fk in fks:
        fk_name = fk["name"]
        fk_cols = ", ".join(fk["columns"])
        ref_table = f"{fqn_prefix}{fk['references']['table']}"
        ref_cols = ", ".join(fk["references"]["columns"])
        results.append(
            f"CONSTRAINT {fk_name} FOREIGN KEY ({fk_cols}) REFERENCES {ref_table} ({ref_cols})"
        )
    return results
