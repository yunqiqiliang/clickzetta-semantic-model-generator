from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
from loguru import logger

from semantic_model_generator.clickzetta_utils.clickzetta_connector import (
    _TABLE_NAME_COL,
    get_table_representation,
    get_valid_schemas_tables_columns_df,
)
from semantic_model_generator.data_processing import data_types
from semantic_model_generator.data_processing.data_types import Column, FQNParts, Table
from semantic_model_generator.generate_model import (
    _DEFAULT_N_SAMPLE_VALUES_PER_COL,
    _infer_relationships,
)
from semantic_model_generator.protos import semantic_model_pb2

try:  # pragma: no cover - optional dependency for type checking
    from clickzetta.zettapark.session import Session
except Exception:  # pragma: no cover
    Session = Any  # type: ignore

DEFAULT_MAX_WORKERS = 4


@dataclass
class RelationshipSummary:
    total_tables: int
    total_columns: int
    total_relationships_found: int
    processing_time_ms: int
    limited_by_timeout: bool = False
    limited_by_max_relationships: bool = False
    limited_by_table_cap: bool = False
    notes: Optional[str] = None


@dataclass
class RelationshipDiscoveryResult:
    relationships: List[semantic_model_pb2.Relationship]
    tables: List[Table]
    summary: RelationshipSummary


def _normalize_table_names(table_names: Optional[Iterable[str]]) -> Optional[List[str]]:
    if table_names is None:
        return None
    normalized: List[str] = []
    for name in table_names:
        parts = [
            part.strip().strip("`").strip('"')
            for part in str(name).split(".")
            if part and part.strip()
        ]
        normalized.append(".".join(parts))
    return normalized


def _build_tables_from_dataframe(
    session: Session,
    workspace: str,
    schema: str,
    columns_df: pd.DataFrame,
    sample_values_per_column: int,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> List[Tuple[FQNParts, Table]]:
    if columns_df.empty:
        return []

    if _TABLE_NAME_COL not in columns_df.columns:
        raise KeyError(
            f"Expected '{_TABLE_NAME_COL}' column in metadata dataframe. "
            "Ensure information_schema query returned table names."
        )

    table_order = (
        columns_df[_TABLE_NAME_COL].astype(str).str.upper().drop_duplicates().tolist()
    )

    tables: List[Tuple[FQNParts, Table]] = []
    for idx, table_name in enumerate(table_order):
        table_columns_df = columns_df[columns_df[_TABLE_NAME_COL] == table_name]
        if table_columns_df.empty:
            continue

        max_workers_for_table = min(max_workers, len(table_columns_df.index) or 1)
        table_proto = get_table_representation(
            session=session,
            workspace=workspace,
            schema_name=schema,
            table_name=table_name,
            table_index=idx,
            ndv_per_column=sample_values_per_column,
            columns_df=table_columns_df,
            max_workers=max_workers_for_table,
        )
        tables.append(
            (
                FQNParts(database=workspace, schema_name=schema, table=table_name),
                table_proto,
            )
        )

    return tables


def _tables_payload_to_raw_tables(
    tables: Sequence[Mapping[str, Any]],
    *,
    default_workspace: str = "OFFLINE",
    default_schema: str = "PUBLIC",
) -> List[Tuple[FQNParts, Table]]:
    raw_tables: List[Tuple[FQNParts, Table]] = []
    for table_index, table_entry in enumerate(tables):
        if not isinstance(table_entry, Mapping):
            raise TypeError("Each table definition must be a mapping of table metadata")

        raw_table_identifier = str(
            table_entry.get("table_name")
            or table_entry.get("name")
            or table_entry.get("table")
            or ""
        ).strip()
        if not raw_table_identifier:
            raise ValueError("Table definition missing 'table_name'")

        identifier_workspace, identifier_schema, identifier_table = _split_table_identifier(
            raw_table_identifier
        )

        workspace = str(
            table_entry.get("workspace")
            or table_entry.get("database")
            or identifier_workspace
            or default_workspace
        ).strip() or default_workspace
        schema = str(
            table_entry.get("schema")
            or table_entry.get("schema_name")
            or identifier_schema
            or default_schema
        ).strip() or default_schema

        table_name = identifier_table.strip()
        if not table_name:
            raise ValueError(f"Unable to parse table name from '{raw_table_identifier}'")

        columns_payload = table_entry.get("columns")
        if not isinstance(columns_payload, Sequence) or not columns_payload:
            raise ValueError(
                f"Table '{table_name}' must include a non-empty 'columns' list"
            )

        columns: List[Column] = []
        for column_index, column_entry in enumerate(columns_payload):
            if not isinstance(column_entry, Mapping):
                raise TypeError(
                    f"Column definition for table '{table_name}' must be a mapping"
                )

            column_name = str(
                column_entry.get("name")
                or column_entry.get("column_name")
                or column_entry.get("field")
                or ""
            ).strip()
            if not column_name:
                raise ValueError(
                    f"Column definition in table '{table_name}' missing 'name'"
                )

            column_type = str(
                column_entry.get("type")
                or column_entry.get("data_type")
                or "STRING"
            ).strip()

            values = column_entry.get("sample_values") or column_entry.get("values")
            if isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
                sample_values = [str(value) for value in values]
            else:
                sample_values = None

            is_primary = bool(
                column_entry.get("is_primary_key")
                or column_entry.get("primary_key")
                or column_entry.get("is_primary")
            )

            columns.append(
                Column(
                    id_=column_index,
                    column_name=column_name,
                    column_type=column_type,
                    values=sample_values,
                    comment=column_entry.get("comment"),
                    is_primary_key=is_primary,
                )
            )

        table_proto = Table(
            id_=table_index,
            name=table_name.upper(),
            columns=columns,
            comment=table_entry.get("comment"),
        )
        fqn = FQNParts(
            database=workspace.upper(),
            schema_name=schema.upper(),
            table=table_name,
        )
        raw_tables.append((fqn, table_proto))

    return raw_tables


def _discover_relationships(
    raw_tables: List[Tuple[FQNParts, Table]],
    strict_join_inference: bool,
    session: Optional[Session],
    *,
    max_relationships: Optional[int] = None,
    min_confidence: float = 0.5,
    timeout_seconds: Optional[float] = None,
) -> Tuple[List[semantic_model_pb2.Relationship], Dict[str, bool]]:
    if not raw_tables:
        return [], {"limited_by_timeout": False, "limited_by_max_relationships": False}

    status: Dict[str, bool] = {}
    relationships = _infer_relationships(
        raw_tables,
        session=session if strict_join_inference else None,
        strict_join_inference=strict_join_inference,
        status=status,
        max_relationships=max_relationships,
        min_confidence=min_confidence,
        timeout_seconds=timeout_seconds,
    )
    return relationships, status


def discover_relationships_from_tables(
    tables: Sequence[Tuple[FQNParts, Table]],
    *,
    strict_join_inference: bool = False,
    session: Optional[Session] = None,
    max_relationships: Optional[int] = None,
    min_confidence: float = 0.5,
    timeout_seconds: Optional[float] = 30.0,
    max_tables: Optional[int] = None,
) -> RelationshipDiscoveryResult:
    """
    Run relationship inference using pre-constructed table metadata.
    """
    start = time.perf_counter()
    raw_tables = list(tables)
    limited_by_table_cap = False
    notes: List[str] = []

    if max_tables is not None and len(raw_tables) > max_tables:
        limited_by_table_cap = True
        notes.append(
            f"Input contained {len(raw_tables)} tables; analysis limited to first {max_tables}."
        )
        raw_tables = raw_tables[:max_tables]

    relationships, status = _discover_relationships(
        raw_tables,
        strict_join_inference=strict_join_inference,
        session=session,
        max_relationships=max_relationships,
        min_confidence=min_confidence,
        timeout_seconds=timeout_seconds,
    )
    end = time.perf_counter()

    all_columns = sum(len(table.columns) for _, table in raw_tables)
    summary = RelationshipSummary(
        total_tables=len(raw_tables),
        total_columns=all_columns,
        total_relationships_found=len(relationships),
        processing_time_ms=int((end - start) * 1000),
        limited_by_timeout=status.get("limited_by_timeout", False),
        limited_by_max_relationships=status.get("limited_by_max_relationships", False),
        limited_by_table_cap=limited_by_table_cap,
        notes=" ".join(notes) if notes else None,
    )

    return RelationshipDiscoveryResult(
        relationships=relationships,
        tables=[table for _, table in raw_tables],
        summary=summary,
    )


def discover_relationships_from_table_definitions(
    table_definitions: Sequence[Mapping[str, Any]],
    *,
    default_workspace: str = "OFFLINE",
    default_schema: str = "PUBLIC",
    strict_join_inference: bool = False,
    session: Optional[Session] = None,
    max_relationships: Optional[int] = None,
    min_confidence: float = 0.5,
    timeout_seconds: Optional[float] = 15.0,
    max_tables: Optional[int] = None,
) -> RelationshipDiscoveryResult:
    """Run relationship inference using raw table metadata dictionaries."""

    raw_tables = _tables_payload_to_raw_tables(
        table_definitions,
        default_workspace=default_workspace,
        default_schema=default_schema,
    )

    return discover_relationships_from_tables(
        raw_tables,
        strict_join_inference=strict_join_inference,
        session=session,
        max_relationships=max_relationships,
        min_confidence=min_confidence,
        timeout_seconds=timeout_seconds,
        max_tables=max_tables,
    )


def discover_relationships_from_schema(
    session: Session,
    workspace: str,
    schema: str,
    *,
    table_names: Optional[Sequence[str]] = None,
    sample_values_per_column: int = _DEFAULT_N_SAMPLE_VALUES_PER_COL,
    strict_join_inference: bool = False,
    max_workers: int = DEFAULT_MAX_WORKERS,
    max_relationships: Optional[int] = None,
    min_confidence: float = 0.5,
    timeout_seconds: Optional[float] = 30.0,
    max_tables: Optional[int] = 60,
) -> RelationshipDiscoveryResult:
    """
    Discover table relationships for all tables in a ClickZetta schema.
    """
    normalized_tables = _normalize_table_names(table_names)

    metadata_df = get_valid_schemas_tables_columns_df(
        session=session,
        workspace=workspace,
        table_schema=schema,
        table_names=normalized_tables,
    )
    metadata_df.columns = [str(col).upper() for col in metadata_df.columns]

    if metadata_df.empty:
        logger.warning(
            "No column metadata found for workspace={} schema={} tables={}",
            workspace,
            schema,
            table_names,
        )
        return RelationshipDiscoveryResult(
            relationships=[],
            tables=[],
            summary=RelationshipSummary(
                total_tables=0,
                total_columns=0,
                total_relationships_found=0,
                processing_time_ms=0,
            ),
        )

    raw_tables = _build_tables_from_dataframe(
        session=session,
        workspace=workspace,
        schema=schema,
        columns_df=metadata_df,
        sample_values_per_column=sample_values_per_column,
        max_workers=max_workers,
    )

    return discover_relationships_from_tables(
        raw_tables,
        strict_join_inference=strict_join_inference,
        session=session,
        max_relationships=max_relationships,
        min_confidence=min_confidence,
        timeout_seconds=timeout_seconds,
        max_tables=max_tables,
    )
def _split_table_identifier(identifier: str) -> Tuple[Optional[str], Optional[str], str]:
    """
    Split a table identifier that may include workspace/schema prefixes.

    Supported formats:
      - workspace.schema.table
      - schema.table
      - table
    """

    parts = [part.strip() for part in identifier.split(".") if part.strip()]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return None, None, parts[0]
