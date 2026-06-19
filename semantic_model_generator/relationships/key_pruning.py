"""Progressive relationship-discovery pruning (Stage 0 + Stage 1 + Stage 2).

This module implements the cheap, early stages of the funnel described in
``docs/design/relationship_discovery_production_design.md`` (in the MCP server
repo):

  Stage 0  role filtering        zero scan   -> keep only join-key-like columns
  Stage 1  statistical profiling  1 scan/table -> prune by cardinality / range
  Stage 2  HLL approximate IND    1-2 scans   -> coverage via approx_count_distinct

The functions here are intentionally *pure* (no DB session required) so they can
be unit-tested offline. SQL builders return strings the caller executes against
ClickZetta. All SQL uses only verified ClickZetta features:
``approx_count_distinct`` (HyperLogLog), ``MIN``/``MAX``/``COUNT``.

Design goal: at 100 tables / 2000 columns, only profile the *key-like* columns
and only run inclusion checks on candidates that survive cardinality + range
pruning, cutting expensive operations from O(columns) / O(column-pairs) down to
O(surviving-candidates).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from semantic_model_generator.generate_model import (
    _base_type_from_type,
    _could_be_identifier_column,
    _is_identifier_like,
    _looks_like_primary_key,
    _should_exclude_from_relationship_matching,
)

# Base types that can act as an equi-join key. Float/decimal/date/bool/complex
# types are deliberately excluded as *direct* key candidates.
_KEY_BASE_TYPES = {
    "NUMBER",
    "BIGINT",
    "INTEGER",
    "INT",
    "SMALLINT",
    "TINYINT",
    "LONG",
    "STRING",
    "VARCHAR",
    "CHAR",
    "TEXT",  # short codes sometimes typed TEXT; range/inclusion stages refine this
}

_NUMERIC_BASE_TYPES = {
    "NUMBER",
    "BIGINT",
    "INTEGER",
    "INT",
    "SMALLINT",
    "TINYINT",
    "LONG",
}
_STRING_BASE_TYPES = {"STRING", "VARCHAR", "CHAR", "TEXT"}


def _type_family(base_type: str) -> Optional[str]:
    bt = (base_type or "").upper()
    if bt in _NUMERIC_BASE_TYPES:
        return "numeric"
    if bt in _STRING_BASE_TYPES:
        return "string"
    return None


# ---------------------------------------------------------------------------
# Stage 0 - role filtering (zero scan)
# ---------------------------------------------------------------------------


def is_join_key_candidate(
    column_name: str,
    column_type: str,
    *,
    is_primary_key: bool = False,
    table_name: str = "",
) -> bool:
    """Return True if a column could plausibly take part in a join key.

    Conservative by design: never drops a primary key, and keeps anything that
    looks identifier-like. Drops obvious non-keys (free text, measures,
    timestamps, names) via the shared exclusion heuristic so they are never
    profiled or inclusion-checked.
    """
    if not column_name:
        return False

    base_type = _base_type_from_type(column_type)

    # Primary keys always qualify (they are the parent side of relationships).
    if is_primary_key:
        return True

    # Type must be join-key-able at all.
    if base_type.upper() not in _KEY_BASE_TYPES:
        return False

    # Drop clearly non-key columns (created_at, amount, name, description, ...).
    if _should_exclude_from_relationship_matching(column_name, base_type):
        return False

    # Keep identifier-like columns and table-aware key candidates.
    if _is_identifier_like(column_name, base_type):
        return True
    if _could_be_identifier_column(column_name, base_type, table_name):
        return True
    if table_name and _looks_like_primary_key(table_name, column_name):
        return True

    # Keep natural-key and sequence suffixes that are common FK/PK columns in
    # operational schemas (ERP, accounting, HR).  These are intentionally broader
    # than what _could_be_identifier_column accepts so that key_prefilter doesn't
    # discard composite-key columns like txn_seq, acct_code, parent_code, ref_seq.
    from semantic_model_generator.generate_model import _identifier_tokens
    tokens = _identifier_tokens(column_name)
    _NATURAL_KEY_TOKENS = {"CODE", "SEQ", "NUM", "NO", "NR", "NBR", "REF", "IDX"}
    if any(t in _NATURAL_KEY_TOKENS for t in tokens):
        return True

    # Keep integer columns that passed the exclude check — integer columns are
    # almost always potential FK/PK surrogates (e.g. order_date BIGINT which is
    # a date surrogate key, not a real date).  Non-integer columns reaching this
    # point are too ambiguous to keep without a clearer naming signal.
    if base_type.upper() in _NUMERIC_BASE_TYPES:
        return True

    return False


def filter_key_columns(
    columns: Sequence["ColumnRef"],
) -> List["ColumnRef"]:
    """Stage 0: keep only the join-key-like columns from a list."""
    return [
        c
        for c in columns
        if is_join_key_candidate(
            c.column_name,
            c.column_type,
            is_primary_key=c.is_primary_key,
            table_name=c.table_name,
        )
    ]


@dataclass(frozen=True)
class ColumnRef:
    table_name: str
    column_name: str
    column_type: str
    is_primary_key: bool = False

    @property
    def base_type(self) -> str:
        return _base_type_from_type(self.column_type)


# ---------------------------------------------------------------------------
# Stage 1 - statistical profiling (one scan per table)
# ---------------------------------------------------------------------------


@dataclass
class ColumnProfile:
    table_name: str
    column_name: str
    base_type: str
    row_count: int
    ndv: int  # approx_count_distinct
    non_null: int
    min_value: Optional[object] = None
    max_value: Optional[object] = None

    @property
    def uniqueness(self) -> float:
        """NDV / non-null. ~1.0 means the column is (near) unique -> parent side."""
        if self.non_null <= 0:
            return 0.0
        return min(self.ndv / self.non_null, 1.0)

    @property
    def is_key_like(self) -> bool:
        # Near-unique and not a constant/near-constant column.
        return self.ndv >= 2 and self.uniqueness >= 0.9


def _qident(name: str) -> str:
    """Backtick-quote a ClickZetta identifier."""
    return "`" + str(name).replace("`", "") + "`"


def build_table_profile_sql(
    workspace: str,
    schema: str,
    table: str,
    columns: Sequence[str],
    *,
    sample_percent: Optional[float] = None,
) -> str:
    """Stage 1: profile a *whole table* in ONE query (single scan).

    Emits ``approx_count_distinct`` / ``COUNT`` / ``MIN`` / ``MAX`` per column,
    producing a single result row. Profiling 100 tables is therefore ~100
    queries rather than ~2000 per-column samples.

    ``sample_percent`` (0-100) optionally wraps the scan in ``TABLESAMPLE`` to
    cap cost on very large tables.
    """
    if not columns:
        raise ValueError("build_table_profile_sql requires at least one column")

    fqtn = f"{_qident(workspace)}.{_qident(schema)}.{_qident(table)}"

    exprs: List[str] = ["COUNT(*) AS row_count"]
    for col in columns:
        c = _qident(col)
        alias = str(col).replace("`", "")
        exprs.append(f"approx_count_distinct({c}) AS `ndv__{alias}`")
        exprs.append(f"COUNT({c}) AS `nn__{alias}`")
        exprs.append(f"MIN({c}) AS `min__{alias}`")
        exprs.append(f"MAX({c}) AS `max__{alias}`")

    select_list = ",\n  ".join(exprs)
    from_clause = fqtn
    if sample_percent is not None and 0 < sample_percent < 100:
        # ClickZetta TABLESAMPLE syntax.
        from_clause = f"{fqtn} TABLESAMPLE ({sample_percent} PERCENT)"

    return f"SELECT\n  {select_list}\nFROM {from_clause}"


def parse_table_profile_row(
    table_name: str,
    columns: Sequence[str],
    column_types: Dict[str, str],
    row: Dict[str, object],
) -> List[ColumnProfile]:
    """Turn a single profile result row into per-column ColumnProfile objects."""
    profiles: List[ColumnProfile] = []
    row_count = int(row.get("row_count", row.get("ROW_COUNT", 0)) or 0)
    for col in columns:
        alias = str(col).replace("`", "")

        def _get(prefix: str):
            return row.get(f"{prefix}__{alias}", row.get(f"{prefix}__{alias}".upper()))

        ndv = int(_get("ndv") or 0)
        non_null = int(_get("nn") or 0)
        profiles.append(
            ColumnProfile(
                table_name=table_name,
                column_name=col,
                base_type=_base_type_from_type(column_types.get(col, "")),
                row_count=row_count,
                ndv=ndv,
                non_null=non_null,
                min_value=_get("min"),
                max_value=_get("max"),
            )
        )
    return profiles


def can_contain(
    child: ColumnProfile,
    parent: ColumnProfile,
    *,
    ndv_tolerance: float = 1.05,
    require_parent_keylike: bool = True,
) -> bool:
    """Stage 1 pruning: could ``child`` values be a subset of ``parent`` values?

    Cheap necessary conditions (not sufficient - Stage 2/3 confirm):
      * compatible type family
      * NDV(child) <= NDV(parent) * tolerance   (HLL error band)
      * parent is (near) unique -> it is the referenced/key side
      * numeric/comparable range containment when min/max available
    """
    cf = _type_family(child.base_type)
    pf = _type_family(parent.base_type)
    if cf is None or pf is None or cf != pf:
        return False

    if child.ndv <= 0 or parent.ndv <= 0:
        return False

    # A child key with only 1 distinct value carries no relationship signal.
    if child.ndv < 2:
        return False

    if child.ndv > parent.ndv * ndv_tolerance:
        return False

    if require_parent_keylike and not parent.is_key_like:
        return False

    # Range containment (only meaningful for numeric; strings skipped here and
    # left to Stage 2 inclusion).
    if cf == "numeric":
        try:
            if (
                child.min_value is not None
                and parent.min_value is not None
                and float(child.min_value) < float(parent.min_value)
            ):
                return False
            if (
                child.max_value is not None
                and parent.max_value is not None
                and float(child.max_value) > float(parent.max_value)
            ):
                return False
        except (TypeError, ValueError):
            # Non-castable values -> don't prune on range, defer to Stage 2.
            pass

    return True


def prune_candidate_pairs(
    profiles: Dict[Tuple[str, str], ColumnProfile],
    candidate_pairs: Sequence[Tuple[Tuple[str, str], Tuple[str, str]]],
    **kwargs,
) -> List[Tuple[Tuple[str, str], Tuple[str, str]]]:
    """Filter (child, parent) candidate pairs by ``can_contain``.

    ``profiles`` is keyed by ``(table_name, column_name)``. Pairs missing a
    profile are kept (cannot prune without data) so recall is not hurt.
    """
    survivors: List[Tuple[Tuple[str, str], Tuple[str, str]]] = []
    for child_key, parent_key in candidate_pairs:
        child = profiles.get(child_key)
        parent = profiles.get(parent_key)
        if child is None or parent is None:
            survivors.append((child_key, parent_key))
            continue
        if can_contain(child, parent, **kwargs):
            survivors.append((child_key, parent_key))
    return survivors


# ---------------------------------------------------------------------------
# Stage 2 - HLL approximate inclusion (FAIDA-style, no JOIN)
# ---------------------------------------------------------------------------


def build_inclusion_sql(
    child: ColumnRef,
    parent: ColumnRef,
    workspace: str,
    schema: str,
) -> str:
    """Stage 2: approximate ``child subset of parent`` coverage with no JOIN.

    Uses the FAIDA identity: child ⊆ parent  <=>  NDV(parent) == NDV(child ∪ parent).
    Returns ndv_child / ndv_parent / ndv_union; caller computes
    ``coverage = (ndv_child + ndv_parent - ndv_union) / ndv_child``.
    """
    c_tbl = f"{_qident(workspace)}.{_qident(schema)}.{_qident(child.table_name)}"
    p_tbl = f"{_qident(workspace)}.{_qident(schema)}.{_qident(parent.table_name)}"
    c_col = _qident(child.column_name)
    p_col = _qident(parent.column_name)

    return (
        "WITH u AS (\n"
        f"  SELECT DISTINCT {c_col} AS k FROM {c_tbl} WHERE {c_col} IS NOT NULL\n"
        "  UNION ALL\n"
        f"  SELECT DISTINCT {p_col} AS k FROM {p_tbl} WHERE {p_col} IS NOT NULL\n"
        ")\n"
        "SELECT\n"
        f"  (SELECT approx_count_distinct({c_col}) FROM {c_tbl}) AS ndv_child,\n"
        f"  (SELECT approx_count_distinct({p_col}) FROM {p_tbl}) AS ndv_parent,\n"
        "  approx_count_distinct(k) AS ndv_union\n"
        "FROM u"
    )


def inclusion_coverage(ndv_child: int, ndv_parent: int, ndv_union: int) -> float:
    """Approximate fraction of child distinct values found in parent (0..1)."""
    if ndv_child <= 0:
        return 0.0
    intersection = ndv_child + ndv_parent - ndv_union
    return max(0.0, min(intersection / ndv_child, 1.0))
