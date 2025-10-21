from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
import pytest

from semantic_model_generator.relationships.discovery import (
    discover_relationships_from_schema,
    discover_relationships_from_table_definitions,
)
from semantic_model_generator.generate_model import _analyze_composite_key_patterns


class _FakeResult:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def to_pandas(self) -> pd.DataFrame:
        return self._df.copy()


class _FakeSession:
    def __init__(self, tables: List[str], columns_df: pd.DataFrame):
        self.tables = tables
        self.columns_df = columns_df

    def sql(self, query: str):
        normalized = query.upper()
        if "SHOW CATALOGS" in normalized:
            return _FakeResult(
                pd.DataFrame(
                    {
                        "CATALOG_NAME": ["CLICKZETTA_SAMPLE_DATA"],
                        "CATEGORY": ["MANAGED"],
                    }
                )
            )
        if "INFORMATION_SCHEMA.COLUMNS" in normalized:
            return _FakeResult(self.columns_df)
        if "FROM INFORMATION_SCHEMA.TABLES" in normalized:
            data = {
                "TABLE_SCHEMA": ["TPCH_100G"] * len(self.tables),
                "TABLE_NAME": self.tables,
            }
            return _FakeResult(pd.DataFrame(data))
        if "SELECT DISTINCT" in normalized:
            # Return single column of sample values
            return _FakeResult(pd.DataFrame({"VALUE": [1, 2, 3]}))
        raise AssertionError(f"Unexpected query: {query}")


def _build_columns_df() -> pd.DataFrame:
    records: List[Dict[str, Any]] = []
    # Orders table
    records.extend(
        [
            {
                "TABLE_SCHEMA": "TPCH_100G",
                "TABLE_NAME": "ORDERS",
                "COLUMN_NAME": "ORDER_ID",
                "DATA_TYPE": "NUMBER",
                "IS_PRIMARY_KEY": True,
            },
            {
                "TABLE_SCHEMA": "TPCH_100G",
                "TABLE_NAME": "ORDERS",
                "COLUMN_NAME": "CUSTOMER_ID",
                "DATA_TYPE": "NUMBER",
                "IS_PRIMARY_KEY": False,
            },
        ]
    )
    # Customer table
    records.extend(
        [
            {
                "TABLE_SCHEMA": "TPCH_100G",
                "TABLE_NAME": "CUSTOMER",
                "COLUMN_NAME": "CUSTOMER_ID",
                "DATA_TYPE": "NUMBER",
                "IS_PRIMARY_KEY": True,
            },
            {
                "TABLE_SCHEMA": "TPCH_100G",
                "TABLE_NAME": "CUSTOMER",
                "COLUMN_NAME": "NAME",
                "DATA_TYPE": "STRING",
                "IS_PRIMARY_KEY": False,
            },
        ]
    )
    return pd.DataFrame.from_records(records)


def test_discover_relationships_from_schema_builds_relationships():
    tables = ["ORDERS", "CUSTOMER"]
    columns_df = _build_columns_df()
    session = _FakeSession(tables, columns_df)

    result = discover_relationships_from_schema(
        session=session,
        workspace="CLICKZETTA_SAMPLE_DATA",
        schema="TPCH_100G",
        strict_join_inference=False,
    )

    assert result.summary.total_tables == 2
    assert result.summary.total_relationships_found >= 1

    names = {rel.name for rel in result.relationships}
    assert any("ORDERS" in name and "CUSTOMER" in name for name in names)

    left_tables = {rel.left_table for rel in result.relationships}
    right_tables = {rel.right_table for rel in result.relationships}
    assert "ORDERS" in left_tables
    assert "CUSTOMER" in right_tables


def test_discover_relationships_from_table_definitions_allows_manual_metadata() -> None:
    payload = [
        {
            "table_name": "orders",
            "columns": [
                {"name": "order_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "customer_id", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "customers",
            "columns": [
                {"name": "customer_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "name", "type": "STRING"},
            ],
        },
    ]

    result = discover_relationships_from_table_definitions(
        payload,
        default_workspace="demo",
        default_schema="sales",
        max_relationships=5,
        timeout_seconds=5.0,
    )

    assert result.summary.total_tables == 2
    assert result.summary.total_relationships_found >= 1
    assert not result.summary.limited_by_timeout
    assert any(
        rel.left_table == "ORDERS" and rel.right_table == "CUSTOMERS"
        for rel in result.relationships
    )


def _order_items_orders_products_payload() -> List[Dict[str, Any]]:
    return [
        {
            "table_name": "order_items",
            "columns": [
                {"name": "order_item_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "order_reference", "type": "STRING"},
                {"name": "product_id", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "orders",
            "columns": [
                {"name": "order_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "customer_id", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "products",
            "columns": [
                {"name": "product_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "product_name", "type": "STRING"},
            ],
        },
    ]


def test_misaligned_id_relationships_are_filtered() -> None:
    payload = _order_items_orders_products_payload()

    result = discover_relationships_from_table_definitions(
        payload,
        min_confidence=0.6,
        max_relationships=10,
    )

    pairs = {(rel.left_table, rel.right_table) for rel in result.relationships}
    assert ("ORDER_ITEMS", "ORDERS") in pairs
    assert ("ORDER_ITEMS", "PRODUCTS") in pairs

    direct = [
        rel
        for rel in result.relationships
        if rel.left_table == "ORDERS" and rel.right_table == "PRODUCTS"
    ]
    assert direct, "Expected bridge-derived ORDERS -> PRODUCTS relationship"
    assert all("_via_" in rel.name.lower() for rel in direct)




def test_relationship_discovery_is_order_invariant() -> None:
    payload = _order_items_orders_products_payload()
    result_forward = discover_relationships_from_table_definitions(payload)

    result_reversed = discover_relationships_from_table_definitions(
        list(reversed(payload))
    )

    forward_pairs = {(rel.left_table, rel.right_table) for rel in result_forward.relationships}
    reversed_pairs = {(rel.left_table, rel.right_table) for rel in result_reversed.relationships}

    assert forward_pairs == reversed_pairs


def test_discover_relationships_from_table_definitions_filters_generic_ids() -> None:
    payload = [
        {
            "table_name": "table_a",
            "columns": [
                {"name": "id", "type": "NUMBER", "is_primary_key": True},
                {"name": "value", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "table_b",
            "columns": [
                {"name": "id", "type": "NUMBER", "is_primary_key": True},
                {"name": "value", "type": "NUMBER"},
            ],
        },
    ]

    result = discover_relationships_from_table_definitions(
        payload,
        min_confidence=0.6,
        max_relationships=5,
    )

    assert result.summary.total_relationships_found == 0
    assert not result.relationships


def test_table_definitions_support_fully_qualified_names() -> None:
    payload = [
        {
            "table_name": "demo.sales.orders",
            "columns": [
                {"name": "order_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "customer_id", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "sales.customers",
            "workspace": "demo",
            "columns": [
                {"name": "customer_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "name", "type": "STRING"},
            ],
        },
        {
            "table_name": "products",
            "workspace": "demo",
            "schema": "sales",
            "columns": [
                {"name": "product_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "name", "type": "STRING"},
            ],
        },
    ]

    result = discover_relationships_from_table_definitions(
        payload,
        default_workspace="fallback",
        default_schema="fallback_schema",
    )

    table_names = {table.name for table in result.tables}
    assert table_names == {"ORDERS", "CUSTOMERS", "PRODUCTS"}

    # Ensure relationships include the orders -> customers edge despite mixed identifiers
    assert any(
        rel.left_table == "ORDERS" and rel.right_table == "CUSTOMERS"
        for rel in result.relationships
    )


def test_generic_id_columns_do_not_join_unrelated_tables() -> None:
    payload = [
        {
            "table_name": "users",
            "columns": [
                {"name": "id", "type": "NUMBER", "is_primary_key": True},
                {"name": "name", "type": "STRING"},
            ],
        },
        {
            "table_name": "posts",
            "columns": [
                {"name": "id", "type": "NUMBER", "is_primary_key": True},
                {"name": "user_id", "type": "NUMBER"},
                {"name": "title", "type": "STRING"},
            ],
        },
        {
            "table_name": "comments",
            "columns": [
                {"name": "id", "type": "NUMBER", "is_primary_key": True},
                {"name": "post_id", "type": "NUMBER"},
                {"name": "user_id", "type": "NUMBER"},
            ],
        },
    ]

    result = discover_relationships_from_table_definitions(
        payload,
        min_confidence=0.6,
        max_relationships=10,
    )

    pairs = {(rel.left_table, rel.right_table) for rel in result.relationships}
    assert ("POSTS", "USERS") in pairs
    assert len(pairs) == 1
    # ensure bridge relationship references COMMENTS
    bridge_names = [rel.name for rel in result.relationships if rel.left_table == "POSTS" and rel.right_table == "USERS"]
    assert bridge_names and all("_VIA_" in name.upper() for name in bridge_names)


def test_shared_id_columns_without_prefix_are_rejected() -> None:
    payload = [
        {
            "table_name": "orders",
            "columns": [
                {"name": "id", "type": "NUMBER", "is_primary_key": True},
                {"name": "order_date", "type": "DATE"},
            ],
        },
        {
            "table_name": "products",
            "columns": [
                {"name": "id", "type": "NUMBER", "is_primary_key": True},
                {"name": "order_reference", "type": "NUMBER"},
            ],
        },
    ]

    result = discover_relationships_from_table_definitions(
        payload,
        min_confidence=0.4,
        max_relationships=5,
    )

    pairs = {(rel.left_table, rel.right_table) for rel in result.relationships}
    assert not pairs  # No relationships expected



def test_custom_table_name_variants_are_detected() -> None:
    payload = [
        {
            "table_name": "entity_alpha",
            "columns": [
                {"name": "alpha_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "alpha_name", "type": "STRING"},
            ],
        },
        {
            "table_name": "entity_beta",
            "columns": [
                {"name": "beta_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "alpha_id", "type": "NUMBER"},
            ],
        },
    ]

    result = discover_relationships_from_table_definitions(payload)
    pairs = {(rel.left_table, rel.right_table) for rel in result.relationships}
    assert ("ENTITY_BETA", "ENTITY_ALPHA") in pairs



def test_suffix_match_without_semantic_prefix_is_ignored() -> None:
    payload = [
        {
            "table_name": "orders",
            "columns": [
                {"name": "order_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "customer_id", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "products",
            "columns": [
                {"name": "product_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "order_reference", "type": "STRING"},
            ],
        },
    ]

    result = discover_relationships_from_table_definitions(
        payload,
        min_confidence=0.6,
        max_relationships=10,
    )

    pairs = {(rel.left_table, rel.right_table) for rel in result.relationships}
    assert ("PRODUCTS", "ORDERS") not in pairs


def test_dictionary_iteration_order_for_nation_table() -> None:
    """
    Test to demonstrate dictionary iteration order issue with NATION table columns.

    This test shows that when we have columns like n_regionkey and n_nationkey,
    the iteration order can cause n_regionkey to be checked before n_nationkey.
    If the algorithm uses 'continue' after finding the first valid match,
    it may select n_regionkey instead of the better match n_nationkey.
    """
    # Simulate NATION table with columns in a specific order
    nation_columns = {
        "n_nationkey": {"type": "NUMBER", "names": ["N_NATIONKEY"]},
        "n_name": {"type": "STRING", "names": ["N_NAME"]},
        "n_regionkey": {"type": "NUMBER", "names": ["N_REGIONKEY"]},
        "n_comment": {"type": "STRING", "names": ["N_COMMENT"]},
    }

    # Test iteration order
    iteration_order = list(nation_columns.keys())
    print(f"Dictionary iteration order: {iteration_order}")

    # In Python 3.7+, dictionaries maintain insertion order
    # So the iteration order should be: n_nationkey, n_name, n_regionkey, n_comment
    assert iteration_order == ["n_nationkey", "n_name", "n_regionkey", "n_comment"]


def test_relationship_discovery_selects_best_match_not_first_match() -> None:
    """
    Test that relationship discovery selects the best matching column,
    not just the first valid column it encounters during iteration.

    CUSTOMER.c_nationkey should match NATION.n_nationkey (exact match),
    not NATION.n_regionkey (weaker match that might appear earlier in iteration).
    """
    payload = [
        {
            "table_name": "customer",
            "columns": [
                {"name": "c_custkey", "type": "NUMBER", "is_primary_key": True},
                {"name": "c_nationkey", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "nation",
            "columns": [
                # Deliberately order n_regionkey before n_nationkey to test iteration order
                {"name": "n_regionkey", "type": "NUMBER"},
                {"name": "n_nationkey", "type": "NUMBER", "is_primary_key": True},
                {"name": "n_name", "type": "STRING"},
            ],
        },
    ]

    result = discover_relationships_from_table_definitions(
        payload,
        min_confidence=0.6,
        max_relationships=10,
    )

    # Find the relationship from CUSTOMER to NATION
    customer_nation_rels = [
        rel for rel in result.relationships
        if rel.left_table == "CUSTOMER" and rel.right_table == "NATION"
    ]

    assert len(customer_nation_rels) >= 1, "Expected at least one CUSTOMER -> NATION relationship"

    # The relationship should use n_nationkey (best match), not n_regionkey
    for rel in customer_nation_rels:
        # relationship_columns is a list of RelationKey objects with left_column and right_column
        right_cols = [rc.right_column for rc in rel.relationship_columns]
        assert "N_NATIONKEY" in right_cols, (
            f"Expected relationship to use N_NATIONKEY, but got {right_cols}. "
            f"This indicates the algorithm selected the first valid match (n_regionkey) "
            f"instead of the best match (n_nationkey)."
        )


def test_composite_pk_analysis_uses_correct_column_side() -> None:
    """
    Regression: ensure composite key analysis counts PK coverage on the correct table side.

    Before the fix, the right-side table always inspected column_pairs[0], producing zero
    coverage and causing legitimate composite relationships to be dropped.
    """
    table_meta = {
        "columns": {
            "order_id": {"names": ["ORDER_ID"], "base_type": "NUMBER"},
            "line_id": {"names": ["LINE_ID"], "base_type": "NUMBER"},
        },
        "pk_candidates": {
            "order_id": ["ORDER_ID"],
            "line_id": ["LINE_ID"],
        },
    }
    column_pairs = [
        ("L_ORDER_ID", "ORDER_ID"),
        ("L_LINE_ID", "LINE_ID"),
    ]

    analysis = _analyze_composite_key_patterns(
        table_meta,
        column_pairs,
        column_index=1,
    )

    # Both PK columns should be detected, yielding full coverage.
    assert analysis["pk_column_count"] == 2
    assert analysis["pk_coverage_ratio"] == pytest.approx(1.0)
    assert analysis["is_composite_pk"]
