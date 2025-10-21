#!/usr/bin/env python3
"""
Final relationship discovery fix for TPC-H schema.

This script provides a comprehensive test with corrected table structure
and debugging of the exact matching process.
"""

import sys
import os
from typing import List, Dict, Any

# Add the project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from semantic_model_generator.relationships.discovery import (
    discover_relationships_from_table_definitions,
    RelationshipDiscoveryResult
)


def create_corrected_tpch_tables() -> List[Dict[str, Any]]:
    """Create properly structured TPC-H table definitions with correct primary keys."""

    tables = [
        {
            "table_name": "CUSTOMER",
            "columns": [
                {"name": "C_CUSTKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "C_NAME", "type": "STRING"},
                {"name": "C_ADDRESS", "type": "STRING"},
                {"name": "C_NATIONKEY", "type": "NUMBER"},  # FK to NATION.N_NATIONKEY
                {"name": "C_PHONE", "type": "STRING"},
                {"name": "C_ACCTBAL", "type": "NUMBER"},
                {"name": "C_MKTSEGMENT", "type": "STRING"},
                {"name": "C_COMMENT", "type": "STRING"}
            ]
        },
        {
            "table_name": "ORDERS",
            "columns": [
                {"name": "O_ORDERKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "O_CUSTKEY", "type": "NUMBER"},  # FK to CUSTOMER.C_CUSTKEY
                {"name": "O_ORDERSTATUS", "type": "STRING"},
                {"name": "O_TOTALPRICE", "type": "NUMBER"},
                {"name": "O_ORDERDATE", "type": "DATE"},
                {"name": "O_ORDERPRIORITY", "type": "STRING"},
                {"name": "O_CLERK", "type": "STRING"},
                {"name": "O_SHIPPRIORITY", "type": "NUMBER"},
                {"name": "O_COMMENT", "type": "STRING"}
            ]
        },
        {
            "table_name": "LINEITEM",
            "columns": [
                {"name": "L_ORDERKEY", "type": "NUMBER", "is_primary_key": True},  # FK to ORDERS.O_ORDERKEY
                {"name": "L_PARTKEY", "type": "NUMBER", "is_primary_key": True},   # FK to PART.P_PARTKEY
                {"name": "L_SUPPKEY", "type": "NUMBER", "is_primary_key": True},   # FK to SUPPLIER.S_SUPPKEY
                {"name": "L_LINENUMBER", "type": "NUMBER", "is_primary_key": True},
                {"name": "L_QUANTITY", "type": "NUMBER"},
                {"name": "L_EXTENDEDPRICE", "type": "NUMBER"},
                {"name": "L_DISCOUNT", "type": "NUMBER"},
                {"name": "L_TAX", "type": "NUMBER"},
                {"name": "L_RETURNFLAG", "type": "STRING"},
                {"name": "L_LINESTATUS", "type": "STRING"},
                {"name": "L_SHIPDATE", "type": "DATE"},
                {"name": "L_COMMITDATE", "type": "DATE"},
                {"name": "L_RECEIPTDATE", "type": "DATE"},
                {"name": "L_SHIPINSTRUCT", "type": "STRING"},
                {"name": "L_SHIPMODE", "type": "STRING"},
                {"name": "L_COMMENT", "type": "STRING"}
            ]
        },
        {
            "table_name": "PART",
            "columns": [
                {"name": "P_PARTKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "P_NAME", "type": "STRING"},
                {"name": "P_MFGR", "type": "STRING"},
                {"name": "P_BRAND", "type": "STRING"},
                {"name": "P_TYPE", "type": "STRING"},
                {"name": "P_SIZE", "type": "NUMBER"},
                {"name": "P_CONTAINER", "type": "STRING"},
                {"name": "P_RETAILPRICE", "type": "NUMBER"},
                {"name": "P_COMMENT", "type": "STRING"}
            ]
        },
        {
            "table_name": "SUPPLIER",
            "columns": [
                {"name": "S_SUPPKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "S_NAME", "type": "STRING"},
                {"name": "S_ADDRESS", "type": "STRING"},
                {"name": "S_NATIONKEY", "type": "NUMBER"},  # FK to NATION.N_NATIONKEY
                {"name": "S_PHONE", "type": "STRING"},
                {"name": "S_ACCTBAL", "type": "NUMBER"},
                {"name": "S_COMMENT", "type": "STRING"}
            ]
        },
        {
            "table_name": "PARTSUPP",
            "columns": [
                {"name": "PS_PARTKEY", "type": "NUMBER", "is_primary_key": True},    # FK to PART.P_PARTKEY
                {"name": "PS_SUPPKEY", "type": "NUMBER", "is_primary_key": True},   # FK to SUPPLIER.S_SUPPKEY
                {"name": "PS_AVAILQTY", "type": "NUMBER"},
                {"name": "PS_SUPPLYCOST", "type": "NUMBER"},
                {"name": "PS_COMMENT", "type": "STRING"}
            ]
        },
        {
            "table_name": "NATION",
            "columns": [
                {"name": "N_NATIONKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "N_NAME", "type": "STRING"},
                {"name": "N_REGIONKEY", "type": "NUMBER"},  # FK to REGION.R_REGIONKEY
                {"name": "N_COMMENT", "type": "STRING"}
            ]
        },
        {
            "table_name": "REGION",
            "columns": [
                {"name": "R_REGIONKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "R_NAME", "type": "STRING"},
                {"name": "R_COMMENT", "type": "STRING"}
            ]
        }
    ]

    return tables


def test_with_verbose_analysis(confidence_threshold: float = 0.3) -> None:
    """Test with detailed analysis of each potential match."""

    print(f"\n🔍 TESTING WITH CONFIDENCE THRESHOLD: {confidence_threshold}")
    print("=" * 80)

    table_definitions = create_corrected_tpch_tables()

    try:
        result = discover_relationships_from_table_definitions(
            table_definitions,
            default_workspace="TPC_H",
            default_schema="PUBLIC",
            min_confidence=confidence_threshold,
            timeout_seconds=30.0
        )

        print(f"\n📊 RESULTS:")
        print(f"   Tables: {result.summary.total_tables}")
        print(f"   Relationships: {result.summary.total_relationships_found}")
        print(f"   Processing time: {result.summary.processing_time_ms}ms")

        if result.relationships:
            print(f"\n🔗 DISCOVERED RELATIONSHIPS:")
            for i, rel in enumerate(result.relationships, 1):
                print(f"\n   {i}. {rel.name}")
                print(f"      Type: {rel.relationship_type}")
                for col in rel.relationship_columns:
                    print(f"      {rel.left_table}.{col.left_column} → {rel.right_table}.{col.right_column}")

        # Analyze specific problematic cases
        print(f"\n🎯 SPECIFIC CASE ANALYSIS:")

        expected_correct = [
            ("CUSTOMER.C_NATIONKEY", "NATION.N_NATIONKEY"),
            ("ORDERS.O_CUSTKEY", "CUSTOMER.C_CUSTKEY"),
            ("LINEITEM.L_ORDERKEY", "ORDERS.O_ORDERKEY"),
            ("LINEITEM.L_PARTKEY", "PART.P_PARTKEY"),
            ("NATION.N_REGIONKEY", "REGION.R_REGIONKEY"),
        ]

        found_relationships = set()
        for rel in result.relationships:
            for col in rel.relationship_columns:
                fk = f"{rel.left_table}.{col.left_column}"
                pk = f"{rel.right_table}.{col.right_column}"
                found_relationships.add((fk, pk))

        for fk, pk in expected_correct:
            found = (fk, pk) in found_relationships
            status = "✅ FOUND" if found else "❌ MISSING"
            print(f"   {status}: {fk} → {pk}")

        # Check for incorrect matches
        print(f"\n❌ INCORRECT MATCHES TO INVESTIGATE:")
        incorrect_patterns = [
            "C_NATIONKEY → N_REGIONKEY",
            "C_NATIONKEY → R_REGIONKEY",
            "S_NATIONKEY → N_REGIONKEY",
            "S_NATIONKEY → R_REGIONKEY",
        ]

        for rel in result.relationships:
            for col in rel.relationship_columns:
                match_desc = f"{col.left_column} → {col.right_column}"
                if any(pattern in match_desc for pattern in incorrect_patterns):
                    print(f"   🔥 INCORRECT: {rel.left_table}.{col.left_column} → {rel.right_table}.{col.right_column}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


def test_simple_case() -> None:
    """Test with a simple 3-table case to isolate the problem."""

    print("\n🧪 SIMPLE 3-TABLE TEST")
    print("=" * 80)

    simple_tables = [
        {
            "table_name": "CUSTOMER",
            "columns": [
                {"name": "C_CUSTKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "C_NATIONKEY", "type": "NUMBER"},  # Should match N_NATIONKEY
            ]
        },
        {
            "table_name": "NATION",
            "columns": [
                {"name": "N_NATIONKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "N_REGIONKEY", "type": "NUMBER"},  # Should match R_REGIONKEY
            ]
        },
        {
            "table_name": "REGION",
            "columns": [
                {"name": "R_REGIONKEY", "type": "NUMBER", "is_primary_key": True},
            ]
        }
    ]

    print("Expected relationships:")
    print("  ✅ CUSTOMER.C_NATIONKEY → NATION.N_NATIONKEY")
    print("  ✅ NATION.N_REGIONKEY → REGION.R_REGIONKEY")
    print("  ❌ CUSTOMER.C_NATIONKEY → REGION.R_REGIONKEY (should NOT happen)")

    result = discover_relationships_from_table_definitions(
        simple_tables,
        min_confidence=0.3,
        timeout_seconds=10.0
    )

    print(f"\nActual results ({result.summary.total_relationships_found} relationships):")
    for rel in result.relationships:
        for col in rel.relationship_columns:
            print(f"  {rel.left_table}.{col.left_column} → {rel.right_table}.{col.right_column}")


def main():
    """Run comprehensive tests."""

    print("🔧 FINAL TPC-H RELATIONSHIP DISCOVERY FIX")
    print("Testing corrected implementation with proper debugging\n")

    # Test simple case first
    test_simple_case()

    # Test with different confidence thresholds
    for threshold in [0.2, 0.3, 0.4, 0.5]:
        test_with_verbose_analysis(threshold)

    print("\n" + "=" * 80)
    print("🎯 ANALYSIS COMPLETE")
    print("=" * 80)
    print("The results show whether our enhanced algorithm correctly")
    print("prioritizes C_NATIONKEY → N_NATIONKEY over incorrect matches.")


if __name__ == "__main__":
    main()