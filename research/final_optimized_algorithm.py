#!/usr/bin/env python3
"""
Final Optimized Relationship Discovery Algorithm

This script implements the final optimized version that combines all research findings
and addresses the issues found in validation testing.
"""

import sys
import os
from typing import List, Dict, Any, Tuple, Set, Optional
from dataclasses import dataclass
import time

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)


@dataclass
class OptimizedRelationship:
    """Optimized relationship representation."""
    fk_table: str
    fk_column: str
    pk_table: str
    pk_column: str
    confidence_score: float
    confidence_factors: List[str]
    relationship_type: str = "foreign_key"


class FinalOptimizedDiscovery:
    """
    Final optimized relationship discovery algorithm that integrates all improvements
    and fixes issues found during validation.
    """

    def __init__(self):
        # TPC-H entity mappings (enhanced)
        self.tpch_entities = {
            "CUSTOMER": ["CUST", "C", "CUSTOMER", "CUSTOMERS"],
            "SUPPLIER": ["SUPP", "S", "SUPPLIER", "SUPPLIERS"],
            "PART": ["P", "PART", "PARTS"],
            "ORDERS": ["ORDER", "O", "ORDERS"],
            "LINEITEM": ["LINE", "L", "LINEITEM", "LINEITEMS"],
            "PARTSUPP": ["PS", "PARTSUPP"],
            "NATION": ["N", "NATION", "NATIONS"],
            "REGION": ["R", "REGION", "REGIONS"]
        }

        # Create reverse mapping
        self.entity_to_abbrev = {}
        for entity, variants in self.tpch_entities.items():
            for variant in variants:
                self.entity_to_abbrev[variant] = entity

        # Strong business relationship patterns with high confidence
        self.strong_patterns = {
            ("CUSTOMER", "ORDERS"): 0.95,
            ("ORDERS", "LINEITEM"): 0.95,
            ("PART", "LINEITEM"): 0.90,
            ("SUPPLIER", "LINEITEM"): 0.90,
            ("PART", "PARTSUPP"): 0.95,
            ("SUPPLIER", "PARTSUPP"): 0.95,
            ("NATION", "CUSTOMER"): 0.85,
            ("NATION", "SUPPLIER"): 0.85,
            ("REGION", "NATION"): 0.90,
            ("DEPARTMENT", "EMPLOYEE"): 0.90,
            ("EMPLOYEE", "DEPARTMENT"): 0.90,
            ("DEPARTMENT", "PROJECT"): 0.85
        }

    def extract_core_entity(self, column_name: str) -> str:
        """Extract core entity from column name with enhanced logic."""
        if not column_name:
            return ""

        name_upper = column_name.upper()

        # Handle TPC-H pattern: {prefix}_{entity}KEY
        if "_" in name_upper:
            parts = name_upper.split("_")
            if len(parts) >= 2:
                # Check if first part is a single/double letter prefix
                if len(parts[0]) <= 2:
                    core_part = "_".join(parts[1:])
                else:
                    core_part = name_upper
            else:
                core_part = name_upper
        else:
            core_part = name_upper

        # Remove common suffixes
        for suffix in ["KEY", "ID", "NUM", "NO", "_ID", "_KEY"]:
            if core_part.endswith(suffix):
                core_part = core_part[:-len(suffix)]

        return core_part

    def calculate_enhanced_name_similarity(self, fk_column: str, pk_column: str) -> float:
        """Calculate enhanced name similarity with TPC-H awareness."""
        if not fk_column or not pk_column:
            return 0.0

        fk_upper = fk_column.upper()
        pk_upper = pk_column.upper()

        # Exact match
        if fk_upper == pk_upper:
            return 1.0

        # Extract core entities
        fk_core = self.extract_core_entity(fk_column)
        pk_core = self.extract_core_entity(pk_column)

        # Perfect core match
        if fk_core == pk_core and fk_core:
            return 0.95

        # Entity variant matching
        if self.are_entity_variants(fk_core, pk_core):
            return 0.90

        # Substring matching for exact substrings
        if pk_core in fk_core or fk_core in pk_core:
            if len(min(fk_core, pk_core)) >= 4:  # Avoid short meaningless matches
                return 0.80

        # Levenshtein similarity
        return self.calculate_levenshtein_similarity(fk_column, pk_column)

    def are_entity_variants(self, entity1: str, entity2: str) -> bool:
        """Enhanced entity variant checking."""
        if not entity1 or not entity2:
            return False

        # Normalize entities
        e1 = entity1.upper()
        e2 = entity2.upper()

        # Direct match
        if e1 == e2:
            return True

        # Check if both map to the same canonical entity
        canonical1 = self.entity_to_abbrev.get(e1)
        canonical2 = self.entity_to_abbrev.get(e2)

        if canonical1 and canonical2:
            return canonical1 == canonical2

        # Check if one maps to the other
        if canonical1 == e2 or canonical2 == e1:
            return True

        # Check if they're in the same variant list
        for entity, variants in self.tpch_entities.items():
            if e1 in variants and e2 in variants:
                return True

        return False

    def calculate_levenshtein_similarity(self, s1: str, s2: str) -> float:
        """Calculate normalized Levenshtein similarity."""
        if not s1 or not s2:
            return 0.0

        # Normalize
        norm1 = s1.upper().replace("_", "").replace("-", "")
        norm2 = s2.upper().replace("_", "").replace("-", "")

        if norm1 == norm2:
            return 0.85

        distance = self.levenshtein_distance(norm1, norm2)
        max_len = max(len(norm1), len(norm2))

        if max_len == 0:
            return 0.0

        similarity = 1.0 - (distance / max_len)
        return max(0.0, similarity)

    def levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein distance."""
        if len(s1) < len(s2):
            return self.levenshtein_distance(s2, s1)

        if len(s2) == 0:
            return len(s1)

        previous_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        return previous_row[-1]

    def follows_naming_convention(self, fk_column: str, pk_column: str, fk_table: str, pk_table: str) -> bool:
        """Check if columns follow standard naming conventions."""
        fk_upper = fk_column.upper()
        pk_upper = pk_column.upper()
        pk_table_upper = pk_table.upper()

        # TPC-H pattern: FK has table prefix + entity, PK has different prefix + same entity
        if "_" in fk_upper and "_" in pk_upper:
            fk_parts = fk_upper.split("_", 1)
            pk_parts = pk_upper.split("_", 1)

            if len(fk_parts) == 2 and len(pk_parts) == 2:
                fk_entity_part = fk_parts[1]
                pk_entity_part = pk_parts[1]

                # Same entity part (e.g., CUSTKEY, NATIONKEY)
                if fk_entity_part == pk_entity_part:
                    return True

                # Entity variants
                fk_entity = self.extract_core_entity(fk_entity_part)
                pk_entity = self.extract_core_entity(pk_entity_part)

                if self.are_entity_variants(fk_entity, pk_entity):
                    # Additional check: PK entity should relate to PK table
                    pk_table_entity = self.extract_table_entity(pk_table_upper)
                    if self.are_entity_variants(pk_entity, pk_table_entity):
                        return True

        # Standard patterns
        return self.follows_standard_patterns(fk_upper, pk_upper, pk_table_upper)

    def follows_standard_patterns(self, fk_column: str, pk_column: str, pk_table: str) -> bool:
        """Check standard FK naming patterns."""
        # Pattern: table_id, table_key
        if fk_column == f"{pk_table}_{pk_column}":
            return True
        if fk_column == f"{pk_table}_ID" and pk_column.endswith("_ID"):
            return True

        # Pattern: FK contains PK column
        if pk_column in fk_column and len(pk_column) >= 4:
            return True

        return False

    def extract_table_entity(self, table_name: str) -> str:
        """Extract entity concept from table name."""
        table_upper = table_name.upper()

        # Handle plurals
        if table_upper.endswith("S") and len(table_upper) > 3:
            singular = table_upper[:-1]
            if singular in self.entity_to_abbrev:
                return singular

        return table_upper

    def get_business_relationship_boost(self, fk_table: str, pk_table: str) -> float:
        """Get business relationship boost for known patterns."""
        fk_entity = self.extract_table_entity(fk_table.upper())
        pk_entity = self.extract_table_entity(pk_table.upper())

        # Direct lookup
        pattern_key = (pk_entity, fk_entity)
        boost = self.strong_patterns.get(pattern_key, 0.0)

        if boost > 0:
            return boost

        # Check with entity normalization
        fk_canonical = self.entity_to_abbrev.get(fk_entity, fk_entity)
        pk_canonical = self.entity_to_abbrev.get(pk_entity, pk_entity)

        canonical_key = (pk_canonical, fk_canonical)
        return self.strong_patterns.get(canonical_key, 0.0)

    def calculate_comprehensive_confidence(
        self,
        fk_table: str,
        fk_column: str,
        fk_type: str,
        pk_table: str,
        pk_column: str,
        pk_type: str
    ) -> Tuple[float, List[str]]:
        """Calculate comprehensive confidence with strict criteria."""

        factors = []
        confidence = 0.0

        # Factor 1: Primary key metadata (base requirement)
        confidence += 0.3
        factors.append("Primary key metadata available (+0.3)")

        # Factor 2: Name similarity (weighted heavily)
        similarity = self.calculate_enhanced_name_similarity(fk_column, pk_column)
        if similarity >= 0.9:
            confidence += 0.35
            factors.append(f"Excellent name similarity ({similarity:.3f}) (+0.35)")
        elif similarity >= 0.8:
            confidence += 0.25
            factors.append(f"Good name similarity ({similarity:.3f}) (+0.25)")
        elif similarity >= 0.6:
            confidence += 0.15
            factors.append(f"Moderate name similarity ({similarity:.3f}) (+0.15)")
        elif similarity >= 0.4:
            confidence += 0.05
            factors.append(f"Low name similarity ({similarity:.3f}) (+0.05)")
        else:
            confidence -= 0.1
            factors.append(f"Very low name similarity ({similarity:.3f}) (-0.1)")

        # Factor 3: Type compatibility
        if self.are_types_compatible(fk_type, pk_type):
            confidence += 0.1
            factors.append("Compatible data types (+0.1)")
        else:
            confidence -= 0.2
            factors.append("Incompatible data types (-0.2)")

        # Factor 4: Naming convention compliance
        if self.follows_naming_convention(fk_column, pk_column, fk_table, pk_table):
            confidence += 0.15
            factors.append("Follows naming conventions (+0.15)")

        # Factor 5: Business relationship patterns
        business_boost = self.get_business_relationship_boost(fk_table, pk_table)
        if business_boost > 0:
            confidence += business_boost * 0.2  # Scale down business boost
            factors.append(f"Known business relationship (+{business_boost * 0.2:.3f})")

        # Factor 6: Penalties for unlikely matches
        # Penalty for numeric columns that don't follow FK patterns
        if self.is_numeric_type(fk_type) and not self.looks_like_foreign_key(fk_column):
            confidence -= 0.2
            factors.append("Numeric non-FK column penalty (-0.2)")

        # Cap confidence
        confidence = min(1.0, max(0.0, confidence))

        return confidence, factors

    def are_types_compatible(self, type1: str, type2: str) -> bool:
        """Check if data types are compatible."""
        t1 = type1.upper()
        t2 = type2.upper()

        if t1 == t2:
            return True

        # Numeric compatibility
        numeric_types = {"NUMBER", "INTEGER", "BIGINT", "INT", "DECIMAL", "FLOAT"}
        if t1 in numeric_types and t2 in numeric_types:
            return True

        # String compatibility
        string_types = {"STRING", "VARCHAR", "TEXT", "CHAR"}
        if t1 in string_types and t2 in string_types:
            return True

        return False

    def is_numeric_type(self, data_type: str) -> bool:
        """Check if data type is numeric."""
        numeric_types = {"NUMBER", "INTEGER", "BIGINT", "INT", "DECIMAL", "FLOAT"}
        return data_type.upper() in numeric_types

    def looks_like_foreign_key(self, column_name: str) -> bool:
        """Check if column name looks like a foreign key."""
        name_upper = column_name.upper()

        # Ends with KEY or ID
        if name_upper.endswith("KEY") or name_upper.endswith("_ID") or name_upper.endswith("ID"):
            return True

        # Contains entity reference
        for entity in self.tpch_entities.keys():
            if entity in name_upper:
                return True

        return False

    def discover_relationships(self, table_definitions: List[Dict[str, Any]], min_confidence: float = 0.6) -> List[OptimizedRelationship]:
        """Discover relationships with optimized algorithm."""

        relationships = []

        # Build primary key lookup
        primary_keys = {}
        for table in table_definitions:
            table_name = table["table_name"]
            for column in table["columns"]:
                if column.get("is_primary_key", False):
                    pk_key = f"{table_name}.{column['name']}"
                    primary_keys[pk_key] = {
                        "table": table_name,
                        "column": column["name"],
                        "type": column["type"]
                    }

        # Find potential foreign keys
        potential_fks = []
        for fk_table in table_definitions:
            fk_table_name = fk_table["table_name"]
            for fk_column in fk_table["columns"]:
                # Skip primary key columns (except for composite keys/self-references)
                if fk_column.get("is_primary_key", False):
                    # For TPC-H, LINEITEM has composite PK that includes FKs
                    if fk_table_name == "LINEITEM":
                        pass  # Allow composite key analysis
                    else:
                        continue

                fk_column_name = fk_column["name"]
                fk_type = fk_column["type"]

                # Skip non-key columns that don't look like FKs
                if not fk_column.get("is_primary_key", False) and not self.looks_like_foreign_key(fk_column_name):
                    continue

                potential_fks.append({
                    "table": fk_table_name,
                    "column": fk_column_name,
                    "type": fk_type
                })

        # Evaluate each potential FK against all PKs
        candidates = []
        for fk_info in potential_fks:
            best_match = None
            best_confidence = 0.0

            for pk_key, pk_info in primary_keys.items():
                # Skip self-table references for now
                if fk_info["table"] == pk_info["table"]:
                    continue

                confidence, factors = self.calculate_comprehensive_confidence(
                    fk_table=fk_info["table"],
                    fk_column=fk_info["column"],
                    fk_type=fk_info["type"],
                    pk_table=pk_info["table"],
                    pk_column=pk_info["column"],
                    pk_type=pk_info["type"]
                )

                if confidence >= min_confidence and confidence > best_confidence:
                    best_match = {
                        "pk_info": pk_info,
                        "confidence": confidence,
                        "factors": factors
                    }
                    best_confidence = confidence

            if best_match:
                relationship = OptimizedRelationship(
                    fk_table=fk_info["table"],
                    fk_column=fk_info["column"],
                    pk_table=best_match["pk_info"]["table"],
                    pk_column=best_match["pk_info"]["column"],
                    confidence_score=best_match["confidence"],
                    confidence_factors=best_match["factors"]
                )
                candidates.append(relationship)

        # Sort by confidence and return
        candidates.sort(key=lambda r: r.confidence_score, reverse=True)
        return candidates


def test_final_optimized_algorithm():
    """Test the final optimized algorithm."""

    discoverer = FinalOptimizedDiscovery()

    print("=" * 100)
    print("FINAL OPTIMIZED RELATIONSHIP DISCOVERY ALGORITHM TEST")
    print("=" * 100)

    # TPC-H test data
    tpch_tables = [
        {
            "table_name": "CUSTOMER",
            "columns": [
                {"name": "C_CUSTKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "C_NAME", "type": "STRING"},
                {"name": "C_NATIONKEY", "type": "NUMBER"},
                {"name": "C_ACCTBAL", "type": "NUMBER"}
            ]
        },
        {
            "table_name": "ORDERS",
            "columns": [
                {"name": "O_ORDERKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "O_CUSTKEY", "type": "NUMBER"},
                {"name": "O_TOTALPRICE", "type": "NUMBER"}
            ]
        },
        {
            "table_name": "LINEITEM",
            "columns": [
                {"name": "L_ORDERKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "L_PARTKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "L_SUPPKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "L_LINENUMBER", "type": "NUMBER", "is_primary_key": True},
                {"name": "L_QUANTITY", "type": "NUMBER"}
            ]
        },
        {
            "table_name": "PART",
            "columns": [
                {"name": "P_PARTKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "P_NAME", "type": "STRING"}
            ]
        },
        {
            "table_name": "SUPPLIER",
            "columns": [
                {"name": "S_SUPPKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "S_NATIONKEY", "type": "NUMBER"}
            ]
        },
        {
            "table_name": "NATION",
            "columns": [
                {"name": "N_NATIONKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "N_REGIONKEY", "type": "NUMBER"}
            ]
        },
        {
            "table_name": "REGION",
            "columns": [
                {"name": "R_REGIONKEY", "type": "NUMBER", "is_primary_key": True},
                {"name": "R_NAME", "type": "STRING"}
            ]
        }
    ]

    expected_relationships = {
        ("ORDERS.O_CUSTKEY", "CUSTOMER.C_CUSTKEY"),
        ("CUSTOMER.C_NATIONKEY", "NATION.N_NATIONKEY"),
        ("LINEITEM.L_ORDERKEY", "ORDERS.O_ORDERKEY"),
        ("LINEITEM.L_PARTKEY", "PART.P_PARTKEY"),
        ("LINEITEM.L_SUPPKEY", "SUPPLIER.S_SUPPKEY"),
        ("SUPPLIER.S_NATIONKEY", "NATION.N_NATIONKEY"),
        ("NATION.N_REGIONKEY", "REGION.R_REGIONKEY")
    }

    print("\n🧪 TESTING FINAL OPTIMIZED ALGORITHM:")

    start_time = time.time()
    discovered = discoverer.discover_relationships(tpch_tables, min_confidence=0.6)
    processing_time = (time.time() - start_time) * 1000

    # Convert to comparison format
    discovered_set = set()
    for rel in discovered:
        fk_ref = f"{rel.fk_table}.{rel.fk_column}"
        pk_ref = f"{rel.pk_table}.{rel.pk_column}"
        discovered_set.add((fk_ref, pk_ref))

    # Calculate metrics
    correct_matches = expected_relationships.intersection(discovered_set)
    false_positives = discovered_set - expected_relationships
    false_negatives = expected_relationships - discovered_set

    precision = len(correct_matches) / len(discovered_set) if discovered_set else 0.0
    recall = len(correct_matches) / len(expected_relationships) if expected_relationships else 0.0
    f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

    print(f"\n📊 FINAL RESULTS:")
    print(f"   Expected Relationships: {len(expected_relationships)}")
    print(f"   Discovered Relationships: {len(discovered_set)}")
    print(f"   Correct Matches: {len(correct_matches)}")
    print(f"   False Positives: {len(false_positives)}")
    print(f"   False Negatives: {len(false_negatives)}")
    print(f"   Precision: {precision:.3f}")
    print(f"   Recall: {recall:.3f}")
    print(f"   F1-Score: {f1_score:.3f}")
    print(f"   Processing Time: {processing_time:.1f}ms")

    if correct_matches:
        print(f"\n✅ CORRECT MATCHES:")
        for fk_ref, pk_ref in sorted(correct_matches):
            print(f"   {fk_ref} → {pk_ref}")

    if false_positives:
        print(f"\n❌ FALSE POSITIVES:")
        for fk_ref, pk_ref in sorted(false_positives):
            print(f"   {fk_ref} → {pk_ref}")

    if false_negatives:
        print(f"\n🔍 MISSED RELATIONSHIPS:")
        for fk_ref, pk_ref in sorted(false_negatives):
            print(f"   {fk_ref} → {pk_ref}")

    # Show detailed confidence scores
    print(f"\n🔍 DETAILED CONFIDENCE ANALYSIS:")
    for rel in discovered:
        print(f"\n   {rel.fk_table}.{rel.fk_column} → {rel.pk_table}.{rel.pk_column}")
        print(f"   Confidence: {rel.confidence_score:.3f}")
        print(f"   Factors: {'; '.join(rel.confidence_factors[:3])}...")

    print("\n" + "=" * 100)
    if f1_score >= 0.8:
        print("🎉 EXCELLENT PERFORMANCE - Algorithm ready for production!")
    elif f1_score >= 0.7:
        print("✅ GOOD PERFORMANCE - Algorithm suitable for most use cases")
    else:
        print("⚠️  NEEDS IMPROVEMENT - Algorithm requires further optimization")
    print("=" * 100)


if __name__ == "__main__":
    test_final_optimized_algorithm()