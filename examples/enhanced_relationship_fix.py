#!/usr/bin/env python3
"""
Enhanced relationship discovery fix based on ClickZetta implementation analysis.

This script provides improved algorithms for TPC-H and similar naming conventions.
"""

import re
from typing import Dict, List, Set, Tuple
from dataclasses import dataclass


@dataclass
class MatchCandidate:
    """Represents a potential FK-PK relationship match."""
    fk_column: str
    pk_column: str
    fk_table: str
    pk_table: str
    similarity_score: float
    confidence_factors: List[str]
    base_confidence: float


class EnhancedRelationshipMatcher:
    """Enhanced relationship matcher with TPC-H support."""

    def __init__(self):
        # TPC-H specific entity mappings
        self.tpch_entity_mappings = {
            "CUST": ["CUSTOMER", "CUSTOMERS"],
            "SUPP": ["SUPPLIER", "SUPPLIERS"],
            "PART": ["PART", "PARTS"],
            "ORDER": ["ORDERS", "ORDER"],
            "NATION": ["NATION", "NATIONS"],
            "REGION": ["REGION", "REGIONS"],
            "LINE": ["LINEITEM", "LINEITEMS"],
        }

        # Reverse mapping for quick lookup
        self.entity_to_abbreviation = {}
        for abbrev, full_names in self.tpch_entity_mappings.items():
            for full_name in full_names:
                self.entity_to_abbreviation[full_name] = abbrev

        # Standard business patterns
        self.business_patterns = {
            ("CUSTOMER", "ORDER"): 0.25,
            ("ORDER", "LINEITEM"): 0.30,
            ("PART", "LINEITEM"): 0.20,
            ("SUPPLIER", "LINEITEM"): 0.20,
            ("NATION", "CUSTOMER"): 0.15,
            ("NATION", "SUPPLIER"): 0.15,
            ("REGION", "NATION"): 0.20,
        }

    def enhanced_name_similarity(self, name1: str, name2: str) -> float:
        """Enhanced name similarity with domain knowledge."""
        if not name1 or not name2:
            return 0.0

        name1_upper = name1.upper()
        name2_upper = name2.upper()

        # Exact match
        if name1_upper == name2_upper:
            return 1.0

        # Remove prefixes and analyze core entities
        core1 = self._extract_core_entity(name1_upper)
        core2 = self._extract_core_entity(name2_upper)

        # Perfect core match (e.g., CUSTKEY vs CUSTKEY)
        if core1 == core2 and core1:
            return 0.95

        # Entity abbreviation match (e.g., CUST vs CUSTOMER)
        if self._are_entity_variants(core1, core2):
            return 0.90

        # Standard Levenshtein similarity
        return self._levenshtein_similarity(name1_upper, name2_upper)

    def _extract_core_entity(self, column_name: str) -> str:
        """Extract core entity from column name."""
        # Remove table prefixes (C_, O_, L_, etc.)
        if "_" in column_name:
            parts = column_name.split("_", 1)
            if len(parts) == 2 and len(parts[0]) <= 2:
                core_part = parts[1]
            else:
                core_part = column_name
        else:
            core_part = column_name

        # Remove common suffixes
        for suffix in ["KEY", "ID", "NUM", "NO"]:
            if core_part.endswith(suffix):
                return core_part[:-len(suffix)]

        return core_part

    def _are_entity_variants(self, entity1: str, entity2: str) -> bool:
        """Check if two entities are variants of the same concept."""
        if not entity1 or not entity2:
            return False

        # Direct mapping lookup
        entity1_variants = self.tpch_entity_mappings.get(entity1, [entity1])
        entity2_variants = self.tpch_entity_mappings.get(entity2, [entity2])

        # Check if either entity appears in the other's variant list
        if entity1 in entity2_variants or entity2 in entity1_variants:
            return True

        # Check reverse mapping
        abbrev1 = self.entity_to_abbreviation.get(entity1)
        abbrev2 = self.entity_to_abbreviation.get(entity2)

        if abbrev1 and abbrev1 == entity2:
            return True
        if abbrev2 and abbrev2 == entity1:
            return True

        return False

    def _levenshtein_similarity(self, s1: str, s2: str) -> float:
        """Calculate Levenshtein-based similarity."""
        # Remove underscores and hyphens for comparison
        norm1 = s1.replace("_", "").replace("-", "")
        norm2 = s2.replace("_", "").replace("-", "")

        if norm1 == norm2:
            return 0.95

        max_len = max(len(norm1), len(norm2))
        if max_len == 0:
            return 0.0

        distance = self._levenshtein_distance(norm1, norm2)
        similarity = 1.0 - (distance / max_len)

        return max(0.0, similarity)

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein distance between two strings."""
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)

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

    def enhanced_suffix_match(self, fk_column: str, pk_column: str, pk_table: str) -> bool:
        """Enhanced suffix matching with TPC-H support."""
        fk_upper = fk_column.upper()
        pk_upper = pk_column.upper()
        pk_table_upper = pk_table.upper()

        # Exact match
        if fk_upper == pk_upper:
            return True

        # TPC-H prefix-based matching
        if "_" in fk_upper and "_" in pk_upper:
            fk_parts = fk_upper.split("_", 1)
            pk_parts = pk_upper.split("_", 1)

            if len(fk_parts) == 2 and len(pk_parts) == 2:
                fk_core = fk_parts[1]
                pk_core = pk_parts[1]

                # Core identifiers match exactly
                if fk_core == pk_core:
                    return True

                # Entity variant matching
                fk_entity = self._extract_core_entity(fk_core)
                pk_entity = self._extract_core_entity(pk_core)

                if self._are_entity_variants(fk_entity, pk_entity):
                    # Also check if PK entity relates to PK table
                    pk_table_entity = self._extract_table_entity(pk_table_upper)
                    if self._are_entity_variants(pk_entity, pk_table_entity):
                        return True

        # Standard pattern matching
        return self._standard_suffix_match(fk_upper, pk_upper)

    def _extract_table_entity(self, table_name: str) -> str:
        """Extract entity concept from table name."""
        # Handle plurals
        if table_name.endswith("S") and len(table_name) > 3:
            singular = table_name[:-1]
            if singular in self.entity_to_abbreviation:
                return singular

        return table_name

    def _standard_suffix_match(self, fk_column: str, pk_column: str) -> bool:
        """Standard suffix matching patterns."""
        # Pattern: {entity}_{pk_column}
        if fk_column.endswith(f"_{pk_column}"):
            prefix = fk_column[:-(len(pk_column) + 1)]
            return len(prefix) >= 2

        # Pattern: {pk_column}_{suffix}
        if fk_column.startswith(f"{pk_column}_"):
            suffix = fk_column[len(pk_column) + 1:]
            return len(suffix) >= 1

        # Pattern: contains pk_column as component
        if pk_column in fk_column and pk_column != fk_column:
            return len(pk_column) >= 3

        return False

    def calculate_enhanced_confidence(
        self,
        fk_column: str,
        pk_column: str,
        fk_table: str,
        pk_table: str,
        has_pk_metadata: bool = True,
        sample_overlap: float = 0.0
    ) -> Tuple[float, List[str]]:
        """Calculate enhanced confidence score with detailed factors."""

        factors = []
        confidence = 0.0

        # Factor 1: Primary key metadata (highest weight)
        if has_pk_metadata:
            confidence += 0.4
            factors.append("Primary key metadata available (+0.4)")

        # Factor 2: Column name similarity
        similarity = self.enhanced_name_similarity(fk_column, pk_column)
        if similarity >= 0.9:
            confidence += 0.25
            factors.append(f"High column name similarity ({similarity:.2f}) (+0.25)")
        elif similarity >= 0.7:
            confidence += 0.15
            factors.append(f"Good column name similarity ({similarity:.2f}) (+0.15)")
        elif similarity >= 0.5:
            confidence += 0.05
            factors.append(f"Moderate column name similarity ({similarity:.2f}) (+0.05)")

        # Factor 3: Suffix matching
        if self.enhanced_suffix_match(fk_column, pk_column, pk_table):
            confidence += 0.2
            factors.append("Column follows FK naming pattern (+0.2)")

        # Factor 4: Business relationship patterns
        business_boost = self._get_business_relationship_boost(fk_table, pk_table)
        if business_boost > 0:
            confidence += business_boost
            factors.append(f"Business relationship pattern (+{business_boost:.2f})")

        # Factor 5: Sample data overlap
        if sample_overlap > 0.8:
            confidence += 0.15
            factors.append(f"High sample overlap ({sample_overlap:.2f}) (+0.15)")
        elif sample_overlap > 0.5:
            confidence += 0.1
            factors.append(f"Moderate sample overlap ({sample_overlap:.2f}) (+0.1)")

        # Penalty factors
        if similarity < 0.3:
            confidence -= 0.2
            factors.append(f"Low column similarity penalty (-0.2)")

        # Cap confidence at 1.0
        confidence = min(1.0, max(0.0, confidence))

        return confidence, factors

    def _get_business_relationship_boost(self, fk_table: str, pk_table: str) -> float:
        """Get business relationship boost based on known patterns."""
        fk_entity = self._extract_table_entity(fk_table.upper())
        pk_entity = self._extract_table_entity(pk_table.upper())

        # Direct pattern lookup
        for (entity1, entity2), boost in self.business_patterns.items():
            if (fk_entity == entity1 and pk_entity == entity2) or \
               (self._are_entity_variants(fk_entity, entity1) and self._are_entity_variants(pk_entity, entity2)):
                return boost

        return 0.0

    def find_best_matches(
        self,
        fk_candidates: List[Tuple[str, str]],  # (column, table)
        pk_candidates: List[Tuple[str, str]],  # (column, table)
        min_confidence: float = 0.5
    ) -> List[MatchCandidate]:
        """Find best FK-PK matches with confidence scoring."""

        matches = []

        for fk_col, fk_table in fk_candidates:
            best_match = None
            best_confidence = 0.0

            for pk_col, pk_table in pk_candidates:
                confidence, factors = self.calculate_enhanced_confidence(
                    fk_col, pk_col, fk_table, pk_table
                )

                if confidence >= min_confidence and confidence > best_confidence:
                    similarity = self.enhanced_name_similarity(fk_col, pk_col)
                    best_match = MatchCandidate(
                        fk_column=fk_col,
                        pk_column=pk_col,
                        fk_table=fk_table,
                        pk_table=pk_table,
                        similarity_score=similarity,
                        confidence_factors=factors,
                        base_confidence=confidence
                    )
                    best_confidence = confidence

            if best_match:
                matches.append(best_match)

        return matches


def test_enhanced_matcher():
    """Test the enhanced matcher with TPC-H examples."""

    matcher = EnhancedRelationshipMatcher()

    print("=" * 80)
    print("ENHANCED RELATIONSHIP MATCHER TEST")
    print("=" * 80)

    # Test cases from the problematic TPC-H scenario
    test_cases = [
        # Expected matches
        ("C_NATIONKEY", "N_NATIONKEY", "CUSTOMER", "NATION", "Should match with high confidence"),
        ("O_CUSTKEY", "C_CUSTKEY", "ORDERS", "CUSTOMER", "Should match with high confidence"),
        ("L_ORDERKEY", "O_ORDERKEY", "LINEITEM", "ORDERS", "Should match with high confidence"),
        ("L_PARTKEY", "P_PARTKEY", "LINEITEM", "PART", "Should match with high confidence"),

        # Should NOT match
        ("C_NATIONKEY", "N_REGIONKEY", "CUSTOMER", "NATION", "Should have low confidence"),
        ("C_NATIONKEY", "R_REGIONKEY", "CUSTOMER", "REGION", "Should have low confidence"),
    ]

    print("\n🧪 TESTING ENHANCED CONFIDENCE CALCULATION:")

    for fk_col, pk_col, fk_table, pk_table, expected in test_cases:
        confidence, factors = matcher.calculate_enhanced_confidence(
            fk_col, pk_col, fk_table, pk_table
        )

        status = "✅ PASS" if (
            ("high confidence" in expected and confidence >= 0.7) or
            ("low confidence" in expected and confidence < 0.5)
        ) else "❌ FAIL"

        print(f"\n{status} {fk_table}.{fk_col} → {pk_table}.{pk_col}")
        print(f"   Confidence: {confidence:.3f} ({expected})")
        print(f"   Factors: {', '.join(factors[:3])}...")

    print("\n" + "=" * 80)
    print("🎯 ENHANCED MATCHER READY FOR INTEGRATION")
    print("=" * 80)


if __name__ == "__main__":
    test_enhanced_matcher()