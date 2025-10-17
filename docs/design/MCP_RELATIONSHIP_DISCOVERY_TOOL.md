# Relationship Discovery MCP Tool Design Document

## Overview

This document outlines the design for a Model Context Protocol (MCP) tool that encapsulates the relationship discovery functionality from the semantic model generator as a standalone service. The tool will provide intelligent table relationship inference capabilities through a well-defined MCP interface.

## Background

The semantic model generator contains sophisticated relationship inference logic in `semantic_model_generator/generate_model.py:_infer_relationships()` that:

- Analyzes table metadata and column characteristics
- Identifies potential foreign key relationships based on naming patterns
- Infers cardinality using statistical analysis of sample data
- Supports composite key relationships
- Provides adaptive thresholds for different data scenarios

This functionality needs to be exposed as an MCP tool to enable external systems to leverage relationship discovery capabilities.

## Design Approaches

### Approach 1: Direct Integration (Recommended)

**Architecture:**
- Create a new MCP tool that directly imports and uses the existing relationship inference code
- Minimal code duplication with maximum reuse of proven logic
- Clean separation between MCP interface and core algorithm

**Pros:**
- Quick implementation with proven, battle-tested relationship inference logic
- Maintains consistency with existing semantic model generation
- Easy to maintain and update
- Lower risk of introducing bugs

**Cons:**
- Direct dependency on semantic model generator package
- Less flexibility for standalone deployment

### Approach 2: Extracted Service

**Architecture:**
- Extract relationship inference logic into a separate library
- Create MCP tool that uses the extracted library
- Both semantic model generator and MCP tool depend on shared library

**Pros:**
- Clean separation of concerns
- Reusable across multiple projects
- Better testability
- Standalone deployment capability

**Cons:**
- More complex initial setup
- Requires refactoring existing code
- Higher maintenance overhead

### Approach 3: API-Based Integration

**Architecture:**
- Expose relationship inference as REST/gRPC API
- MCP tool calls the API service
- Complete separation between inference engine and MCP interface

**Pros:**
- Complete decoupling
- Language-agnostic interface
- Scalable and distributable
- Easy to integrate with non-Python systems

**Cons:**
- Highest complexity
- Network latency considerations
- Additional infrastructure requirements
- Overkill for current use case

## Recommended Approach: Direct Integration

Given the current requirements and project context, **Approach 1: Direct Integration** is recommended because:

1. **Time to Market:** Fastest implementation with proven code
2. **Reliability:** Leverages existing, tested relationship inference logic
3. **Maintenance:** Simpler to maintain with fewer moving parts
4. **Resource Efficiency:** No additional infrastructure required

### Implementation Requirements

**Dependency Management:**
- semantic-model-generator will be published as `clickzetta-semantic-model-generator` pip package
- MCP Server will declare clickzetta-semantic-model-generator as dependency in pyproject.toml
- Version pinning for stability and compatibility

**Package Publication:**
```toml
# In mcp-clickzetta-server/pyproject.toml
[tool.poetry.dependencies]
clickzetta-semantic-model-generator = "^1.0.0"
```

## Tool Design Specification

### Tool Definition

```python
Tool(
    name="discover_table_relationships",
    tags=["read", "metadata", "relationships"],
    handler=handle_discover_table_relationships,
    description="""Intelligent table relationship discovery tool

    Analyzes table metadata and sample data to automatically discover:
    • Foreign key relationships between tables
    • Composite key relationships
    • Relationship cardinality (1:1, 1:*, *:*)
    • Join recommendations with confidence scores

    Supports advanced features:
    • Adaptive similarity thresholds based on data characteristics
    • Composite key pattern recognition
    • Business semantic understanding
    • SQL-based null probe validation (when database session provided)
    """,
    input_schema={
        "type": "object",
        "properties": {
            "source_type": {
                "type": "string",
                "enum": ["tables", "schema"],
                "default": "tables",
                "description": "Whether to analyze specific tables or entire schema"
            },
            "tables": {
                "type": "array",
                "description": "List of table metadata for relationship analysis (when source_type=tables)",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Table name"},
                        "database": {"type": "string", "description": "Database name"},
                        "schema": {"type": "string", "description": "Schema name"},
                        "columns": {
                            "type": "array",
                            "description": "Column definitions with metadata",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"},
                                    "type": {"type": "string"},
                                    "sample_values": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "description": "Sample values for analysis"
                                    },
                                    "is_primary_key": {"type": "boolean", "default": False},
                                    "is_nullable": {"type": "boolean", "default": True}
                                },
                                "required": ["name", "type"]
                            }
                        }
                    },
                    "required": ["name", "columns"]
                }
            },
            "schema_config": {
                "type": "object",
                "description": "Schema-level analysis configuration (when source_type=schema)",
                "properties": {
                    "database": {"type": "string", "description": "Target database name"},
                    "schema": {"type": "string", "description": "Target schema name"},
                    "sample_size": {
                        "type": "integer",
                        "default": 1000,
                        "description": "Number of sample rows to fetch per table"
                    },
                    "table_filter": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of specific table names to include"
                    },
                    "exclude_tables": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of table names to exclude from analysis"
                    }
                }
            },
            "strict_join_inference": {
                "type": "boolean",
                "default": False,
                "description": "Enable SQL-based validation for stricter relationship inference"
            },
            "confidence_threshold": {
                "type": "number",
                "default": 0.5,
                "minimum": 0.0,
                "maximum": 1.0,
                "description": "Minimum confidence score for relationship suggestions"
            },
            "include_composite_keys": {
                "type": "boolean",
                "default": True,
                "description": "Include multi-column composite key relationships"
            },
            "max_relationships": {
                "type": "integer",
                "default": 50,
                "minimum": 1,
                "description": "Maximum number of relationships to return"
            },
            "max_tables": {
                "type": "integer",
                "minimum": 1,
                "default": 60,
                "description": "Maximum number of tables to analyze before returning partial results"
            },
            "timeout_seconds": {
                "type": "number",
                "minimum": 1,
                "default": 30,
                "description": "Execution timeout (seconds) before returning partial results"
            },
            "output_format": {
                "type": "string",
                "enum": ["yaml", "json", "detailed"],
                "default": "yaml",
                "description": "Output format: yaml (semantic model compatible), json (structured), detailed (with metadata)"
            }
        },
        "anyOf": [
            {"required": ["source_type", "tables"]},
            {"required": ["source_type", "schema_config"]}
        ]
    }
)
```

### Input/Output Schema

#### Input Format

**Option 1: Analyze Specific Tables**
```json
{
    "source_type": "tables",
    "tables": [
        {
            "name": "orders",
            "database": "sales_db",
            "schema": "public",
            "columns": [
                {
                    "name": "order_id",
                    "type": "INTEGER",
                    "sample_values": ["1", "2", "3"],
                    "is_primary_key": true
                },
                {
                    "name": "customer_id",
                    "type": "INTEGER",
                    "sample_values": ["101", "102", "101"]
                }
            ]
        },
        {
            "name": "customers",
            "database": "sales_db",
            "schema": "public",
            "columns": [
                {
                    "name": "customer_id",
                    "type": "INTEGER",
                    "sample_values": ["101", "102", "103"],
                    "is_primary_key": true
                }
            ]
        }
    ],
    "confidence_threshold": 0.7,
    "output_format": "detailed"
}
```

**Option 2: Analyze Entire Schema**
```json
{
    "source_type": "schema",
    "schema_config": {
        "database": "sales_db",
        "schema": "public",
        "sample_size": 1000,
        "exclude_tables": ["temp_table", "staging_data"]
    },
    "confidence_threshold": 0.7,
    "strict_join_inference": true,
    "output_format": "detailed"
}
```

**Option 3: Analyze Specific Tables in Schema**
```json
{
    "source_type": "schema",
    "schema_config": {
        "database": "sales_db",
        "schema": "public",
        "table_filter": ["orders", "customers", "products", "order_items"],
        "sample_size": 500
    },
    "confidence_threshold": 0.8,
    "include_composite_keys": true,
    "output_format": "sql_ready"
}
```

#### Output Format

**Standard YAML Format (Compatible with semantic model spec):**
```yaml
# Relationships between logical tables
relationships:
  - name: orders_to_customers
    left_table: orders
    right_table: customers
    relationship_columns:
      - left_column: customer_id
        right_column: customer_id
    join_type: left_outer
    relationship_type: many_to_one

analysis_summary:
  total_tables: 2
  total_relationships_found: 1
  high_confidence_relationships: 1
  composite_key_relationships: 0
  processing_time_ms: 45
  limited_by_timeout: false
  limited_by_max_relationships: false
  limited_by_table_cap: false
  notes: null

# Additional metadata for analysis (when output_format=detailed)
relationship_metadata:
  - relationship_name: orders_to_customers
    confidence_score: 0.95
    inference_reasons:
      - "Exact column name match"
      - "Compatible data types"
      - "Primary key relationship detected"
      - "Sample value overlap: 100%"
    suggested_join_sql: "LEFT JOIN customers ON orders.customer_id = customers.customer_id"

recommendations:
  - "Consider adding foreign key constraints for high-confidence relationships"
  - "Review sample data coverage for better inference accuracy"
```

**JSON Format (when output_format=json):**
```json
{
    "relationships": [
        {
            "name": "orders_to_customers",
            "left_table": "orders",
            "right_table": "customers",
            "relationship_columns": [
                {
                    "left_column": "customer_id",
                    "right_column": "customer_id"
                }
            ],
            "join_type": "left_outer",
            "relationship_type": "many_to_one"
        }
    ],
    "analysis_summary": {
        "total_tables": 2,
        "total_relationships_found": 1,
        "high_confidence_relationships": 1,
        "composite_key_relationships": 0,
        "processing_time_ms": 45,
        "limited_by_timeout": false,
        "limited_by_max_relationships": false,
        "limited_by_table_cap": false,
        "notes": null
    },
    "relationship_metadata": [
        {
            "relationship_name": "orders_to_customers",
            "confidence_score": 0.95,
            "inference_reasons": [
                "Exact column name match",
                "Compatible data types",
                "Primary key relationship detected",
                "Sample value overlap: 100%"
            ],
            "suggested_join_sql": "LEFT JOIN customers ON orders.customer_id = customers.customer_id"
        }
    ],
"recommendations": [
        "Consider adding foreign key constraints for high-confidence relationships",
        "Review sample data coverage for better inference accuracy"
    ]
}
```

> The response summary now exposes guardrail flags (`limited_by_timeout`, `limited_by_max_relationships`, `limited_by_table_cap`) and optional `notes` so callers can detect when the run returned partial results.

## Implementation Plan

### Phase 1: Core MCP Tool Implementation

#### 1.1 Tool Structure
```
mcp-clickzetta-server/
├── src/mcp_clickzetta_server/
│   ├── tools/
│   │   └── relationship_tools.py          # New tool definition
│   ├── relationship/
│   │   ├── __init__.py
│   │   ├── handlers.py                    # MCP handler implementation
│   │   ├── core/
│   │   │   ├── __init__.py
│   │   │   ├── inference_engine.py        # Core relationship logic
│   │   │   ├── data_models.py             # Input/output data models
│   │   │   └── utils.py                   # Utility functions
│   │   └── adapters/
│   │       ├── __init__.py
│   │       └── semantic_model_adapter.py  # Adapter for existing code
│   └── common/
│       └── utilities.py                   # Shared utilities
```

#### 1.2 Core Components

**1.2.1 Tool Definition (`relationship_tools.py`)**
- Define MCP tool interface
- Input schema validation
- Integration with handler

**1.2.2 Handler Implementation (`handlers.py`)**
- MCP-compatible handler function
- Input validation and transformation
- Output formatting
- Error handling and logging

**1.2.3 Inference Engine (`inference_engine.py`)**
- Adapter layer for existing relationship inference code
- Import and wrap `_infer_relationships()` function
- Handle data format conversion
- Provide simplified interface

**1.2.4 Data Models (`data_models.py`)**
- Pydantic models for input/output validation
- Type-safe data structures
- Serialization/deserialization logic

### Phase 2: Integration and Testing

#### 2.1 Integration Points
- Import existing relationship inference functions from semantic model generator
- Adapt data structures between MCP format and internal format
- Handle session-based SQL validation when available

#### 2.2 Testing Strategy
- Unit tests for core inference engine
- Integration tests with sample table metadata
- Performance tests with large table sets
- Validation against existing semantic model generator test cases

### Phase 3: Documentation and Examples

#### 3.1 Usage Documentation
- Tool usage examples
- Input format specifications
- Output interpretation guide
- Best practices and limitations

#### 3.2 Integration Examples
- Sample table metadata formats
- Common use case scenarios
- Error handling examples

## Technical Considerations

### Performance

**Data Volume:**
- Handle up to 100 tables efficiently
- Sample-based analysis to avoid full table scans
- Configurable analysis depth based on requirements

**Memory Usage:**
- Streaming analysis for large metadata sets
- Efficient caching of intermediate results
- Garbage collection for long-running analyses

**Response Time:**
- Target <5 seconds for typical datasets (10-20 tables)
- Async processing for large datasets
- Progress reporting for long-running operations

### Reliability

**Error Handling:**
- Graceful degradation when sample data is insufficient
- Validation of input data formats
- Clear error messages with actionable guidance

**Data Quality:**
- Handle missing or inconsistent metadata
- Robust inference with partial information
- Confidence scoring for relationship quality

### Security

**Data Privacy:**
- Process only metadata and sample values
- No storage of sensitive business data
- Optional anonymization of table/column names

**Input Validation:**
- Schema validation for all inputs
- SQL injection prevention
- Resource limit enforcement

## Future Enhancements

### Advanced Features
- Machine learning-based relationship confidence scoring
- Integration with data catalogs for enhanced metadata
- Real-time relationship monitoring and drift detection
- Support for non-relational data sources

### Integration Capabilities
- REST API wrapper for non-MCP clients
- Webhook notifications for relationship changes
- Integration with popular data modeling tools
- Export formats for various data catalogs

### Scalability Improvements
- Distributed processing for very large schemas
- Caching layer for frequently analyzed schemas
- Incremental analysis for schema evolution

## Migration Strategy

### For Existing Semantic Model Generator Users
1. **Backwards Compatibility:** Existing `_infer_relationships()` function remains unchanged
2. **Gradual Migration:** MCP tool can be used alongside existing workflow
3. **Enhanced Features:** New capabilities available only through MCP tool
4. **Validation:** Cross-validation between existing and MCP tool results

### Integration Timeline
- **Week 1-2:** Core MCP tool implementation
- **Week 3:** Integration with existing inference code
- **Week 4:** Testing and validation
- **Week 5:** Documentation and examples
- **Week 6:** Deployment and monitoring

## Conclusion

The relationship discovery MCP tool will provide a clean, well-defined interface to the sophisticated relationship inference capabilities already present in the semantic model generator. By following the direct integration approach, we can quickly deliver a reliable tool that leverages proven algorithms while maintaining the flexibility to enhance and extend capabilities in the future.

The tool will serve as a foundation for broader data discovery and cataloging capabilities, enabling better understanding of data relationships across complex data ecosystems.
