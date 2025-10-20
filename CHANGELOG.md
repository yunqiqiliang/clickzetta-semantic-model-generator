# Changelog

You must follow the format of `## [VERSION-NUMBER]` for the GitHub workflow to pick up the text.

## [1.0.9] - 2025-10-17

### Fixes

- Preserve original table names when falling back to `SHOW COLUMNS` so case-sensitive external tables resolve correctly.

### Testing

- `POETRY_CACHE_DIR=.poetry-cache poetry run pytest -q`

## [1.0.8] - 2025-10-17

### Fixes

- Treat `EXTERNAL` catalogs like managed workspaces when querying metadata so both information schema and `SHOW` fallbacks are used without forcing `IN SHARE` semantics.

### Testing

- `POETRY_CACHE_DIR=.poetry-cache poetry run pytest -q`

## [1.0.7] - 2025-10-17

### Fixes

- When discovering relationships for an entire schema, enumerate tables via `SHOW TABLES` before falling back to `SHOW COLUMNS`, ensuring external tables without information schema rows are analyzed.

### Testing

- `POETRY_CACHE_DIR=.poetry-cache poetry run pytest -q`

## [1.0.6] - 2025-10-17

### Compatibility

- Relaxed dependency pins so the package remains compatible when newer ClickZetta connectors or ingestion utilities pull in updated stacks.
  - `clickzetta-connector-python` now accepts any `<0.9` release newer than `0.8.92`.
  - `numpy` requirement extended to support the 2.x line; `urllib3` bumped to allow 2.x as well.
  - Regenerated `poetry.lock` to capture the wider constraints.

## [0.1.34] - 2025-10-16

### Bug Fixes

- **[CRITICAL]** Fixed f-string syntax error in `_quote_identifier()` function.
  - **Problem**: f-string expression contained backslashes (`return f'"{text.replace(\'"\', \'""\')}"'`), causing `SyntaxError: f-string expression part cannot include a backslash`.
  - **Solution**: Extracted `.replace()` operation to external variable before f-string interpolation.
  - **Impact**: Prevents module import failure and Streamlit application crash.
  - See `FSTRING_SYNTAX_FIX.md` for details.

### Major Bug Fix

- **[CRITICAL]** Fixed cardinality inference regression that caused false one-to-one relationships in star/snowflake schemas.
  - **Problem**: Small sample sizes (<50 rows) led to accidental uniqueness in fact table foreign keys, causing incorrect 1:1 inference instead of many-to-one.
  - **Solution**: Added `MIN_SAMPLE_SIZE = 50` threshold; fall back to safe `many_to_one` default when sample is insufficient.
  - **Impact**: Prevents query planner errors in ClickZetta workspaces without explicit primary key metadata.
  - See `CARDINALITY_FIX_REPORT.md` for detailed analysis.

### Performance Improvements

- **Sampling controls exposed in UI**:
  - Default sample count remains 10 rows (`_DEFAULT_N_SAMPLE_VALUES_PER_COL = 10`) to avoid YAML bloat.
  - The Streamlit generator now offers a 10‑500 range so analysts can dial up sampling (50/100/200…) when higher confidence is required.
  - Documentation clarifies how larger samples interact with the `MIN_SAMPLE_SIZE = 50` threshold.

### Enhancements

- **Phase 1 Relationship Inference Improvements**:
  - Added Levenshtein distance-based fuzzy column name matching (threshold: 0.6).
  - Enhanced foreign key pattern detection (supports `{table}_id`, `{table}_key`, abbreviated forms).
  - Automatic relationship type inference (`one_to_one`, `many_to_one`, `one_to_many`) based on primary key metadata and uniqueness ratios.
  - **NEW: Intelligent JOIN TYPE inference** (replaces hardcoded `INNER` join):
    - Detects nullable foreign keys (NULL values) → `LEFT OUTER JOIN`
    - Recognizes optional relationship patterns (promo, discount, alternate, etc.) → `LEFT OUTER JOIN`
    - Defaults to `INNER JOIN` for standard FK relationships (ensures data integrity)
    - See `JOIN_TYPE_INFERENCE.md` for detailed design documentation
  - Added debug logging for cardinality and join type inference decisions.
  - See `RELATIONSHIP_INFERENCE_IMPROVEMENTS.md` for full documentation.
- **Relationship discovery tooling refresh**:
  - Introduced `discover_relationships_from_table_definitions()` to accept offline table metadata (`source_type="tables"`) without needing a ClickZetta session.
  - Added execution guardrails (`max_relationships`, `timeout_seconds`, `max_tables`) and surfaced summary flags (`limited_by_timeout`, `limited_by_max_relationships`, `limited_by_table_cap`) for large schemas.
  - Tightened inference heuristics by penalising generic identifiers (`id`, `name`, `code`) and increasing the default confidence threshold to 0.5, significantly reducing false-positive joins.
- **Strict join inference toggle**:
  - Added `strict_join_inference` flag to `_infer_relationships` and exposed a “Strict join inference” checkbox in the Streamlit sidebar.
  - When enabled, the generator issues targeted `WHERE <fk> IS NULL LIMIT 1` queries to conclusively detect optional relationships and emit `LEFT_OUTER` joins.
  - Default remains disabled to avoid extra queries; documentation updated (`JOIN_TYPE_INFERENCE.md`, `NULL_DETECTION_EXPLAINED.md`).
- **DashScope enrichment customization**:
  - Table selector dialog now includes an optional prompt text area shown when enrichment is enabled.
  - User-supplied guidance is appended to the DashScope prompt, enabling bespoke descriptions and business context.
  - Back-end plumbing `run_generate_model_str_from_clickzetta` → `generate_model_str_from_clickzetta` → `enrich_semantic_model` now accepts `llm_custom_prompt`.
- **Model-level metrics and verified queries**:
  - After table enrichment, DashScope now proposes semantic-model metrics (`semantic_model.metrics`) to highlight business KPIs.
  - Verified queries (question + SQL) are generated, executed against ClickZetta for validation, and appended to the YAML when successful.

### Testing

- Added comprehensive test suite: `test_cardinality_standalone.py` (6/6 tests passing).
- Verified boundary conditions (49 vs 50 samples), realistic fact→dimension scenarios, and edge cases.

## [0.1.33] - 2024-08-07

### Updates

- Throw an error during validation if a user adds duplicate verified queries to their semantic model.

## [0.1.32] - 2024-07-30

### Updates

- Bump context length validation limit.
- Fix union type hints for support with Python <3.10.

## [0.1.31] - 2024-07-29

### Updates

- Include new `secure-local-storage` extra package for the ClickZetta connector dependency.

## [0.1.30] - 2024-07-12

### Updates

- Restrict Python version to < 3.12 in order to avoid issues with pyarrow dependency.

## [0.1.29] - 2024-07-10

### Updates

- Allow single sign on auth.

## [0.1.28] - 2024-07-09

### Updates

- Allow auto-generation of descriptions for semantic models.

## [0.1.27] - 2024-07-03

### Updates

- Fix VQR validation for measures with aggregation calculation.
- Update pulling sample value by dimension vs. measures; fix length validation logic.

## [0.1.26] - 2024-07-02

### Updates

- Semantic model size validation allows for many more sample values.
  This corresponds with a release of the Cortex Analyst that does dynamic sample value retrieval by default.

## [0.1.25] - 2024-06-18

### Updates

- Plumb through column and table comments
- Skip host name match verification for now

## [0.1.24] - 2024-06-17

### Updates

- Consolidate validations to use the same set of utils
- Handle the validation for expr with aggregations properly

## [0.1.23] - 2024-06-13

### Updates

- Remove VQR from context length calculation.
- Add toggle for number of sample values.

## [0.1.22] - 2024-06-11

### Updates

- Fix small streamlit app components to be compatible with python 3.8

## [0.1.21] - 2024-06-10

### Updates

- Add validation for verified queries;
- Add streamlit admin app for semantic model generation, validation and verified query flow.

## [0.1.20] - 2024-05-31

### Updates

- Fix for validation CLI and README

## [0.1.19] - 2024-05-31

### Updates

- Fix protobuf version to be compatible with streamlit
- Small refactor in validation file

## [0.1.18] - 2024-05-31

### Updates

- Add proto definition for verified queries; also add proto for Column (for backward compatibility only)

## [0.1.17] - 2024-05-21

### Updates

- Allow flow style in yaml validation

## [0.1.16] - 2024-05-15

### Updates

- Remove validation of context length to after save.
- Uppercase db/schema/table(s)

## [0.1.15] - 2024-05-14

### Updates

- Use strictyaml to validate the semantic model yaml matches the expected schema and has all required fields

## [0.1.14] - 2024-05-13

### Updates

- Fix aggregations
- Context limit

## [0.1.13] - 2024-05-08

### Updates

- Object types not supported in generation or validation.

## [0.1.12] - 2024-05-03

### Updates

- Naming
- Validate no expressions in cols in yaml

## [0.1.11] - 2024-05-01

### Updates

- Save path location

## [0.1.10] - 2024-05-01

### Updates

- Save path location

## [0.1.9] - 2024-04-29

### Updates

- Add additional validation for mismatched quotes. Test incorrect enums.

## [0.1.8] - 2024-04-23

### Updates

- run select against given cols in semantic model for validation

## [0.1.7] - 2024-04-18

### Updates

- Parse yaml model into protos, validate cols and col naming

## [0.1.6] - 2024-04-16

### Updates

- First yaml validation included.

## [0.1.5] - 2024-04-15d

### Updates

- Downgrade pyarrow

## [0.1.4] - 2024-04-15c

### Updates

- Spacing typo

## [0.1.3] - 2024-04-15b

### Updates

- Fix 3.8 typing
- Some function renaming
- Support all ClickZetta SQL dialect datatypes

## [0.1.2] - 2024-04-15

### Updates

- Downgrade to python 3.8 and resolve typing issues with optional.
- Fix FQN parts for pydantic errors.
- Update README to be less restrictive for installs.

## [0.1.1] - 2024-04-09

### Released

- Verify release workflow works as intended

## [0.1.0] - 2024-04-08

### Released

- Initial release of the project.
