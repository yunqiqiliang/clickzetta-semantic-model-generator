from __future__ import annotations

import json
import re
import time
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Sequence, Tuple

from loguru import logger

from semantic_model_generator.data_processing import data_types
from semantic_model_generator.protos import semantic_model_pb2

from .dashscope_client import DashscopeClient, DashscopeError
from .progress_tracker import EnrichmentProgressTracker, EnrichmentStage

if TYPE_CHECKING:  # pragma: no cover
    from clickzetta.zettapark.session import Session
else:  # Fallback type when ClickZetta libraries are unavailable
    Session = Any  # type: ignore

_JSON_BLOCK_PATTERN = re.compile(r"\{.*\}", re.DOTALL)
_NUMERIC_TYPES = {
    "NUMBER",
    "DECIMAL",
    "INT",
    "INTEGER",
    "FLOAT",
    "DOUBLE",
    "BIGINT",
    "SMALLINT",
}
_NON_ALNUM_RE = re.compile(r"[^0-9a-zA-Z]+")

SYSTEM_PROMPT = (
    "You are an experienced ClickZetta data analyst. "
    "Only respond in English. Write concise, professional descriptions that help analysts understand table purpose, column semantics, and business metrics. "
    "Preserve original column names when you reference them."
)


def enrich_semantic_model(
    model: semantic_model_pb2.SemanticModel,
    raw_tables: Sequence[Tuple[data_types.FQNParts, data_types.Table]],
    client: DashscopeClient,
    placeholder: str = "  ",
    custom_prompt: str = "",
    session: Optional[Session] = None,
    progress_tracker: Optional[EnrichmentProgressTracker] = None,
) -> None:
    """
    Enriches the semantic model in-place using DashScope generated descriptions.

    Args:
        model: The semantic model proto to enrich (modified in-place).
        raw_tables: Sequence of raw table metadata paired with FQN parts.
        client: DashScope chat client used to execute completions.
        placeholder: Placeholder string designating missing content.
        custom_prompt: Optional user-provided guidance appended to the LLM prompt.
        session: Optional ClickZetta session used for validating generated SQL (e.g., verified queries).
        progress_tracker: Optional progress tracker for reporting enrichment progress.
    """

    if not model.tables or not raw_tables:
        return

    # Initialize progress tracking
    total_tables = len(model.tables)

    if progress_tracker:
        progress_tracker.update_progress(
            EnrichmentStage.TABLE_ENRICHMENT,
            0,
            total_tables,
            message="Starting table enrichment",
        )

    raw_lookup: Dict[str, data_types.Table] = {
        tbl.name.upper(): tbl for _, tbl in raw_tables
    }
    metric_notes: List[str] = []

    # Process each table with progress tracking
    for table_index, table in enumerate(model.tables):
        raw_table = raw_lookup.get(table.name.upper())
        if not raw_table:
            logger.debug(
                "No raw metadata for table {}; skipping enrichment.", table.name
            )
            continue

        # Update progress for current table
        if progress_tracker:
            progress_tracker.update_progress(
                EnrichmentStage.TABLE_ENRICHMENT,
                table_index + 1,
                total_tables,
                table_name=table.name,
                message=f"Enriching table {table.name}",
            )

        try:
            payload = _serialize_table_prompt(
                table, raw_table, model.description, placeholder, custom_prompt
            )
            response = client.chat_completion(payload["messages"])
            enrichment = _parse_llm_response(response.content)
            if enrichment:
                updates = _apply_enrichment(table, raw_table, enrichment, placeholder)
                note = updates.get("business_notes")
                if note and not updates.get("metrics_added"):
                    metric_notes.append(f"{table.name}: {note}")
                model_description = updates.get("model_description")
                if (
                    model_description
                    and isinstance(model_description, str)
                    and (
                        model.description == placeholder
                        or not model.description.strip()
                    )
                ):
                    model.description = model_description.strip()
        except (
            DashscopeError
        ) as exc:  # pragma: no cover - network failures or remote errors
            logger.warning("DashScope enrichment failed for {}: {}", table.name, exc)
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.exception("Unexpected error enriching table {}: {}", table.name, exc)
    # Model description generation
    if progress_tracker:
        progress_tracker.update_progress(
            EnrichmentStage.MODEL_DESCRIPTION,
            0,
            1,
            message="Generating model description",
        )

    if model.description == placeholder or not model.description.strip():
        _summarize_model_description(model, client, placeholder)

    if progress_tracker:
        progress_tracker.update_progress(
            EnrichmentStage.MODEL_DESCRIPTION,
            1,
            1,
            message="Model description generated",
        )

    # Model metrics generation
    if progress_tracker:
        progress_tracker.update_progress(
            EnrichmentStage.MODEL_METRICS,
            0,
            1,
            message="Generating model-level metrics",
        )

    overview = _build_model_overview(model, raw_lookup, raw_tables)
    try:
        _generate_model_metrics(model, overview, client, placeholder, custom_prompt)
    except DashscopeError as exc:
        logger.warning("Failed to generate model-level metrics: {}", exc)
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Unexpected error generating model metrics: {}", exc)

    if progress_tracker:
        progress_tracker.update_progress(
            EnrichmentStage.MODEL_METRICS, 1, 1, message="Model metrics generated"
        )

    # Verified queries generation
    if progress_tracker:
        progress_tracker.update_progress(
            EnrichmentStage.VERIFIED_QUERIES,
            0,
            1,
            message="Generating verified queries",
        )

    try:
        _generate_verified_queries(
            model,
            overview,
            client,
            placeholder,
            custom_prompt,
            session=session,
        )
    except DashscopeError as exc:
        logger.warning("Failed to generate verified queries: {}", exc)
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Unexpected error generating verified queries: {}", exc)

    if progress_tracker:
        progress_tracker.update_progress(
            EnrichmentStage.VERIFIED_QUERIES, 1, 1, message="Verified queries generated"
        )

    if metric_notes:
        model.custom_instructions = "\n".join(metric_notes)
    else:
        model.custom_instructions = ""

    # Mark enrichment as complete
    if progress_tracker:
        progress_tracker.mark_complete()


def _serialize_table_prompt(
    table: semantic_model_pb2.Table,
    raw_table: data_types.Table,
    model_description: str,
    placeholder: str,
    custom_prompt: str = "",
) -> Dict[str, Any]:
    column_roles: Dict[str, str] = {}
    column_descriptions: Dict[str, str] = {}

    for dim in table.dimensions:
        column_roles[dim.expr.upper()] = "dimension"
        column_descriptions[dim.expr.upper()] = dim.description
    for td in table.time_dimensions:
        column_roles[td.expr.upper()] = "time_dimension"
        column_descriptions[td.expr.upper()] = td.description
    for fact in table.facts:
        column_roles[fact.expr.upper()] = "fact"
        column_descriptions[fact.expr.upper()] = fact.description

    columns_payload: List[Dict[str, object]] = []
    for col in raw_table.columns:
        upper_name = col.column_name.upper()
        role = column_roles.get(upper_name, "unknown")
        description = column_descriptions.get(upper_name, "")
        if description == placeholder:
            description = ""
        columns_payload.append(
            {
                "name": col.column_name,
                "role": role,
                "data_type": col.column_type,
                "has_description": bool(description.strip()),
                "sample_values": col.values[:5] if col.values else [],
            }
        )

    prompt_payload = {
        "table_name": table.name,
        "table_has_description": table.description.strip() not in {placeholder, ""},
        "table_comment": raw_table.comment or "",
        "columns": columns_payload,
        "filters": [
            {
                "name": nf.name,
                "expr": nf.expr,
                "has_description": bool(nf.description.strip()),
                "has_synonyms": any(
                    s.strip() and s != placeholder for s in nf.synonyms
                ),
            }
            for nf in table.filters
        ],
        "semantic_model_description": model_description,
    }

    extra_instructions = custom_prompt.strip()

    user_instructions = (
        "Review the JSON metadata below and reply with a strictly JSON response.\n"
        "1. If a table or column description is empty, provide a concise English description; do not duplicate existing text.\n"
        "2. For facts (numeric columns), propose business-friendly synonyms and explain what the metric represents.\n"
        "3. For dimensions and time dimensions, include common English aliases when useful.\n"
        "4. For filters, provide helpful descriptions and synonyms when they are missing.\n"
        "5. Optionally suggest up to two derived business metrics in a `business_metrics` list with `name`, `source_columns`, `description`, and optionally `synonyms`.\n"
        "6. Provide `model_description` if you can summarize how this table contributes to the overall semantic model.\n"
        "7. Keep column and filter names unchanged and respond with valid JSON only.\n\n"
        "Example output:\n"
        "{\n"
        '  "table_description": "Orders fact table that captures the status and finances of each order",\n'
        '  "columns": [\n'
        "    {\n"
        '      "name": "O_TOTALPRICE",\n'
        '      "description": "Total order value including tax",\n'
        '      "synonyms": ["Order amount", "Order total"]\n'
        "    }\n"
        "  ],\n"
        '  "business_metrics": [\n'
        "    {\n"
        '      "name": "Gross merchandise value",\n'
        '      "source_columns": ["O_TOTALPRICE"],\n'
        '      "description": "Used to measure GMV derived from the total order price."\n'
        "    }\n"
        "  ]\n"
        "}\n\n"
        f"Metadata: ```json\n{json.dumps(prompt_payload, ensure_ascii=False, indent=2)}\n```"
    )

    if extra_instructions:
        user_instructions += f"\n\nUser guidance: {extra_instructions}"

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_instructions},
    ]
    return {"messages": messages}


def _parse_llm_response(content: str) -> Optional[Dict[str, object]]:
    if not content:
        return None
    match = _JSON_BLOCK_PATTERN.search(content)
    json_text = match.group(0) if match else content
    json_text = json_text.strip().strip("`")
    try:
        data = json.loads(json_text)
    except json.JSONDecodeError as exc:
        logger.warning(
            "Unable to parse DashScope response as JSON: {} | raw={}", exc, content
        )
        return None
    if not isinstance(data, dict):
        return None
    return data


def _apply_enrichment(
    table: semantic_model_pb2.Table,
    raw_table: data_types.Table,
    enrichment: Dict[str, object],
    placeholder: str,
) -> Dict[str, Optional[str]]:
    result: Dict[str, Optional[str]] = {
        "business_notes": None,
        "model_description": None,
        "metrics_added": False,
    }
    table_description = enrichment.get("table_description")
    if isinstance(table_description, str) and table.description == placeholder:
        table.description = table_description.strip()

    column_entries = enrichment.get("columns", [])
    if isinstance(column_entries, list):
        _apply_column_enrichment(table, column_entries, placeholder)

    business_metrics = enrichment.get("business_metrics")
    business_notes = None
    if isinstance(business_metrics, list) and business_metrics:
        business_notes, metrics_added = _apply_metric_enrichment(
            table, raw_table, business_metrics, placeholder
        )
        result["metrics_added"] = metrics_added
    _apply_filter_enrichment(table, enrichment, placeholder)
    result["business_notes"] = business_notes
    model_description = enrichment.get("model_description")
    if isinstance(model_description, str) and model_description.strip():
        result["model_description"] = model_description.strip()
    return result


def _apply_column_enrichment(
    table: semantic_model_pb2.Table,
    column_entries: Iterable[object],
    placeholder: str,
) -> None:
    dim_map = {dim.expr.upper(): dim for dim in table.dimensions}
    time_map = {td.expr.upper(): td for td in table.time_dimensions}
    fact_map = {fact.expr.upper(): fact for fact in table.facts}

    for entry in column_entries:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not isinstance(name, str):
            continue
        upper = name.upper()
        target = dim_map.get(upper) or time_map.get(upper) or fact_map.get(upper)
        if not target:
            continue

        description = entry.get("description")
        if (
            isinstance(description, str)
            and getattr(target, "description", "") == placeholder
        ):
            target.description = description.strip()

        synonyms = entry.get("synonyms")
        if isinstance(synonyms, list):
            _apply_synonyms(target, synonyms, placeholder)


def _apply_synonyms(
    target: object, synonyms: Sequence[object], placeholder: str
) -> None:
    clean_synonyms: List[str] = []
    for item in synonyms:
        if isinstance(item, str):
            text = item.strip()
            if text:
                clean_synonyms.append(text)
    if not clean_synonyms:
        return

    existing = [
        syn
        for syn in getattr(target, "synonyms", [])
        if syn.strip() and syn != placeholder
    ]
    merged = _deduplicate(existing + clean_synonyms)

    if hasattr(target, "synonyms"):
        container = getattr(target, "synonyms")
        del container[:]
        container.extend(merged)


def _deduplicate(values: Sequence[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for value in values:
        upper = value.upper()
        if upper in seen:
            continue
        seen.add(upper)
        result.append(value)
    return result


def _build_business_metric_notes(metrics: Sequence[object]) -> str:
    lines: List[str] = []
    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        name = metric.get("name")
        description = metric.get("description")
        sources = metric.get("source_columns", [])
        if not isinstance(name, str) or not name.strip():
            continue
        detail_parts: List[str] = [name.strip()]
        if isinstance(sources, list):
            clean_sources = [str(src).strip() for src in sources if str(src).strip()]
            if clean_sources:
                detail_parts.append(f"(source columns: {', '.join(clean_sources)})")
        if isinstance(description, str) and description.strip():
            detail_parts.append(f"- {description.strip()}")
        lines.append(" ".join(detail_parts))

    return "\n".join(lines)


def _apply_filter_enrichment(
    table: semantic_model_pb2.Table,
    enrichment: Dict[str, object],
    placeholder: str,
) -> None:
    if "filters" not in enrichment:
        return
    filter_map = {nf.name: nf for nf in table.filters}
    filters = enrichment.get("filters", [])
    if not isinstance(filters, list):
        return
    for entry in filters:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        target = filter_map.get(name)
        if not target:
            continue
        description = entry.get("description")
        if isinstance(description, str) and target.description == placeholder:
            target.description = description.strip()
        synonyms = entry.get("synonyms")
        if isinstance(synonyms, list):
            clean_synonyms = [
                str(item).strip()
                for item in synonyms
                if isinstance(item, (str, int, float))
            ]
            if clean_synonyms:
                del target.synonyms[:]
                target.synonyms.extend(clean_synonyms)


def _sanitize_metric_name(name: str, existing: set[str]) -> str:
    cleaned = _NON_ALNUM_RE.sub("_", name.strip().lower()).strip("_")
    if not cleaned:
        cleaned = "metric"
    if cleaned[0].isdigit():
        cleaned = f"metric_{cleaned}"
    candidate = cleaned
    counter = 2
    while candidate in existing:
        candidate = f"{cleaned}_{counter}"
        counter += 1
    existing.add(candidate)
    return candidate


_COUNT_KEYWORDS = (
    "count",
    "number of",
    "total number",
    "how many",
    "volume of",
    "frequency",
    "headcount",
)
_DISTINCT_KEYWORDS = ("distinct", "unique", "deduplicated")
_AVERAGE_KEYWORDS = (
    "average",
    "avg",
    "mean",
    "typical",
    "expected",
    "per order",
    "per customer",
)
_SUM_KEYWORDS = (
    "total",
    "sum",
    "revenue",
    "value",
    "amount",
    "volume",
    "sales",
    "inventory",
    "cost",
    "spend",
    "margin",
)
_PRODUCT_KEYWORDS = (
    "multiply",
    "multiplied",
    "times",
    "product of",
    "extended price",
    "net",
    "after discount",
    "inventory value",
    "combined value",
)


def _collect_metric_text(entry: Dict[str, object]) -> str:
    parts: List[str] = []
    for field in ("name", "description"):
        value = entry.get(field)
        if isinstance(value, str):
            parts.append(value)
    synonyms = entry.get("synonyms")
    if isinstance(synonyms, list):
        for syn in synonyms:
            if isinstance(syn, (str, int, float)):
                parts.append(str(syn))
    return " ".join(parts).lower()


def _is_numeric_type(column_type: str) -> bool:
    upper_type = (column_type or "").upper()
    return any(token in upper_type for token in _NUMERIC_TYPES)


def _derive_metric_intent(
    entry: Dict[str, object],
    source_columns: Sequence[str],
    column_type_map: Dict[str, str],
) -> Tuple[str, bool]:
    """
    Determine the preferred aggregation function and whether a product expression
    should be used when multiple source columns are present.
    """
    text = _collect_metric_text(entry)
    aggregation: Optional[str] = None

    if any(keyword in text for keyword in _AVERAGE_KEYWORDS):
        aggregation = "AVG"

    if aggregation is None and any(keyword in text for keyword in _COUNT_KEYWORDS):
        aggregation = "COUNT"
        if any(keyword in text for keyword in _DISTINCT_KEYWORDS):
            aggregation = "COUNT_DISTINCT"

    if aggregation is None and any(keyword in text for keyword in _SUM_KEYWORDS):
        aggregation = "SUM"

    if aggregation is None:
        aggregation = "SUM"

    use_product = False
    if (
        aggregation == "SUM"
        and len(source_columns) >= 2
        and any(keyword in text for keyword in _PRODUCT_KEYWORDS)
    ):
        first_type = column_type_map.get(source_columns[0].upper(), "")
        second_type = column_type_map.get(source_columns[1].upper(), "")
        if _is_numeric_type(first_type) and _is_numeric_type(second_type):
            use_product = True

    return aggregation, use_product


def _build_metric_expression(
    source_columns: Sequence[str],
    column_type_map: Dict[str, str],
    aggregation: str,
    use_product: bool,
) -> str:
    if not source_columns:
        raise ValueError("No source columns provided for metric enrichment.")

    column_name = source_columns[0]
    column_type = column_type_map.get(column_name.upper(), "")

    if aggregation == "COUNT_DISTINCT":
        return f"COUNT(DISTINCT {column_name})"

    if aggregation == "COUNT":
        return f"COUNT({column_name})"

    if aggregation == "AVG":
        if _is_numeric_type(column_type):
            return f"AVG({column_name})"
        # Fallback to COUNT if average is requested on a non-numeric column.
        return f"COUNT({column_name})"

    # SUM or default aggregation path.
    if use_product and len(source_columns) >= 2:
        return f"SUM({source_columns[0]} * {source_columns[1]})"

    if _is_numeric_type(column_type):
        return f"SUM({column_name})"

    # Fallback for non-numeric columns when SUM was requested.
    return f"COUNT({column_name})"


def _apply_metric_enrichment(
    table: semantic_model_pb2.Table,
    raw_table: data_types.Table,
    business_metrics: Sequence[object],
    placeholder: str,
) -> tuple[Optional[str], bool]:
    column_type_map = {
        col.column_name.upper(): col.column_type for col in raw_table.columns
    }
    existing_names: set[str] = {metric.name for metric in table.metrics}
    notes: List[Dict[str, object]] = []
    metrics_added = False

    for entry in business_metrics:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not isinstance(name, str) or not name.strip():
            continue
        raw_sources = entry.get("source_columns")
        resolved_sources: List[str] = []
        if isinstance(raw_sources, list):
            for col in raw_sources:
                if isinstance(col, str) and col.strip():
                    resolved_sources.append(col.strip())
        if not resolved_sources:
            if table.facts:
                resolved_sources = [table.facts[0].expr]
            else:
                continue

        metric_name = _sanitize_metric_name(name, existing_names)
        aggregation, use_product = _derive_metric_intent(
            entry, resolved_sources, column_type_map
        )
        expression = _build_metric_expression(
            resolved_sources, column_type_map, aggregation, use_product
        )

        metric = table.metrics.add()
        metric.name = metric_name
        metric.expr = expression

        description = entry.get("description")
        metric.description = (
            description.strip()
            if isinstance(description, str) and description.strip()
            else placeholder
        )

        synonyms = entry.get("synonyms")
        synonyms_list: List[str] = []
        if isinstance(synonyms, list):
            for syn in synonyms:
                if isinstance(syn, (str, int, float)):
                    text = str(syn).strip()
                    if text:
                        synonyms_list.append(text)
        if not synonyms_list:
            synonyms_list.append(name.strip())
        metric.synonyms.extend(synonyms_list)

        notes.append(
            {
                "name": name.strip(),
                "source_columns": (
                    raw_sources
                    if isinstance(raw_sources, list) and raw_sources
                    else resolved_sources
                ),
                "description": (
                    description.strip()
                    if isinstance(description, str) and description.strip()
                    else ""
                ),
            }
        )
        metrics_added = True

    if notes:
        return _build_business_metric_notes(notes), metrics_added
    return None, metrics_added


def _summarize_model_description(
    model: semantic_model_pb2.SemanticModel,
    client: DashscopeClient,
    placeholder: str,
) -> None:
    if model.description != placeholder and model.description.strip():
        return

    table_lines = []
    for table in model.tables:
        role = "fact" if table.facts or table.metrics else "dimension"
        desc = (
            table.description.strip() if table.description.strip() else "No description"
        )
        metrics = ", ".join(metric.name for metric in table.metrics) or "None"
        table_lines.append(f"- {table.name} ({role}): {desc}. Metrics: {metrics}")

    relationship_lines = []
    for rel in model.relationships:
        parts = [f"{rel.left_table} -> {rel.right_table}"]
        if rel.relationship_columns:
            columns = ", ".join(
                f"{col.left_column}={col.right_column}"
                for col in rel.relationship_columns
            )
            parts.append(f"on {columns}")
        relationship_lines.append(" ".join(parts))
    if not relationship_lines:
        relationship_lines.append("No relationships provided")

    messages = [
        {
            "role": "system",
            "content": (
                "You are a data modeling assistant. Given the semantic model information, "
                "write one or two concise English sentences that describe the overall purpose of the model "
                "and how its tables relate."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Semantic model name: {model.name}\n"
                f"Tables:\n{chr(10).join(table_lines)}\n"
                f"Relationships:\n{chr(10).join(relationship_lines)}"
            ),
        },
    ]

    try:
        response = client.chat_completion(messages)
        summary = response.content.strip()
        if summary:
            model.description = summary
    except DashscopeError as exc:  # pragma: no cover - defensive
        logger.warning("Failed to summarize semantic model description: {}", exc)


def _build_model_overview(
    model: semantic_model_pb2.SemanticModel,
    raw_lookup: Dict[str, data_types.Table],
    raw_tables: Sequence[Tuple[data_types.FQNParts, data_types.Table]],
) -> Dict[str, Any]:
    overview: Dict[str, Any] = {
        "name": model.name,
        "description": (model.description or "").strip(),
        "tables": [],
        "relationships": [],
        "custom_instructions": (model.custom_instructions or "").strip(),
    }

    base_lookup: Dict[str, Dict[str, str]] = {}
    for fqn, _ in raw_tables:
        key = fqn.table.upper()
        base_lookup[key] = {
            "database": fqn.database,
            "schema": fqn.schema_name,
            "table": fqn.table,
        }

    for table in model.tables:
        table_info: Dict[str, Any] = {
            "name": table.name,
            "description": (table.description or "").strip(),
            "base_table": {
                "database": (
                    table.base_table.database if table.HasField("base_table") else ""
                ),
                "schema": (
                    table.base_table.schema if table.HasField("base_table") else ""
                ),
                "table": table.base_table.table if table.HasField("base_table") else "",
            },
            "dimensions": [
                {
                    "name": dim.name,
                    "expr": dim.expr,
                    "data_type": dim.data_type,
                    "description": (dim.description or "").strip(),
                }
                for dim in table.dimensions
            ],
            "time_dimensions": [
                {
                    "name": dim.name,
                    "expr": dim.expr,
                    "data_type": dim.data_type,
                    "description": (dim.description or "").strip(),
                }
                for dim in table.time_dimensions
            ],
            "facts": [
                {
                    "name": fact.name,
                    "expr": fact.expr,
                    "data_type": fact.data_type,
                    "description": (fact.description or "").strip(),
                }
                for fact in table.facts
            ],
            "metrics": [
                {
                    "name": metric.name,
                    "expr": metric.expr,
                    "description": (metric.description or "").strip(),
                }
                for metric in table.metrics
            ],
            "filters": [
                {
                    "name": nf.name,
                    "expr": nf.expr,
                    "description": (nf.description or "").strip(),
                }
                for nf in table.filters
            ],
        }

        # Provide raw column snapshot for additional context.
        raw_table = raw_lookup.get(table.name.upper())
        if raw_table:
            sample_columns = []
            for col in raw_table.columns[:5]:
                sample_columns.append(
                    {
                        "name": col.column_name,
                        "data_type": col.column_type,
                        "sample_values": (col.values or [])[:3] if col.values else [],
                    }
                )
            if sample_columns:
                table_info["sample_columns"] = sample_columns

        if not table_info["base_table"].get("table"):
            # Fallback to raw table mapping when proto base_table is missing.
            fallback = base_lookup.get(table.name.upper())
            if fallback:
                table_info["base_table"] = fallback

        overview["tables"].append(table_info)

    for rel in model.relationships:
        overview["relationships"].append(
            {
                "name": rel.name,
                "left_table": rel.left_table,
                "right_table": rel.right_table,
                "join_type": semantic_model_pb2.JoinType.Name(rel.join_type),
                "relationship_type": semantic_model_pb2.RelationshipType.Name(
                    rel.relationship_type
                ),
                "columns": [
                    {"left_column": col.left_column, "right_column": col.right_column}
                    for col in rel.relationship_columns
                ],
            }
        )

    return overview


def _generate_model_metrics(
    model: semantic_model_pb2.SemanticModel,
    overview: Dict[str, Any],
    client: DashscopeClient,
    placeholder: str,
    custom_prompt: str,
    max_items: int = 5,
) -> None:
    if not overview.get("tables"):
        return

    # Robust pre-check for metrics field accessibility
    metrics_accessible = False

    # Step 1: Check if metrics attribute exists
    if not hasattr(model, "metrics"):
        logger.warning(
            "Model object missing 'metrics' attribute, skipping model-level metrics generation"
        )
        return

    # Step 2: Test basic read access
    try:
        current_count = len(model.metrics)
        logger.debug("Metrics field read access OK, current count: {}", current_count)
        metrics_accessible = True
    except Exception as exc:
        logger.warning("Cannot read model.metrics field: {}", str(exc))
        return

    # Step 3: Test write access only if read access succeeded
    if metrics_accessible:
        try:
            # Try a minimal write operation
            test_metric = model.metrics.add()
            test_metric.name = "__test__"
            test_metric.expr = "COUNT(1)"

            # Verify the metric was actually added
            new_count = len(model.metrics)
            if new_count == current_count + 1:
                # Success - clean up the test metric
                del model.metrics[-1]
                logger.debug("Metrics field write access verified and cleaned up")
            else:
                # Metric add appeared to succeed but count didn't change - something is wrong
                logger.warning(
                    "Metrics field write access inconsistent (count: {} -> {}), skipping model-level metrics",
                    current_count,
                    new_count,
                )
                return

        except Exception as exc:
            logger.warning("Cannot write to model.metrics field: {}", str(exc))
            # Try to provide diagnostic information without causing more errors
            try:
                logger.debug(
                    "Model type: {}, metrics type: {}",
                    type(model).__name__,
                    type(getattr(model, "metrics", None)),
                )
            except Exception:
                pass
            return

    # Count total facts across all tables to determine if model-level metrics make sense
    total_facts = sum(len(table.facts) for table in model.tables)

    # Skip model-level metrics only if there are no facts at all
    if total_facts < 1:
        logger.debug("Skipping model-level metrics because no facts were detected.")
        return

    # Allow model-level metrics for most scenarios:
    # - Multiple fact tables (cross-table metrics)
    # - Single fact table with multiple facts (combined metrics)
    # - Single fact table with relationships (cross-table potential)
    # - Even single fact table without relationships (still useful for model-level aggregation)
    # Only skip in very limited cases: no fact tables at all (handled above)

    prompt_json = json.dumps(overview, ensure_ascii=False, indent=2)
    instructions = (
        "Design up to three model-level business metrics (KPIs) using the semantic model summary below.\n"
        "Return JSON with the structure:\n"
        "{\n"
        '  "model_metrics": [\n'
        "    {\n"
        '      "name": "...",\n'
        '      "expr": "SUM(FACT_SALES.total_amount)",\n'
        '      "description": "...",\n'
        '      "synonyms": ["..."]\n'
        "    }\n"
        "  ]\n"
        "}\n"
        "Guidelines: use the provided table and column names exactly; prefer SUM/AVG/COUNT-style aggregates; avoid duplicates of existing table metrics."
        f"\n\nSemantic model summary:```json\n{prompt_json}\n```"
    )
    if custom_prompt.strip():
        instructions += f"\n\nUser guidance: {custom_prompt.strip()}"

    messages = [
        {
            "role": "system",
            "content": (
                "You are an analytics engineer. Only respond in JSON and propose business-friendly metrics."
            ),
        },
        {"role": "user", "content": instructions},
    ]

    response = client.chat_completion(messages)
    payload = _parse_llm_response(response.content)
    if not isinstance(payload, dict):
        logger.debug("Failed to parse LLM response as dict for model metrics")
        return

    entries = payload.get("model_metrics")
    if not isinstance(entries, list):
        logger.debug(
            "No model_metrics list found in LLM response: {}",
            payload.keys() if payload else "None",
        )
        return

    logger.debug("Found {} model metrics entries to process", len(entries))

    # Get existing metric names (pre-check ensures this will work)
    existing_names: set[str] = {metric.name for metric in model.metrics}

    for table in model.tables:
        existing_names.update(metric.name for metric in table.metrics)

    added = 0
    for entry in entries:
        if added >= max_items:
            break
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        expr = entry.get("expr")
        if not isinstance(name, str) or not name.strip():
            continue
        if not isinstance(expr, str) or not expr.strip():
            continue

        # Add metric with additional safety check
        try:
            metric = model.metrics.add()
        except Exception as exc:
            logger.warning(
                "Failed to add model-level metric '{}' despite pre-check: {}",
                name,
                str(exc),
            )
            logger.info(
                "Aborting model-level metrics generation due to unexpected field access failure"
            )
            return

        metric.name = _sanitize_metric_name(name, existing_names)
        metric.expr = expr.strip().rstrip(";")

        description = entry.get("description")
        if isinstance(description, str) and description.strip():
            metric.description = description.strip()
        else:
            metric.description = placeholder

        synonyms = entry.get("synonyms")
        if isinstance(synonyms, list):
            clean_synonyms = [
                str(item).strip()
                for item in synonyms
                if isinstance(item, (str, int, float)) and str(item).strip()
            ]
            if clean_synonyms:
                metric.synonyms.extend(clean_synonyms)

        if not metric.synonyms:
            metric.synonyms.append(name.strip())
        added += 1


def _sanitize_query_name(name: str, existing: set[str]) -> str:
    base = name.strip() or "Verified query"
    candidate = base
    counter = 2
    while candidate.lower() in existing:
        candidate = f"{base} ({counter})"
        counter += 1
    existing.add(candidate.lower())
    return candidate


def _ensure_limit_clause(sql: str, default_limit: int = 200) -> str:
    normalized = sql.rstrip().rstrip(";")
    if " limit " in normalized.lower():
        return normalized
    return f"{normalized} LIMIT {default_limit}"


def _generate_verified_queries(
    model: semantic_model_pb2.SemanticModel,
    overview: Dict[str, Any],
    client: DashscopeClient,
    placeholder: str,
    custom_prompt: str,
    session: Optional[Session] = None,
    max_items: int = 3,
) -> None:
    if session is None:
        logger.debug(
            "Skipping verified query generation because no ClickZetta session was provided."
        )
        return

    prompt_json = json.dumps(overview, ensure_ascii=False, indent=2)
    instructions = (
        "Propose up to three verified analytics queries for the semantic model below. Each query must include:\n"
        "- `name`: short title\n"
        "- `question`: business question answered\n"
        "- `sql`: runnable ClickZetta SQL referencing the provided logical tables\n"
        "Return JSON with `verified_queries`. Ensure every SQL statement includes an ORDER BY when needed and a LIMIT (<=200) to keep result sets small."
        f"\n\nSemantic model summary:```json\n{prompt_json}\n```"
    )
    if custom_prompt.strip():
        instructions += f"\n\nUser guidance: {custom_prompt.strip()}"

    messages = [
        {
            "role": "system",
            "content": (
                "You create example analytical queries. Return JSON only. Make sure SQL uses valid column names and respects join relationships."
            ),
        },
        {"role": "user", "content": instructions},
    ]

    response = client.chat_completion(messages)
    payload = _parse_llm_response(response.content)
    if not isinstance(payload, dict):
        return

    entries = payload.get("verified_queries")
    if not isinstance(entries, list):
        return

    existing_names = {vq.name.lower() for vq in model.verified_queries}
    existing_sql = {vq.sql.strip().lower() for vq in model.verified_queries}

    for entry in entries[:max_items]:
        if not isinstance(entry, dict):
            continue
        question = entry.get("question")
        sql = entry.get("sql")
        if not isinstance(sql, str) or not sql.strip():
            continue
        query_name = entry.get("name")
        if not isinstance(query_name, str) or not query_name.strip():
            query_name = (
                question
                if isinstance(question, str) and question.strip()
                else "Verified query"
            )

        normalized_sql = _ensure_limit_clause(sql)
        if normalized_sql.strip().lower() in existing_sql:
            continue

        try:
            session.sql(normalized_sql).to_pandas()
        except Exception as exc:  # pragma: no cover - ClickZetta query failed
            logger.warning(
                "Skipping verified query '{}' due to validation failure: {}",
                query_name,
                exc,
            )
            continue

        verified_query = model.verified_queries.add()
        verified_query.name = _sanitize_query_name(query_name, existing_names)
        if isinstance(question, str) and question.strip():
            verified_query.question = question.strip()
        else:
            verified_query.question = verified_query.name
        verified_query.sql = normalized_sql
        if hasattr(verified_query, "semantic_model_name"):
            verified_query.semantic_model_name = model.name
        verified_query.verified_at = int(time.time())
        verified_query.verified_by = "DashScope Auto-Validation"

        use_as_onboarding = entry.get("use_as_onboarding_question")
        if isinstance(use_as_onboarding, bool):
            verified_query.use_as_onboarding_question = use_as_onboarding

        existing_sql.add(normalized_sql.strip().lower())
