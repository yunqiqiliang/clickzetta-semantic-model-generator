import json

from semantic_model_generator.data_processing.data_types import Column, FQNParts, Table
from semantic_model_generator.llm.dashscope_client import DashscopeResponse
from semantic_model_generator.llm.enrichment import enrich_semantic_model
from semantic_model_generator.protos import semantic_model_pb2


class _FakeDashscopeClient:
    def __init__(self, payloads):
        if isinstance(payloads, list):
            self._payloads = payloads
        else:
            self._payloads = [payloads]
        self._index = 0

    def chat_completion(self, messages):  # type: ignore[no-untyped-def]
        payload = (
            self._payloads[self._index]
            if self._index < len(self._payloads)
            else self._payloads[-1]
        )
        self._index += 1
        return DashscopeResponse(
            content=json.dumps(payload, ensure_ascii=False),
            request_id=f"test_{self._index}",
        )


def test_enrich_semantic_model_populates_descriptions_and_synonyms() -> None:
    raw_table = Table(
        id_=0,
        name="orders",
        columns=[
            Column(
                id_=0,
                column_name="order_status",
                column_type="STRING",
                values=["OPEN", "CLOSED"],
            ),
            Column(
                id_=1,
                column_name="total_amount",
                column_type="NUMBER",
                values=["12.5", "18.3"],
            ),
        ],
    )

    table_proto = semantic_model_pb2.Table(
        name="ORDERS",
        description="  ",
        base_table=semantic_model_pb2.FullyQualifiedTable(
            database="SALES", schema="PUBLIC", table="ORDERS"
        ),
        dimensions=[
            semantic_model_pb2.Dimension(
                name="order_status",
                expr="order_status",
                data_type="STRING",
                description="  ",
                synonyms=["  "],
                sample_values=["OPEN", "CLOSED"],
            )
        ],
        time_dimensions=[
            semantic_model_pb2.TimeDimension(
                name="order_date",
                expr="order_date",
                data_type="TIMESTAMP_NTZ",
                description="  ",
                sample_values=["2024-02-15", "2024-03-10"],
            )
        ],
        facts=[
            semantic_model_pb2.Fact(
                name="total_amount",
                expr="total_amount",
                data_type="DECIMAL",
                description="  ",
                synonyms=["  "],
                sample_values=["12.5", "18.3"],
            )
        ],
        filters=[
            semantic_model_pb2.NamedFilter(
                name="order_status_include_values",
                expr="order_status IN ('OPEN', 'CLOSED')",
                description="  ",
                synonyms=["  "],
            )
        ],
    )

    model = semantic_model_pb2.SemanticModel(name="test", tables=[table_proto])

    fake_response = {
        "table_description": "Orders fact table that records order status and total amount.",
        "columns": [
            {
                "name": "order_status",
                "description": "Current execution status for each order.",
                "synonyms": ["Order status", "Fulfillment state"],
            },
            {
                "name": "total_amount",
                "description": "Order total including taxes.",
                "synonyms": ["Order amount", "Order total"],
            },
        ],
        "business_metrics": [
            {
                "name": "GMV",
                "source_columns": ["total_amount"],
                "description": "Based on total_amount and used as gross merchandise value.",
            }
        ],
        "filters": [
            {
                "name": "order_status_include_values",
                "description": "Limit the result set to a sample of order statuses.",
                "synonyms": ["Order status filter"],
            }
        ],
        "model_description": "Semantic model for customer orders and related metrics.",
    }

    client = _FakeDashscopeClient(
        [fake_response, {"model_metrics": []}, {"verified_queries": []}]
    )
    enrich_semantic_model(
        model,
        [(FQNParts(database="SALES", schema_name="PUBLIC", table="ORDERS"), raw_table)],
        client,
        placeholder="  ",
    )

    table = model.tables[0]
    assert (
        table.description
        == "Orders fact table that records order status and total amount."
    )

    dimension = next(dim for dim in table.dimensions if dim.expr == "order_status")
    assert dimension.description == "Current execution status for each order."
    assert "Order status" in list(dimension.synonyms)

    fact = next(f for f in table.facts if f.expr == "total_amount")
    assert fact.description == "Order total including taxes."
    assert "Order total" in list(fact.synonyms)

    filter_obj = next(
        flt for flt in table.filters if flt.name == "order_status_include_values"
    )
    assert (
        filter_obj.description == "Limit the result set to a sample of order statuses."
    )
    assert "Order status filter" in list(filter_obj.synonyms)

    assert len(table.metrics) == 1
    metric = table.metrics[0]
    assert metric.name.startswith("gmv")
    assert metric.expr == "SUM(total_amount)"
    assert "GMV" in list(metric.synonyms)
    assert (
        metric.description
        == "Based on total_amount and used as gross merchandise value."
    )

    assert model.custom_instructions == ""
    assert (
        model.description == "Semantic model for customer orders and related metrics."
    )


class _FakeSession:
    class _Result:
        @staticmethod
        def to_pandas():  # type: ignore[no-untyped-def]
            return None

    def __init__(self) -> None:
        self.queries: list[str] = []

    def sql(self, query):  # type: ignore[no-untyped-def]
        self.queries.append(query)
        return _FakeSession._Result()


def test_enrich_semantic_model_generates_model_metrics_and_verified_queries() -> None:
    raw_orders = Table(
        id_=0,
        name="orders",
        columns=[
            Column(
                id_=0, column_name="order_id", column_type="NUMBER", values=["1", "2"]
            ),
            Column(
                id_=1,
                column_name="total_amount",
                column_type="NUMBER",
                values=["10", "20"],
            ),
        ],
    )

    raw_payments = Table(
        id_=1,
        name="payments",
        columns=[
            Column(
                id_=0, column_name="payment_id", column_type="NUMBER", values=["1", "2"]
            ),
            Column(
                id_=1, column_name="amount", column_type="NUMBER", values=["5", "15"]
            ),
        ],
    )

    orders_proto = semantic_model_pb2.Table(
        name="ORDERS",
        description="  ",
        base_table=semantic_model_pb2.FullyQualifiedTable(
            database="SALES", schema="PUBLIC", table="ORDERS"
        ),
        facts=[
            semantic_model_pb2.Fact(
                name="total_amount",
                expr="total_amount",
                data_type="DECIMAL",
                description="  ",
            )
        ],
    )

    payments_proto = semantic_model_pb2.Table(
        name="PAYMENTS",
        description="  ",
        base_table=semantic_model_pb2.FullyQualifiedTable(
            database="SALES", schema="PUBLIC", table="PAYMENTS"
        ),
        facts=[
            semantic_model_pb2.Fact(
                name="amount",
                expr="amount",
                data_type="DECIMAL",
                description="  ",
            )
        ],
    )

    model = semantic_model_pb2.SemanticModel(
        name="Orders Model", tables=[orders_proto, payments_proto]
    )

    table_payload = {
        "table_description": "Orders fact table with totals.",
        "columns": [
            {
                "name": "total_amount",
                "description": "Order total amount including taxes.",
                "synonyms": ["Order amount"],
            }
        ],
        "business_metrics": [],
        "filters": [],
    }

    table_payload_payments = {
        "table_description": "Payments fact table with amounts.",
        "columns": [
            {
                "name": "amount",
                "description": "Payment amount per transaction.",
                "synonyms": ["Payment amount"],
            }
        ],
        "business_metrics": [],
        "filters": [],
    }

    model_metrics_payload = {
        "model_metrics": [
            {
                "name": "Total revenue",
                "expr": "SUM(ORDERS.total_amount)",
                "description": "Sum of total_amount across all orders.",
                "synonyms": ["Revenue"],
            }
        ]
    }

    verified_queries_payload = {
        "verified_queries": [
            {
                "name": "Recent orders",
                "question": "What are the most recent orders?",
                "sql": "SELECT order_id, total_amount FROM ORDERS ORDER BY order_id DESC",
                "use_as_onboarding_question": True,
            }
        ]
    }

    # Model description response for when _summarize_model_description is called
    model_description_payload = (
        "This is an orders model for tracking sales and payments."
    )

    client = _FakeDashscopeClient(
        [
            table_payload,
            table_payload_payments,
            model_description_payload,
            model_metrics_payload,
            verified_queries_payload,
        ]
    )
    session = _FakeSession()

    enrich_semantic_model(
        model,
        [
            (
                FQNParts(database="SALES", schema_name="PUBLIC", table="ORDERS"),
                raw_orders,
            ),
            (
                FQNParts(database="SALES", schema_name="PUBLIC", table="PAYMENTS"),
                raw_payments,
            ),
        ],
        client,
        placeholder="  ",
        session=session,
    )

    # Model-level metrics appended
    assert len(model.metrics) == 1
    metric = model.metrics[0]
    assert metric.expr == "SUM(ORDERS.total_amount)"
    assert metric.description == "Sum of total_amount across all orders."
    assert "Revenue" in list(metric.synonyms)

    # Verified queries validated and appended
    assert len(model.verified_queries) == 1
    verified = model.verified_queries[0]
    assert verified.question == "What are the most recent orders?"
    assert verified.sql.strip().lower().endswith("limit 200")
    assert verified.use_as_onboarding_question is True
    assert verified.verified_by
    assert session.queries and session.queries[0] == verified.sql


def test_model_metrics_generated_with_single_fact_table() -> None:
    raw_orders = Table(
        id_=0,
        name="orders",
        columns=[
            Column(
                id_=0, column_name="order_id", column_type="NUMBER", values=["1", "2"]
            ),
            Column(
                id_=1,
                column_name="total_amount",
                column_type="NUMBER",
                values=["10", "20"],
            ),
        ],
    )

    orders_proto = semantic_model_pb2.Table(
        name="ORDERS",
        description="  ",
        base_table=semantic_model_pb2.FullyQualifiedTable(
            database="SALES", schema="PUBLIC", table="ORDERS"
        ),
        facts=[
            semantic_model_pb2.Fact(
                name="total_amount",
                expr="total_amount",
                data_type="DECIMAL",
                description="  ",
            )
        ],
    )

    model = semantic_model_pb2.SemanticModel(name="Orders Model", tables=[orders_proto])

    table_payload = {
        "table_description": "Orders fact table with totals.",
        "columns": [],
        "business_metrics": [],
        "filters": [],
    }

    model_metrics_payload = {
        "model_metrics": [
            {
                "name": "Total Order Value",
                "expr": "SUM(ORDERS.total_amount)",
                "description": "Total value across all orders.",
                "synonyms": ["Revenue"],
            }
        ]
    }

    verified_queries_payload = {"verified_queries": []}

    # Model description response for when _summarize_model_description is called
    model_description_payload = "This is an orders model for tracking order metrics."

    client = _FakeDashscopeClient(
        [
            table_payload,
            model_description_payload,
            model_metrics_payload,
            verified_queries_payload,
        ]
    )
    session = _FakeSession()

    enrich_semantic_model(
        model,
        [
            (
                FQNParts(database="SALES", schema_name="PUBLIC", table="ORDERS"),
                raw_orders,
            )
        ],
        client,
        placeholder="  ",
        session=session,
    )

    # Model-level metrics should now be generated even with single fact table
    assert len(model.metrics) == 1
    metric = model.metrics[0]
    assert metric.expr == "SUM(ORDERS.total_amount)"
    assert metric.description == "Total value across all orders."
    assert "Revenue" in list(metric.synonyms)


def test_model_metrics_skipped_with_no_facts() -> None:
    raw_customers = Table(
        id_=0,
        name="customers",
        columns=[
            Column(
                id_=0,
                column_name="customer_id",
                column_type="NUMBER",
                values=["1", "2"],
            ),
            Column(
                id_=1,
                column_name="customer_name",
                column_type="STRING",
                values=["Alice", "Bob"],
            ),
        ],
    )

    customers_proto = semantic_model_pb2.Table(
        name="CUSTOMERS",
        description="  ",
        base_table=semantic_model_pb2.FullyQualifiedTable(
            database="SALES", schema="PUBLIC", table="CUSTOMERS"
        ),
        dimensions=[
            semantic_model_pb2.Dimension(
                name="customer_name",
                expr="customer_name",
                data_type="STRING",
                description="  ",
            )
        ],
    )

    model = semantic_model_pb2.SemanticModel(
        name="Customer Model", tables=[customers_proto]
    )

    table_payload = {
        "table_description": "Customer dimension table.",
        "columns": [],
        "business_metrics": [],
        "filters": [],
    }

    model_metrics_payload = {
        "model_metrics": [
            {
                "name": "Should not be added",
                "expr": "COUNT(CUSTOMERS.customer_id)",
                "description": "Not expected.",
            }
        ]
    }

    verified_queries_payload = {"verified_queries": []}

    # Model description response for when _summarize_model_description is called
    model_description_payload = "This is a customer dimension model."

    client = _FakeDashscopeClient(
        [
            table_payload,
            model_description_payload,
            model_metrics_payload,
            verified_queries_payload,
        ]
    )
    session = _FakeSession()

    enrich_semantic_model(
        model,
        [
            (
                FQNParts(database="SALES", schema_name="PUBLIC", table="CUSTOMERS"),
                raw_customers,
            )
        ],
        client,
        placeholder="  ",
        session=session,
    )

    # Model-level metrics should be skipped because no facts exist
    assert len(model.metrics) == 0
