from __future__ import annotations

from typing import Iterable, Tuple

from semantic_model_generator.relationships.discovery import (
    RelationshipDiscoveryResult,
    discover_relationships_from_table_definitions,
)


def _discover_relationship_pairs(
    payload: Iterable[dict],
    *,
    min_confidence: float = 0.6,
    max_relationships: int = 50,
) -> Tuple[RelationshipDiscoveryResult, set[Tuple[str, str]]]:
    """Helper that returns discovery result and (left_table, right_table) pairs."""
    result = discover_relationships_from_table_definitions(
        payload,
        min_confidence=min_confidence,
        max_relationships=max_relationships,
    )
    pairs = {(rel.left_table, rel.right_table) for rel in result.relationships}
    return result, pairs


def test_star_schema_fact_orders_links_all_dimensions() -> None:
    """Classic star schema: FACT_ORDERS should link to all surrounding dimensions."""
    payload = [
        {
            "table_name": "dim_customer",
            "columns": [
                {"name": "customer_key", "type": "NUMBER", "is_primary_key": True},
                {"name": "customer_name", "type": "STRING"},
                {"name": "customer_segment", "type": "STRING"},
            ],
        },
        {
            "table_name": "dim_product",
            "columns": [
                {"name": "product_key", "type": "NUMBER", "is_primary_key": True},
                {"name": "product_name", "type": "STRING"},
                {"name": "category", "type": "STRING"},
            ],
        },
        {
            "table_name": "dim_date",
            "columns": [
                {"name": "date_key", "type": "NUMBER", "is_primary_key": True},
                {"name": "calendar_date", "type": "DATE"},
                {"name": "fiscal_week", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "fact_orders",
            "columns": [
                {"name": "order_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "order_date_key", "type": "NUMBER"},
                {"name": "customer_key", "type": "NUMBER"},
                {"name": "product_key", "type": "NUMBER"},
                {"name": "order_amount", "type": "NUMBER"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload)

    expected_pairs = {
        ("FACT_ORDERS", "DIM_CUSTOMER"),
        ("FACT_ORDERS", "DIM_PRODUCT"),
        ("FACT_ORDERS", "DIM_DATE"),
    }
    assert expected_pairs <= pairs


def test_tpch_subset_relationships_detected() -> None:
    """Ensure TPC-H style naming is resolved into the expected join graph."""
    payload = [
        {
            "table_name": "customer",
            "columns": [
                {"name": "c_custkey", "type": "NUMBER", "is_primary_key": True},
                {"name": "c_nationkey", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "orders",
            "columns": [
                {"name": "o_orderkey", "type": "NUMBER", "is_primary_key": True},
                {"name": "o_custkey", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "lineitem",
            "columns": [
                {"name": "l_orderkey", "type": "NUMBER", "is_primary_key": True},
                {"name": "l_linenumber", "type": "NUMBER", "is_primary_key": True},
                {"name": "l_partkey", "type": "NUMBER"},
                {"name": "l_suppkey", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "part",
            "columns": [
                {"name": "p_partkey", "type": "NUMBER", "is_primary_key": True},
            ],
        },
        {
            "table_name": "supplier",
            "columns": [
                {"name": "s_suppkey", "type": "NUMBER", "is_primary_key": True},
                {"name": "s_nationkey", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "nation",
            "columns": [
                {"name": "n_nationkey", "type": "NUMBER", "is_primary_key": True},
                {"name": "n_regionkey", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "region",
            "columns": [
                {"name": "r_regionkey", "type": "NUMBER", "is_primary_key": True},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload)

    expected_pairs = {
        ("ORDERS", "CUSTOMER"),
        ("LINEITEM", "ORDERS"),
        ("LINEITEM", "PART"),
        ("LINEITEM", "SUPPLIER"),
        ("SUPPLIER", "NATION"),
        ("CUSTOMER", "NATION"),
        ("NATION", "REGION"),
    }
    assert expected_pairs <= pairs


def test_bridge_table_creates_many_to_many_link() -> None:
    """
    Two-way fact bridge: ORDER_ITEMS joins ORDERS and PRODUCTS and yields derived relationship.
    """
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
                {"name": "sku", "type": "STRING"},
            ],
        },
        {
            "table_name": "order_items",
            "columns": [
                {"name": "order_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "product_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "quantity", "type": "NUMBER"},
            ],
        },
    ]

    result, pairs = _discover_relationship_pairs(payload)

    assert ("ORDER_ITEMS", "ORDERS") in pairs
    assert ("ORDER_ITEMS", "PRODUCTS") in pairs

    # The derived bridge relationship should reference both tables.
    bridge_names = [
        rel.name.lower()
        for rel in result.relationships
        if rel.left_table == "ORDERS" and rel.right_table == "PRODUCTS"
    ]
    assert bridge_names, "Expected derived ORDERS -> PRODUCTS relationship via bridge"
    assert any("order_items" in name or "_via_" in name for name in bridge_names)


def test_snowflake_style_hub_and_spoke() -> None:
    """
    Snowflake-style schema: DIM_CUSTOMER normalized into hub + satellite tables.
    Ensures relationships propagate through hub to satellites.
    """
    payload = [
        {
            "table_name": "dim_customer",
            "columns": [
                {"name": "customer_key", "type": "NUMBER", "is_primary_key": True},
                {"name": "current_address_key", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "dim_customer_attributes",
            "columns": [
                {"name": "customer_key", "type": "NUMBER", "is_primary_key": True},
                {"name": "email", "type": "STRING"},
                {"name": "phone", "type": "STRING"},
            ],
        },
        {
            "table_name": "dim_customer_address",
            "columns": [
                {"name": "address_key", "type": "NUMBER", "is_primary_key": True},
                {"name": "street", "type": "STRING"},
                {"name": "city", "type": "STRING"},
            ],
        },
        {
            "table_name": "fact_subscription",
            "columns": [
                {"name": "subscription_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "customer_key", "type": "NUMBER"},
                {"name": "start_date_key", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "dim_date",
            "columns": [
                {"name": "date_key", "type": "NUMBER", "is_primary_key": True},
                {"name": "calendar_date", "type": "DATE"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload)

    expected_pairs = {
        ("FACT_SUBSCRIPTION", "DIM_CUSTOMER"),
        ("FACT_SUBSCRIPTION", "DIM_DATE"),
        ("DIM_CUSTOMER", "DIM_CUSTOMER_ATTRIBUTES"),
        ("DIM_CUSTOMER", "DIM_CUSTOMER_ADDRESS"),
    }
    assert expected_pairs <= pairs


def test_saas_crm_pipeline_schema() -> None:
    """
    Salesforce/CRM style pipeline: accounts, opportunities, contacts, users.
    Checks that role-based foreign keys go to the right tables.
    """
    payload = [
        {
            "table_name": "accounts",
            "columns": [
                {"name": "account_id", "type": "STRING", "is_primary_key": True},
                {"name": "parent_account_id", "type": "STRING"},
            ],
        },
        {
            "table_name": "opportunities",
            "columns": [
                {"name": "opportunity_id", "type": "STRING", "is_primary_key": True},
                {"name": "account_id", "type": "STRING"},
                {"name": "owner_user_id", "type": "STRING"},
            ],
        },
        {
            "table_name": "contacts",
            "columns": [
                {"name": "contact_id", "type": "STRING", "is_primary_key": True},
                {"name": "account_id", "type": "STRING"},
                {"name": "owner_user_id", "type": "STRING"},
            ],
        },
        {
            "table_name": "users",
            "columns": [
                {"name": "user_id", "type": "STRING", "is_primary_key": True},
                {"name": "manager_id", "type": "STRING"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload, min_confidence=0.5)

    expected_pairs = {
        ("OPPORTUNITIES", "ACCOUNTS"),
        ("CONTACTS", "ACCOUNTS"),
        ("OPPORTUNITIES", "USERS"),
        ("CONTACTS", "USERS"),
        ("ACCOUNTS", "ACCOUNTS"),  # self-parenting should be ignored
    }
    assert ("OPPORTUNITIES", "ACCOUNTS") in pairs
    assert ("CONTACTS", "ACCOUNTS") in pairs
    assert ("OPPORTUNITIES", "USERS") in pairs
    assert ("CONTACTS", "USERS") in pairs
    # Self relationship must not be created even though parent_account_id exists.
    assert ("ACCOUNTS", "ACCOUNTS") not in pairs


def test_finance_ledger_schema_detects_balanced_relationships() -> None:
    """
    General ledger: journal entries -> journal lines -> accounts, cost centers, employees.
    Ensures composite keys and suffix-based matches work.
    """
    payload = [
        {
            "table_name": "gl_journal_entry",
            "columns": [
                {"name": "journal_entry_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "batch_id", "type": "NUMBER"},
                {"name": "entered_by_employee_id", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "gl_journal_line",
            "columns": [
                {"name": "journal_entry_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "journal_line_number", "type": "NUMBER", "is_primary_key": True},
                {"name": "account_id", "type": "NUMBER"},
                {"name": "cost_center_id", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "dim_account",
            "columns": [
                {"name": "account_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "account_type", "type": "STRING"},
            ],
        },
        {
            "table_name": "dim_cost_center",
            "columns": [
                {"name": "cost_center_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "division", "type": "STRING"},
            ],
        },
        {
            "table_name": "dim_employee",
            "columns": [
                {"name": "employee_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "manager_employee_id", "type": "NUMBER"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload)

    expected_pairs = {
        ("GL_JOURNAL_LINE", "GL_JOURNAL_ENTRY"),
        ("GL_JOURNAL_LINE", "DIM_ACCOUNT"),
        ("GL_JOURNAL_LINE", "DIM_COST_CENTER"),
        ("GL_JOURNAL_ENTRY", "DIM_EMPLOYEE"),
    }
    assert expected_pairs <= pairs


def test_manufacturing_shop_floor_schema() -> None:
    """
    Manufacturing shop floor: production orders, work orders, machines, BOM components.
    Validates that hierarchical IDs connect correctly across operational tables.
    """
    payload = [
        {
            "table_name": "prod_order",
            "columns": [
                {"name": "prod_order_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "item_id", "type": "NUMBER"},
                {"name": "customer_id", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "work_order",
            "columns": [
                {"name": "work_order_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "prod_order_id", "type": "NUMBER"},
                {"name": "machine_id", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "machine",
            "columns": [
                {"name": "machine_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "work_center_id", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "work_center",
            "columns": [
                {"name": "work_center_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "plant_id", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "bom_component",
            "columns": [
                {"name": "prod_order_id", "type": "NUMBER", "is_primary_key": True},
                {"name": "component_id", "type": "NUMBER", "is_primary_key": True},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload)

    expected_pairs = {
        ("WORK_ORDER", "PROD_ORDER"),
        ("WORK_ORDER", "MACHINE"),
        ("MACHINE", "WORK_CENTER"),
        ("BOM_COMPONENT", "PROD_ORDER"),
    }
    assert expected_pairs <= pairs


def test_marketing_attribution_schema() -> None:
    """
    Multi-touch attribution: campaigns -> channels -> touches -> conversions.
    Verifies that channel/touch relationships align without mis-linking conversions.
    """
    payload = [
        {
            "table_name": "dim_campaign",
            "columns": [
                {"name": "campaign_id", "type": "STRING", "is_primary_key": True},
                {"name": "channel_id", "type": "STRING"},
            ],
        },
        {
            "table_name": "dim_channel",
            "columns": [
                {"name": "channel_id", "type": "STRING", "is_primary_key": True},
                {"name": "parent_channel_id", "type": "STRING"},
            ],
        },
        {
            "table_name": "fact_touch",
            "columns": [
                {"name": "touch_id", "type": "STRING", "is_primary_key": True},
                {"name": "campaign_id", "type": "STRING"},
                {"name": "user_id", "type": "STRING"},
            ],
        },
        {
            "table_name": "fact_conversion",
            "columns": [
                {"name": "conversion_id", "type": "STRING", "is_primary_key": True},
                {"name": "touch_id", "type": "STRING"},
                {"name": "user_id", "type": "STRING"},
            ],
        },
        {
            "table_name": "dim_user",
            "columns": [
                {"name": "user_id", "type": "STRING", "is_primary_key": True},
                {"name": "household_id", "type": "STRING"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload, min_confidence=0.5)

    expected_pairs = {
        ("FACT_TOUCH", "DIM_CAMPAIGN"),
        ("DIM_CAMPAIGN", "DIM_CHANNEL"),
        ("FACT_TOUCH", "DIM_USER"),
        ("FACT_CONVERSION", "FACT_TOUCH"),
        ("FACT_CONVERSION", "DIM_USER"),
    }
    assert expected_pairs <= pairs
    # Ensure no direct campaign->conversion relationship is assumed.
    assert ("FACT_CONVERSION", "DIM_CAMPAIGN") not in pairs


def test_healthcare_encounter_schema() -> None:
    """
    Healthcare EMR-style model: patients, encounters, providers, diagnoses, procedures.
    Ensures that encounter-level many-to-many tables connect to both sides.
    """
    payload = [
        {
            "table_name": "dim_patient",
            "columns": [
                {"name": "patient_id", "type": "STRING", "is_primary_key": True},
                {"name": "primary_provider_id", "type": "STRING"},
            ],
        },
        {
            "table_name": "dim_provider",
            "columns": [
                {"name": "provider_id", "type": "STRING", "is_primary_key": True},
                {"name": "specialty", "type": "STRING"},
            ],
        },
        {
            "table_name": "fact_encounter",
            "columns": [
                {"name": "encounter_id", "type": "STRING", "is_primary_key": True},
                {"name": "patient_id", "type": "STRING"},
                {"name": "attending_provider_id", "type": "STRING"},
            ],
        },
        {
            "table_name": "fact_encounter_diagnosis",
            "columns": [
                {"name": "encounter_id", "type": "STRING", "is_primary_key": True},
                {"name": "diagnosis_code", "type": "STRING", "is_primary_key": True},
            ],
        },
        {
            "table_name": "dim_diagnosis",
            "columns": [
                {"name": "diagnosis_code", "type": "STRING", "is_primary_key": True},
                {"name": "icd_chapter", "type": "STRING"},
            ],
        },
        {
            "table_name": "fact_encounter_procedure",
            "columns": [
                {"name": "encounter_id", "type": "STRING", "is_primary_key": True},
                {"name": "procedure_code", "type": "STRING", "is_primary_key": True},
            ],
        },
        {
            "table_name": "dim_procedure",
            "columns": [
                {"name": "procedure_code", "type": "STRING", "is_primary_key": True},
                {"name": "category", "type": "STRING"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload, min_confidence=0.5)

    expected_pairs = {
        ("FACT_ENCOUNTER", "DIM_PATIENT"),
        ("FACT_ENCOUNTER", "DIM_PROVIDER"),
        ("FACT_ENCOUNTER_DIAGNOSIS", "FACT_ENCOUNTER"),
        ("FACT_ENCOUNTER_DIAGNOSIS", "DIM_DIAGNOSIS"),
        ("FACT_ENCOUNTER_PROCEDURE", "FACT_ENCOUNTER"),
        ("FACT_ENCOUNTER_PROCEDURE", "DIM_PROCEDURE"),
    }
    assert expected_pairs <= pairs


def test_banking_core_system_schema() -> None:
    """
    传统银行核心系统：客户、账户、交易、产品（高标准化建模）
    Validates well-structured banking relationships with proper naming conventions.
    Tests composite keys in account-customer relationships.
    """
    payload = [
        {
            "table_name": "cust_info",
            "columns": [
                {"name": "cust_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "cust_name", "type": "VARCHAR"},
                {"name": "cust_type", "type": "VARCHAR"},  # 个人/企业
                {"name": "branch_id", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "branch_info",
            "columns": [
                {"name": "branch_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "branch_name", "type": "VARCHAR"},
                {"name": "region_id", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "region_info",
            "columns": [
                {"name": "region_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "region_name", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "account_info",
            "columns": [
                {"name": "account_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "cust_id", "type": "VARCHAR"},
                {"name": "product_id", "type": "VARCHAR"},
                {"name": "account_status", "type": "VARCHAR"},
                {"name": "open_date", "type": "DATE"},
            ],
        },
        {
            "table_name": "product_info",
            "columns": [
                {"name": "product_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "product_name", "type": "VARCHAR"},
                {"name": "product_type", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "transaction_detail",
            "columns": [
                {"name": "trans_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "account_id", "type": "VARCHAR"},
                {"name": "trans_type", "type": "VARCHAR"},
                {"name": "trans_amount", "type": "DECIMAL"},
                {"name": "trans_date", "type": "TIMESTAMP"},
                {"name": "counterparty_account_id", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "cust_account_rel",
            "columns": [
                {"name": "cust_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "account_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "relation_type", "type": "VARCHAR"},  # 主账户持有人/联名账户
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload, min_confidence=0.5)

    expected_pairs = {
        ("CUST_INFO", "BRANCH_INFO"),
        ("BRANCH_INFO", "REGION_INFO"),
        ("ACCOUNT_INFO", "CUST_INFO"),
        ("ACCOUNT_INFO", "PRODUCT_INFO"),
        ("TRANSACTION_DETAIL", "ACCOUNT_INFO"),
        ("CUST_ACCOUNT_REL", "CUST_INFO"),
        ("CUST_ACCOUNT_REL", "ACCOUNT_INFO"),
    }
    assert expected_pairs <= pairs


def test_internet_lending_platform_schema() -> None:
    """
    互联网借贷平台：用户、贷款、还款、风控（中等标准化建模）
    Tests internet finance with mixed naming patterns and composite keys.
    Validates loan-repayment relationship and risk assessment linkage.
    """
    payload = [
        {
            "table_name": "user",
            "columns": [
                {"name": "user_id", "type": "BIGINT", "is_primary_key": True},
                {"name": "mobile", "type": "VARCHAR"},
                {"name": "id_card_no", "type": "VARCHAR"},
                {"name": "risk_level", "type": "INT"},
            ],
        },
        {
            "table_name": "loan_application",
            "columns": [
                {"name": "application_id", "type": "BIGINT", "is_primary_key": True},
                {"name": "user_id", "type": "BIGINT"},
                {"name": "apply_amount", "type": "DECIMAL"},
                {"name": "product_code", "type": "VARCHAR"},
                {"name": "application_status", "type": "VARCHAR"},
                {"name": "apply_time", "type": "TIMESTAMP"},
            ],
        },
        {
            "table_name": "loan_contract",
            "columns": [
                {"name": "contract_id", "type": "BIGINT", "is_primary_key": True},
                {"name": "application_id", "type": "BIGINT"},
                {"name": "user_id", "type": "BIGINT"},
                {"name": "loan_amount", "type": "DECIMAL"},
                {"name": "interest_rate", "type": "DECIMAL"},
                {"name": "contract_status", "type": "VARCHAR"},
                {"name": "sign_time", "type": "TIMESTAMP"},
            ],
        },
        {
            "table_name": "repayment_plan",
            "columns": [
                {"name": "contract_id", "type": "BIGINT", "is_primary_key": True},
                {"name": "period_no", "type": "INT", "is_primary_key": True},
                {"name": "due_date", "type": "DATE"},
                {"name": "due_amount", "type": "DECIMAL"},
                {"name": "repayment_status", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "repayment_record",
            "columns": [
                {"name": "repayment_id", "type": "BIGINT", "is_primary_key": True},
                {"name": "contract_id", "type": "BIGINT"},
                {"name": "period_no", "type": "INT"},
                {"name": "repay_amount", "type": "DECIMAL"},
                {"name": "repay_time", "type": "TIMESTAMP"},
            ],
        },
        {
            "table_name": "risk_assessment",
            "columns": [
                {"name": "assessment_id", "type": "BIGINT", "is_primary_key": True},
                {"name": "application_id", "type": "BIGINT"},
                {"name": "user_id", "type": "BIGINT"},
                {"name": "risk_score", "type": "INT"},
                {"name": "assessment_time", "type": "TIMESTAMP"},
            ],
        },
        {
            "table_name": "loan_product",
            "columns": [
                {"name": "product_code", "type": "VARCHAR", "is_primary_key": True},
                {"name": "product_name", "type": "VARCHAR"},
                {"name": "max_amount", "type": "DECIMAL"},
                {"name": "min_rate", "type": "DECIMAL"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload, min_confidence=0.5)

    expected_pairs = {
        ("LOAN_APPLICATION", "USER"),
        ("LOAN_APPLICATION", "LOAN_PRODUCT"),
        ("LOAN_CONTRACT", "LOAN_APPLICATION"),
        ("LOAN_CONTRACT", "USER"),
        ("REPAYMENT_PLAN", "LOAN_CONTRACT"),
        ("REPAYMENT_RECORD", "LOAN_CONTRACT"),
        ("RISK_ASSESSMENT", "LOAN_APPLICATION"),
        ("RISK_ASSESSMENT", "USER"),
    }
    assert expected_pairs <= pairs


def test_payment_transaction_schema() -> None:
    """
    第三方支付平台：订单、支付、清算、对账（低标准化建模）
    Tests poorly normalized schema with inconsistent naming and missing proper FKs.
    Validates the system can still discover relationships despite non-standard patterns.
    """
    payload = [
        {
            "table_name": "t_order",
            "columns": [
                {"name": "order_no", "type": "VARCHAR", "is_primary_key": True},
                {"name": "merchant_no", "type": "VARCHAR"},
                {"name": "user_no", "type": "VARCHAR"},
                {"name": "amount", "type": "DECIMAL"},
                {"name": "create_time", "type": "TIMESTAMP"},
            ],
        },
        {
            "table_name": "t_merchant",
            "columns": [
                {"name": "merchant_no", "type": "VARCHAR", "is_primary_key": True},
                {"name": "merchant_name", "type": "VARCHAR"},
                {"name": "industry_code", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "t_user_account",
            "columns": [
                {"name": "user_no", "type": "VARCHAR", "is_primary_key": True},
                {"name": "balance", "type": "DECIMAL"},
                {"name": "status", "type": "INT"},
            ],
        },
        {
            "table_name": "t_pay",
            "columns": [
                {"name": "pay_no", "type": "VARCHAR", "is_primary_key": True},
                {"name": "order_no", "type": "VARCHAR"},
                {"name": "channel", "type": "VARCHAR"},
                {"name": "pay_amount", "type": "DECIMAL"},
                {"name": "pay_time", "type": "TIMESTAMP"},
            ],
        },
        {
            "table_name": "t_channel",
            "columns": [
                {"name": "channel", "type": "VARCHAR", "is_primary_key": True},
                {"name": "channel_name", "type": "VARCHAR"},
                {"name": "fee_rate", "type": "DECIMAL"},
            ],
        },
        {
            "table_name": "t_settlement",
            "columns": [
                {"name": "settlement_id", "type": "BIGINT", "is_primary_key": True},
                {"name": "merchant_no", "type": "VARCHAR"},
                {"name": "settlement_date", "type": "DATE"},
                {"name": "settlement_amount", "type": "DECIMAL"},
            ],
        },
        {
            "table_name": "t_settlement_detail",
            "columns": [
                {"name": "settlement_id", "type": "BIGINT", "is_primary_key": True},
                {"name": "pay_no", "type": "VARCHAR", "is_primary_key": True},
                {"name": "settle_amount", "type": "DECIMAL"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload, min_confidence=0.5)

    expected_pairs = {
        ("T_ORDER", "T_MERCHANT"),
        ("T_ORDER", "T_USER_ACCOUNT"),
        ("T_PAY", "T_ORDER"),
        ("T_PAY", "T_CHANNEL"),
        ("T_SETTLEMENT", "T_MERCHANT"),
        ("T_SETTLEMENT_DETAIL", "T_SETTLEMENT"),
        ("T_SETTLEMENT_DETAIL", "T_PAY"),
    }
    assert expected_pairs <= pairs


def test_securities_trading_schema() -> None:
    """
    证券交易系统：账户、委托、成交、持仓（高标准化，复杂复合键）
    Tests securities trading with composite keys and temporal relationships.
    Validates order-fill matching and position calculation linkage.
    """
    payload = [
        {
            "table_name": "trading_account",
            "columns": [
                {"name": "account_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "customer_id", "type": "VARCHAR"},
                {"name": "account_type", "type": "VARCHAR"},
                {"name": "cash_balance", "type": "DECIMAL"},
            ],
        },
        {
            "table_name": "customer",
            "columns": [
                {"name": "customer_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "customer_name", "type": "VARCHAR"},
                {"name": "id_number", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "stock_order",
            "columns": [
                {"name": "order_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "account_id", "type": "VARCHAR"},
                {"name": "stock_code", "type": "VARCHAR"},
                {"name": "order_type", "type": "VARCHAR"},  # 买入/卖出
                {"name": "order_price", "type": "DECIMAL"},
                {"name": "order_qty", "type": "BIGINT"},
                {"name": "order_time", "type": "TIMESTAMP"},
                {"name": "order_status", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "stock_info",
            "columns": [
                {"name": "stock_code", "type": "VARCHAR", "is_primary_key": True},
                {"name": "stock_name", "type": "VARCHAR"},
                {"name": "exchange", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "trade_fill",
            "columns": [
                {"name": "fill_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "order_id", "type": "VARCHAR"},
                {"name": "fill_price", "type": "DECIMAL"},
                {"name": "fill_qty", "type": "BIGINT"},
                {"name": "fill_time", "type": "TIMESTAMP"},
            ],
        },
        {
            "table_name": "position",
            "columns": [
                {"name": "account_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "stock_code", "type": "VARCHAR", "is_primary_key": True},
                {"name": "position_qty", "type": "BIGINT"},
                {"name": "cost_price", "type": "DECIMAL"},
                {"name": "market_value", "type": "DECIMAL"},
            ],
        },
        {
            "table_name": "daily_pnl",
            "columns": [
                {"name": "account_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "trade_date", "type": "DATE", "is_primary_key": True},
                {"name": "realized_pnl", "type": "DECIMAL"},
                {"name": "unrealized_pnl", "type": "DECIMAL"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload, min_confidence=0.5)

    expected_pairs = {
        ("TRADING_ACCOUNT", "CUSTOMER"),
        ("STOCK_ORDER", "TRADING_ACCOUNT"),
        ("STOCK_ORDER", "STOCK_INFO"),
        ("TRADE_FILL", "STOCK_ORDER"),
        ("POSITION", "TRADING_ACCOUNT"),
        ("POSITION", "STOCK_INFO"),
        ("DAILY_PNL", "TRADING_ACCOUNT"),
    }
    assert expected_pairs <= pairs


def test_insurance_policy_schema() -> None:
    """
    保险核心系统：投保人、保单、理赔、再保险（中等标准化，多对多关系）
    Tests insurance domain with beneficiary relationships and reinsurance links.
    Validates composite keys in policy-insured relationships.
    """
    payload = [
        {
            "table_name": "policy_holder",
            "columns": [
                {"name": "holder_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "holder_name", "type": "VARCHAR"},
                {"name": "contact_phone", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "insurance_policy",
            "columns": [
                {"name": "policy_no", "type": "VARCHAR", "is_primary_key": True},
                {"name": "holder_id", "type": "VARCHAR"},
                {"name": "product_code", "type": "VARCHAR"},
                {"name": "premium", "type": "DECIMAL"},
                {"name": "policy_status", "type": "VARCHAR"},
                {"name": "effective_date", "type": "DATE"},
            ],
        },
        {
            "table_name": "insurance_product",
            "columns": [
                {"name": "product_code", "type": "VARCHAR", "is_primary_key": True},
                {"name": "product_name", "type": "VARCHAR"},
                {"name": "insurance_type", "type": "VARCHAR"},
                {"name": "coverage_amount", "type": "DECIMAL"},
            ],
        },
        {
            "table_name": "insured_person",
            "columns": [
                {"name": "insured_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "insured_name", "type": "VARCHAR"},
                {"name": "birth_date", "type": "DATE"},
            ],
        },
        {
            "table_name": "policy_insured_rel",
            "columns": [
                {"name": "policy_no", "type": "VARCHAR", "is_primary_key": True},
                {"name": "insured_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "insured_type", "type": "VARCHAR"},  # 主被保险人/附加被保险人
            ],
        },
        {
            "table_name": "claim",
            "columns": [
                {"name": "claim_no", "type": "VARCHAR", "is_primary_key": True},
                {"name": "policy_no", "type": "VARCHAR"},
                {"name": "insured_id", "type": "VARCHAR"},
                {"name": "claim_amount", "type": "DECIMAL"},
                {"name": "claim_date", "type": "DATE"},
                {"name": "claim_status", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "claim_detail",
            "columns": [
                {"name": "claim_no", "type": "VARCHAR", "is_primary_key": True},
                {"name": "detail_seq", "type": "INT", "is_primary_key": True},
                {"name": "expense_type", "type": "VARCHAR"},
                {"name": "expense_amount", "type": "DECIMAL"},
            ],
        },
        {
            "table_name": "reinsurance_contract",
            "columns": [
                {"name": "reinsurance_id", "type": "VARCHAR", "is_primary_key": True},
                {"name": "policy_no", "type": "VARCHAR"},
                {"name": "reinsurer_code", "type": "VARCHAR"},
                {"name": "ceded_premium", "type": "DECIMAL"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload, min_confidence=0.5)

    expected_pairs = {
        ("INSURANCE_POLICY", "POLICY_HOLDER"),
        ("INSURANCE_POLICY", "INSURANCE_PRODUCT"),
        ("POLICY_INSURED_REL", "INSURANCE_POLICY"),
        ("POLICY_INSURED_REL", "INSURED_PERSON"),
        ("CLAIM", "INSURANCE_POLICY"),
        ("CLAIM", "INSURED_PERSON"),
        ("CLAIM_DETAIL", "CLAIM"),
        ("REINSURANCE_CONTRACT", "INSURANCE_POLICY"),
    }
    assert expected_pairs <= pairs


def test_no_pk_metadata_retail_ecommerce() -> None:
    """
    电商零售场景：完全没有is_primary_key元数据
    Tests relationship discovery when NO primary key metadata is available.
    System must infer PKs from column naming patterns and uniqueness.
    """
    payload = [
        {
            "table_name": "customer",
            "columns": [
                {"name": "customer_id", "type": "BIGINT"},
                {"name": "customer_name", "type": "VARCHAR"},
                {"name": "email", "type": "VARCHAR"},
                {"name": "register_date", "type": "DATE"},
            ],
        },
        {
            "table_name": "product",
            "columns": [
                {"name": "product_id", "type": "BIGINT"},
                {"name": "product_name", "type": "VARCHAR"},
                {"name": "category_id", "type": "INT"},
                {"name": "price", "type": "DECIMAL"},
            ],
        },
        {
            "table_name": "category",
            "columns": [
                {"name": "category_id", "type": "INT"},
                {"name": "category_name", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "orders",
            "columns": [
                {"name": "order_id", "type": "BIGINT"},
                {"name": "customer_id", "type": "BIGINT"},
                {"name": "order_date", "type": "TIMESTAMP"},
                {"name": "total_amount", "type": "DECIMAL"},
            ],
        },
        {
            "table_name": "order_item",
            "columns": [
                {"name": "order_id", "type": "BIGINT"},
                {"name": "item_seq", "type": "INT"},
                {"name": "product_id", "type": "BIGINT"},
                {"name": "quantity", "type": "INT"},
                {"name": "item_price", "type": "DECIMAL"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload, min_confidence=0.5)

    # System should infer PKs from naming patterns
    expected_pairs = {
        ("PRODUCT", "CATEGORY"),
        ("ORDERS", "CUSTOMER"),
        ("ORDER_ITEM", "ORDERS"),
        ("ORDER_ITEM", "PRODUCT"),
    }
    assert expected_pairs <= pairs


def test_partial_pk_metadata_banking() -> None:
    """
    银行系统：部分表有is_primary_key，部分表没有
    Tests mixed scenario where some tables have PK metadata, others don't.
    Validates system can handle heterogeneous metadata quality.
    """
    payload = [
        {
            "table_name": "account",
            "columns": [
                {"name": "account_no", "type": "VARCHAR", "is_primary_key": True},
                {"name": "customer_id", "type": "VARCHAR"},
                {"name": "account_type", "type": "VARCHAR"},
                {"name": "balance", "type": "DECIMAL"},
            ],
        },
        {
            "table_name": "customer",
            "columns": [
                # NO is_primary_key metadata!
                {"name": "customer_id", "type": "VARCHAR"},
                {"name": "customer_name", "type": "VARCHAR"},
                {"name": "id_number", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "transaction",
            "columns": [
                # Has is_primary_key
                {"name": "trans_id", "type": "BIGINT", "is_primary_key": True},
                {"name": "account_no", "type": "VARCHAR"},
                {"name": "trans_amount", "type": "DECIMAL"},
                {"name": "trans_time", "type": "TIMESTAMP"},
            ],
        },
        {
            "table_name": "card",
            "columns": [
                # NO is_primary_key metadata!
                {"name": "card_no", "type": "VARCHAR"},
                {"name": "account_no", "type": "VARCHAR"},
                {"name": "card_type", "type": "VARCHAR"},
                {"name": "expire_date", "type": "DATE"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload, min_confidence=0.5)

    expected_pairs = {
        ("ACCOUNT", "CUSTOMER"),
        ("TRANSACTION", "ACCOUNT"),
        ("CARD", "ACCOUNT"),
    }
    assert expected_pairs <= pairs


def test_no_pk_with_poor_naming_data_lake() -> None:
    """
    数据湖场景：无PK元数据 + 不规范命名
    Tests challenging scenario: no PK metadata AND somewhat inconsistent naming.
    Validates system can still work with suboptimal but recognizable patterns.
    """
    payload = [
        {
            "table_name": "user_profile",
            "columns": [
                {"name": "user_id", "type": "BIGINT"},  # Still recognizable
                {"name": "name", "type": "VARCHAR"},
                {"name": "phone", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "order_data",
            "columns": [
                {"name": "order_id", "type": "BIGINT"},  # Recognizable
                {"name": "user_id", "type": "BIGINT"},
                {"name": "amount", "type": "DECIMAL"},
                {"name": "create_time", "type": "TIMESTAMP"},
            ],
        },
        {
            "table_name": "payment_info",
            "columns": [
                {"name": "pay_id", "type": "BIGINT"},  # Inconsistent prefix
                {"name": "order_id", "type": "BIGINT"},
                {"name": "pay_amount", "type": "DECIMAL"},
                {"name": "pay_method", "type": "VARCHAR"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload, min_confidence=0.5)

    # System should discover relationships even without explicit PK metadata
    # when column names follow recognizable patterns
    expected_pairs = {
        ("ORDER_DATA", "USER_PROFILE"),
        ("PAYMENT_INFO", "ORDER_DATA"),
    }
    assert expected_pairs <= pairs


def test_no_pk_composite_key_inference() -> None:
    """
    供应链场景：无PK元数据 + 复合键表
    Tests composite key inference without explicit PK metadata.
    System must infer composite PKs from column naming patterns.
    """
    payload = [
        {
            "table_name": "warehouse",
            "columns": [
                {"name": "warehouse_id", "type": "VARCHAR"},
                {"name": "warehouse_name", "type": "VARCHAR"},
                {"name": "region", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "product",
            "columns": [
                {"name": "product_id", "type": "VARCHAR"},
                {"name": "product_name", "type": "VARCHAR"},
                {"name": "category", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "inventory",
            "columns": [
                # Composite key: (warehouse_id, product_id) but no PK metadata
                {"name": "warehouse_id", "type": "VARCHAR"},
                {"name": "product_id", "type": "VARCHAR"},
                {"name": "stock_quantity", "type": "INT"},
                {"name": "last_update", "type": "TIMESTAMP"},
            ],
        },
        {
            "table_name": "stock_movement",
            "columns": [
                {"name": "movement_id", "type": "BIGINT"},
                {"name": "warehouse_id", "type": "VARCHAR"},
                {"name": "product_id", "type": "VARCHAR"},
                {"name": "movement_type", "type": "VARCHAR"},  # IN/OUT
                {"name": "quantity", "type": "INT"},
                {"name": "movement_date", "type": "DATE"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload, min_confidence=0.5)

    expected_pairs = {
        ("INVENTORY", "WAREHOUSE"),
        ("INVENTORY", "PRODUCT"),
        ("STOCK_MOVEMENT", "WAREHOUSE"),
        ("STOCK_MOVEMENT", "PRODUCT"),
    }
    assert expected_pairs <= pairs


def test_no_pk_with_uuid_keys() -> None:
    """
    微服务场景：使用UUID但无PK元数据
    Tests UUID-based keys without PK metadata.
    System should recognize id/uuid column patterns and infer PKs.
    """
    payload = [
        {
            "table_name": "user",
            "columns": [
                {"name": "user_id", "type": "VARCHAR"},  # UUID as string, better naming
                {"name": "username", "type": "VARCHAR"},
                {"name": "email", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "post",
            "columns": [
                {"name": "post_id", "type": "VARCHAR"},  # UUID
                {"name": "user_id", "type": "VARCHAR"},  # FK to user
                {"name": "title", "type": "VARCHAR"},
                {"name": "content", "type": "TEXT"},
                {"name": "created_at", "type": "TIMESTAMP"},
            ],
        },
        {
            "table_name": "comment",
            "columns": [
                {"name": "comment_id", "type": "VARCHAR"},  # UUID, better naming
                {"name": "post_id", "type": "VARCHAR"},  # FK to post
                {"name": "user_id", "type": "VARCHAR"},  # FK to user
                {"name": "content", "type": "TEXT"},
                {"name": "created_at", "type": "TIMESTAMP"},
            ],
        },
        {
            "table_name": "post_like",
            "columns": [
                {"name": "user_id", "type": "VARCHAR"},  # Composite key part 1
                {"name": "post_id", "type": "VARCHAR"},  # Composite key part 2
                {"name": "created_at", "type": "TIMESTAMP"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload, min_confidence=0.5)

    # System should discover basic FK relationships even without PK metadata
    # when using descriptive UUID column names
    expected_pairs = {
        ("POST", "USER"),
        ("COMMENT", "POST"),
        ("COMMENT", "USER"),
        ("POST_LIKE", "USER"),
        ("POST_LIKE", "POST"),
    }
    assert expected_pairs <= pairs


def test_extreme_poor_naming_should_fail() -> None:
    """
    极端情况：超短列名 + 无PK元数据（预期部分失败）
    Tests system boundary: extremely poor naming (uid, oid) with no PK metadata.
    This test documents the limitation - system may not discover all relationships.
    """
    payload = [
        {
            "table_name": "usr",
            "columns": [
                {"name": "uid", "type": "BIGINT"},  # Too short
                {"name": "nm", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "ord",
            "columns": [
                {"name": "oid", "type": "BIGINT"},  # Too short
                {"name": "uid", "type": "BIGINT"},
                {"name": "amt", "type": "DECIMAL"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload, min_confidence=0.5)

    # With extremely poor naming, system cannot reliably discover relationships
    # This is a documented limitation, not a bug
    # The test passes if we find at least 0 relationships (no crash)
    assert isinstance(pairs, set)  # Just verify no crash
    # Note: We don't assert any specific relationships because
    # the naming is too poor for reliable discovery


def test_sample_data_inference_poor_naming() -> None:
    """
    样例数据推断：列名差但有样例数据
    Tests relationship discovery using sample data when column naming is poor.
    System should analyze uniqueness patterns in sample values.
    """
    payload = [
        {
            "table_name": "usr",
            "columns": [
                {
                    "name": "uid",
                    "type": "BIGINT",
                    "sample_values": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                                     11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                                     21, 22, 23, 24, 25],  # Unique values (PK pattern)
                },
                {"name": "nm", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "ord",
            "columns": [
                {
                    "name": "oid",
                    "type": "BIGINT",
                    "sample_values": [101, 102, 103, 104, 105, 106, 107, 108, 109, 110,
                                     111, 112, 113, 114, 115, 116, 117, 118, 119, 120,
                                     121, 122, 123, 124, 125],  # Unique values
                },
                {
                    "name": "uid",
                    "type": "BIGINT",
                    "sample_values": [1, 2, 1, 3, 2, 4, 3, 5, 4, 6,
                                     5, 7, 6, 8, 7, 9, 8, 10, 9, 11,
                                     10, 12, 11, 13, 12],  # Repeating values (FK pattern)
                },
                {"name": "amt", "type": "DECIMAL"},
            ],
        },
        {
            "table_name": "pay",
            "columns": [
                {
                    "name": "pid",
                    "type": "BIGINT",
                    "sample_values": [201, 202, 203, 204, 205, 206, 207, 208, 209, 210,
                                     211, 212, 213, 214, 215, 216, 217, 218, 219, 220,
                                     221, 222, 223, 224, 225],  # Unique values
                },
                {
                    "name": "oid",
                    "type": "BIGINT",
                    "sample_values": [101, 102, 103, 101, 104, 105, 102, 106, 107, 103,
                                     108, 109, 104, 110, 111, 105, 112, 113, 106, 114,
                                     115, 107, 116, 117, 108],  # Repeating values (FK pattern)
                },
                {"name": "method", "type": "VARCHAR"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload, min_confidence=0.5)

    # With sample data showing clear uniqueness patterns, system should discover:
    # - ord.uid -> usr.uid (FK has repeating values, PK has unique values)
    # - pay.oid -> ord.oid (FK has repeating values, PK has unique values)
    expected_pairs = {
        ("ORD", "USR"),
        ("PAY", "ORD"),
    }
    assert expected_pairs <= pairs


def test_sample_data_composite_key_inference() -> None:
    """
    样例数据推断复合键：通过样例数据识别复合主键
    Tests composite key inference from sample data patterns.
    System should detect when multiple columns form a unique combination.
    """
    payload = [
        {
            "table_name": "store",
            "columns": [
                {
                    "name": "sid",
                    "type": "INT",
                    "sample_values": [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4,
                                     5, 5, 5, 6, 6, 6, 7, 7],  # Repeating
                },
                {"name": "store_name", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "product",
            "columns": [
                {
                    "name": "pid",
                    "type": "INT",
                    "sample_values": [101, 102, 103, 104, 105, 106, 107, 108, 109, 110,
                                     111, 112, 113, 114, 115, 116, 117, 118, 119, 120],  # Unique
                },
                {"name": "product_name", "type": "VARCHAR"},
            ],
        },
        {
            "table_name": "inventory",
            "columns": [
                {
                    "name": "sid",
                    "type": "INT",
                    "sample_values": [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4,
                                     5, 5, 5, 6, 6, 6, 7, 7],  # Repeating (part of composite PK)
                },
                {
                    "name": "pid",
                    "type": "INT",
                    "sample_values": [101, 102, 103, 101, 102, 103, 101, 102, 103, 101,
                                     102, 103, 101, 102, 103, 101, 102, 103, 101, 102],  # Repeating (part of composite PK)
                },
                {"name": "qty", "type": "INT"},
            ],
        },
    ]

    _, pairs = _discover_relationship_pairs(payload, min_confidence=0.5)

    # System should recognize:
    # - inventory.sid -> store.sid (even though both have repeating values in samples)
    # - inventory.pid -> product.pid (FK has repeating values, PK has unique values)
    expected_pairs = {
        ("INVENTORY", "STORE"),
        ("INVENTORY", "PRODUCT"),
    }
    assert expected_pairs <= pairs
