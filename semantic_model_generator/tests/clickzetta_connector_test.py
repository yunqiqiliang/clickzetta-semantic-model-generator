from importlib import reload
from unittest import mock

import pandas as pd

from semantic_model_generator.clickzetta_utils import clickzetta_connector as connector
from semantic_model_generator.clickzetta_utils import env_vars


def test_fetch_stages_includes_user_volume(monkeypatch):
    data = pd.DataFrame({"name": ["shared_stage"]})
    with mock.patch.object(connector, "_execute_query_to_pandas", return_value=data):
        stages = connector.fetch_stages_in_schema(
            connection=mock.MagicMock(), schema_name="WORKSPACE.SCHEMA"
        )
    assert stages[0] == "volume:user://~/semantic_models/"
    assert "shared_stage" in stages


def test_fetch_yaml_names_in_user_volume(monkeypatch):
    data = pd.DataFrame(
        {
            "relative_path": [
                "semantic_models/example.yaml",
                "semantic_models/duplicate.yaml",
                "semantic_models/duplicate.yaml",
            ]
        }
    )
    with mock.patch.object(connector, "_execute_query_to_pandas", return_value=data):
        files = connector.fetch_yaml_names_in_stage(
            connection=mock.MagicMock(),
            stage="volume:user://~/semantic_models/",
            include_yml=True,
        )
    assert files == ["example.yaml", "duplicate.yaml"]


def test_build_base_connection_config_includes_hints(monkeypatch):
    monkeypatch.setenv("CLICKZETTA_SERVICE", "svc")
    monkeypatch.setenv("CLICKZETTA_INSTANCE", "inst")
    monkeypatch.setenv("CLICKZETTA_WORKSPACE", "ws")
    monkeypatch.setenv("CLICKZETTA_SCHEMA", "PUBLIC")
    monkeypatch.setenv("CLICKZETTA_USERNAME", "user")
    monkeypatch.setenv("CLICKZETTA_PASSWORD", "secret")

    reload(env_vars)
    config = env_vars.build_base_connection_config()
    assert config["service"] == "svc"
    assert "hints" in config


def test_get_valid_columns_falls_back_to_show_columns():
    class DummyResult:
        def __init__(self, df: pd.DataFrame):
            self._df = df

        def to_pandas(self) -> pd.DataFrame:
            return self._df

    def sql_side_effect(query: str):
        if "SHOW COLUMNS" in query:
            df = pd.DataFrame(
                {
                    "SCHEMA_NAME": ["TPCH_100G"],
                    "TABLE_NAME": ["PARTSUPP"],
                    "COLUMN_NAME": ["PS_PARTKEY"],
                    "DATA_TYPE": ["NUMBER"],
                    "COMMENT": [""],
                }
            )
            return DummyResult(df)
        raise Exception("information_schema unavailable")

    session = mock.MagicMock()
    connector._CATALOG_CATEGORY_CACHE.clear()
    with mock.patch.object(connector, "_catalog_category", return_value="SHARED"):
        session.sql.side_effect = sql_side_effect

        df = connector.get_valid_schemas_tables_columns_df(
            session=session,
            workspace="CLICKZETTA_SAMPLE_DATA",
            table_schema="TPCH_100G",
            table_names=["PARTSUPP"],
        )
    assert not df.empty
    assert df["TABLE_NAME"].iloc[0] == "PARTSUPP"
    assert df["COLUMN_NAME"].iloc[0] == "PS_PARTKEY"


def test_get_valid_columns_handles_fully_qualified_filters():
    class DummyResult:
        def __init__(self, df: pd.DataFrame):
            self._df = df

        def to_pandas(self) -> pd.DataFrame:
            return self._df

    table_df = pd.DataFrame(
        {
            "schema_name": ["S1"],
            "table_name": ["TABLE_ONE"],
            "column_name": ["ID"],
            "data_type": ["INT"],
            "comment": [""],
        }
    )

    call_log: list[str] = []

    def sql_side_effect(query: str):
        call_log.append(query)
        if "information_schema" in query:
            raise RuntimeError("info schema unavailable")
        if query == "SHOW COLUMNS IN TEST_WS.S1.TABLE_ONE":
            return DummyResult(table_df)
        raise RuntimeError("unsupported query")

    session = mock.MagicMock()
    session.sql.side_effect = sql_side_effect
    connector._CATALOG_CATEGORY_CACHE.clear()

    df = connector.get_valid_schemas_tables_columns_df(
        session=session,
        workspace="TEST_WS",
        table_schema="S1",
        table_names=["TEST_WS.S1.TABLE_ONE"],
    )

    assert not df.empty
    assert any("SHOW COLUMNS IN TEST_WS.S1.TABLE_ONE" in q for q in call_log)
    assert all("TEST_WS.S1.TEST_WS.S1" not in q for q in call_log)


def test_fetch_tables_views_in_schema_shared_catalog_does_not_use_share_clause():
    class DummyResult:
        def __init__(self, df: pd.DataFrame):
            self._df = df

        def to_pandas(self) -> pd.DataFrame:
            return self._df

    tables_df = pd.DataFrame(
        {
            "workspace_name": ["lakehouse_ai"],
            "schema_name": ["schema_for_opencatalog"],
            "table_name": ["czcustomer"],
            "is_view": [False],
            "is_materialized_view": [False],
        }
    )

    executed_queries: list[str] = []

    def sql_side_effect(query: str):
        executed_queries.append(query)
        if query.startswith("SHOW TABLES IN"):
            return DummyResult(tables_df)
        raise RuntimeError("Unexpected query")

    session = mock.MagicMock()
    session.sql.side_effect = sql_side_effect
    connector._CATALOG_CATEGORY_CACHE.clear()

    with mock.patch.object(connector, "_catalog_category", return_value="SHARED"):
        tables = connector.fetch_tables_views_in_schema(
            session=session, schema_name="lakehouse_ai.schema_for_opencatalog"
        )

    assert tables == ["lakehouse_ai.schema_for_opencatalog.czcustomer"]
    assert all("IN SHARE" not in query for query in executed_queries)
