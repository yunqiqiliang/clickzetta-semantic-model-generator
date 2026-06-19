from typing import Any, Dict, List, Optional

from pydantic.dataclasses import dataclass


@dataclass
class FQNParts:
    database: str
    schema_name: str
    table: str

    def __post_init__(self: Any) -> None:
        """Uppercase table name"""
        self.table = self.table.upper()


@dataclass
class Column:
    id_: int
    column_name: str
    column_type: str
    values: Optional[List[str]] = None
    comment: Optional[str] = (
        None  # comment field's to save the column comment user specified on the column
    )
    is_primary_key: bool = False
    # Fraction of distinct values in the raw (duplicate-preserving) row sample,
    # i.e. distinct_count / non_null_count. None when not measured. Used to veto
    # primary-key candidates whose sample shows clear duplication (a column that
    # repeats values cannot be a single-column primary key).
    sample_uniqueness: Optional[float] = None

    def __post_init__(self: Any) -> None:
        """
        Update column_type to cleaned up version, eg. NUMBER(38,0) -> NUMBER
        """

        self.column_type = self.column_type.split("(")[0].strip().upper()


@dataclass
class Table:
    id_: int
    name: str
    columns: List[Column]
    comment: Optional[str] = (
        None  # comment field's to save the table comment user specified on the table
    )

    def __post_init__(self: Any) -> None:
        for col in self.columns:
            if col.column_name == "":
                raise ValueError("column name in table must be nonempty")
