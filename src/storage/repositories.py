"""Repository functions for inserting and querying platform data."""

from __future__ import annotations

import pandas as pd

from src.storage.database import engine


def write_dataframe(table_name: str, frame: pd.DataFrame) -> None:
    frame.to_sql(table_name, engine, if_exists="append", index=False)


def read_table(table_name: str) -> pd.DataFrame:
    return pd.read_sql_table(table_name, engine)

