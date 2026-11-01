"""Lakehouse table locations and write helpers, so every job reads and writes the same way."""
from pathlib import Path

from src.common.config import LAKEHOUSE

FORMAT = "delta"

#function that returns the path to the table in the lakehouse given the layer and name of the table. It returns a string representation of the path.
def table_path(layer: str, name: str) -> str:
    return (LAKEHOUSE / layer / name).as_posix()

#function that reads a table from the lakehouse as a DataFrame using Spark. It takes in the Spark session, layer, and name of the table as arguments. If the table exists, it returns the DataFrame; otherwise, it returns None.
def read(spark, layer: str, name: str):
    """The table as a DataFrame, or None if it doesn't exist yet."""
    path = table_path(layer, name)
    return spark.read.format(FORMAT).load(path) if Path(path).exists() else None

#function that appends a DataFrame to a table in the lakehouse. It takes in the DataFrame, layer, and name of the table as arguments. It writes the DataFrame to the specified table in append mode.
def append(df, layer: str, name: str) -> None:
    df.write.format(FORMAT).mode("append").save(table_path(layer, name))

#function that overwrites a table in the lakehouse with a new DataFrame. It takes in the DataFrame, layer, and name of the table as arguments. It writes the DataFrame to the specified table in overwrite mode.
def overwrite_partitions(df, layer: str, name: str, partition_cols: list[str]) -> None:
    """Replace only the partitions present in df (e.g. one day + one source). Re-runs never duplicate."""
    (df.write.format(FORMAT).mode("overwrite")
       .option("partitionOverwriteMode", "dynamic")
       .partitionBy(*partition_cols)
       .save(table_path(layer, name)))