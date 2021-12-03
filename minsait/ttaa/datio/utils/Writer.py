from pyspark.sql import DataFrame

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.common.naming.PlayerInput import *


class Writer:
    def write(self, df: DataFrame):
        df \
            .coalesce(2) \
            .write \
            .partitionBy(team_position.name) \
            .mode(OVERWRITE) \
            .parquet(OUTPUT_PATH);

    def write(self, df: DataFrame, file_quantity: int, colname: str):
        df \
            .coalesce(file_quantity) \
            .write \
            .partitionBy(colname) \
            .mode(OVERWRITE) \
            .parquet(OUTPUT_PATH);

