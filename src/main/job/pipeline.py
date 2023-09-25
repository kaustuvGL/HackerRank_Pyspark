from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import isnull, when, count, col,avg
from main.base import (PySparkJobInterface)


class PySparkJob(PySparkJobInterface):

    def init_spark_session(self) -> SparkSession:
        spark = SparkSession.builder.getOrCreate()
        return spark

    def distinct_ids(self, data_file1: DataFrame) -> int:
        # TODO: Put your code here
        # distinct_id_count = data_file1.select("id").distinct().count()
        # return distinct_id_count


    def valid_age_count(self, data_file2: DataFrame) -> int:
        # TODO: Put your code here
        # valid_age_records_count = data_file2.filter(col("age") >= 18).count()
        # return valid_age_records_count
