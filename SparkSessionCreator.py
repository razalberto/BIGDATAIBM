from pyspark.sql import SparkSession


class SparkSessionCreator:
    @staticmethod
    def create_spark_session(app_name):
        return SparkSession \
            .builder \
            .master("local") \
            .appName(app_name) \
            .getOrCreate()
