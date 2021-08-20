from pyspark.sql.functions import avg, round


class DataFrame:
    @staticmethod
    def create_data_frame(spark, file_name):
        return spark.read \
            .option('header', 'true') \
            .option('inferSchema', 'true') \
            .csv(file_name)

    @staticmethod
    def transform_data_frame(data_frame, col_name1, col_name2, col_name3):
        return data_frame.groupBy("School unit name", "Gender"). \
            agg(round(avg(col_name1), 2).alias("Elementary school average"),
                round(avg(col_name2), 2).alias("Middle school average"),
                round(avg(col_name3), 2).alias("High school average"))
