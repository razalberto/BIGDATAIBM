class DatabaseManager:

    @staticmethod
    def read_from_db(spark, db_name, db_table_name, user, password):
        return spark.read \
            .format("jdbc") \
            .option("driver", 'com.mysql.cj.jdbc.Driver') \
            .option("url", 'jdbc:mysql://localhost:3306/' + db_name + '?serverTimezone=UTC&useSSL=false'
                    + '&allowPublicKeyRetrieval=true') \
            .option("user", user) \
            .option("password", password) \
            .option("dbtable", db_table_name) \
            .load()

    @staticmethod
    def write_data_frame_to_db(data_frame, db_name, db_table_name, user, password, mode):
        data_frame.write.format('jdbc') \
            .option('url', 'jdbc:mysql://localhost:3306/' + db_name + '?serverTimezone=UTC&useSSL=false'
                    + '&allowPublicKeyRetrieval=true') \
            .option('driver', 'com.mysql.cj.jdbc.Driver') \
            .option('dbtable', db_table_name) \
            .option('user', user) \
            .option('password', password) \
            .mode(mode) \
            .save()
