import findspark
import mysql.connector
from DatabaseManager import DatabaseManager
from DataFrame import DataFrame
from SparkSessionCreator import SparkSessionCreator


findspark.add_packages('mysql:mysql-connector-java:8.0.11')


conn = mysql.connector.connect(
   user='razalberto', password='1234', host='127.0.0.1', database='covid19db'
)


spark = SparkSessionCreator.create_spark_session('COVID-19 incidence rate in schools')


df_csv = DataFrame.create_data_frame(spark, 'Covid19V2.csv')
df_csv = DataFrame.transform_data_frame(df_csv, "Elementary school cases", "Middle school cases", "High school cases")


df_sql = DatabaseManager.read_from_db(spark, 'covid19db', 'covid19', 'razalberto', '1234')


df = df_csv.union(df_sql)
df = DataFrame.transform_data_frame(df, "Elementary school average", "Middle school average", "High school average")
df.show()
df.printSchema()


DatabaseManager.write_data_frame_to_db(df, 'covid19db', 'covid19intermediate', 'razalberto', '1234', 'overwrite')


cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS covid19")
cursor.execute("ALTER TABLE covid19intermediate RENAME TO covid19")

conn.close()
