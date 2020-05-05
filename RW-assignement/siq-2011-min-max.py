from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import StringType
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, FloatType
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()

df = spark.read.format("parquet").load("hdfs://namenode/output/itmd-521/siq/2011/valid-records-temp")

df.createOrReplaceTempView("MainTable")


df_month_data =spark.sql("select month(Observation_Date) as month ,max(Air_Temperature) as Max_Temp from MainTable where Air_Temperature between -7.3 and 4.6 orderby month(Observation_Date)")

df_month_data.write.format("parquet").mode("overwrite").save("hdfs://namenode/output/itmd-521/siq/2011/min-max-airtemp")