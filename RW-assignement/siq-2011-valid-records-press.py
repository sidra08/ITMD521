from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import StringType
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, FloatType
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()

df = spark.read.format("parquet").load("hdfs://namenode/output/itmd-521/siq/2011/parquet-file")


# filtering out Air_Temperature
df_valid_temp=df.filter(df.Air_Temperature!= 999.9)

df_valid_temp.count() #125266430 

df_valid.write.format("parquet").mode("overwrite").save("hdfs://namenode/output/itmd-521/siq/2011/valid-records-temp")