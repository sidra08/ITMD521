from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import StringType
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, FloatType
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://namenode/output/itmd-521/siq/2011/csv-file")




df.write.format("csv").option("compression","lz4").mode("overwrite").option("header","true").save("hdfs://namenode/output/itmd-521/siq/2011/lz4")