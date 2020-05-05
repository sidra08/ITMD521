from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import StringType
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, FloatType
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()


df = spark.read.format("parquet").load("hdfs://namenode/output/itmd-521/siq/2011/parquet-file")

df_bad_press=df.filter(df.Atmospheric_Pressure== 9999.9)

total_count = df.count()

bad_count = df_bad_press.count() 

# Spark-The Definitive Guide(E-book) 73 Creating DataFrames
myManualSchema = StructType([
StructField("bad_record_count", IntegerType(), True),
StructField("Total_record_count", IntegerType(), True),
StructField("Percentage", FloatType(), True)
])

myRow = Row(bad_count, total_count,float((bad_count*100.0)/total_count))


df_data = spark.createDataFrame([myRow], myManualSchema)

df_data.write.format("parquet").mode("overwrite").save("hdfs://namenode/output/itmd-521/siq/2011/records-count-temp")