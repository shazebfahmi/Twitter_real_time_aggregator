from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import os

spark = SparkSession.builder.appName('abc').getOrCreate()

df = spark.readStream.format(source = 'socket').option('host','localhost').option('port',5599).load()

try:
    os.mkdir('./destination_folder')
except OSError as error:
    print(error)

df.writeStream.format(source='csv').option("checkpointLocation", "chpt/").option('path','destination_folder')d.outputMode(outputMode ='append').start().awaitTermination()
