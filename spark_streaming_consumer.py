from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("FIFA_STREAM_ANALISIS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Leer desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "fifa-stream") \
    .option("startingOffsets", "earliest") \
    .load()

# Esquema de datos
schema = StructType([
    StructField("Name", StringType()),
    StructField("Age", StringType()),
    StructField("Country", StringType()),
    StructField("Position", StringType()),
    StructField("Overall_Rating", StringType()),
    StructField("Team", StringType())
])

# Convertir JSON
data = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Análisis en tiempo real
result = data.groupBy("Country").agg(count("*").alias("total_jugadores"))

# Mostrar en consola
query = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()