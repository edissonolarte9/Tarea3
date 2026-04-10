from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min

spark = SparkSession.builder.appName("FIFA_ANALISIS").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\n=== CARGANDO DATASET FIFA ===")

# Cargar datos
df = spark.read.csv("fifa.csv", header=True, inferSchema=True)

# Ver columnas
print("\nColumnas disponibles:")
print(df.columns)

# Mostrar datos
print("\nPrimeros registros:")
df.show(5)

# Limpiar datos
df = df.dropna()

# Seleccionar columnas importantes
df = df.select(
    "Name",
    "Age",
    "Country",
    "Position",
    "Overall_Rating",
    "Team"
)

# Promedio de edad
print("\nPromedio de edad:")
df.select(avg("Age")).show()

# Top jugadores
print("\nTop 10 jugadores:")
df.orderBy(col("Overall_Rating").desc()).show(10)

# Jugadores por país
print("\nJugadores por país:")
df.groupBy("Country").count().show()

# Edad máxima y mínima
print("\nEdad máxima y mínima:")
df.select(max("Age"), min("Age")).show()

print("\n=== ANALISIS FINALIZADO ===")

spark.stop()
