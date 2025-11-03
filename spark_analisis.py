from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

# Crear sesión Spark
spark = SparkSession.builder.appName("AnalisisFlujoGymshark").getOrCreate()

# Cargar CSV
df = spark.read.csv("data_simulada.csv", header=True, inferSchema=True)

print("=== Vista previa de los datos ===")
df.show(5)

# Calcular métricas agregadas por país
metricas = df.groupBy("pais").agg(
    avg("clicks").alias("Promedio_Clicks"),
    avg("tiempo_sesion").alias("Promedio_Tiempo_Sesion")
)

print("=== Resultados del análisis ===")
metricas.show()

# Exportar resultados a CSV
metricas.coalesce(1).write.mode("overwrite").csv("resultados_analisis", header=True)

spark.stop()
