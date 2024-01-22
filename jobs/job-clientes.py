from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, concat_ws

spark = SparkSession.builder.appName("transform-clientes").getOrCreate()

input_json_path = "gs://datawarehouse_eq1/clientes.jsonl" # Ruta Bucket del archivo jsonl
df = spark.read.json(input_json_path)
df.show(truncate=0)
df.printSchema()

# Eliminar registros con valores nulos
df = df.na.drop()

df = df.withColumn("nombre", concat_ws(" ", col("nombres"), col("apellidos")))
df = df.drop("nombres", "apellidos")
df.show(truncate=0)
df.printSchema()

df = df.select("dni", "nombre", "email", "local")
df.show(truncate=0)
df.printSchema()

output_json_path = "gs://dataproc_eq1/data-clean/transform_clientes"  # Ruta Bucket archivo JSON
df.write.mode("overwrite").json(output_json_path)