from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, concat_ws, when, regexp_replace

spark = SparkSession.builder.appName("transform-proveedores").getOrCreate()

input_json_path = "gs://datawarehouse_eq1/proveedores.jsonl" #Ruta Bucket del archivo jsonl
df = spark.read.json(input_json_path)
df.show(truncate=0)
df.printSchema()

df_sector = df.withColumn(
    "sector",
    when(df["nombres"].like("%Textil%"), "Textil")
    .when(df["nombres"].like("%Textiles%"), "Textil")
    .when(df["nombres"].like("%Confecciones%"), "Textil")
    .when(df["nombres"].like("%Moda%"), "Moda")
    .otherwise("Otra")
)
df_sector.show(truncate=0)

# Elimina la palabra 'Textil' o 'Moda' de la columna 'proveedores'
df_resultado = df_sector.withColumn(
    "nombres",
    regexp_replace(df_sector["nombres"], "(Textiles|Textil|Moda)(?!(\\sde\\s|\\sdel\\s))\\s*", "")
)
df_resultado.show(truncate=0)

# Filtra las filas donde la columna 'proveedores' contiene 'Empresas de'
df_filtrado = df_resultado.filter(~col("nombres").contains("Empresas de"))
df_filtrado.show(truncate=0)

# Ordena las columnas del dataframe
df_ordenado = df_filtrado.select("ruc", "nombres", "local", "sector")
df_ordenado.show(truncate=0)
df_ordenado.printSchema()

output_json_path = "gs://dataproc_eq1/data-clean/transform_proveedores"  # Ruta Bucket archivo JSON
df_ordenado.write.mode("overwrite").json(output_json_path)

