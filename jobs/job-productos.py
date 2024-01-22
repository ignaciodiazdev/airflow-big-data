from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder.appName("transform-producto").getOrCreate()

input_csv_path = "gs://datawarehouse_eq1/Datos-Producto.csv" #Ruta Bucket del archivo csv
df = spark.read.csv(input_csv_path, inferSchema=True, sep=";")
df.show(truncate=0)
df.printSchema()

# Define los nuevos nombres de las columnas
nuevos_nombres = ["idProducto", "NombreProducto", "categoria"]

# Crea un nuevo DataFrame con los nombres de columna actualizados
df_columnas_renombradas = df.toDF(*nuevos_nombres)
df_columnas_renombradas.show(truncate=0)
df_columnas_renombradas.printSchema()

output_csv_path = "gs://dataproc_eq1/data-clean/transform_productos" #Ruta Bucket del archivo csv
df_columnas_renombradas.write.csv(output_csv_path, header=True, mode="overwrite", sep=",")