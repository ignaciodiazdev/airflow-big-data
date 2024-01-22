from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, concat_ws

spark2 = SparkSession.builder.appName("transform-orden").getOrCreate()
input_csv_path = "gs://datawarehouse_eq1/Datos-Orden.csv" #Ruta Bucket del archivo csv
df_orden = spark2.read.csv(input_csv_path, inferSchema=True, sep=";")
df_orden.show(truncate=0)
df_orden.printSchema()

# Define los nuevos nombres de las columnas
nuevos_nombres_orden = ["idProducto", "idCliente", "idEmpleado", "idTiempo","idProveedor","precio","cantidad"]

# Crea un nuevo DataFrame con los nombres de columna actualizados
df_columnas_renombradas_orden = df_orden.toDF(*nuevos_nombres_orden)
df_columnas_renombradas_orden.show(truncate=0)
df_columnas_renombradas_orden.printSchema()

df_tipo_precio = df_columnas_renombradas_orden.withColumn("precio", col("precio").cast("double"))
df_tipo_precio.show(truncate=0)
df_tipo_precio.printSchema()

output_csv_path = "gs://dataproc_eq1/data-clean/transform_ordenes" #Ruta Bucket del archivo csv
df_tipo_precio.write.csv(output_csv_path, header=True, mode="overwrite", sep=",")