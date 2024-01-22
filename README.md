# Orquestación de Datos con Apache Airflow y GCP

¡Bienvenido a mi repositorio! Aquí encontrarás el código en Python que utilizo para orquestar un flujo de trabajo de transformación de datos con Apache Airflow y Google Cloud Platform (GCP). Este proyecto abarca varias etapas clave:

1. **Creación de un Cluster Dataproc**: Utilizo Airflow para programar la creación de un clúster Dataproc en GCP, proporcionando un entorno escalable para ejecutar trabajos de procesamiento de datos.

2. **Ejecución de Trabajos en Dataproc**: Defino y envío trabajos de transformación de datos al clúster Dataproc recién creado. Estos trabajos aprovechan la potencia de procesamiento distribuido para realizar operaciones complejas de manera eficiente.

3. **Almacenamiento en Cloud Storage**: Después de la transformación, almaceno los resultados en Cloud Storage, aprovechando la capacidad de almacenamiento y la durabilidad de GCP.

4. **Carga de Datos en BigQuery**: Airflow también orquesta la carga de datos desde Cloud Storage a BigQuery, permitiendo un análisis escalable y eficiente de los datos transformados.

## Estructura del Proyecto

- `dag/`: Aquí están los archivos de definición de los DAGs de Airflow.
- `scripts/`: Contiene los scripts de transformación de datos enviados a Dataproc.
- `utils/`: Incluye utilidades y funciones auxiliares necesarias para el flujo de trabajo.

## Requisitos

Antes de ejecutar el código, asegúrate de tener instaladas las bibliotecas necesarias especificadas en `requirements.txt`.

## Configuración

Antes de ejecutar los DAGs, configura las credenciales de GCP y ajusta los parámetros en los archivos de configuración.

¡Me encantaría recibir contribuciones y sugerencias! Si encuentras algún problema o tienes ideas para mejorar el proyecto, por favor, abre un problema o envía una solicitud de extracción.

¡Gracias por explorar este proyecto conmigo!
