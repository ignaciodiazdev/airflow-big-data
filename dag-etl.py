from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'depends_on_past': False   
}

CLUSTER_NAME = 'cluster-dataproc'
REGION = 'us-central1'
PROJECT_ID = 'bussines-analytics-equipo-01'
PYSPARK_URI_CLIENTES = 'gs://dataproc_eq1/jobs/job-clientes.py'
PYSPARK_URI_EMPLEADOS = 'gs://dataproc_eq1/jobs/job-empleados.py'
PYSPARK_URI_PRODUCTOS = 'gs://dataproc_eq1/jobs/job-productos.py'
PYSPARK_URI_PROVEEDORES = 'gs://dataproc_eq1/jobs/job-proveedores.py'
PYSPARK_URI_TIEMPOS = 'gs://dataproc_eq1/jobs/job-tiempos.py'
PYSPARK_URI_ORDENES = 'gs://dataproc_eq1/jobs/job-ordenes.py'

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    },
}

# Configuración del JOB (Trabajo - Tarea)
PYSPARK_JOB_CLIENTE = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_CLIENTES},
}
PYSPARK_JOB_EMPLEADO = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_EMPLEADOS},
}
PYSPARK_JOB_PRODUCTO = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_PRODUCTOS},
}
PYSPARK_JOB_PROVEEDOR = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_PROVEEDORES},
}
PYSPARK_JOB_TIEMPO = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_TIEMPOS},
}
PYSPARK_JOB_ORDEN = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_ORDENES},
}

with DAG(
    'dag-airflow-etl',
    default_args=default_args,
    description='DAG - ETL con Airflow',
    schedule_interval=None,
    start_date=days_ago(1)
) as dag:
    
#<----------------------- CLUSTER ---------------------->#
    create_cluster = DataprocCreateClusterOperator(
        task_id='creando_cluster',
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

#<-------------------------- JOBS ---------------------->#
    pyspark_task_cliente = DataprocSubmitJobOperator(
        task_id='pyspark_task_cliente',
        job=PYSPARK_JOB_CLIENTE,
        region=REGION, 
        project_id=PROJECT_ID
    )  
    pyspark_task_empleado = DataprocSubmitJobOperator(
        task_id='pyspark_task_empleado',
        job=PYSPARK_JOB_EMPLEADO,
        region=REGION, 
        project_id=PROJECT_ID
    )   
    pyspark_task_producto = DataprocSubmitJobOperator(
        task_id='pyspark_task_producto',
        job=PYSPARK_JOB_PRODUCTO,
        region=REGION, 
        project_id=PROJECT_ID
    )
    pyspark_task_proveedor = DataprocSubmitJobOperator(
        task_id='pyspark_task_proveedor',
        job=PYSPARK_JOB_PROVEEDOR,
        region=REGION, 
        project_id=PROJECT_ID
    )
    pyspark_task_tiempo = DataprocSubmitJobOperator(
        task_id='pyspark_task_tiempo',
        job=PYSPARK_JOB_TIEMPO,
        region=REGION, 
        project_id=PROJECT_ID
    ) 
    pyspark_task_orden = DataprocSubmitJobOperator(
        task_id='pyspark_task_orden',
        job=PYSPARK_JOB_ORDEN,
        region=REGION, 
        project_id=PROJECT_ID
    ) 
#<------------------ LOAD TO BIGQUERY -------------------># 
    carga_clientes_a_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id='carga_clientes_a_bigquery',
        bucket='dataproc_eq1/data-clean/transform_clientes',
        source_objects=['*.json'],
        destination_project_dataset_table='bussines-analytics-equipo-01.dataset.clientes',
        schema_fields=[
                        {'name': 'dni', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'nombre', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'local', 'type': 'STRING', 'mode': 'REQUIRED'}
                      ],
        source_format='NEWLINE_DELIMITED_JSON',  
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE', 
    dag=dag)

    carga_empleados_a_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id='carga_empleados_a_bigquery',
        bucket='dataproc_eq1/data-clean/transform_empleados',
        source_objects=['*.json'],
        destination_project_dataset_table='bussines-analytics-equipo-01.dataset.empleados',
        schema_fields=[
                        {'name': 'dni', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'nombre', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'local', 'type': 'STRING', 'mode': 'REQUIRED'}
                      ],
        source_format='NEWLINE_DELIMITED_JSON',  
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE', 
    dag=dag)

    carga_productos_a_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id='carga_productos_a_bigquery',
        bucket='dataproc_eq1/data-clean/transform_productos',
        source_objects=['*.csv'],
        destination_project_dataset_table='bussines-analytics-equipo-01.dataset.productos',
        schema_fields=[
                        {'name': 'idProducto', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                        {'name': 'NombreProducto', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'categoria', 'type': 'STRING', 'mode': 'REQUIRED'}
                      ],
        source_format='CSV',  
        field_delimiter=',',
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE', 
    dag=dag)

    carga_proveedores_a_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id='carga_proveedores_a_bigquery',
        bucket='dataproc_eq1/data-clean/transform_proveedores',
        source_objects=['*.json'],
        destination_project_dataset_table='bussines-analytics-equipo-01.dataset.proveedores',
        schema_fields=[
                        {'name': 'ruc', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'nombres', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'local', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'sector', 'type': 'STRING', 'mode': 'REQUIRED'}
                      ],
        source_format='NEWLINE_DELIMITED_JSON',  
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE', 
    dag=dag)

    carga_tiempos_a_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id='carga_tiempos_a_bigquery',
        bucket='dataproc_eq1/data-clean/transform_tiempos',
        source_objects=['*.csv'],
        destination_project_dataset_table='bussines-analytics-equipo-01.dataset.tiempos',
        schema_fields=[
                        {'name': 'idFecha', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                        {'name': 'Fecha', 'type': 'DATE', 'mode': 'REQUIRED'},
                        {'name': 'anio', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'trimestre', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'mes', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'dia', 'type': 'INTEGER', 'mode': 'NULLABLE'}
                      ],
        source_format='CSV',  
        field_delimiter=',',
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE', 
    dag=dag)

    carga_ordenes_a_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id='carga_ordenes_a_bigquery',
        bucket='dataproc_eq1/data-clean/transform_ordenes',
        source_objects=['*.csv'],
        destination_project_dataset_table='bussines-analytics-equipo-01.dataset.ordenes',
        schema_fields=[
                        {'name': 'idProducto', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                        {'name': 'idCliente', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                        {'name': 'idEmpleado', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                        {'name': 'idTiempo', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                        {'name': 'idProveedor', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                        {'name': 'precio', 'type': 'FLOAT', 'mode': 'REQUIRED'},
                        {'name': 'cantidad', 'type': 'INTEGER', 'mode': 'REQUIRED'}
                      ],
        source_format='CSV',  
        field_delimiter=',',
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
    dag=dag)

#<---------------------- CLUSTER ------------------------>#
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='eliminando_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION
    )

    inicio = DummyOperator(
        task_id='inicio',
        dag=dag
    )

    iniciando_extraccion = DummyOperator(
        task_id='iniciando_extraccion',
        dag=dag
    )

    transformacion_exitosa = DummyOperator(
        task_id='transformacion_exitosa',
        dag=dag
    )
    carga_exitosa = DummyOperator(
        task_id='carga_exitosa',
        dag=dag
    )

    fin = DummyOperator(
        task_id='fin',
        dag=dag
    )

    inicio >> create_cluster >> iniciando_extraccion >> [pyspark_task_cliente, pyspark_task_empleado, pyspark_task_orden, pyspark_task_producto, pyspark_task_proveedor, pyspark_task_tiempo] >> transformacion_exitosa >> [carga_clientes_a_bigquery, carga_empleados_a_bigquery, carga_ordenes_a_bigquery, carga_productos_a_bigquery, carga_proveedores_a_bigquery, carga_tiempos_a_bigquery]>> carga_exitosa >> delete_cluster >> fin