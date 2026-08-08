"""spark_ui_demo.py — Levanta una sesión Spark con la UI activa (puerto 4040), ejecuta T3
(join) varias veces para dejar jobs/stages completados con shuffle, y mantiene la sesión viva
para poder capturar la pantalla del DAG en http://localhost:4040 (evidencia obligatoria del
Paso 3 de la guía).
"""
import time

from transformaciones_spark import cargar_dataset, cargar_regiones, crear_sesion, t3_join

DATA = "../data/raw/fcc_consumer_complaints.csv"

if __name__ == "__main__":
    spark = crear_sesion(master="local[4]", app_name="pe-u4-acc-spark-ui-demo", executor_instances="4")
    df = cargar_dataset(spark, DATA)
    regiones = cargar_regiones(spark)

    print(">>> Ejecutando T3 (join) para generar jobs/stages con shuffle ...")
    for _ in range(3):
        t3_join(df, regiones).count()

    print(">>> Spark UI disponible en http://localhost:4040 (mapeado desde el contenedor)")
    print(">>> Manteniendo la sesion viva 900s para permitir la captura de pantalla ...")
    time.sleep(900)
    spark.stop()
