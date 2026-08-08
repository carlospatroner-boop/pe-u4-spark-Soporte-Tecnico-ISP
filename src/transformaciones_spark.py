"""transformaciones_spark.py — Implementación distribuida (PySpark) de T1..T5, Paso 3 de la
guía GA-SUM-05/PE-U4 (equipo ACC, dataset FCC Consumer Complaints).

Mismas cinco transformaciones que transformaciones_pandas.py, reutilizando la lógica de
`referencia.py` para que T3 (join) y T4 (columna derivada) sean exactamente equivalentes entre
motores (criterio 1.4). T1, T2, T4 y T5 se miden con la sesión "por defecto" (local[4],
spark.executor.instances=4, tal como exige el Paso 3). T3 (join) se mide además con 1, 2 y 4
executors por separado para habilitar el análisis de escalabilidad de Amdahl (Paso 4).
"""

import argparse
import csv
import json
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

import referencia
from medicion import medir

COLUMNAS_T1 = ["id", "ticket_created", "issue_type", "method", "issue", "city", "state"]

_prioridad_udf = F.udf(referencia.clasificar_prioridad, StringType())


def crear_sesion(master: str, app_name: str = "pe-u4-acc-spark", executor_instances: str = "4") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.executor.instances", executor_instances)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def cargar_dataset(spark: SparkSession, path: str):
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    # El CSV crudo trae, para algunas filas, el literal "None" (no un campo vacío) en columnas
    # de texto (p. ej. city). pandas.read_csv trata "None" como NaN por defecto (está en su
    # lista de tokens NA); Spark solo trata como nulo el campo vacío (su nullValue por
    # defecto), así que sin este reemplazo explícito el literal "None" queda como string y no
    # como nulo, rompiendo la equivalencia de T1 (criterio 1.4). Se reemplaza tras la carga en
    # vez de con la opción nullValue del reader porque esa opción sustituye —no complementa— el
    # tratamiento por defecto de "" como nulo.
    df = df.replace("None", None)
    df = df.withColumn(
        "ticket_created", F.to_timestamp("ticket_created", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    )
    return df


def cargar_regiones(spark: SparkSession):
    rows = [(estado, region) for estado, region in referencia.REGION_POR_ESTADO.items()]
    return spark.createDataFrame(rows, ["state", "region"])


def t1_filtrado(df):
    return df.filter(
        F.col("issue_type").isin("Internet", "Phone")
        & F.col("method").isNotNull()
        & F.col("city").isNotNull()
        & F.col("state").isNotNull()
    ).select(*COLUMNAS_T1)


def t2_agrupacion(df):
    return df.groupBy("state").agg(
        F.count("id").alias("total_tickets"),
        F.countDistinct("issue").alias("tipos_issue_distintos"),
        F.min("ticket_created").alias("primer_ticket"),
        F.max("ticket_created").alias("ultimo_ticket"),
    )


def t3_join(df, regiones):
    return df.join(regiones, on="state", how="left")


def t4_columna_derivada(df):
    return df.withColumn("prioridad", _prioridad_udf(F.col("issue_type"), F.col("issue")))


def t5_top_n(df, n: int = 1000):
    return df.orderBy(F.col("ticket_created").desc()).limit(n)


def _guardar(df, out_dir: str, nombre: str):
    df.toPandas().to_csv(os.path.join(out_dir, f"{nombre}.csv"), index=False)


def _append_crudos(resultados_dir: str, filas):
    path = os.path.join(resultados_dir, "tiempos_crudos.csv")
    nuevo = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if nuevo:
            w.writerow(["motor", "transformacion", "repeticion", "tiempo_s"])
        w.writerows(filas)


def medir_t1_t2_t4_t5(data_path: str, out_dir: str, resultados_dir: str):
    """T1, T2, T4, T5 con la configuración "por defecto" del Paso 3 (local[4],
    spark.executor.instances=4)."""
    spark = crear_sesion(master="local[4]", executor_instances="4")
    config_efectiva = dict(spark.sparkContext.getConf().getAll())
    with open(os.path.join(resultados_dir, "spark_config_efectiva_local4.json"), "w") as f:
        json.dump(config_efectiva, f, indent=2)

    df = cargar_dataset(spark, data_path)
    regiones = cargar_regiones(spark)
    print(f"Filas cargadas: {df.count()}  Columnas: {df.columns}")

    transformaciones = {
        "T1_filtrado": lambda: t1_filtrado(df),
        "T2_agrupacion": lambda: t2_agrupacion(df),
        "T4_columna_derivada": lambda: t4_columna_derivada(df),
        "T5_top_n": lambda: t5_top_n(df),
    }

    resumen = {}
    filas_crudas = []
    for nombre, func in transformaciones.items():
        print(f"Midiendo {nombre} (pyspark, local[4]) ...")
        r = medir(nombre, func, reps=5, warmup=1, materialize=lambda d: d.count())
        for i, t in enumerate(r.tiempos_s, start=1):
            filas_crudas.append(["pyspark", nombre, i, round(t, 6)])
        resumen[nombre] = r.mediana_s
        _guardar(r.resultado, out_dir, nombre)
        print(f"  mediana = {r.mediana_s:.4f} s  (tiempos: {[round(t,4) for t in r.tiempos_s]})")

    _append_crudos(resultados_dir, filas_crudas)
    spark.stop()
    return resumen, df, regiones


def medir_t3_escalado(data_path: str, out_dir: str, resultados_dir: str):
    """T3 (join) medido por separado con 1, 2 y 4 executors (local[N]), Paso 3 y 4."""
    resumen_por_n = {}
    filas_crudas = []
    resultado_n4 = None

    for n in (1, 2, 4):
        print(f"\n>>> T3_join con local[{n}] ...")
        spark = crear_sesion(master=f"local[{n}]", app_name=f"pe-u4-acc-spark-t3-n{n}", executor_instances=str(n))
        config_efectiva = dict(spark.sparkContext.getConf().getAll())
        with open(os.path.join(resultados_dir, f"spark_config_efectiva_t3_n{n}.json"), "w") as f:
            json.dump(config_efectiva, f, indent=2)

        df = cargar_dataset(spark, data_path)
        regiones = cargar_regiones(spark)

        r = medir("T3_join", lambda: t3_join(df, regiones), reps=5, warmup=1, materialize=lambda d: d.count())
        for i, t in enumerate(r.tiempos_s, start=1):
            filas_crudas.append(["pyspark", f"T3_join_N{n}", i, round(t, 6)])
        resumen_por_n[n] = r.mediana_s
        print(f"  N={n}  mediana = {r.mediana_s:.4f} s  (tiempos: {[round(t,4) for t in r.tiempos_s]})")

        if n == 4:
            resultado_n4 = r.resultado
            _guardar(resultado_n4, out_dir, "T3_join")

        spark.stop()

    _append_crudos(resultados_dir, filas_crudas)
    return resumen_por_n


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="../data/raw/fcc_consumer_complaints.csv")
    parser.add_argument("--out-dir", default="../data/spark")
    parser.add_argument("--resultados-dir", default="../resultados")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    os.makedirs(args.resultados_dir, exist_ok=True)

    resumen_t1245, _, _ = medir_t1_t2_t4_t5(args.data, args.out_dir, args.resultados_dir)
    resumen_t3_por_n = medir_t3_escalado(args.data, args.out_dir, args.resultados_dir)

    resumen_completo = dict(resumen_t1245)
    resumen_completo["T3_join"] = resumen_t3_por_n[4]  # entrada "estandar" (local[4]) para la
    # tabla comparativa con pandas; el detalle de escalado 1/2/4 vive aparte.

    with open(os.path.join(args.resultados_dir, "tiempos_resumen_spark.json"), "w") as f:
        json.dump(resumen_completo, f, indent=2)
    with open(os.path.join(args.resultados_dir, "t3_escalado_executors.json"), "w") as f:
        json.dump(resumen_t3_por_n, f, indent=2)

    print("\nResumen PySpark (local[4], salvo T3 detallado abajo):")
    for k, v in resumen_completo.items():
        print(f"  {k}: {v:.4f} s")
    print("\nT3_join por número de executors (local[N]):")
    for n, v in resumen_t3_por_n.items():
        print(f"  N={n}: {v:.4f} s")


if __name__ == "__main__":
    main()
