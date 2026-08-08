"""build_notebook.py — Genera PE_U4_pipeline_spark.ipynb (sin ejecutar) a partir de celdas de
código que reutilizan exactamente las mismas funciones de ../src/ (nada de lógica duplicada).
Después se ejecuta con `jupyter nbconvert --to notebook --execute --inplace` para dejarlo con
salidas visibles, y se exporta a HTML.
"""
import json

def code(src: str):
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": src.strip("\n").splitlines(keepends=True),
    }


def md(src: str):
    return {"cell_type": "markdown", "metadata": {}, "source": src.strip("\n").splitlines(keepends=True)}


cells = [
    md("""
# PE-U4 — Comprobación experimental de la Ley de Amdahl con Apache Spark
**Equipo ACC — Soporte Técnico ISP** · Aplicaciones Distribuidas (ISR-701) · GA-SUM-05/PE-U4

Dataset: FCC Consumer Complaints Data (Phone/Internet/TV), 600,000 tickets reales — ver
`../data/README_dataset.md`. Este notebook ejecuta de punta a punta las cinco transformaciones
en pandas y en PySpark, mide sus tiempos (protocolo de 1 calentamiento + 5 repeticiones,
mediana), verifica la equivalencia de resultados entre ambos motores, y ajusta la Ley de Amdahl
a partir del escalado de T3 (join) con 1, 2 y 4 executors.
"""),
    code("""
import sys, os
sys.path.insert(0, os.path.abspath("../src"))
import pandas as pd
import json as _json

DATA = "../data/raw/fcc_consumer_complaints.csv"
"""),
    md("## 1. Verificación del dataset"),
    code("""
raw = pd.read_csv(DATA)
print("Filas:", len(raw))
print("Columnas:", list(raw.columns))
print("Rango de fechas:", raw["ticket_created"].min(), "a", raw["ticket_created"].max())
raw["issue_type"].value_counts()
"""),
    md("## 2. Implementación secuencial (pandas) — T1..T5, protocolo de medición"),
    code("""
from medicion import medir
from transformaciones_pandas import (
    cargar_dataset, cargar_regiones, t1_filtrado, t2_agrupacion, t3_join,
    t4_columna_derivada, t5_top_n,
)

df_pd = cargar_dataset(DATA)
regiones_pd = cargar_regiones()

transformaciones_pd = {
    "T1_filtrado": lambda: t1_filtrado(df_pd),
    "T2_agrupacion": lambda: t2_agrupacion(df_pd),
    "T3_join": lambda: t3_join(df_pd, regiones_pd),
    "T4_columna_derivada": lambda: t4_columna_derivada(df_pd),
    "T5_top_n": lambda: t5_top_n(df_pd),
}

resultados_pandas = {}
for nombre, func in transformaciones_pd.items():
    r = medir(nombre, func, reps=5, warmup=1)
    resultados_pandas[nombre] = r
    print(f"{nombre}: mediana = {r.mediana_s:.4f} s  tiempos={[round(t,4) for t in r.tiempos_s]}")
"""),
    code("""
resultados_pandas["T3_join"].resultado.head()
"""),
    md("## 3. Implementación distribuida (PySpark) — sesión por defecto (local[4], spark.executor.instances=4)"),
    code("""
from transformaciones_spark import (
    crear_sesion, cargar_dataset as cargar_dataset_spark, cargar_regiones as cargar_regiones_spark,
    t1_filtrado as t1_spark, t2_agrupacion as t2_spark, t3_join as t3_spark,
    t4_columna_derivada as t4_spark, t5_top_n as t5_spark,
)

spark = crear_sesion(master="local[4]", executor_instances="4")
spark.sparkContext.setLogLevel("ERROR")
config_efectiva = dict(spark.sparkContext.getConf().getAll())
print("Configuración efectiva (subset):")
for k in ["spark.master", "spark.executor.instances", "spark.sql.shuffle.partitions", "spark.app.name"]:
    print(f"  {k} = {config_efectiva.get(k)}")

df_spark = cargar_dataset_spark(spark, DATA)
regiones_spark = cargar_regiones_spark(spark)
print("Filas cargadas:", df_spark.count())
"""),
    code("""
transformaciones_spark_dict = {
    "T1_filtrado": lambda: t1_spark(df_spark),
    "T2_agrupacion": lambda: t2_spark(df_spark),
    "T4_columna_derivada": lambda: t4_spark(df_spark),
    "T5_top_n": lambda: t5_spark(df_spark),
}

resultados_spark = {}
for nombre, func in transformaciones_spark_dict.items():
    r = medir(nombre, func, reps=5, warmup=1, materialize=lambda d: d.count())
    resultados_spark[nombre] = r
    print(f"{nombre}: mediana = {r.mediana_s:.4f} s  tiempos={[round(t,4) for t in r.tiempos_s]}")
"""),
    code("""
resultados_spark["T1_filtrado"].resultado.show(5)
"""),
    md("## 4. T3 (join) con 1, 2 y 4 executors — escalado para la Ley de Amdahl"),
    code("""
t3_tiempos_por_n = {}
resultado_t3_n4 = None
spark.stop()

for n in (1, 2, 4):
    spark_n = crear_sesion(master=f"local[{n}]", app_name=f"pe-u4-t3-n{n}", executor_instances=str(n))
    spark_n.sparkContext.setLogLevel("ERROR")
    df_n = cargar_dataset_spark(spark_n, DATA)
    regiones_n = cargar_regiones_spark(spark_n)
    r = medir("T3_join", lambda: t3_spark(df_n, regiones_n), reps=5, warmup=1, materialize=lambda d: d.count())
    t3_tiempos_por_n[n] = r.mediana_s
    if n == 4:
        resultado_t3_n4 = r.resultado
    print(f"N={n}: mediana = {r.mediana_s:.4f} s  tiempos={[round(t,4) for t in r.tiempos_s]}")
    spark_n.stop()

t3_tiempos_por_n
"""),
    md("## 5. Ajuste de la Ley de Amdahl (Ecs. 1-5)"),
    code("""
from amdahl import fraccion_serial_inversa, speedup_maximo, n_para_fraccion_de_maximo, speedup_amdahl, eficiencia

t1_time = t3_tiempos_por_n[1]
speedup_obs = {n: t1_time / t for n, t in t3_tiempos_por_n.items()}
p = fraccion_serial_inversa(4, speedup_obs[4])
s_max = speedup_maximo(p)
n_90 = n_para_fraccion_de_maximo(p, 0.9)

print("Speedup observado por N:", {n: round(s, 3) for n, s in speedup_obs.items()})
print(f"Fracción serial observada p (Ec. 4, N=4) = {p:.4f}")
print(f"Speedup máximo teórico S_max (Ec. 2) = {s_max:.3f}")
print(f"N necesario para 90% de S_max = {n_90:.2f}")
"""),
    md("## 6. Tabla comparativa de tiempos y speedup pandas vs PySpark"),
    code("""
tabla = []
for nombre in ["T1_filtrado", "T2_agrupacion", "T3_join", "T4_columna_derivada", "T5_top_n"]:
    t_pd = resultados_pandas[nombre].mediana_s
    t_sp = resultados_spark[nombre].mediana_s if nombre in resultados_spark else t3_tiempos_por_n[4]
    tabla.append({"transformacion": nombre, "mediana_pandas_s": round(t_pd, 4),
                  "mediana_pyspark_s": round(t_sp, 4), "speedup": round(t_pd / t_sp, 4)})

pd.DataFrame(tabla)
"""),
    md("## 7. Figuras (300 DPI)"),
    code("""
from IPython.display import Image, display

for fig in ["fig1_barras.png", "fig2_speedup.png", "fig3_eficiencia.png"]:
    display(Image(filename=f"../resultados/figuras/{fig}"))
"""),
    md("""## 8. Verificación de equivalencia pandas vs PySpark

Ver `../resultados/equivalencia.json` (generado por `verificar_equivalencia.py`): las cinco
transformaciones coinciden exactamente en cardinalidad y en los agregados de control
(distribución de `issue_type`, suma de `total_tickets`, distribución de `region`, distribución
de `prioridad`, primer id de T5) entre pandas y PySpark."""),
    code("""
with open("../resultados/equivalencia.json") as f:
    equivalencia = _json.load(f)
{k: v["cardinalidad_igual"] for k, v in equivalencia.items()}
"""),
    md("""## 9. Evidencia de ejecución — Spark UI (T3, join)

![Spark UI — DAG y stages de T3](../evidencia/spark_ui_t3.png)

*(pendiente de insertar tras capturar la pantalla del DAG de Spark UI para el job de T3)*"""),
]

nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.10"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

with open("PE_U4_pipeline_spark.ipynb", "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)

print("Notebook (sin ejecutar) escrito en PE_U4_pipeline_spark.ipynb")
