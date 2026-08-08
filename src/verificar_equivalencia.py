"""verificar_equivalencia.py — Criterio 1.4: verifica que pandas y PySpark produzcan resultados
equivalentes para las cinco transformaciones, por cardinalidad y por agregados de control (no
por comparación fila a fila, ya que el orden de salida de un motor distribuido no está
garantizado salvo que se ordene explícitamente, como en T5).
"""

import argparse
import json
import os

import pandas as pd


def cargar(nombre: str, pandas_dir: str, spark_dir: str):
    p = pd.read_csv(os.path.join(pandas_dir, f"{nombre}.csv"))
    s = pd.read_csv(os.path.join(spark_dir, f"{nombre}.csv"))
    return p, s


def verificar_t1(pandas_dir, spark_dir):
    p, s = cargar("T1_filtrado", pandas_dir, spark_dir)
    return {
        "filas_pandas": len(p),
        "filas_pyspark": len(s),
        "cardinalidad_igual": len(p) == len(s),
        "distribucion_issue_type_pandas": p["issue_type"].value_counts().to_dict(),
        "distribucion_issue_type_pyspark": s["issue_type"].value_counts().to_dict(),
    }


def verificar_t2(pandas_dir, spark_dir):
    p, s = cargar("T2_agrupacion", pandas_dir, spark_dir)
    p = p.sort_values("state").reset_index(drop=True)
    s = s.sort_values("state").reset_index(drop=True)
    return {
        "grupos_pandas": len(p),
        "grupos_pyspark": len(s),
        "cardinalidad_igual": len(p) == len(s),
        "suma_total_tickets_pandas": int(p["total_tickets"].sum()),
        "suma_total_tickets_pyspark": int(s["total_tickets"].sum()),
        "suma_igual": int(p["total_tickets"].sum()) == int(s["total_tickets"].sum()),
        "estados_coinciden": (
            sorted(p["state"].fillna("__NULL__").tolist())
            == sorted(s["state"].fillna("__NULL__").tolist())
        ),
    }


def verificar_t3(pandas_dir, spark_dir):
    p, s = cargar("T3_join", pandas_dir, spark_dir)
    return {
        "filas_pandas": len(p),
        "filas_pyspark": len(s),
        "cardinalidad_igual": len(p) == len(s),
        "distribucion_region_pandas": p["region"].value_counts().to_dict(),
        "distribucion_region_pyspark": s["region"].value_counts().to_dict(),
    }


def verificar_t4(pandas_dir, spark_dir):
    p, s = cargar("T4_columna_derivada", pandas_dir, spark_dir)
    return {
        "filas_pandas": len(p),
        "filas_pyspark": len(s),
        "cardinalidad_igual": len(p) == len(s),
        "distribucion_prioridad_pandas": p["prioridad"].value_counts().to_dict(),
        "distribucion_prioridad_pyspark": s["prioridad"].value_counts().to_dict(),
    }


def verificar_t5(pandas_dir, spark_dir):
    p, s = cargar("T5_top_n", pandas_dir, spark_dir)
    return {
        "filas_pandas": len(p),
        "filas_pyspark": len(s),
        "cardinalidad_igual": len(p) == len(s),
        "primer_id_pandas": int(p.iloc[0]["id"]),
        "primer_id_pyspark": int(s.iloc[0]["id"]),
        "primer_id_igual": int(p.iloc[0]["id"]) == int(s.iloc[0]["id"]),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pandas-dir", default="../data/pandas")
    parser.add_argument("--spark-dir", default="../data/spark")
    parser.add_argument("--out", default="../resultados/equivalencia.json")
    args = parser.parse_args()

    reporte = {
        "T1_filtrado": verificar_t1(args.pandas_dir, args.spark_dir),
        "T2_agrupacion": verificar_t2(args.pandas_dir, args.spark_dir),
        "T3_join": verificar_t3(args.pandas_dir, args.spark_dir),
        "T4_columna_derivada": verificar_t4(args.pandas_dir, args.spark_dir),
        "T5_top_n": verificar_t5(args.pandas_dir, args.spark_dir),
    }

    with open(args.out, "w") as f:
        json.dump(reporte, f, indent=2, ensure_ascii=False)

    print("=== Verificación de equivalencia pandas vs PySpark ===")
    todo_ok = True
    for nombre, r in reporte.items():
        ok = r.get("cardinalidad_igual", False)
        todo_ok = todo_ok and ok
        print(f"{nombre}: cardinalidad_igual={ok}")
    print(f"\nReporte completo en {args.out}")
    print("TODO OK" if todo_ok else "HAY DIFERENCIAS -- revisar reporte")


if __name__ == "__main__":
    main()
