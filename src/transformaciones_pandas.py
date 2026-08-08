"""transformaciones_pandas.py — Implementación secuencial (pandas) de T1..T5, Paso 2 de la guía
GA-SUM-05/PE-U4 (equipo ACC, dataset FCC Consumer Complaints).

Cada transformación es independiente: parte siempre del DataFrame crudo (`cargar_dataset`),
nunca encadena el resultado de la transformación anterior (advertencia metodológica del Paso 2).
Medición con el protocolo de `medicion.py`: 1 repetición de calentamiento descartada + 5
repeticiones cronometradas con time.perf_counter(), mediana reportada.
"""

import argparse
import csv
import json
import os

import pandas as pd

from medicion import medir
from referencia import REGION_POR_ESTADO, clasificar_prioridad

COLUMNAS_T1 = ["id", "ticket_created", "issue_type", "method", "issue", "city", "state"]


def cargar_dataset(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["ticket_created"] = pd.to_datetime(df["ticket_created"], utc=True)
    return df


def cargar_regiones() -> pd.DataFrame:
    return pd.DataFrame(
        [(estado, region) for estado, region in REGION_POR_ESTADO.items()],
        columns=["state", "region"],
    )


def t1_filtrado(df: pd.DataFrame) -> pd.DataFrame:
    """T1 — filtrado por condición compuesta y selección de columnas: tickets de Internet o
    Phone (categorías directamente ligadas a un ISP) con method/city/state completos."""
    mask = (
        df["issue_type"].isin(["Internet", "Phone"])
        & df["method"].notna()
        & df["city"].notna()
        & df["state"].notna()
    )
    return df.loc[mask, COLUMNAS_T1]


def t2_agrupacion(df: pd.DataFrame) -> pd.DataFrame:
    """T2 — agrupación (groupby) por estado con cuatro funciones de agregación (>=3 exigidas)."""
    return (
        df.groupby("state", dropna=False)  # PySpark conserva el grupo state=NULL por defecto;
        # dropna=False iguala el comportamiento para que la comparación de cardinalidad entre
        # motores (criterio 1.4) sea válida.
        .agg(
            total_tickets=("id", "count"),
            tipos_issue_distintos=("issue", "nunique"),
            primer_ticket=("ticket_created", "min"),
            ultimo_ticket=("ticket_created", "max"),
        )
        .reset_index()
    )


def t3_join(df: pd.DataFrame, regiones: pd.DataFrame) -> pd.DataFrame:
    """T3 — join de al menos dos DataFrames: tickets con su región censal por estado."""
    return df.merge(regiones, on="state", how="left")


def t4_columna_derivada(df: pd.DataFrame) -> pd.DataFrame:
    """T4 — columna derivada compleja: prioridad de atención técnica, calculada con una función
    definida por el usuario sobre issue_type + issue (referencia.clasificar_prioridad)."""
    out = df.copy()
    out["prioridad"] = [
        clasificar_prioridad(it, iss) for it, iss in zip(out["issue_type"], out["issue"])
    ]
    return out


def t5_top_n(df: pd.DataFrame, n: int = 1000) -> pd.DataFrame:
    """T5 — ordenamiento (por fecha de apertura, descendente) y selección de los n primeros
    registros (top-N): los tickets más recientes."""
    return df.sort_values("ticket_created", ascending=False).head(n)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="../data/raw/fcc_consumer_complaints.csv")
    parser.add_argument("--out-dir", default="../data/pandas")
    parser.add_argument("--resultados-dir", default="../resultados")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    os.makedirs(args.resultados_dir, exist_ok=True)

    print(f"Cargando dataset desde {args.data} ...")
    df = cargar_dataset(args.data)
    regiones = cargar_regiones()
    print(f"Filas: {len(df)}  Columnas: {list(df.columns)}")

    transformaciones = {
        "T1_filtrado": lambda: t1_filtrado(df),
        "T2_agrupacion": lambda: t2_agrupacion(df),
        "T3_join": lambda: t3_join(df, regiones),
        "T4_columna_derivada": lambda: t4_columna_derivada(df),
        "T5_top_n": lambda: t5_top_n(df),
    }

    filas_crudas = []
    resumen = []
    for nombre, func in transformaciones.items():
        print(f"Midiendo {nombre} (pandas) ...")
        r = medir(nombre, func, reps=5, warmup=1)
        for i, t in enumerate(r.tiempos_s, start=1):
            filas_crudas.append(["pandas", nombre, i, round(t, 6)])
        resumen.append(["pandas", nombre, round(r.mediana_s, 6)])
        r.resultado.to_csv(os.path.join(args.out_dir, f"{nombre}.csv"), index=False)
        print(f"  mediana = {r.mediana_s:.4f} s  (tiempos: {[round(t,4) for t in r.tiempos_s]})")

    crudos_path = os.path.join(args.resultados_dir, "tiempos_crudos.csv")
    nuevo = not os.path.exists(crudos_path)
    with open(crudos_path, "a", newline="") as f:
        w = csv.writer(f)
        if nuevo:
            w.writerow(["motor", "transformacion", "repeticion", "tiempo_s"])
        w.writerows(filas_crudas)

    resumen_path = os.path.join(args.resultados_dir, "tiempos_resumen_pandas.json")
    with open(resumen_path, "w") as f:
        json.dump({nombre: mediana for _, nombre, mediana in resumen}, f, indent=2)

    print(f"\nTiempos crudos anexados en {crudos_path}")
    print(f"Resumen pandas guardado en {resumen_path}")


if __name__ == "__main__":
    main()
