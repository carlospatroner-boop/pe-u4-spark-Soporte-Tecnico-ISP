"""graficas.py — Paso 4: análisis cuantitativo (speedup, fracción serial, límite de Amdahl) y
las tres figuras exigidas por la guía a 300 DPI (criterio 2.3):
  (a) tiempos pandas vs PySpark por transformación (barras)
  (b) speedup observado vs N (T3, escalado de executors) con la curva de Amdahl superpuesta
  (c) eficiencia E = S/N vs N
"""

import argparse
import csv
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy.optimize import curve_fit

from amdahl import (
    eficiencia,
    fraccion_serial_inversa,
    n_para_fraccion_de_maximo,
    speedup_amdahl,
    speedup_maximo,
)

NOMBRES_LEGIBLES = {
    "T1_filtrado": "T1 Filtrado",
    "T2_agrupacion": "T2 Agrupación",
    "T3_join": "T3 Join",
    "T4_columna_derivada": "T4 Col. derivada",
    "T5_top_n": "T5 Top-N",
}


def cargar_json(path):
    with open(path) as f:
        return json.load(f)


def figura_barras(pandas_t, spark_t, out_path):
    transformaciones = ["T1_filtrado", "T2_agrupacion", "T3_join", "T4_columna_derivada", "T5_top_n"]
    x = np.arange(len(transformaciones))
    width = 0.35

    tiempos_pandas = [pandas_t[t] for t in transformaciones]
    tiempos_spark = [spark_t[t] for t in transformaciones]

    plt.figure(figsize=(8, 5))
    plt.bar(x - width / 2, tiempos_pandas, width, label="pandas (secuencial)")
    plt.bar(x + width / 2, tiempos_spark, width, label="PySpark local[4] (distribuido)")
    plt.xticks(x, [NOMBRES_LEGIBLES[t] for t in transformaciones], rotation=15, ha="right")
    plt.ylabel("Tiempo mediana (s), 5 repeticiones")
    plt.title("Tiempo de ejecución: pandas vs PySpark por transformación")
    plt.legend()
    plt.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=300)
    plt.close()


def analisis_amdahl(t3_por_n: dict, resultados_dir: str):
    ns = sorted(int(n) for n in t3_por_n.keys())
    tiempos = {int(n): t3_por_n[str(n)] if str(n) in t3_por_n else t3_por_n[n] for n in ns}
    t1 = tiempos[1]
    speedup_obs = {n: t1 / tiempos[n] for n in ns}

    # Fracción serial observada (Ec. 4) para cada N>1 disponible.
    p_por_n = {n: fraccion_serial_inversa(n, speedup_obs[n]) for n in ns if n > 1}
    # Se reporta como valor principal el ajustado con el N mayor disponible (más informativo),
    # y como verificación cruzada el resto.
    n_max = max(p_por_n.keys())
    p_principal = p_por_n[n_max]

    # Ajuste adicional por mínimos cuadrados no lineales sobre todos los puntos (N, speedup_obs)
    # como verificación de robustez frente al cálculo algebraico directo de la Ec. (4).
    ns_arr = np.array(ns, dtype=float)
    s_arr = np.array([speedup_obs[n] for n in ns], dtype=float)
    popt, _ = curve_fit(speedup_amdahl, ns_arr, s_arr, p0=[0.8], bounds=(0, 0.999))
    p_ajustado_minimos_cuadrados = float(popt[0])

    s_max = speedup_maximo(p_principal)
    n_90 = n_para_fraccion_de_maximo(p_principal, 0.9)

    resultado = {
        "N_medidos": ns,
        "tiempos_s": tiempos,
        "speedup_observado": speedup_obs,
        "p_por_N_ec4": p_por_n,
        "p_principal_ec4": p_principal,
        "N_usado_para_p_principal": n_max,
        "p_ajustado_minimos_cuadrados": p_ajustado_minimos_cuadrados,
        "S_max": s_max,
        "N_para_90pct_S_max": n_90,
    }

    with open(os.path.join(resultados_dir, "amdahl_fit.json"), "w") as f:
        json.dump(resultado, f, indent=2)

    print("=== Análisis de Amdahl (T3, escalado de executors) ===")
    for n in ns:
        print(f"  N={n}: T={tiempos[n]:.4f}s  S_obs={speedup_obs[n]:.3f}")
    print(f"  p (Ec. 4, N={n_max}) = {p_principal:.4f}")
    print(f"  p (ajuste mínimos cuadrados, todos los N) = {p_ajustado_minimos_cuadrados:.4f}")
    print(f"  S_max (Ec. 2) = {s_max:.3f}")
    print(f"  N para 90% de S_max = {n_90:.2f}")

    return resultado


def figura_speedup(amdahl_res: dict, out_path: str):
    ns = amdahl_res["N_medidos"]
    speedup_obs = amdahl_res["speedup_observado"]
    p = amdahl_res["p_principal_ec4"]
    s_max = amdahl_res["S_max"]

    n_smooth = np.linspace(1, max(16, max(ns) * 2), 300)
    s_smooth = speedup_amdahl(n_smooth, p)

    plt.figure(figsize=(7, 5))
    plt.plot(ns, [speedup_obs[n] for n in ns], "o-", label="Speedup observado (T3, join)")
    plt.plot(n_smooth, s_smooth, "--", label=f"Amdahl teórico (p={p:.3f})")
    plt.axhline(s_max, color="gray", linestyle=":", label=f"$S_{{max}}$={s_max:.2f}")
    plt.xlabel("Número de executors (N)")
    plt.ylabel("Speedup S(N)")
    plt.title("T3 (join): speedup observado vs. curva de Amdahl")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=300)
    plt.close()


def figura_eficiencia(amdahl_res: dict, out_path: str):
    ns = amdahl_res["N_medidos"]
    speedup_obs = amdahl_res["speedup_observado"]
    p = amdahl_res["p_principal_ec4"]

    n_smooth = np.linspace(1, max(16, max(ns) * 2), 300)
    e_smooth = [eficiencia(speedup_amdahl(n, p), n) for n in n_smooth]
    e_obs = [eficiencia(speedup_obs[n], n) for n in ns]

    plt.figure(figsize=(7, 5))
    plt.plot(ns, e_obs, "o-", label="Eficiencia observada (T3, join)")
    plt.plot(n_smooth, e_smooth, "--", label=f"Eficiencia teórica Amdahl (p={p:.3f})")
    plt.xlabel("Número de executors (N)")
    plt.ylabel("Eficiencia E(N) = S(N)/N")
    plt.title("T3 (join): eficiencia del paralelismo vs. N")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=300)
    plt.close()


def tabla_resumen(pandas_t, spark_t, resultados_dir):
    transformaciones = ["T1_filtrado", "T2_agrupacion", "T3_join", "T4_columna_derivada", "T5_top_n"]
    path = os.path.join(resultados_dir, "tiempos_resumen.csv")
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["transformacion", "mediana_pandas_s", "mediana_pyspark_s", "speedup"])
        for t in transformaciones:
            tp, ts = pandas_t[t], spark_t[t]
            w.writerow([t, round(tp, 6), round(ts, 6), round(tp / ts, 4)])
    print(f"\nTabla de tiempos guardada en {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--resultados-dir", default="../resultados")
    args = parser.parse_args()
    d = args.resultados_dir
    os.makedirs(os.path.join(d, "figuras"), exist_ok=True)

    pandas_t = cargar_json(os.path.join(d, "tiempos_resumen_pandas.json"))
    spark_t = cargar_json(os.path.join(d, "tiempos_resumen_spark.json"))
    t3_por_n = cargar_json(os.path.join(d, "t3_escalado_executors.json"))

    tabla_resumen(pandas_t, spark_t, d)
    figura_barras(pandas_t, spark_t, os.path.join(d, "figuras", "fig1_barras.png"))

    amdahl_res = analisis_amdahl(t3_por_n, d)
    figura_speedup(amdahl_res, os.path.join(d, "figuras", "fig2_speedup.png"))
    figura_eficiencia(amdahl_res, os.path.join(d, "figuras", "fig3_eficiencia.png"))

    print(f"\nFiguras guardadas en {os.path.join(d, 'figuras')}/")


if __name__ == "__main__":
    main()
