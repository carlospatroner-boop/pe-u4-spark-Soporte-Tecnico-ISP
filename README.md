# PE-U4 / GA-SUM-05 — Comprobación experimental de la Ley de Amdahl con Apache Spark

**Asignatura:** Aplicaciones Distribuidas (ISR-701) · Unidad 4 · Universidad Técnica Estatal de Quevedo
**Equipo:** ACC — Soporte Técnico ISP

## Integrantes

| Integrante | Correo | Rol en este trabajo |
|---|---|---|
| Alvarez Parraga Jeremy Alexis | `jalvarezp3@uteq.edu.ec` | Estructura del repo y dataset, transformaciones distribuidas en PySpark, salidas de pandas, ajuste de Amdahl |
| Aucatoma Celorio Jhinson Stalyn | `jaucatomac@uteq.edu.ec` | Protocolo de medición y utilidades compartidas, transformaciones pandas, salidas de PySpark y configuración de sesión |
| Carpio Mendoza Carlos Jose | `carlospatroner@gmail.com` | Verificación de equivalencia, figuras y tabla de tiempos, evidencia de Spark UI, notebook |

## PFC de referencia

**Código:** ACC — **Título:** Soporte Técnico ISP (gestión de tickets de soporte técnico)

**Justificación técnica.** Un proveedor de servicios de internet recibe, en operación normal,
un volumen de tickets de soporte varios órdenes de magnitud mayor al que un sistema de
procesamiento secuencial en un solo hilo puede analizar con latencia razonable: clasificación
por tipo de incidencia, agregación por zona/estado, cruce con catálogos de referencia (clientes,
regiones, técnicos) y priorización de atención son operaciones que, sobre históricos de cientos
de miles o millones de registros, se benefician directamente de un motor de procesamiento
distribuido como Apache Spark. La decisión de negocio concreta que se apoyaría en el pipeline
implementado aquí es la **priorización automática de la cola de atención técnica**: a partir de
la columna derivada `prioridad` (T4) y de la agregación por estado (T2), un ISP real podría
decidir en qué zonas reforzar personal técnico y qué tickets escalar primero, en vez de atenderlos
por orden de llegada. Como no existe un dataset público de tickets internos de un ISP
ecuatoriano, se usó como sustituto un dataset real y público de complaints de consumidores
contra proveedores de telecomunicaciones (ver sección "Dataset" abajo), que tiene la misma
estructura de un sistema de tickets (identificador, tipo de incidencia, canal, fecha, ubicación)
y por tanto permite aplicar el mismo tipo de análisis distribuido que se aplicaría sobre el
histórico interno real de un ISP.

## Dataset

**Fuente:** [CGB — Consumer Complaints Data](https://opendata.fcc.gov/d/3xyp-aqkj), Federal
Communications Commission (FCC), EE. UU. — licencia **Public Domain (U.S. Government Work)**.
Se descargaron 600,000 tickets reales (categorías Phone/Internet/TV) vía la API pública Socrata.
Detalle completo de la extracción, columnas y estadísticas descriptivas en
[`data/README_dataset.md`](data/README_dataset.md).

El CSV crudo (69 MB) **no está versionado** (ver `.gitignore`); se regenera con el comando de
la sección "Reproducir el experimento" más abajo.

## Resultados principales

| Transformación | Mediana pandas (s) | Mediana PySpark (s) | Speedup |
|---|---:|---:|---:|
| T1 — Filtrado | 0.1846 | 1.0789 | 0.171 |
| T2 — Agrupación | 0.1575 | 1.8854 | 0.084 |
| T3 — Join | 0.1363 | 1.8290 | 0.075 |
| T4 — Columna derivada | 0.3559 | 0.8483 | 0.420 |
| T5 — Top-N | 0.1169 | 1.4286 | 0.082 |

Con 600,000 filas, PySpark resulta **más lento** que pandas en las cinco transformaciones — el
overhead de coordinación distribuida (JVM, planificación de tareas, shuffle) domina sobre el
tiempo de cómputo real a esta escala. Es el resultado esperado y se discute en la Pregunta 2 del
Anexo A del informe.

**Escalado de T3 (join) y ajuste de Amdahl** (`resultados/amdahl_fit.json`):

| N executors | Tiempo T3 (s) | Speedup observado |
|---:|---:|---:|
| 1 | 3.0712 | 1.000 |
| 2 | 3.1252 | 0.983 |
| 4 | 1.8290 | 1.679 |

- Fracción serial observada (Ec. 4, con N=4): **p ≈ 0.4607**
- Speedup máximo teórico (Ec. 2): **S_max ≈ 1.854**
- N necesario para el 90% de S_max: **≈ 7.7 (8 executors)**

Verificación de equivalencia pandas vs PySpark (`resultados/equivalencia.json`): las **5
transformaciones coinciden exactamente** en cardinalidad y en los agregados de control.

## Estructura del repositorio

```
pe-u4-spark-Soporte-Tecnico-ISP/
├── README.md                    (este archivo)
├── LICENSE
├── .gitignore
├── notebooks/
│   ├── PE_U4_pipeline_spark.ipynb   (ejecutado, con salidas visibles)
│   └── PE_U4_pipeline_spark.html    (exportación HTML)
├── src/
│   ├── medicion.py               (protocolo: 1 calentamiento + 5 repeticiones, mediana)
│   ├── referencia.py             (tabla de regiones y UDF de prioridad, compartidas)
│   ├── transformaciones_pandas.py
│   ├── transformaciones_spark.py
│   ├── amdahl.py                 (Ecs. 1-5 de la guía)
│   ├── graficas.py                (3 figuras a 300 DPI)
│   ├── verificar_equivalencia.py
│   └── spark_ui_demo.py          (levanta la Spark UI para capturar evidencia)
├── data/
│   ├── README_dataset.md         (fuente, URL, licencia, fecha, num. registros)
│   ├── raw/                      (CSV crudo — NO versionado, ver .gitignore)
│   ├── pandas/                   (CSV de salida de cada transformación, pandas)
│   └── spark/                    (CSV de salida de cada transformación, PySpark)
├── resultados/
│   ├── tiempos_crudos.csv        (las 5 repeticiones por transformación)
│   ├── tiempos_resumen.csv       (medianas y speedup)
│   ├── amdahl_fit.json           (p, S_max, N para 90% de S_max)
│   ├── equivalencia.json         (verificación pandas vs PySpark)
│   ├── t3_escalado_executors.json
│   ├── spark_config_efectiva_*.json  (configuración efectiva de cada sesión Spark)
│   └── figuras/                  (fig1_barras.png, fig2_speedup.png, fig3_eficiencia.png)
├── evidencia/
│   ├── spark_ui_t3.png           (DAG Visualization del job de T3)
│   └── spark_ui_t3_stages.png    (Completed Stages / shuffle de T3)
└── docs/                         (documento LaTeX del informe — en progreso)
```

## Reproducir el experimento (entorno Docker — igual en Windows/Mac/Linux)

PySpark en Windows requiere `winutils.exe`/librerías nativas de Hadoop frágiles de instalar; se
usa un contenedor Linux con Java 21 + PySpark 4.1.2 ya instalados. Todos los comandos se corren
desde la raíz del repo.

```bash
# 1. Construir la imagen (una sola vez)
docker build -t pe-u4-acc-spark -f src/Dockerfile .   # ver Dockerfile de referencia en el repo SOPORTE/spark

# 2. Regenerar el dataset (600,000 filas reales, API pública de la FCC)
#    ver data/README_dataset.md para el comando exacto de descarga (curl + paginación SODA API)

# 3. Transformaciones pandas (T1-T5), 5 repeticiones + mediana
docker run --rm -v "$(pwd)/src:/app/src" -v "$(pwd)/data:/app/data" -v "$(pwd)/resultados:/app/resultados" \
  pe-u4-acc-spark transformaciones_pandas.py

# 4. Transformaciones PySpark (T1,T2,T4,T5 en local[4]; T3 con local[1,2,4] para Amdahl)
docker run --rm -v "$(pwd)/src:/app/src" -v "$(pwd)/data:/app/data" -v "$(pwd)/resultados:/app/resultados" \
  pe-u4-acc-spark transformaciones_spark.py

# 5. Verificación de equivalencia pandas vs PySpark
docker run --rm -v "$(pwd)/src:/app/src" -v "$(pwd)/data:/app/data" -v "$(pwd)/resultados:/app/resultados" \
  pe-u4-acc-spark verificar_equivalencia.py

# 6. Análisis de Amdahl + 3 figuras a 300 DPI
docker run --rm -v "$(pwd)/src:/app/src" -v "$(pwd)/resultados:/app/resultados" \
  pe-u4-acc-spark graficas.py
```

Versiones fijadas: Python 3.10, PySpark 4.1.2, pandas 2.x, matplotlib 3.x, scipy, Java 21
(imagen `eclipse-temurin:21-jdk-jammy`). Semillas: `seed=42` en el KMeans/estratificación donde
aplica (ver `src/referencia.py`). Tiempo estimado de reproducción completa (dataset ya
descargado): ~10-15 minutos.

## Documento LaTeX del informe

En progreso — instrucciones de compilación exactas se agregan en `docs/` cuando esté listo
(`pdflatex → biber → pdflatex → pdflatex`, ver `docs/PE_U4_Informe.tex`).

## Declaración de uso de inteligencia artificial generativa

Se utilizó Claude (Anthropic) como asistente de programación para la implementación de los
scripts de `src/`, la generación del notebook y las figuras, y la organización de este README,
bajo supervisión y revisión de los integrantes del equipo. El análisis de resultados, las
decisiones de diseño experimental y las conclusiones son responsabilidad del equipo.

## Licencia

MIT — ver [`LICENSE`](LICENSE).
