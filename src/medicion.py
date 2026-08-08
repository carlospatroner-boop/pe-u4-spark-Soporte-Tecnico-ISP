"""medicion.py — Protocolo de medición común (pandas y PySpark), Paso 2/3 y criterio 2.1.

Una ejecución de calentamiento descartada explícitamente + cinco repeticiones cronometradas
con time.perf_counter(); se reporta la mediana. Para PySpark, la evaluación perezosa exige
forzar la materialización del resultado con una acción (p. ej. count()) antes de detener el
cronómetro; para pandas basta con que la propia transformación ya sea eager.
"""

import statistics
import time
from dataclasses import dataclass, field


@dataclass
class ResultadoMedicion:
    transformacion: str
    tiempos_s: list = field(default_factory=list)
    mediana_s: float = 0.0
    resultado: object = None


def medir(transformacion: str, func, reps: int = 5, warmup: int = 1, materialize=None) -> ResultadoMedicion:
    """Ejecuta `func()` `warmup` veces (descartadas) y luego `reps` veces cronometradas.

    func: callable sin argumentos; ejecuta la transformación y devuelve el DataFrame resultante.
    materialize: callable opcional `materialize(df)` que fuerza una acción (Spark). None para
        pandas, donde cada operación ya es eager.
    """
    for _ in range(warmup):
        r = func()
        if materialize is not None:
            materialize(r)

    tiempos = []
    resultado = None
    for _ in range(reps):
        t0 = time.perf_counter()
        resultado = func()
        if materialize is not None:
            materialize(resultado)
        t1 = time.perf_counter()
        tiempos.append(t1 - t0)

    mediana = statistics.median(tiempos)
    return ResultadoMedicion(transformacion, tiempos, mediana, resultado)
