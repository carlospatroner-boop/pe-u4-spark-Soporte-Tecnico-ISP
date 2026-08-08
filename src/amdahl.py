"""amdahl.py — Formalización de la Ley de Amdahl exigida por la guía (Sección 5.1, Ecs. 1-5).

Reutilizado tanto por el análisis de T3 (escalado 1/2/4 executors) como por la generación de
figuras (graficas.py).
"""


def speedup_amdahl(n: float, p: float) -> float:
    """Ecuación (1): S(N) = 1 / ((1-p) + p/N)."""
    return 1.0 / ((1 - p) + p / n)


def speedup_maximo(p: float) -> float:
    """Ecuación (2): S_max = lim N->inf S(N) = 1 / (1-p)."""
    return 1.0 / (1 - p)


def speedup_gustafson(n: float, p: float) -> float:
    """Ecuación (3): S_G(N) = N - (1-p)(N-1)."""
    return n - (1 - p) * (n - 1)


def fraccion_serial_inversa(n: float, s: float) -> float:
    """Ecuación (4): fracción serial observada p, despejada de (1) a partir de una medición
    experimental con N unidades y speedup observado S (forma de Karp-Flatt)."""
    if n <= 1:
        raise ValueError("La forma inversa (4) requiere N > 1.")
    return ((1 / s) - (1 / n)) / (1 - (1 / n))


def eficiencia(s: float, n: float) -> float:
    """Ecuación (5): E(N) = S(N) / N."""
    return s / n


def n_para_fraccion_de_maximo(p: float, fraccion: float = 0.9) -> float:
    """N necesario para alcanzar `fraccion` (por defecto 90%) del speedup máximo S_max.

    Despejando N de S(N) = fraccion * S_max = fraccion / (1-p):
        (1-p) + p/N = 1 / (fraccion * S_max) = (1-p) / fraccion
        p/N = (1-p) * (1/fraccion - 1)
        N = p / ((1-p) * (1/fraccion - 1))
    """
    denom = (1 - p) * (1 / fraccion - 1)
    return p / denom
