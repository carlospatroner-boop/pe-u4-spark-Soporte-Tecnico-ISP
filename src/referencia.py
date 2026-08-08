"""referencia.py — Datos de referencia compartidos por pandas y PySpark, para que T3 (join) y
T4 (columna derivada) apliquen exactamente la misma lógica en ambos motores y la verificación
de equivalencia (criterio 1.4) sea válida.
"""

# Regiones censales de EE. UU. (U.S. Census Bureau) por estado/territorio — dimensión de apoyo
# para el join de T3. No es un dato de negocio del dominio ACC: solo cumple el rol de la
# "segunda tabla" que exige la transformación de join.
REGION_POR_ESTADO = {
    # Northeast
    "CT": "Northeast", "ME": "Northeast", "MA": "Northeast", "NH": "Northeast",
    "RI": "Northeast", "VT": "Northeast", "NJ": "Northeast", "NY": "Northeast", "PA": "Northeast",
    # Midwest
    "IL": "Midwest", "IN": "Midwest", "MI": "Midwest", "OH": "Midwest", "WI": "Midwest",
    "IA": "Midwest", "KS": "Midwest", "MN": "Midwest", "MO": "Midwest", "NE": "Midwest",
    "ND": "Midwest", "SD": "Midwest",
    # South
    "DE": "South", "FL": "South", "GA": "South", "MD": "South", "NC": "South", "SC": "South",
    "VA": "South", "DC": "South", "WV": "South", "AL": "South", "KY": "South", "MS": "South",
    "TN": "South", "AR": "South", "LA": "South", "OK": "South", "TX": "South",
    # West
    "AZ": "West", "CO": "West", "ID": "West", "MT": "West", "NV": "West", "NM": "West",
    "UT": "West", "WY": "West", "AK": "West", "CA": "West", "HI": "West", "OR": "West",
    "WA": "West",
    # Territorios (no continentales)
    "PR": "Territorio", "VI": "Territorio", "GU": "Territorio", "AS": "Territorio",
    "MP": "Territorio",
}

# Clasificación de prioridad de atención técnica según el tipo de incidencia (T4, columna
# derivada). Regla de negocio propia del equipo: issues de disponibilidad/interferencia/
# velocidad se tratan como incidentes de red (ALTA), issues de cuenta/facturación como MEDIA,
# y quejas de contenido/spam como BAJA por no requerir intervención técnica de campo.
PRIORIDAD_POR_ISSUE = {
    "Availability (including rural call completion)": "ALTA",
    "Availability": "ALTA",
    "Rural Call Completion": "ALTA",
    "Interference (including signal jammers)": "ALTA",
    "Interference": "ALTA",
    "Speed": "ALTA",
    "Equipment": "ALTA",
    "Billing": "MEDIA",
    "Cramming (unauthorized charges on your phone bill)": "MEDIA",
    "Slamming (change of your carrier without permission)": "MEDIA",
    "Number Portability (keeping your number if you change providers)": "MEDIA",
    "Unwanted Calls": "BAJA",
    "Privacy": "BAJA",
    "Junk Faxes": "BAJA",
    "Loud Commercials": "BAJA",
    "Indecency": "BAJA",
}
PRIORIDAD_DEFAULT = "MEDIA"


def clasificar_prioridad(issue_type: str, issue) -> str:
    """UDF de clasificación de prioridad (T4). Recibe issue_type e issue (puede ser None/NaN)."""
    if issue is None or (isinstance(issue, float) and issue != issue):  # NaN check sin pandas/numpy
        return PRIORIDAD_DEFAULT
    return PRIORIDAD_POR_ISSUE.get(issue, PRIORIDAD_DEFAULT)
