# Dataset — PE-U4 (GA-SUM-05, Aplicaciones Distribuidas ISR-701)

## Fuente

**CGB — Consumer Complaints Data**, Federal Communications Commission (FCC), Consumer and
Governmental Affairs Bureau (Consumer Inquiries and Complaints Division).

- **URL permanente (ficha del dataset):** https://opendata.fcc.gov/d/3xyp-aqkj
- **Endpoint de descarga (Socrata Open Data API / SODA):** https://opendata.fcc.gov/resource/3xyp-aqkj.csv
- **Licencia:** Public Domain — U.S. Government Work (sin restricciones de uso ni atribución
  obligatoria).
- **Fecha de descarga:** 2026-08-03.
- **Volumen total disponible en el portal:** 3,599,354 registros (verificado con
  `$select=count(*)` sobre el mismo endpoint el día de la descarga).

## Por qué este dataset para el dominio ACC (Soporte Técnico ISP)

Cada fila del dataset es, literalmente, un **ticket** (`Ticket ID`) de queja de un consumidor
contra un proveedor de telecomunicaciones, clasificado por tipo de servicio (`issue_type`:
Phone / Internet / TV / Radio / Emergency / Accessibility / Request for Dispute Assistance) y
por tipo de problema específico (`issue`: facturación, equipo, interferencia, llamadas no
deseadas, etc.), con método de acceso (`method`, p. ej. "Fiber", "Wireless", "Internet (VOIP)")
y ubicación (ciudad, estado, zip). Esta estructura es directamente análoga al sistema de tickets
de soporte técnico de un ISP: tipo de incidencia, canal/tecnología, severidad implícita y zona
geográfica del cliente.

## Extracción aplicada

El portal expone 3,599,354 registros totales, de siete/ocho categorías de `issue_type`. Se
extrajeron los **600,000 tickets más recientes** restringidos a las tres categorías directamente
relacionadas con servicios de un ISP (`Phone`, `Internet`, `TV`), ordenados por
`ticket_created` descendente, vía paginación de la API SODA (`$limit`/`$offset` en bloques de
50,000 filas):

```
GET https://opendata.fcc.gov/resource/3xyp-aqkj.csv
    ?$select=id,ticket_created,issue_type,method,issue,city,state,zip
    &$where=issue_type in('Phone','Internet','TV')
    &$order=ticket_created DESC
    &$limit=50000&$offset={0,50000,...,550000}
```

## Características del extracto (`data/raw/fcc_consumer_complaints.csv`)

| Propiedad | Valor |
|---|---|
| Registros | 600,000 |
| Columnas | 8 |
| Tamaño en disco | 69 MB (CSV, sin comprimir) |
| Rango de fechas (`ticket_created`) | 2024-09-06 a 2026-08-03 |
| Distribución por `issue_type` | Phone 462,443 · Internet 103,785 · TV 33,772 |
| Estados/territorios distintos (`state`) | 56 |
| Valores nulos | `method` 15,466 · `issue` 2,456 · `city` 694 · `state` 181 · `zip` 192 |

Columnas: `id` (identificador del ticket), `ticket_created` (timestamp de apertura),
`issue_type`, `method`, `issue`, `city`, `state`, `zip`.

Los valores nulos en `method`/`issue`/`city`/`state`/`zip` son reales (no inducidos) y se
aprovechan en T1 como condición de filtrado (registros con campos completos).
