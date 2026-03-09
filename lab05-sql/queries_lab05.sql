CREATE SCHEMA IF NOT EXISTS dw;

-- 1)
CREATE TABLE IF NOT EXISTS public.pais_envejecimiento (
    id_pais INTEGER PRIMARY KEY,
    nombre_pais TEXT,
    capital TEXT,
    continente TEXT,
    region TEXT,
    poblacion BIGINT,
    tasa_de_envejecimiento NUMERIC(6,2)
);

TRUNCATE TABLE public.pais_envejecimiento;

-- 2)
CREATE TABLE IF NOT EXISTS dw.paises_integrados_python (
    id_pais INTEGER,
    nombre_pais TEXT,
    tasa_de_envejecimiento NUMERIC(6,2),
    mongo_id TEXT,
    continente TEXT,
    region TEXT,
    pais_mongo TEXT,
    capital TEXT,
    poblacion BIGINT,
    hospedaje_bajo NUMERIC(10,2),
    hospedaje_promedio NUMERIC(10,2),
    hospedaje_alto NUMERIC(10,2),
    comida_bajo NUMERIC(10,2),
    comida_promedio NUMERIC(10,2),
    comida_alto NUMERIC(10,2),
    transporte_bajo NUMERIC(10,2),
    transporte_promedio NUMERIC(10,2),
    transporte_alto NUMERIC(10,2),
    entretenimiento_bajo NUMERIC(10,2),
    entretenimiento_promedio NUMERIC(10,2),
    entretenimiento_alto NUMERIC(10,2)
);

CREATE INDEX IF NOT EXISTS idx_dw_paises_integrados_python_id_pais
    ON dw.paises_integrados_python (id_pais);

CREATE INDEX IF NOT EXISTS idx_dw_paises_integrados_python_continente
    ON dw.paises_integrados_python (continente);

-- 3)

--3.1
SELECT COUNT(*) AS filas_sql
FROM public.pais_envejecimiento;

--3.2
SELECT COUNT(*) AS filas_dw
FROM dw.paises_integrados_python;

--3.3
SELECT id_pais, nombre_pais, tasa_de_envejecimiento
FROM dw.paises_integrados_python
WHERE mongo_id IS NULL
ORDER BY id_pais;

--3.4
SELECT id_pais, COUNT(*) AS repeticiones
FROM dw.paises_integrados_python
GROUP BY id_pais
HAVING COUNT(*) > 1
ORDER BY repeticiones DESC, id_pais;

--3.5
SELECT
    COUNT(*) FILTER (WHERE tasa_de_envejecimiento IS NULL) AS nulos_tasa,
    COUNT(*) FILTER (WHERE continente IS NULL) AS nulos_continente,
    COUNT(*) FILTER (WHERE hospedaje_promedio IS NULL) AS nulos_hospedaje,
    COUNT(*) FILTER (WHERE comida_promedio IS NULL) AS nulos_comida,
    COUNT(*) FILTER (WHERE transporte_promedio IS NULL) AS nulos_transporte,
    COUNT(*) FILTER (WHERE entretenimiento_promedio IS NULL) AS nulos_entretenimiento
FROM dw.paises_integrados_python;

-- 4)

-- Insight 1
SELECT
    nombre_pais,
    continente,
    tasa_de_envejecimiento,
    ROUND(
        COALESCE(hospedaje_promedio, 0)
      + COALESCE(comida_promedio, 0)
      + COALESCE(transporte_promedio, 0)
      + COALESCE(entretenimiento_promedio, 0),
      2
    ) AS costo_total_diario_promedio
FROM dw.paises_integrados_python
WHERE tasa_de_envejecimiento IS NOT NULL
  AND hospedaje_promedio IS NOT NULL
  AND comida_promedio IS NOT NULL
  AND transporte_promedio IS NOT NULL
  AND entretenimiento_promedio IS NOT NULL
ORDER BY tasa_de_envejecimiento DESC, costo_total_diario_promedio DESC
LIMIT 10;

-- Insight 2
SELECT
    continente,
    ROUND(AVG(tasa_de_envejecimiento), 2) AS promedio_envejecimiento,
    ROUND(AVG(
        COALESCE(hospedaje_promedio, 0)
      + COALESCE(comida_promedio, 0)
      + COALESCE(transporte_promedio, 0)
      + COALESCE(entretenimiento_promedio, 0)
    ), 2) AS costo_diario_promedio_continente,
    COUNT(*) AS cantidad_paises
FROM dw.paises_integrados_python
WHERE continente IS NOT NULL
GROUP BY continente
ORDER BY costo_diario_promedio_continente DESC;

-- Insight 3
SELECT
    nombre_pais,
    continente,
    ROUND(
        (COALESCE(hospedaje_alto, 0) + COALESCE(comida_alto, 0) + COALESCE(transporte_alto, 0) + COALESCE(entretenimiento_alto, 0))
      - (COALESCE(hospedaje_bajo, 0) + COALESCE(comida_bajo, 0) + COALESCE(transporte_bajo, 0) + COALESCE(entretenimiento_bajo, 0)),
      2
    ) AS brecha_alto_vs_bajo
FROM dw.paises_integrados_python
WHERE hospedaje_alto IS NOT NULL
  AND comida_alto IS NOT NULL
  AND transporte_alto IS NOT NULL
  AND entretenimiento_alto IS NOT NULL
  AND hospedaje_bajo IS NOT NULL
  AND comida_bajo IS NOT NULL
  AND transporte_bajo IS NOT NULL
  AND entretenimiento_bajo IS NOT NULL
ORDER BY brecha_alto_vs_bajo DESC
LIMIT 10;
