-- Que es: archivo de consultas para el Data Warehouse en BigQuery.
-- Que hace: no sube datos. Sirve para verificar que el ETL funciono
-- y para analizar ventas (por mes, productos, clientes, ciudades).
-- Como usarlo: copiar cada consulta en la consola de BigQuery
-- (proyecto alex-512401, dataset urbanrush_dw).

-- 1. Ventas mensuales
SELECT
  t.anio,
  t.mes,
  t.trimestre,
  COUNT(*) AS lineas,
  ROUND(SUM(f.subtotal_linea), 2) AS ingresos,
  ROUND(AVG(f.total_pedido), 2) AS ticket_promedio
FROM `alex-512401.urbanrush_dw.fact_ventas` AS f
JOIN `alex-512401.urbanrush_dw.dim_tiempo` AS t
  ON f.fecha_id = t.fecha_id
GROUP BY t.anio, t.mes, t.trimestre
ORDER BY t.anio, t.mes;

-- 2. Top 10 productos por ingresos
SELECT
  p.producto_id,
  p.nombre,
  p.categoria,
  SUM(f.cantidad) AS unidades,
  ROUND(SUM(f.subtotal_linea), 2) AS ingresos
FROM `alex-512401.urbanrush_dw.fact_ventas` AS f
JOIN `alex-512401.urbanrush_dw.dim_producto` AS p
  ON f.producto_id = p.producto_id
GROUP BY p.producto_id, p.nombre, p.categoria
ORDER BY ingresos DESC
LIMIT 10;

-- 3. Ticket promedio por segmento de cliente
SELECT
  c.segmento,
  COUNT(DISTINCT c.cliente_id) AS clientes,
  COUNT(*) AS lineas,
  ROUND(AVG(f.total_pedido), 2) AS ticket_promedio,
  ROUND(SUM(f.subtotal_linea), 2) AS ingresos
FROM `alex-512401.urbanrush_dw.fact_ventas` AS f
JOIN `alex-512401.urbanrush_dw.dim_cliente` AS c
  ON f.cliente_id = c.cliente_id
GROUP BY c.segmento
ORDER BY ingresos DESC;

-- 4. Ingresos por ciudad y metodo de pago
SELECT
  f.ciudad_entrega,
  f.metodo_pago,
  COUNT(*) AS lineas,
  ROUND(SUM(f.subtotal_linea), 2) AS ingresos
FROM `alex-512401.urbanrush_dw.fact_ventas` AS f
GROUP BY f.ciudad_entrega, f.metodo_pago
ORDER BY ingresos DESC;
