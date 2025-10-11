-- ============================================================================
-- HIVE Job 5: Executive Dashboard - KPIs Consolidados
-- Objetivo: Métricas principales para dashboard ejecutivo
-- ============================================================================

USE rideshare_db;

-- Resumen General
SELECT 
    'Total Metrics' as category,
    COUNT(*) as total_trips,
    COUNT(DISTINCT business) as services,
    ROUND(SUM(passenger_fare), 2) as total_revenue,
    ROUND(AVG(passenger_fare), 2) as avg_fare,
    ROUND(SUM(rideshare_profit), 2) as total_profit,
    ROUND(AVG(trip_length), 2) as avg_distance
FROM rideshare_data
WHERE passenger_fare > 0 AND trip_length > 0

UNION ALL

-- Por Servicio
SELECT 
    business as category,
    COUNT(*) as total_trips,
    1 as services,
    ROUND(SUM(passenger_fare), 2) as total_revenue,
    ROUND(AVG(passenger_fare), 2) as avg_fare,
    ROUND(SUM(rideshare_profit), 2) as total_profit,
    ROUND(AVG(trip_length), 2) as avg_distance
FROM rideshare_data
WHERE passenger_fare > 0 AND trip_length > 0
GROUP BY business

ORDER BY total_trips DESC;
