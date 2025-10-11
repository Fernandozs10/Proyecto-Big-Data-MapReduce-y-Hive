-- ============================================================================
-- HIVE Job 3: Comparación de Servicios (Uber vs Lyft)
-- Objetivo: Análisis comparativo entre servicios con window functions
-- ============================================================================

USE rideshare_db;

SELECT 
    business,
    COUNT(*) as total_trips,
    ROUND(AVG(passenger_fare), 2) as avg_fare,
    ROUND(AVG(driver_total_pay), 2) as avg_driver_pay,
    ROUND(AVG(rideshare_profit), 2) as avg_profit,
    ROUND(AVG(trip_length), 2) as avg_distance,
    ROUND(SUM(passenger_fare), 2) as total_revenue,
    ROUND(SUM(rideshare_profit), 2) as total_profit,
    ROUND(AVG(passenger_fare / trip_length), 2) as price_per_mile
FROM rideshare_data
WHERE passenger_fare > 0 AND trip_length > 0
GROUP BY business
ORDER BY total_trips DESC;
