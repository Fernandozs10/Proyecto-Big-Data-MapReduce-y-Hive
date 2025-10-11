-- ============================================================================
-- HIVE Job 2: Análisis Temporal por Hora del Día
-- Objetivo: Analizar patrones de demanda y precios por hora
-- ============================================================================

USE rideshare_db;

SELECT 
    hour_of_day,
    COUNT(*) as total_trips,
    ROUND(AVG(passenger_fare), 2) as avg_price,
    ROUND(MIN(passenger_fare), 2) as min_price,
    ROUND(MAX(passenger_fare), 2) as max_price,
    ROUND(AVG(trip_length), 2) as avg_distance,
    ROUND(SUM(passenger_fare), 2) as total_revenue,
    ROUND(SUM(rideshare_profit), 2) as total_profit,
    SUM(CASE WHEN business = 'Uber' THEN 1 ELSE 0 END) as uber_trips,
    SUM(CASE WHEN business = 'Lyft' THEN 1 ELSE 0 END) as lyft_trips
FROM rideshare_data
WHERE passenger_fare > 0 AND trip_length > 0
GROUP BY hour_of_day
ORDER BY hour_of_day;
