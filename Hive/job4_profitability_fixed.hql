-- ============================================================================
-- HIVE Job 4: Análisis de Rentabilidad
-- Objetivo: Calcular márgenes y eficiencia por hora y servicio
-- ============================================================================

USE rideshare_db;

SELECT 
    hour_of_day,
    business,
    COUNT(*) as trips,
    ROUND(AVG(passenger_fare), 2) as avg_fare,
    ROUND(AVG(rideshare_profit), 2) as avg_profit,
    ROUND(AVG(rideshare_profit / passenger_fare * 100), 2) as profit_margin_pct,
    ROUND(SUM(rideshare_profit), 2) as total_profit,
    ROUND(AVG(trip_length), 2) as avg_distance,
    ROUND(AVG(passenger_fare / trip_length), 2) as revenue_per_mile
FROM rideshare_data
WHERE passenger_fare > 0 AND trip_length > 0 AND rideshare_profit > 0
GROUP BY hour_of_day, business
ORDER BY total_profit DESC
LIMIT 50;
