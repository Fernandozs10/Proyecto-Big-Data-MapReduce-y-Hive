-- ============================================================================
-- HIVE Job 1: Crear Tabla Externa y Cargar Datos (HDFS LOCAL)
-- Objetivo: Crear tabla HIVE sobre HDFS y preparar para análisis
-- Técnica: External table + particionamiento básico
-- ============================================================================

-- Configuración básica
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Crear base de datos
CREATE DATABASE IF NOT EXISTS rideshare_db
COMMENT 'Análisis de datos Rideshare Uber/Lyft';

USE rideshare_db;

-- ============================================================================
-- Tabla Externa: Datos crudos en HDFS
-- ============================================================================
DROP TABLE IF EXISTS rideshare_data;

CREATE EXTERNAL TABLE rideshare_data (
    business STRING,
    pickup_location INT,
    dropoff_location INT,
    trip_length DOUBLE,
    request_to_dropoff DOUBLE,
    request_to_pickup DOUBLE,
    total_ride_time DOUBLE,
    on_scene_to_pickup DOUBLE,
    on_scene_to_dropoff DOUBLE,
    time_of_day STRING,
    date_field DATE,
    hour_of_day INT,
    week_of_year INT,
    month_of_year INT,
    passenger_fare DOUBLE,
    driver_total_pay DOUBLE,
    rideshare_profit DOUBLE,
    hourly_rate DOUBLE,
    dollars_per_mile DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/rideshare_data/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Verificar carga
SELECT COUNT(*) as total_records FROM rideshare_data;
SELECT * FROM rideshare_data LIMIT 5;

-- Estadísticas básicas
SELECT 
    'Job 1 Completed' as status,
    COUNT(*) as total_records,
    COUNT(DISTINCT business) as unique_services,
    ROUND(SUM(passenger_fare), 2) as total_revenue
FROM rideshare_data;
