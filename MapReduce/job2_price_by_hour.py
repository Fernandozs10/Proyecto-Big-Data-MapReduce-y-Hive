#!/usr/bin/env python3
"""
MapReduce Job 2: Análisis de Precio Promedio por Hora
Objetivo: Calcular precio promedio por hora del día
Técnica: Combiner optimizado para calcular promedios
"""

import sys

def mapper():
    """
    Mapper: Extrae hora y precio, emite para agregación
    Input: CSV con columnas del dataset
    Output: hora \t precio
    """
    for line in sys.stdin:
        line = line.strip()
        if not line or line.startswith('business'):
            continue
        
        try:
            fields = line.split(',')
            hour_of_day = int(fields[11])  # Columna 11: hour_of_day
            passenger_fare = float(fields[14])  # Columna 14: passenger_fare
            
            # Validar datos
            if not (0 <= hour_of_day <= 23):
                continue
            if passenger_fare < 0:
                continue
            
            # Emit: hora \t precio
            print(f"{hour_of_day:02d}\t{passenger_fare:.2f}")
            
        except (IndexError, ValueError) as e:
            sys.stderr.write(f"Error: {e}\n")
            continue


def combiner():
    """
    Combiner: Calcula suma parcial y conteo para optimizar promedio
    Esta es la clave de la optimización
    """
    current_hour = None
    sum_price = 0.0
    count = 0
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            hour, price = line.split('\t')
            price = float(price)
            
            if current_hour and current_hour != hour:
                # Emitir suma parcial y conteo
                print(f"{current_hour}\t{sum_price:.2f},{count}")
                sum_price = 0.0
                count = 0
            
            current_hour = hour
            sum_price += price
            count += 1
            
        except ValueError:
            continue
    
    # Emitir último grupo
    if current_hour:
        print(f"{current_hour}\t{sum_price:.2f},{count}")


def reducer():
    """
    Reducer: Calcula promedio global por hora
    Output: hora, total_viajes, precio_promedio, precio_total
    """
    current_hour = None
    total_sum = 0.0
    total_count = 0
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            hour, data = line.split('\t')
            sum_price, count = data.split(',')
            sum_price = float(sum_price)
            count = int(count)
            
            if current_hour and current_hour != hour:
                # Calcular promedio y emitir
                avg_price = total_sum / total_count if total_count > 0 else 0
                print(f"{current_hour}\t{total_count}\t${avg_price:.2f}\t${total_sum:.2f}")
                total_sum = 0.0
                total_count = 0
            
            current_hour = hour
            total_sum += sum_price
            total_count += count
            
        except ValueError:
            continue
    
    # Emitir último grupo
    if current_hour and total_count > 0:
        avg_price = total_sum / total_count
        print(f"{current_hour}\t{total_count}\t${avg_price:.2f}\t${total_sum:.2f}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python job2_price_by_hour.py [mapper|combiner|reducer]")
        sys.exit(1)
    
    mode = sys.argv[1]
    
    if mode == "mapper":
        mapper()
    elif mode == "combiner":
        combiner()
    elif mode == "reducer":
        reducer()
    else:
        print(f"Modo inválido: {mode}")
        sys.exit(1)
