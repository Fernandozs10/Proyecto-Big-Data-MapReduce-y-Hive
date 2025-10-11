#!/usr/bin/env python3
"""
MapReduce Job 3: Análisis de Distancia por Servicio
Objetivo: Calcular estadísticas de distancia (promedio, min, max) por servicio
Técnica: Múltiples métricas en un solo job
"""

import sys

def mapper():
    """
    Mapper: Extrae servicio y distancia
    Input: CSV del dataset rideshare
    Output: servicio \t distancia
    """
    for line in sys.stdin:
        line = line.strip()
        if not line or line.startswith('business'):
            continue
        
        try:
            fields = line.split(',')
            business = fields[0].strip()  # Uber o Lyft
            trip_length = float(fields[3])  # Columna 3: trip_length
            
            # Validar datos
            if business not in ['Uber', 'Lyft']:
                continue
            if trip_length < 0 or trip_length > 200:  # Filtrar distancias absurdas
                continue
            
            # Emit: servicio \t distancia
            print(f"{business}\t{trip_length:.2f}")
            
        except (IndexError, ValueError) as e:
            sys.stderr.write(f"Error: {e}\n")
            continue


def combiner():
    """
    Combiner: Calcula estadísticas parciales
    Para calcular min/max globales necesitamos mantenerlos
    """
    current_service = None
    sum_dist = 0.0
    count = 0
    min_dist = float('inf')
    max_dist = float('-inf')
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            service, distance = line.split('\t')
            distance = float(distance)
            
            if current_service and current_service != service:
                # Emitir estadísticas parciales
                print(f"{current_service}\t{sum_dist:.2f},{count},{min_dist:.2f},{max_dist:.2f}")
                sum_dist = 0.0
                count = 0
                min_dist = float('inf')
                max_dist = float('-inf')
            
            current_service = service
            sum_dist += distance
            count += 1
            min_dist = min(min_dist, distance)
            max_dist = max(max_dist, distance)
            
        except ValueError:
            continue
    
    # Emitir último grupo
    if current_service:
        print(f"{current_service}\t{sum_dist:.2f},{count},{min_dist:.2f},{max_dist:.2f}")


def reducer():
    """
    Reducer: Calcula estadísticas globales por servicio
    Output: servicio, num_viajes, avg_distance, min_distance, max_distance
    """
    current_service = None
    total_sum = 0.0
    total_count = 0
    global_min = float('inf')
    global_max = float('-inf')
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            service, data = line.split('\t')
            sum_dist, count, min_dist, max_dist = data.split(',')
            sum_dist = float(sum_dist)
            count = int(count)
            min_dist = float(min_dist)
            max_dist = float(max_dist)
            
            if current_service and current_service != service:
                # Calcular y emitir resultados
                avg_dist = total_sum / total_count if total_count > 0 else 0
                print(f"{current_service}\t{total_count}\t{avg_dist:.2f}mi\t{global_min:.2f}mi\t{global_max:.2f}mi")
                total_sum = 0.0
                total_count = 0
                global_min = float('inf')
                global_max = float('-inf')
            
            current_service = service
            total_sum += sum_dist
            total_count += count
            global_min = min(global_min, min_dist)
            global_max = max(global_max, max_dist)
            
        except ValueError:
            continue
    
    # Emitir último grupo
    if current_service and total_count > 0:
        avg_dist = total_sum / total_count
        print(f"{current_service}\t{total_count}\t{avg_dist:.2f}mi\t{global_min:.2f}mi\t{global_max:.2f}mi")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python job3_distance_stats.py [mapper|combiner|reducer]")
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
