#!/usr/bin/env python3
"""
MapReduce Job 1: Conteo de Viajes por Servicio con Estadísticas
Objetivo: Contar viajes Uber vs Lyft y calcular ingresos totales
Técnica: Mapper simple + Combiner para optimización
"""

import sys

def mapper():
    """
    Mapper: Lee datos Parquet (convertidos a CSV) y emite (servicio, 1|precio)
    Input: CSV con columnas del dataset rideshare
    Output: business \t 1,precio
    """
    for line in sys.stdin:
        line = line.strip()
        if not line or line.startswith('business'):  # Skip header
            continue
        
        try:
            fields = line.split(',')
            business = fields[0].strip()  # Uber o Lyft
            passenger_fare = float(fields[14])  # Columna 14: passenger_fare
            
            # Validar datos
            if business not in ['Uber', 'Lyft']:
                continue
            if passenger_fare < 0:  # Ignorar reembolsos/cancelaciones
                continue
            
            # Emit: servicio \t 1,precio
            print(f"{business}\t1,{passenger_fare:.2f}")
            
        except (IndexError, ValueError) as e:
            sys.stderr.write(f"Error en línea: {line[:50]}... - {e}\n")
            continue


def combiner():
    """
    Combiner: Suma parcial local (optimización)
    Reduce data shuffle entre nodos
    """
    current_service = None
    trip_count = 0
    total_fare = 0.0
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            service, data = line.split('\t')
            count, fare = data.split(',')
            count = int(count)
            fare = float(fare)
            
            if current_service and current_service != service:
                # Emitir suma parcial
                print(f"{current_service}\t{trip_count},{total_fare:.2f}")
                trip_count = 0
                total_fare = 0.0
            
            current_service = service
            trip_count += count
            total_fare += fare
            
        except ValueError:
            continue
    
    # Emitir último grupo
    if current_service:
        print(f"{current_service}\t{trip_count},{total_fare:.2f}")


def reducer():
    """
    Reducer: Calcula totales globales y métricas finales
    Output: servicio, total_viajes, ingreso_total, precio_promedio
    """
    current_service = None
    total_trips = 0
    total_revenue = 0.0
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            service, data = line.split('\t')
            count, fare = data.split(',')
            count = int(count)
            fare = float(fare)
            
            if current_service and current_service != service:
                # Calcular y emitir resultados
                avg_fare = total_revenue / total_trips if total_trips > 0 else 0
                print(f"{current_service}\t{total_trips}\t${total_revenue:.2f}\t${avg_fare:.2f}")
                total_trips = 0
                total_revenue = 0.0
            
            current_service = service
            total_trips += count
            total_revenue += fare
            
        except ValueError:
            continue
    
    # Emitir último grupo
    if current_service and total_trips > 0:
        avg_fare = total_revenue / total_trips
        print(f"{current_service}\t{total_trips}\t${total_revenue:.2f}\t${avg_fare:.2f}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python job1_service_count.py [mapper|combiner|reducer]")
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
