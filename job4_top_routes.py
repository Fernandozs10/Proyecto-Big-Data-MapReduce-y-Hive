#!/usr/bin/env python3
"""
MapReduce Job 4: Top 10 Rutas Más Populares
Objetivo: Identificar las rutas (pickup → dropoff) con más viajes
Técnica: Aggregation con ordenamiento en reducer
"""

import sys

def mapper():
    """
    Mapper: Crea clave de ruta desde ubicación origen-destino
    Input: CSV del dataset
    Output: ruta_key \t 1
    """
    for line in sys.stdin:
        line = line.strip()
        if not line or line.startswith('business'):
            continue
        
        try:
            fields = line.split(',')
            pickup_location = fields[1].strip()  # Columna 1: pickup_location
            dropoff_location = fields[2].strip()  # Columna 2: dropoff_location
            
            # Crear clave de ruta: "origen->destino"
            route_key = f"{pickup_location}->{dropoff_location}"
            
            # Emit: ruta \t 1
            print(f"{route_key}\t1")
            
        except (IndexError, ValueError) as e:
            sys.stderr.write(f"Error: {e}\n")
            continue


def combiner():
    """
    Combiner: Suma parcial de viajes por ruta
    """
    current_route = None
    count = 0
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            route, trips = line.split('\t')
            trips = int(trips)
            
            if current_route and current_route != route:
                print(f"{current_route}\t{count}")
                count = 0
            
            current_route = route
            count += trips
            
        except ValueError:
            continue
    
    if current_route:
        print(f"{current_route}\t{count}")


def reducer():
    """
    Reducer: Suma total por ruta y ordena para Top 10
    Output: ranking, origen, destino, num_viajes
    """
    routes = {}
    
    # Primera pasada: acumular todos los conteos
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            route, count = line.split('\t')
            count = int(count)
            
            if route in routes:
                routes[route] += count
            else:
                routes[route] = count
            
        except ValueError:
            continue
    
    # Ordenar por frecuencia (descendente) y tomar Top 10
    sorted_routes = sorted(routes.items(), key=lambda x: x[1], reverse=True)
    top_10 = sorted_routes[:10]
    
    # Emitir Top 10 con ranking
    for rank, (route, count) in enumerate(top_10, start=1):
        origin, destination = route.split('->')
        print(f"{rank}\t{origin}\t{destination}\t{count}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python job4_top_routes.py [mapper|combiner|reducer]")
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
