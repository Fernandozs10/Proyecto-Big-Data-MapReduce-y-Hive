#!/usr/bin/env python3
"""
MapReduce Job 5: Detección de Viajes Anómalos (Alto Precio)
Objetivo: Identificar viajes con precios superiores al percentil 95
Técnica: Cálculo de percentiles y filtrado
"""

import sys

def mapper():
    """
    Mapper: Extrae precios para análisis
    Input: CSV del dataset
    Output: precio \t datos_viaje
    """
    for line in sys.stdin:
        line = line.strip()
        if not line or line.startswith('business'):
            continue
        
        try:
            fields = line.split(',')
            business = fields[0].strip()
            trip_length = float(fields[3])
            passenger_fare = float(fields[14])
            driver_pay = float(fields[15])
            profit = float(fields[16])
            
            # Validar datos
            if passenger_fare <= 0:
                continue
            
            # Emit: precio \t servicio,distancia,pago_conductor,ganancia
            print(f"{passenger_fare:.2f}\t{business},{trip_length:.2f},{driver_pay:.2f},{profit:.2f}")
            
        except (IndexError, ValueError):
            continue


def reducer():
    """
    Reducer: Calcula percentil 95 y filtra viajes caros
    Output: viajes con precio > percentil 95
    """
    trips = []
    
    # Recolectar todos los datos
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            price, data = line.split('\t')
            price = float(price)
            business, distance, driver_pay, profit = data.split(',')
            
            trips.append({
                'price': price,
                'business': business,
                'distance': float(distance),
                'driver_pay': float(driver_pay),
                'profit': float(profit)
            })
            
        except ValueError:
            continue
    
    if not trips:
        return
    
    # Ordenar por precio
    trips.sort(key=lambda x: x['price'])
    
    # Calcular percentil 95
    p95_index = int(len(trips) * 0.95)
    p95_threshold = trips[p95_index]['price'] if p95_index < len(trips) else trips[-1]['price']
    
    # Emitir encabezado
    print(f"# Percentil 95: ${p95_threshold:.2f}")
    print(f"# Total viajes analizados: {len(trips)}")
    print(f"# Viajes anómalos (>P95): {len(trips) - p95_index}")
    print("#")
    print("# Servicio\tPrecio\tDistancia\tPago_Driver\tGanancia")
    
    # Emitir viajes con precio > percentil 95
    for trip in trips[p95_index:]:
        print(f"{trip['business']}\t${trip['price']:.2f}\t{trip['distance']:.2f}mi\t"
              f"${trip['driver_pay']:.2f}\t${trip['profit']:.2f}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python job5_anomaly_detection.py [mapper|reducer]")
        sys.exit(1)
    
    mode = sys.argv[1]
    
    if mode == "mapper":
        mapper()
    elif mode == "reducer":
        reducer()
    else:
        print(f"Modo inválido: {mode}")
        sys.exit(1)
