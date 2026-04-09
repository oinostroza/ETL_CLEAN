import csv
import random
import os
from datetime import datetime, timedelta

# Configuración
NUM_CLIENTES = 50000
NUM_PRODUCTOS = 60000  # Un cliente puede tener más de un producto
TOTAL_TRANSACCIONES = 50000000 
FOLDER_RAW = "data/raw"
FILE_PATH = f"{FOLDER_RAW}/transacciones.csv"

os.makedirs(FOLDER_RAW, exist_ok=True)

def generate_data():
    # 1. Preparar datos base en memoria
    print("--- Preparando catálogos base ---")
    regiones = ['Metropolitana', 'Biobío', 'Valparaíso', 'Maule', '']
    tipos_tx = ['TRANSFERENCIA', 'COMPRA', 'CAJERO', 'PAGO_SERV']
    canales = ['WEB', 'APP', 'PRESENCIAL', 'web', 'App ']
    tipos_prod = ['VISA_GOLD', 'CUENTA_CORRIENTE', 'CREDITO_CONSUMO', 'AHORRO']
    start_date = datetime(2025, 1, 1)
    
    # 2. Generar Clientes
    print("--- Escribiendo Clientes ---")
    with open(f"{FOLDER_RAW}/clientes.csv", 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['cliente_id', 'edad', 'ingreso_mensual', 'region'])
        for i in range(1, NUM_CLIENTES + 1):
            ingreso = random.uniform(400000, 6000000) if random.random() > 0.02 else ""
            writer.writerow([i, random.randint(18, 90), ingreso, random.choice(regiones)])

    # 3. Generar Productos (Relación con clientes)
    print("--- Escribiendo Productos ---")
    with open(f"{FOLDER_RAW}/productos.csv", 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['cliente_id', 'tipo_producto'])
        for _ in range(NUM_PRODUCTOS):
            writer.writerow([
                random.randint(1, NUM_CLIENTES), 
                random.choice(tipos_prod)
            ])

    # 4. Generar Transacciones (El archivo de 5GB)
    print(f"--- Escribiendo {TOTAL_TRANSACCIONES} Transacciones (Stream Puro) ---")
    with open(FILE_PATH, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id_transaccion', 'cliente_id', 'fecha', 'monto', 'tipo_tx', 'canal'])
        
        for i in range(TOTAL_TRANSACCIONES):
            # Lógica de ensuciamiento manual (Monto)
            monto_raw = random.uniform(10.0, 2000000.0)
            dice = random.random()
            
            if dice < 0.02: # 2% Nulos reales
                monto = ""
            elif dice < 0.07: # 5% Formato sucio chileno (ej: 1.234,56)
                monto = f"{monto_raw:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            else:
                monto = round(monto_raw, 2)

            # Escribir fila directamente al buffer de disco
            writer.writerow([
                f"TX-{i}",
                random.randint(1, NUM_CLIENTES),
                (start_date + timedelta(days=random.randint(0, 120))).strftime('%Y-%m-%d'),
                monto,
                random.choice(tipos_tx),
                random.choice(canales)
            ])

            if i % 1000000 == 0 and i > 0:
                print(f"Progreso: {i // 1000000}M / {TOTAL_TRANSACCIONES // 1000000}M registros escritos...")

if __name__ == "__main__":
    generate_data()
    print(f"🚀 Proceso terminado con éxito.")
    print(f"📁 Transacciones: {os.path.getsize(FILE_PATH) / (1024**3):.2f} GB")
    print(f"📁 Clientes: {os.path.getsize(f'{FOLDER_RAW}/clientes.csv') / 1024:.2f} KB")
    print(f"📁 Productos: {os.path.getsize(f'{FOLDER_RAW}/productos.csv') / 1024:.2f} KB")  