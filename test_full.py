# src/test_full.py
import os
from data_processor import procesar_datos

# Configuración
FOLDER_ID_DRIVE = '1JerMocOXjC1pL6PFllj5COzDKm9qr6_W'
RUTA_EXCEL_LOCAL = r"C:\Users\patol\Downloads\20251205084649.xls" # Pon la ruta a un excel que tengas

def test():
    print("🧪 Iniciando prueba de integración...")
    
    # Simular lectura del archivo (como si viniera del mail)
    with open(RUTA_EXCEL_LOCAL, 'rb') as f:
        contenido_excel = f.read()
    
    # Ejecutar proceso
    procesar_datos(contenido_excel, FOLDER_ID_DRIVE)

if __name__ == "__main__":
    test()