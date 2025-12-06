import functions_framework
import json
import traceback
import pandas as pd
from data_processor import procesar_datos, download_file_as_bytes, get_drive_service
from looker_reporter import ejecutar_reportes_looker

# --- CONFIGURACIÓN DE CARPETAS ---
# Carpeta 02: Donde viven los parquets (Base de Datos)
FOLDER_ID_DB = '1q7rGJjb3qCTNcyDUYzpn9v4JveLjsk6t' 

@functions_framework.http
def entry_point(request):
    """
    Función HTTP disparada por Apps Script cuando llega un mail.
    Espera JSON: {"file_id": "...", "file_name": "..."}
    """
    try:
        request_json = request.get_json(silent=True)
        
        # Validación básica
        if not request_json or 'file_id' not in request_json:
            return '❌ Error: Falta el parametro file_id', 400

        file_id = request_json['file_id']
        file_name = request_json.get('file_name', 'Archivo desconocido')
        
        print(f"🔔 Solicitud recibida. Procesando: {file_name} ({file_id})")

        # 1. Conectar a Drive
        service = get_drive_service()

        # 2. Descargar el Excel que llegó por mail
        excel_bytes = download_file_as_bytes(service, file_id)

        # 3. Ejecutar la maquinaria ETL
        # CORRECCIÓN 1: Usamos FOLDER_ID_DB (no OUTPUT) y capturamos el resultado en una variable
        df_limpio = procesar_datos(excel_bytes, FOLDER_ID_DB)

        # 4. Actualizar Reportes de Looker
        # CORRECCIÓN 2: Llamamos a la función del reportero si hay datos
        if df_limpio is not None and not df_limpio.empty:
            print("🚀 Iniciando actualización de Looker...")
            ejecutar_reportes_looker(df_limpio)
        else:
            print("⚠️ El procesamiento no devolvió datos o el dataframe está vacío.")

        return f'✅ Procesamiento y Reportes exitosos para {file_name}', 200

    except Exception as e:
        error_msg = f"🔥 Error Crítico: {str(e)}"
        print(error_msg)
        traceback.print_exc() # Imprime el error completo en los logs de Google Cloud
        return error_msg, 500