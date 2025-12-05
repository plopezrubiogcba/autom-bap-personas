import functions_framework
import json
import traceback
from data_processor import procesar_datos, download_file_as_bytes, get_drive_service

# ID de la carpeta donde se guardarán/leerán los parquets
FOLDER_ID_OUTPUT = '1JerMocOXjC1pL6PFllj5COzDKm9qr6_W'


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
        procesar_datos(excel_bytes, FOLDER_ID_OUTPUT)

        return f'✅ Procesamiento exitoso para {file_name}', 200

    except Exception as e:
        error_msg = f"🔥 Error Crítico: {str(e)}"
        print(error_msg)
        traceback.print_exc() # Imprime el error completo en los logs de Google Cloud
        return error_msg, 500