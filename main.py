import os
import sys
# Importamos las funciones necesarias desde tu otro script data_processor.py
from data_processor import get_drive_service, download_file_as_bytes, procesar_datos

# --- CONFIGURACIÓN ---
# ID de la carpeta en Drive donde se buscan los archivos Excel de entrada.
# IMPORTANTE: Asegúrate de que este ID sea el correcto donde subes los Excels.
# Si es la misma carpeta que usaste en dashboard_generator, usa ese ID.
INPUT_FOLDER_ID = '1q7rGJjb3qCTNcyDUYzpn9v4JveLjsk6t' 

def main():
    print("🏁 Iniciando proceso de captura...")
    
    # 1. Autenticación
    try:
        service = get_drive_service()
    except Exception as e:
        print(f"❌ Error de autenticación: {e}")
        return

    # 2. Buscar el Excel más reciente en la carpeta de entrada
    # Filtramos por archivos Excel y que no estén en la papelera
    query = f"'{INPUT_FOLDER_ID}' in parents and mimeType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed = false"
    
    # Ordenamos por fecha de creación descendente para tomar el último
    results = service.files().list(
        q=query, 
        orderBy='createdTime desc', 
        pageSize=1, 
        fields="files(id, name, createdTime)"
    ).execute()
    
    files = results.get('files', [])

    if not files:
        print("⚠️ No se encontraron archivos Excel en la carpeta especificada.")
        return

    archivo_excel = files[0]
    file_id = archivo_excel['id']
    file_name = archivo_excel['name']
    
    print(f"📄 Archivo detectado: {file_name} (ID: {file_id})")

    # 3. Descargar el archivo a memoria (bytes)
    try:
        excel_bytes = download_file_as_bytes(service, file_id)
        print("✅ Descarga completada.")
    except Exception as e:
        print(f"❌ Error descargando archivo: {e}")
        return

    # 4. Enviar al Procesador (ETL + BigQuery)
    # Esto limpiará los datos, creará el parquet y actualizará BigQuery
    try:
        procesar_datos(excel_bytes, INPUT_FOLDER_ID)
        print("🚀 Ciclo completo finalizado con éxito. BigQuery actualizado.")
    except Exception as e:
        print(f"❌ Error durante el procesamiento: {e}")
        # Hacemos raise para que si esto corre en GitHub Actions, marque error rojo
        raise e

# Este es el punto de entrada que le faltaba o estaba mal definido
if __name__ == '__main__':
    main()