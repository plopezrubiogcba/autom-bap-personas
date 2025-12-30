import os
import sys
# Asegúrate de importar las funciones correctamente
from data_processor import get_drive_service, download_file_as_bytes, procesar_datos

# --- CONFIGURACIÓN DE CARPETAS (IDs ACTUALIZADOS) ---

# 1. CARPETA DE ENTRADA (01_insumos): Donde están tus .xls semanales
INPUT_FOLDER_ID = '14kWGqDj-Q_TOl2-F9FqocI9H_SeL_6Ba' 

# 2. CARPETA DE BASE DE DATOS (02_base_datos): Donde vive el parquet y el histórico
DB_FOLDER_ID = '1q7rGJjb3qCTNcyDUYzpn9v4JveLjsk6t'

def main():
    print("🏁 Iniciando proceso de captura...")
    
    # 1. Autenticación
    try:
        service = get_drive_service()
    except Exception as e:
        print(f"❌ Error de autenticación: {e}")
        return

    # 2. Buscar el Excel más reciente en la CARPETA DE INSUMOS
    print(f"🔎 Buscando reportes (.xls / .xlsx) en: {INPUT_FOLDER_ID}...")
    
    # CONSULTA CORREGIDA: Busca tanto formato nuevo (.xlsx) como viejo (.xls)
    query = (
        f"'{INPUT_FOLDER_ID}' in parents "
        "and (mimeType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' "
        "or mimeType = 'application/vnd.ms-excel') "
        "and trashed = false"
    )
    
    results = service.files().list(
        q=query, 
        orderBy='createdTime desc', 
        pageSize=1, 
        fields="files(id, name, createdTime)"
    ).execute()
    
    files = results.get('files', [])

    if not files:
        print("⚠️ No se encontró ningún archivo Excel en '01_insumos'.")
        print("   -> Verifica que los archivos no estén en la papelera.")
        return

    archivo_excel = files[0]
    print(f"📄 Archivo detectado: {archivo_excel['name']} (ID: {archivo_excel['id']})")

    # 3. Descargar el archivo a memoria
    try:
        excel_bytes = download_file_as_bytes(service, archivo_excel['id'])
        print("✅ Descarga del Excel completada.")
    except Exception as e:
        print(f"❌ Error descargando archivo: {e}")
        return

    # 4. Enviar al Procesador (ETL + BigQuery)
    # IMPORTANTE: Pasamos los datos del Excel Y el ID de la carpeta DB para guardar el parquet
    try:
        procesar_datos(excel_bytes, DB_FOLDER_ID)
        print("🚀 Ciclo completo finalizado. BigQuery y Drive actualizados.")
    except Exception as e:
        print(f"❌ Error durante el procesamiento: {e}")
        # Hacemos raise para que GitHub Actions marque error si falla
        raise e

if __name__ == '__main__':
    main()