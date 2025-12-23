from data_processor import get_drive_service, download_parquet_as_df, upload_to_bigquery
from setup_bigquery_views import create_views

FOLDER_ID_DB = '1q7rGJjb3qCTNcyDUYzpn9v4JveLjsk6t'
FILE_NAME_PARQUET = '2025_historico_limpio.parquet'
PROJECT_ID = 'autom-bap-personas'
DATASET_ID = 'tablero_operativo'
TABLE_ID = 'historico_limpio'

def main():
    print("🚀 Iniciando restauración de BigQuery desde Drive...")
    
    # 1. Conectar a Drive
    service = get_drive_service()
    
    # 2. Descargar Parquet
    print(f"⬇️ Descargando {FILE_NAME_PARQUET}...")
    df = download_parquet_as_df(service, FILE_NAME_PARQUET, FOLDER_ID_DB)
    
    if df.empty:
        print("❌ Error: El DataFrame está vacío o no se encontró el archivo.")
        return

    # 3. Subir a BigQuery
    print(f"Num registros: {len(df)}")
    upload_to_bigquery(df, PROJECT_ID, DATASET_ID, TABLE_ID)
    
    # 4. Crear Vistas
    print("\n--- Creando Vistas ---")
    create_views()

if __name__ == "__main__":
    main()
