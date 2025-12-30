from google.cloud import bigquery
from google.oauth2 import service_account
import os

PROJECT_ID = 'autom-bap-personas'
DATASET_ID = 'tablero_operativo'
CREDENTIALS_FILE = 'credentials.json'

def list_tables():
    creds = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
    client = bigquery.Client(credentials=creds, project=PROJECT_ID)

    try:
        tables = client.list_tables(f"{PROJECT_ID}.{DATASET_ID}")
        print(f"📂 Tablas en {DATASET_ID}:")
        found = []
        for table in tables:
            t_type = table.table_type
            print(f"- {table.table_id} ({t_type})")
            found.append(table.table_id)
            
        required = ['historico_limpio', 'vista_intervenciones_enriquecida', 'vista_poblacion_semanal']
        missing = [t for t in required if t not in found]
        
        if not missing:
            print("\n✅ TODO OK: Tabla y Vistas encontradas.")
        else:
            print(f"\n❌ FALTAN: {missing}")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    list_tables()
