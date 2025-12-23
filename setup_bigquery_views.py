import os
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuración
PROJECT_ID = 'autom-bap-personas'
DATASET_ID = 'tablero_operativo'
CREDENTIALS_FILE = 'credentials.json'

def get_bq_client():
    """Obtiene el cliente de BigQuery usando las credenciales locales."""
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(f"❌ No se encontró el archivo {CREDENTIALS_FILE}")
    
    # Scopes necesarios
    SCOPES = [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform'
    ]
    
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE, 
        scopes=SCOPES
    )
    
    return bigquery.Client(credentials=creds, project=PROJECT_ID)

def create_views():
    client = get_bq_client()
    
    # 1. Vista de Intervenciones Enriquecida (KPIs de Contacto)
    sql_intervenciones = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.vista_intervenciones_enriquecida` AS
    SELECT
        *,
        CASE 
            WHEN Estado = 'PENDIENTE' THEN 'Sin cubrir'
            WHEN Resultado IN (
                '12–No se contacta y no se observan pertenencias',
                '11-No se contacta y se observan pertenencias',
                '16-Desestimado (cartas 911 u otras áreas)'
            ) THEN 'No se contacta'
            WHEN Resultado = '15-Sin cubrir' THEN 'Sin cubrir'
            ELSE 'Se contacta'
        END AS Categoria_Contacto
    FROM `{PROJECT_ID}.{DATASET_ID}.historico_limpio`
    """
    
    # 2. Vista de Población Semanal (Logica Nuevo/Recurrente/Migratorio)
    sql_poblacion = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.vista_poblacion_semanal` AS
    WITH BaseSemanal AS (
        -- 1. Obtenemos la PRIMERA ubicación de cada DNI por semana
        SELECT
            DNI_Categorizado AS DNI,
            DATE_TRUNC(DATE(`Fecha Inicio`), WEEK(SUNDAY)) AS Semana,
            -- Tomamos la primera comuna registrada en la semana (orden cronológico)
            ARRAY_AGG(comuna_calculada ORDER BY `Fecha Inicio` ASC LIMIT 1)[OFFSET(0)] AS Comuna_Semanal
        FROM `{PROJECT_ID}.{DATASET_ID}.historico_limpio`
        WHERE DNI_Categorizado NOT IN ('NO BRINDO/NO VISIBLE', 'nan') 
        GROUP BY 1, 2
    ),
    Historial AS (
        -- 2. Calculamos la comuna ANTERIOR usando LAG
        SELECT
            DNI,
            Semana,
            Comuna_Semanal,
            LAG(Comuna_Semanal) OVER (PARTITION BY DNI ORDER BY Semana) AS Comuna_Anterior
        FROM BaseSemanal
    )
    SELECT
        DNI,
        Semana,
        Comuna_Semanal,
        Comuna_Anterior,
        CASE 
            WHEN Comuna_Anterior IS NULL THEN 'Nuevo'
            WHEN Comuna_Anterior = Comuna_Semanal THEN 'Recurrente'
            ELSE 'Migratorio'
        END AS Condicion_Poblacion
    FROM Historial
    """

    print("🚀 Creando vistas en BigQuery...")

    try:
        print("1️⃣ Creando 'vista_intervenciones_enriquecida'...")
        job1 = client.query(sql_intervenciones)
        job1.result() # Esperar resultado
        print("✅ Vista 1 creada exitosamente.")

        print("2️⃣ Creando 'vista_poblacion_semanal'...")
        job2 = client.query(sql_poblacion)
        job2.result() # Esperar resultado
        print("✅ Vista 2 creada exitosamente.")
        
        print("\n🎉 Vistas creadas correctamente. Ya puedes conectarlas a Power BI.")
        
    except Exception as e:
        print(f"\n❌ Error al crear vistas: {e}")

if __name__ == "__main__":
    create_views()
