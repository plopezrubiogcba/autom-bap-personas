import os
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Configuración
SCOPES = ['https://www.googleapis.com/auth/drive']
KEY_FILE = 'credentials.json'  # Asegúrate de que esté en la raíz
FOLDER_NAME = 'BAP_Data'       # <--- CAMBIA ESTO POR TU CARPETA REAL

def test_connection():
    try:
        # Autenticación
        creds = service_account.Credentials.from_service_account_file(
            KEY_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)

        # 1. Buscar el ID de la carpeta
        query = f"mimeType='application/vnd.google-apps.folder' and name='{FOLDER_NAME}' and trashed=false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])

        if not items:
            print(f"❌ No encontré la carpeta '{FOLDER_NAME}'. ¿La compartiste con el email del robot?")
            return

        folder_id = items[0]['id']
        print(f"✅ Carpeta '{FOLDER_NAME}' encontrada. ID: {folder_id}")

        # 2. Intentar crear un archivo de prueba
        file_metadata = {
            'name': 'hola_robot.txt',
            'parents': [folder_id]
        }
        # Crear archivo vacío (mimeType text/plain)
        service.files().create(body=file_metadata, media_body=None).execute()
        print("✅ Archivo 'hola_robot.txt' creado exitosamente dentro de la carpeta.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == '__main__':
    test_connection()