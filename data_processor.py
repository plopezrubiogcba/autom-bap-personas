import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
import os
import io
import re
import gc
import unicodedata
import unidecode
from rapidfuzz import process, fuzz
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# ==========================================
# CONFIGURACIÓN Y UTILIDADES DE GOOGLE (DRIVE & BIGQUERY)
# ==========================================

# Scopes actualizados para incluir BigQuery
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/bigquery'
]

def get_credentials():
    """Obtiene las credenciales para usar en Drive y BigQuery."""
    # Prioridad: Variable de entorno (GitHub Actions) > Archivo local fixed
    creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'credentials.json')
    
    # Crea las credenciales con los scopes necesarios
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return creds

def get_drive_service():
    """Autentica y devuelve el servicio de Drive usando las credenciales compartidas."""
    creds = get_credentials()
    return build('drive', 'v3', credentials=creds)

def download_file_as_bytes(service, file_id):
    """Descarga un archivo cualquiera de Drive y devuelve sus bytes."""
    print(f"⬇️ Descargando archivo ID: {file_id}...")
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read()

def download_parquet_as_df(service, file_name, folder_id):
    """Busca y descarga un parquet de Drive a un DataFrame."""
    print(f"⬇️ Buscando '{file_name}' en Drive...")
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    
    if not files:
        print(f"⚠️ Archivo {file_name} no encontrado. Se creará uno nuevo.")
        return pd.DataFrame() 

    file_id = files[0]['id']
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    
    fh.seek(0)
    return pd.read_parquet(fh)

def upload_df_as_parquet(service, df, file_name, folder_id):
    """Sube un DataFrame como parquet a Drive (sobreescribe o crea)."""
    print(f"⬆️ Subiendo '{file_name}' a Drive...")
    fh = io.BytesIO()
    df.to_parquet(fh, index=False, engine='pyarrow', compression='snappy')
    fh.seek(0)
    
    media = MediaIoBaseUpload(fh, mimetype='application/octet-stream', resumable=True)
    
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])

    if files:
        file_id = files[0]['id']
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"✅ {file_name} actualizado en Drive.")
    else:
        file_metadata = {'name': file_name, 'parents': [folder_id]}
        service.files().create(body=file_metadata, media_body=media).execute()
        print(f"✅ {file_name} creado en Drive.")

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    """Sube el DataFrame a BigQuery reemplazando la tabla existente."""
    destination_table = f"{dataset_id}.{table_id}"
    print(f"⬆️ Iniciando carga a BigQuery: {destination_table} en proyecto {project_id}...")
    
    try:
        creds = get_credentials()
        # if_exists='replace' es CRÍTICO para mantener la consistencia de tu lógica de históricos
        df.to_gbq(
            destination_table, 
            project_id=project_id, 
            if_exists='replace',
            credentials=creds,
            progress_bar=False
        )
        print("✅ Carga a BigQuery exitosa.")
    except Exception as e:
        print(f"❌ Error subiendo a BigQuery: {e}")

# ==========================================
# FUNCIONES DE LIMPIEZA (TU LÓGICA)
# ==========================================

def limpiar_texto(nombre):
    if pd.isna(nombre): return None
    nombre = str(nombre).upper()
    nombre = ''.join(c for c in unicodedata.normalize('NFD', nombre) if unicodedata.category(c) != 'Mn')
    nombre = re.sub(r'[-.,]', ' ', nombre)
    nombre = re.sub(r'[^A-Z ]', '', nombre)
    nombre = re.sub(r'\s+', ' ', nombre).strip()
    return nombre if nombre else None

def limpiar_texto_cierre(s):
    if pd.isna(s): return ""
    s = str(s).lower().strip()
    s = unidecode.unidecode(s)
    s = s.replace("_", " ").replace("-", " ")
    s = re.sub(r"[^a-z0-9áéíóúüñ\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

# --- PATRONES REGEX PARA DNI ---
PATRON_EXTRANJERO = re.compile(r'(extranjero|paraguay|venezol|colombian|uruguay|brasil|chilen|peruano|mexican|español|dominican|dominicana|pasaporte|c\.?d\.?[ie]:?|rnm|cedula|ciudadano\s+extranjero)', flags=re.IGNORECASE)
PATRON_NO_BRINDO_GENERICOS = re.compile(r'(no\s*brind|no\s*bri[nm]d|no\s*aporta|no\s*aporto|no\s*indica|no\s*sabe|no\s*recuerda|no\s*recuerd|no\s*tiene|nunca\s*tuvo|sin\s*dni|sin\s*dato|sin\s*inform|ilegible|invisible|no\s*visible|exhib|no\s*lo\s*sabe|menor\s*de\s*edad)', flags=re.IGNORECASE)
PATRON_NO_BRINDO_SIMBOLOS = re.compile(r'^[xX\*\-\.]+$', flags=re.IGNORECASE)
PATRON_LETRAS_CORTAS = re.compile(r'^[A-Za-z]{1,3}$')
PATRON_SOLO_LETRAS = re.compile(r'^[A-Za-z]+$')

def limpiar_y_categorizar_dni_v3(df, columna_original, columna_salida=None, crear_motivo=True):
    if columna_salida is None: columna_salida = columna_original
    motivo_col = f"{columna_salida}_motivo" if crear_motivo else None

    def procesar_valor(v):
        if pd.isna(v): return ('NO BRINDO/NO VISIBLE', 'nan')
        s = str(v).strip()
        if s == '': return ('NO BRINDO/NO VISIBLE', 'empty')
        s_lower = s.lower()
        if PATRON_NO_BRINDO_GENERICOS.search(s_lower): return ('NO BRINDO/NO VISIBLE', 'patron_no_brindo_genericos')
        if PATRON_NO_BRINDO_SIMBOLOS.match(s) or PATRON_LETRAS_CORTAS.match(s): return ('NO BRINDO/NO VISIBLE', 'simbolos_o_letras_cortas')
        if PATRON_SOLO_LETRAS.match(s) and len(set(s_lower)) <= 2: return ('NO BRINDO/NO VISIBLE', 'solo_letras_repetidas')
        if PATRON_EXTRANJERO.search(s_lower): return ('CONTACTO EXTRANJERO', 'patron_extranjero')
        digits = re.sub(r'\D', '', s)
        if 6 <= len(digits) <= 10: return (int(digits), 'dni_valido')
        if len(digits) < 6 or re.search(r'[A-Za-z]', s_lower): return ('NO BRINDO/NO VISIBLE', 'texto_o_corto')
        return ('NO BRINDO/NO VISIBLE', 'resto_no_brindo')

    print(f"⚙️ Procesando DNI: {columna_original}...")
    resultados = df[columna_original].apply(procesar_valor)
    df[columna_salida] = resultados.apply(lambda x: x[0])
    if crear_motivo: df[motivo_col] = resultados.apply(lambda x: x[1])
    return df

# --- CATEGORIZACIÓN ---
CATEGORIAS_BRINDA_DATOS = ["traslado efectivo a cis", "acepta cis pero no hay vacante", "se activa protocolo de salud mental", "derivacion a same", "traslado/acompanamiento a otros efectores", "mendicidad (menores de edad)"]
CATEGORIAS_NO_BRINDA_DATOS = ["se realiza entrevista", "rechaza entrevista y se retira del lugar", "imposibilidad de abordaje por consumo", "rechaza entrevista y se queda en el lugar", "derivacion a espacio publico", "no se encuentra en situacion de calle"]
CATEGORIAS_NO_CONTACTA = ["no se contacta y se observan pertenencias", "no se contacta y no se observan pertenencias", "sin cubrir", "desestimado (cartas 911 u otras areas)"]
CATEGORIAS_TODAS = CATEGORIAS_BRINDA_DATOS + CATEGORIAS_NO_BRINDA_DATOS + CATEGORIAS_NO_CONTACTA

PATRONES_EXACTOS = {
    "17 dipa derivacion a cis": "traslado efectivo a cis",
    "01 positivo traslado a cis hogar 08 positivo derivacion a sas cud cp identidad etc": "traslado efectivo a cis",
    '10 se contacta pero rechaza pp por desconocimiento voluntad etc':'se realiza entrevisa',
    '21 asesoramiento sobre programas': 'se realiza entrevista',
    '16 dipa entrega de insumos servicios 21 asesoramiento sobre programas' : 'traslado/acompanamiento a otros efectores',
    '16 dipa entrega de insumos servicios 7 positivo entrega de insumos' : 'traslado/acompanamiento a otros efectores',
    '21 asesoramiento sobre programas 16 dipa entrega de insumos servicios':'traslado/acompanamiento a otros efectores',
    '7 positivo entrega de insumos 16 dipa entrega de insumos servicios':'traslado/acompanamiento a otros efectores',
    '7 positivo entrega de insumos 21 asesoramiento sobre programas':'traslado/acompanamiento a otros efectores',
    '21 asesoramiento sobre programas 7 positivo entrega de insumos':'traslado/acompanamiento a otros efectores',
    '16 dipa entrega de insumos servicios 21 asesoramiento sobre programas 7 positivo entrega de insumos':'traslado/acompanamiento a otros efectores',
    '11 se contacta pero rechaza pp por disconformidad egresado' : 'rechaza entrevista y se retira del lugar',
    '10 se contacta pero rechaza pp por desconocimiento voluntad etc 11 se contacta pero rechaza pp por disconformidad egresado':'rechaza entrevista y se retira del lugar',
    '9 se contacta pero rechaza entrevista 21 asesoramiento sobre programas':'rechaza entrevista y se retira del lugar',
    '9 se contacta pero rechaza entrevista 24 persona abandona el lugar por intervencion ep policia':'rechaza entrevista y se retira del lugar',
    '21 asesoramiento sobre programas 10 se contacta pero rechaza pp por desconocimiento voluntad etc':'rechaza entrevista y se retira del lugar',
    '10 se contacta pero rechaza pp por desconocimiento voluntad etc 21 asesoramiento sobre programas':'rechaza entrevista y se retira del lugar',
    '21 asesoramiento sobre programas 9 se contacta pero rechaza entrevista':'rechaza entrevista y se retira del lugar',
    '10 se contacta pero rechaza pp por desconocimiento voluntad etc 9 se contacta pero rechaza entrevista':'rechaza entrevista y se retira del lugar',
    'asesoramiento sobre programas rechan entrevista se quedan en el lugar':'rechaza entrevista y se queda en el lugar',
    '10 se contacta pero rechaza pp por desconocimiento voluntad etc': 'rechaza entrevista y se retira del lugar',
}

PATRONES_PERSONALIZADOS = {
    "derivacion a cis": "traslado efectivo a cis",
    "traslado a cis": "traslado efectivo a cis",
    "acepta cis": "acepta cis pero no hay vacante",
    "protocolo de salud mental": "se activa protocolo de salud mental",
    " same": "derivacion a same",
    "otros efectores": "traslado/acompanamiento a otros efectores",
    "mendicidad": "mendicidad (menores de edad)",
    "se realiza entrevista": "se realiza entrevista",
    "rechaza entrevista y se retira": "rechaza entrevista y se retira del lugar",
    "rechaza entrevista y se queda": "rechaza entrevista y se queda en el lugar",
    "imposibilidad de abordaje": "imposibilidad de abordaje por consumo",
    "espacio publico": "derivacion a espacio publico",
    "no se encuentra en situacion de calle": "no se encuentra en situacion de calle",
    "sin cubrir": "sin cubrir",
    "desestimado": "desestimado (cartas 911 u otras areas)"
}

def mapear_categoria_con_reglas(texto):
    if texto in PATRONES_EXACTOS: return PATRONES_EXACTOS[texto]
    for patron, categoria in PATRONES_PERSONALIZADOS.items():
        if patron in texto: return categoria
    
    # Fuzzy match
    mejor_match, score, _ = process.extractOne(texto, CATEGORIAS_TODAS, scorer=fuzz.WRatio)
    return mejor_match if score >= 80 else "sin_match"

def obtener_niveles(cat):
    if cat in CATEGORIAS_BRINDA_DATOS: return "Contacta", "Brinda datos"
    elif cat in CATEGORIAS_NO_BRINDA_DATOS: return "Contacta", "No brinda datos"
    elif cat in CATEGORIAS_NO_CONTACTA: return "No se contacta", ""
    else: return "Derivaciones/seguimientos", ""

# ==========================================
# LÓGICA PRINCIPAL DEL PROCESO
# ==========================================

def procesar_datos(excel_content_bytes, folder_id):
    service = get_drive_service()
    
    # ---------------------------------------------------------
    # FASE 1: ACTUALIZACIÓN DEL CRUDO (APPEND)
    # ---------------------------------------------------------
    print("🚀 Iniciando Fase 1: Actualización del Crudo...")
    
    df_nuevo = pd.read_excel(io.BytesIO(excel_content_bytes), skiprows=1)
    nombre_crudo = "2025_historico_v2.parquet"
    df_hist = download_parquet_as_df(service, nombre_crudo, folder_id)

    # Normalización de Fechas
    col_fecha = 'Fecha Inicio'
    for df in [df_hist, df_nuevo]:
        if not df.empty:
            for col in [col_fecha, 'Fecha Fin', 'Recurso Fecha Liberado', 'Recurso Fecha asignacion', 'Recurso Arribo']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

    # Normalización Lat/Lon
    for col in ['Latitud', 'Longitud']:
        df_nuevo[col] = df_nuevo[col].astype(str).str.replace(',', '.', regex=False).astype(float)

    # Filtrado (Solo nuevos)
    if not df_hist.empty:
        fecha_corte = df_hist[col_fecha].max()
        print(f"📅 Fecha de corte detectada: {fecha_corte}")
        df_filtrado_nuevo = df_nuevo[df_nuevo[col_fecha] > fecha_corte]
    else:
        df_filtrado_nuevo = df_nuevo
        print("📅 No hay histórico previo. Se procesará todo el Excel.")

    # Concatenar y guardar
    if not df_filtrado_nuevo.empty:
        df_actualizado = pd.concat([df_hist, df_filtrado_nuevo], ignore_index=True)
        upload_df_as_parquet(service, df_actualizado, nombre_crudo, folder_id)
        print(f"✅ Se agregaron {len(df_filtrado_nuevo)} registros nuevos al crudo.")
    else:
        print("⚠️ No hay registros nuevos para agregar. Usando histórico existente.")
        df_actualizado = df_hist

    # Limpieza de memoria
    del df_hist, df_nuevo, df_filtrado_nuevo
    gc.collect()

    # ---------------------------------------------------------
    # FASE 2: ENRIQUECIMIENTO GEOGRÁFICO (COMUNAS)
    # ---------------------------------------------------------
    print("🌍 Iniciando Fase 2: Spatial Join con Comunas...")
    
    # Ruta dinámica al shapefile (assets dentro del src)
    ruta_shp = os.path.join(os.path.dirname(__file__), 'assets', 'comunas', 'comunas.shp')
    
    if not os.path.exists(ruta_shp):
        raise FileNotFoundError(f"❌ No encuentro el shapefile en: {ruta_shp}")

    gdf_comunas = gpd.read_file(ruta_shp)
    
    # Convertir DataFrame a GeoDataFrame
    df_actualizado['geometry'] = df_actualizado.apply(lambda row: Point(row['Longitud'], row['Latitud']), axis=1)
    puntos_gdf = gpd.GeoDataFrame(df_actualizado, crs="EPSG:4326")

    # Spatial Join
    print("📍 Ejecutando cruce espacial...")
    if puntos_gdf.crs != gdf_comunas.crs:
        gdf_comunas = gdf_comunas.to_crs(puntos_gdf.crs)

    resultado_sjoin = gpd.sjoin(puntos_gdf, gdf_comunas[['comuna', 'geometry']], how="left", predicate="within")
    
    df_actualizado['comuna_calculada'] = resultado_sjoin['comuna']
    df_actualizado = df_actualizado.drop(columns=['geometry'])
    
    del puntos_gdf, resultado_sjoin, gdf_comunas
    gc.collect()

    # ---------------------------------------------------------
    # FASE 3: LIMPIEZA Y CATEGORIZACIÓN (CLEAN)
    # ---------------------------------------------------------
    print("🧹 Iniciando Fase 3: Limpieza y Categorización...")
    
    # 1. Limpieza DNI
    df_actualizado = limpiar_y_categorizar_dni_v3(df_actualizado, 'Persona DNI', columna_salida='DNI_Categorizado')
    df_actualizado['DNI_Categorizado'] = df_actualizado['DNI_Categorizado'].astype(str)

    # 2. Limpieza Nombres
    df_actualizado['Persona Nombre'] = df_actualizado['Persona Nombre'].apply(limpiar_texto)
    df_actualizado['Persona Apellido'] = df_actualizado['Persona Apellido'].apply(limpiar_texto)

    # 3. Eliminar Agencias
    agencias_a_eliminar = ['DIPA I COMBATE', 'MAPA DE RIESGO - SEGUIMIENTO', 'MAPA DE REISGO - SEGUIMIENTO','DIPA II ZABALA', 'AREA OPERATIVA', 'SALUD MENTAL']
    df_actualizado = df_actualizado[~df_actualizado['Agencia'].isin(agencias_a_eliminar)]

    # 4. Categorización
    valores_vacios = ['', ' ', '-', 'N/A', '(Vacio)', 'SIN DATO', 'nan', 'NAN', None]
    df_actualizado['Cierre Supervisor'] = df_actualizado['Cierre Supervisor'].replace(valores_vacios, np.nan)
    df_actualizado['Resultado'] = df_actualizado['Resultado'].replace(valores_vacios, np.nan)
    
    df_actualizado['cierre_texto'] = np.where(pd.isna(df_actualizado['Cierre Supervisor']), df_actualizado['Resultado'], df_actualizado['Cierre Supervisor'])
    df_actualizado['texto_limpio'] = df_actualizado['cierre_texto'].apply(limpiar_texto_cierre)
    
    print("🧠 Aplicando reglas y Fuzzy Match...")
    df_actualizado['categoria_final'] = df_actualizado['texto_limpio'].apply(mapear_categoria_con_reglas)

    # 5. Niveles
    niveles = df_actualizado['categoria_final'].apply(lambda x: obtener_niveles(x))
    df_actualizado['contacto'] = niveles.apply(lambda x: x[0])
    df_actualizado['brinda_datos'] = niveles.apply(lambda x: x[1])

    # === INICIO BLOQUE EVOLUCIÓN DNI (Optimización Power BI) ===
    print("🧠 Calculando evolución histórica de DNI (Python)...")
    
    # 1. Aseguramos el orden cronológico estricto por Persona y Fecha
    df_actualizado.sort_values(by=['Persona DNI', 'Fecha Inicio'], ascending=[True, True], inplace=True)

    # 2. Vectorización: 'shift' mueve la columna una posición abajo dentro de cada grupo DNI
    # Esto nos permite comparar la fila actual con la anterior sin usar bucles lentos.
    df_actualizado['comuna_prev'] = df_actualizado.groupby('Persona DNI')['comuna_calculada'].shift(1)

    # 3. Clasificación Vectorizada (Numpy Select)
    conditions = [
        df_actualizado['comuna_prev'].isna(),  # Si no hay registro previo -> Nuevo
        df_actualizado['comuna_prev'] == df_actualizado['comuna_calculada'] # Misma comuna -> Recurrente
    ]
    choices = ['Nuevos', 'Recurrentes']
    
    # El default es 'Migratorios' (Si hay previa y es distinta a la actual)
    df_actualizado['Tipo_Evolucion'] = np.select(conditions, choices, default='Migratorios')
    
    # Limpieza de columna auxiliar
    df_actualizado.drop(columns=['comuna_prev'], inplace=True)
    # === FIN BLOQUE EVOLUCIÓN DNI ===

    # ---------------------------------------------------------
    # GUARDADO FINAL (DRIVE Y BIGQUERY)
    # ---------------------------------------------------------
    nombre_limpio = "2025_historico_limpio.parquet"
    
    # 1. Subida original a Drive (Mantenemos tu lógica existente)
    upload_df_as_parquet(service, df_actualizado, nombre_limpio, folder_id)
    
    # 2. Subida a BigQuery
    PROJECT_ID = 'autom-bap-personas'   # Tu ID de proyecto
    DATASET_ID = 'tablero_operativo'    # Tu Dataset
    TABLE_ID = 'historico_limpio'       # Tu Tabla
    
    upload_to_bigquery(df_actualizado, PROJECT_ID, DATASET_ID, TABLE_ID)
    
    print(f"🎉 Proceso Terminado. Limpio actualizado al día {df_actualizado[col_fecha].max()}")
    
    return df_actualizado