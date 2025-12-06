import pandas as pd
import numpy as np
import gspread
from google.oauth2 import service_account
from datetime import datetime, timedelta
import io
from googleapiclient.http import MediaIoBaseUpload

# Configuración
KEY_FILE = 'credentials.json'
# ID de tu Google Sheet Maestro (Saca este ID de la URL de tu sheet real)
# Ejemplo: https://docs.google.com/spreadsheets/d/ESTE_ES_EL_ID/edit
SHEET_ID_LOOKER = '1EsLO-upDBrHupXnKYvfLvQWIRaiH0kdEVToreAGmuOg' 

def get_gspread_client():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = service_account.Credentials.from_service_account_file(KEY_FILE, scopes=scopes)
    return gspread.authorize(creds)

def update_sheet(gc, sheet_id, worksheet_name, df):
    """Escribe un DataFrame en una hoja de Google Sheets, creándola si no existe."""
    try:
        sh = gc.open_by_key(sheet_id)
        try:
            ws = sh.worksheet(worksheet_name)
            ws.clear()
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=worksheet_name, rows=100, cols=20)
        
        # Convertir fechas a string para evitar errores de JSON
        df_str = df.astype(str)
        # Reemplazar 'nan' y 'NaT' por vacíos
        df_str = df_str.replace({'nan': '', 'NaT': '', '<NA>': ''})
        
        # Preparar datos con cabecera
        data = [df_str.columns.values.tolist()] + df_str.values.tolist()
        ws.update(range_name='A1', values=data)
        print(f"📊 Hoja '{worksheet_name}' actualizada ({len(df)} filas).")
    except Exception as e:
        print(f"❌ Error actualizando hoja '{worksheet_name}': {str(e)}")

# --- LÓGICA DE NEGOCIO (Tus funciones de los Notebooks) ---

def generar_reporte_comuna2_metricas(df_base):
    """Replica la lógica de interevenciones_comuna2.ipynb"""
    df = df_base[df_base['comuna_calculada'] == 2].copy()
    if df.empty: return pd.DataFrame()

    # Agrupar por semana
    df['Semana'] = df['Fecha Inicio'].dt.to_period('W').apply(lambda r: r.start_time)
    
    # Lógica de Brinda Datos / No Brinda
    df_sem = df.groupby(['Semana', 'brinda_datos']).size().unstack(fill_value=0)
    for col in ['Brinda datos', 'No brinda datos', '']:
        if col not in df_sem.columns: df_sem[col] = 0
    
    df_sem['Total'] = df_sem['Brinda datos'] + df_sem['No brinda datos'] + df_sem['']
    
    # Tipos de carta
    df_tipo = df.groupby(['Semana', 'Tipo Carta']).size().unstack(fill_value=0)
    
    # Traslados específicos
    df_traslado = df[df['categoria_final'] == 'traslado efectivo a cis'].groupby('Semana').size().rename('Traslado efectivo a CIS')
    df_acepta = df[df['categoria_final'] == 'acepta cis pero no hay vacante'].groupby('Semana').size().rename('Acepta CIS pero sin vacante')
    
    # Merge final
    df_final = pd.merge(df_sem, df_tipo, on='Semana', how='left').fillna(0)
    df_final = pd.merge(df_final, df_traslado, on='Semana', how='left').fillna(0)
    df_final = pd.merge(df_final, df_acepta, on='Semana', how='left').fillna(0)
    
    # Cálculos de %
    df_final['Brinda datos %'] = (df_final['Brinda datos'] / df_final['Total'] * 100).round(2)
    df_final['No brinda datos %'] = (df_final['No brinda datos'] / df_final['Total'] * 100).round(2)
    
    return df_final.reset_index().sort_values('Semana')

def generar_reporte_seguimiento_dni(df_base):
    """Replica SeguimientoV2.ipynb (Recurrentes/Nuevos/Migratorios)"""
    df = df_base.copy()
    df['Semana'] = df['Fecha Inicio'].dt.to_period('W-SUN').apply(lambda r: r.start_time)
    df = df.sort_values('Fecha Inicio')
    
    # Deduplicar por semana/DNI (último registro)
    df_sem = df.drop_duplicates(subset=['Semana', 'DNI_Categorizado'], keep='last').copy()
    
    def is_recoleta(x):
        return str(x).replace('.0','').strip() == '2'

    semanas = sorted(df_sem['Semana'].unique())
    dni_last_comuna = {}
    dni_seen = set()
    resultados = []
    
    # Análisis histórico cronológico
    for semana in semanas:
        rows_sem = df_sem[df_sem['Semana'] == semana]
        
        # Solo nos interesa contar lo que pasó en Recoleta esta semana
        rows_recoleta = rows_sem[rows_sem['comuna_calculada'].apply(is_recoleta)]
        
        rec = 0
        mig = 0
        nue = 0
        
        for _, row in rows_recoleta.iterrows():
            dni = row['DNI_Categorizado']
            prior_comuna = dni_last_comuna.get(dni)
            
            if prior_comuna is None and dni not in dni_seen:
                nue += 1
            elif prior_comuna is not None and is_recoleta(prior_comuna):
                rec += 1
            else:
                mig += 1
        
        resultados.append({
            'Semana': semana,
            'Recurrentes': rec,
            'Migratorios': mig,
            'Nuevos': nue,
            'Total': len(rows_recoleta)
        })
        
        # Actualizar estado global
        for _, r in rows_sem.iterrows():
            dni_last_comuna[r['DNI_Categorizado']] = r['comuna_calculada']
            dni_seen.add(r['DNI_Categorizado'])
            
    return pd.DataFrame(resultados)

def generar_reporte_caba_sin_c2(df_base):
    """Replica intervenciones_CABA.ipynb"""
    df = df_base[df_base['comuna_calculada'] != 2].copy()
    if df.empty: return pd.DataFrame()
    
    # (Misma lógica que Comuna 2 pero filtrado != 2)
    df['Semana'] = df['Fecha Inicio'].dt.to_period('W').apply(lambda r: r.start_time)
    
    df_sem = df.groupby(['Semana', 'brinda_datos']).size().unstack(fill_value=0)
    for col in ['Brinda datos', 'No brinda datos', '']:
        if col not in df_sem.columns: df_sem[col] = 0
        
    df_sem['Total'] = df_sem['Brinda datos'] + df_sem['No brinda datos'] + df_sem['']
    
    # Tipos de carta
    df_tipo = df.groupby(['Semana', 'Tipo Carta']).size().unstack(fill_value=0)
    
    # Merge
    df_final = pd.merge(df_sem, df_tipo, on='Semana', how='left').fillna(0)
    
    # Agregar traslado efectivo
    df_tras = df[df['categoria_final'] == 'traslado efectivo a cis'].groupby('Semana').size().rename('Traslados CIS')
    df_final = pd.merge(df_final, df_tras, on='Semana', how='left').fillna(0)

    return df_final.reset_index().sort_values('Semana')

# --- FUNCIÓN PRINCIPAL QUE LLAMA EL MAIN.PY ---

def ejecutar_reportes_looker(df_limpio):
    print("📈 Iniciando generación de reportes para Looker...")
    gc = get_gspread_client()
    
    # Asegurar tipos de datos
    df_limpio['Fecha Inicio'] = pd.to_datetime(df_limpio['Fecha Inicio'])
    
    # 1. Evolución Comuna 2
    df_c2 = generar_reporte_comuna2_metricas(df_limpio)
    update_sheet(gc, SHEET_ID_LOOKER, "Evolucion semanal de intervenciones C2", df_c2)
    
    # 2. Seguimiento DNI Recoleta
    df_dni = generar_reporte_seguimiento_dni(df_limpio)
    update_sheet(gc, SHEET_ID_LOOKER, "EvolucionSemanalDNIRecoleta", df_dni)
    
    # 3. Evolución CABA (Sin C2)
    df_caba = generar_reporte_caba_sin_c2(df_limpio)
    update_sheet(gc, SHEET_ID_LOOKER, "Evolucion semanal de intervenciones sin C2", df_caba)
    
    print("✅ Todos los reportes de Looker han sido actualizados.")