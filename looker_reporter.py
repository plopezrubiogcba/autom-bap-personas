import pandas as pd
import numpy as np
import gspread
from google.oauth2 import service_account
from datetime import datetime

# Configuración
KEY_FILE = 'credentials.json'
SHEET_ID_LOOKER = '1EsLO-upDBrHupXnKYvfLvQWIRaiH0kdEVToreAGmuOg' 

def get_gspread_client():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = service_account.Credentials.from_service_account_file(KEY_FILE, scopes=scopes)
    return gspread.authorize(creds)

def update_sheet(gc, sheet_id, worksheet_name, df):
    """Escribe un DataFrame en una hoja de Google Sheets, manejando errores de nulos."""
    try:
        sh = gc.open_by_key(sheet_id)
        try:
            ws = sh.worksheet(worksheet_name)
            ws.clear()
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=worksheet_name, rows=1000, cols=20)
        
        # 1. Convertir todo a string
        df_str = df.astype(str)
        # 2. Limpieza de nulos
        df_str = df_str.replace({'nan': '', 'NaT': '', 'None': '', '<NA>': '', 'Na': ''})
        
        # 3. Preparar datos
        data = [df_str.columns.values.tolist()] + df_str.values.tolist()
        
        ws.update(range_name='A1', values=data)
        print(f"📊 Hoja '{worksheet_name}' actualizada ({len(df)} filas).")
        
    except Exception as e:
        print(f"❌ Error actualizando hoja '{worksheet_name}': {type(e).__name__} - {str(e)}")

# ============================================================
# LÓGICA DE NEGOCIO
# ============================================================

def clasificar_contacto(row):
    """Clasifica el contacto según Resultado y Estado."""
    no_contacta = [
        '12–No se contacta y no se observan pertenencias',
        '11-No se contacta y se observan pertenencias',
        '16-Desestimado (cartas 911 u otras áreas)'
    ]
    
    if row.get('Estado') == 'PENDIENTE':
        return 'Sin cubrir'
    
    resultado = str(row.get('Resultado', ''))
    if resultado in no_contacta:
        return 'No se contacta'
    elif resultado == '15-Sin cubrir':
        return 'Sin cubrir'
    else:
        return 'Se contacta'

def combinar(pct, abs_):
    """Formato visual: 50% (10)"""
    pct = pct.fillna(0)
    abs_ = abs_.fillna(0)
    return pct.astype(int).astype(str) + '% (' + abs_.astype(int).astype(str) + ')'

def procesar_formato_largo_ultimas_8(df_base, es_comuna_2):
    """
    Procesa los datos, filtra las últimas 8 semanas y los transforma 
    a formato vertical para Tabla Dinámica de Looker.
    """
    # 1. Filtrar Comuna
    if es_comuna_2:
        df = df_base[df_base['comuna_calculada'] == 2].copy()
        tag_comuna = 'Comuna 2'
    else:
        df = df_base[df_base['comuna_calculada'] != 2].copy()
        tag_comuna = 'Resto CABA'

    if df.empty: return pd.DataFrame()

    # 2. Preparar Datos
    df['Semana'] = df['Fecha Inicio'].dt.to_period('W-SUN').dt.start_time
    df['Categoria_contacto'] = df.apply(clasificar_contacto, axis=1)

    # 3. Calcular Métricas Semanales
    
    # A. Totales
    total_sem = df.groupby('Semana').size()
    
    # B. Derivaciones CIS
    df_cis = df[df['categoria_final'] == 'traslado efectivo a cis']
    cis_sem = df_cis.groupby('Semana').size()
    
    # C. Automáticas (108)
    df_auto = df[df['Tipo Carta'] == 'AUTOMATICA']
    auto_sem = df_auto.groupby('Semana').size()
    
    # D. Desglose Contactos (Matriz)
    conteo = df_auto.groupby(['Semana', 'Categoria_contacto']).size().unstack(fill_value=0)
    for c in ['Se contacta', 'No se contacta', 'Sin cubrir']:
        if c not in conteo.columns: conteo[c] = 0
            
    # Porcentajes
    totales_auto = conteo.sum(axis=1).replace(0, 1)
    pct = (conteo.div(totales_auto, axis=0) * 100).round(0)

    # ------------------------------------------------------------
    # 4. FILTRAR ÚLTIMAS 8 SEMANAS (CRÍTICO)
    # ------------------------------------------------------------
    # Obtenemos el índice de todas las semanas disponibles
    todas_semanas = total_sem.index.union(cis_sem.index).union(auto_sem.index).sort_values(ascending=False)
    
    # Nos quedamos solo con las 8 más recientes
    ultimas_8 = todas_semanas[:8]
    
    # Reindexamos todas las series para quedarnos solo con esos datos
    total_sem = total_sem.reindex(ultimas_8, fill_value=0)
    cis_sem = cis_sem.reindex(ultimas_8, fill_value=0)
    auto_sem = auto_sem.reindex(ultimas_8, fill_value=0)
    conteo = conteo.reindex(ultimas_8, fill_value=0)
    pct = pct.reindex(ultimas_8, fill_value=0)

    # ------------------------------------------------------------
    # 5. CONSTRUCCIÓN DEL FORMATO LARGO (Indicador | Valor | Orden)
    # ------------------------------------------------------------
    metricas = []

    def add_metric(nombre, serie, orden):
        d = serie.to_frame(name='Valor')
        d['Indicador'] = nombre
        d['Orden'] = orden # Para ordenar las filas en Looker
        metricas.append(d)

    # Agregamos las métricas en el orden que quieres verlas
    add_metric('Intervenciones totales', total_sem, 1)
    add_metric('Derivaciones CIS', cis_sem, 2)
    add_metric('Llamados 108', auto_sem, 3)
    
    # Agregamos las combinadas
    # 4: % Se contacta, 5: % No se contacta, 6: % Sin cubrir
    orden_dict = {'Se contacta': 4, 'No se contacta': 5, 'Sin cubrir': 6}
    
    for cat, orden in orden_dict.items():
        serie_combinada = combinar(pct[cat], conteo[cat])
        add_metric(f'% {cat}', serie_combinada, orden)

    # Unir todo
    df_long = pd.concat(metricas)
    df_long = df_long.reset_index() # La fecha pasa a columna 'Semana'
    
    # Formatear
    df_long['Semana'] = df_long['Semana'].dt.strftime('%Y-%m-%d')
    df_long['Comuna'] = tag_comuna # Agregamos la columna para el filtro

    # Columnas finales: [Semana, Indicador, Valor, Orden, Comuna]
    return df_long

# --- FUNCIÓN PRINCIPAL ---

def ejecutar_reportes_looker(df_limpio):
    print("📈 Generando reporte PIVOTABLE para Looker (Últimas 8 semanas)...")
    gc = get_gspread_client()
    
    # Asegurar tipos
    df_limpio['Fecha Inicio'] = pd.to_datetime(df_limpio['Fecha Inicio'])
    
    # 1. Procesar
    df_c2 = procesar_formato_largo_ultimas_8(df_limpio, es_comuna_2=True)
    df_resto = procesar_formato_largo_ultimas_8(df_limpio, es_comuna_2=False)
    
    # 2. Unificar
    df_final = pd.concat([df_c2, df_resto], ignore_index=True)
    
    # 3. Subir
    update_sheet(gc, SHEET_ID_LOOKER, "Data_Pivot_Looker", df_final)
    
    print("✅ Reporte pivotable actualizado correctamente.")