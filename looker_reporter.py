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

def calcular_acumulados_por_comuna(df_base, fecha_corte='2025-09-01'):
    """
    Calcula los totales acumulados desde la fecha de corte para CADA comuna.
    Devuelve un diccionario o DataFrame pequeño para cruzar.
    """
    df = df_base[df_base['Fecha Inicio'] >= fecha_corte].copy()
    
    # 1. Agrupar por Comuna
    # Totales Generales
    total_comuna = df.groupby('comuna_calculada').size().rename('acum_total')
    
    # CIS
    cis_comuna = df[df['categoria_final'] == 'traslado efectivo a cis'].groupby('comuna_calculada').size().rename('acum_cis')
    
    # Automáticas (108)
    df_auto = df[df['Tipo Carta'] == 'AUTOMATICA']
    llamados_comuna = df_auto.groupby('comuna_calculada').size().rename('acum_llamados')
    
    # Desglose de Contactos (Automáticas)
    conteo_contactos = df_auto.groupby(['comuna_calculada', 'Categoria_contacto']).size().unstack(fill_value=0)
    
    # Asegurar columnas
    for cat in ['Se contacta', 'No se contacta', 'Sin cubrir']:
        if cat not in conteo_contactos.columns: conteo_contactos[cat] = 0
            
    # Unir todo en un DF Maestro de Acumulados
    df_acum = pd.concat([total_comuna, cis_comuna, llamados_comuna, conteo_contactos], axis=1).fillna(0)
    
    return df_acum

def procesar_datos_unificados(df_base):
    """
    Procesa TODO el dataset agrupando por Semana y Comuna.
    """
    if df_base.empty: return pd.DataFrame()
    
    df = df_base.copy()
    
    # 1. Limpieza y Preparación
    df['Semana'] = df['Fecha Inicio'].dt.to_period('W-SUN').dt.start_time
    df['Categoria_contacto'] = df.apply(clasificar_contacto, axis=1)
    
    # Limpiar Comuna (Forzar entero y quitar nulos)
    df = df.dropna(subset=['comuna_calculada'])
    try:
        df['comuna_calculada'] = df['comuna_calculada'].astype(int)
    except:
        # Si falla la conversión, filtramos filas malas
        df = df[pd.to_numeric(df['comuna_calculada'], errors='coerce').notnull()]
        df['comuna_calculada'] = df['comuna_calculada'].astype(int)

    # 2. Calcular Métricas Agrupadas por [Semana, Comuna]
    
    # A. Totales
    grp = df.groupby(['Semana', 'comuna_calculada'])
    df_total = grp.size().rename('Intervenciones totales')
    
    # B. Derivaciones CIS
    df_cis = df[df['categoria_final'] == 'traslado efectivo a cis'].groupby(['Semana', 'comuna_calculada']).size().rename('Derivaciones CIS')
    
    # C. Automáticas (108)
    df_auto = df[df['Tipo Carta'] == 'AUTOMATICA']
    grp_auto = df_auto.groupby(['Semana', 'comuna_calculada'])
    df_llamados = grp_auto.size().rename('Llamados 108')
    
    # D. Matriz de contactos (Semana x Comuna x Categoria)
    df_conteo = df_auto.groupby(['Semana', 'comuna_calculada', 'Categoria_contacto']).size().unstack(fill_value=0)
    
    for cat in ['Se contacta', 'No se contacta', 'Sin cubrir']:
        if cat not in df_conteo.columns: df_conteo[cat] = 0

    # 3. Unir todo en un DataFrame Principal
    # Usamos un merge outer o concat para no perder datos si en una semana solo hubo de un tipo
    resumen = pd.concat([df_total, df_cis, df_llamados], axis=1).fillna(0)
    
    # Unimos el desglose de contactos
    # Como df_conteo tiene índice MultiIndex igual, alinea perfecto
    resumen = pd.concat([resumen, df_conteo], axis=1).fillna(0)
    
    # 4. Calcular Porcentajes Semanales
    # Totales de automáticas para el denominador
    denominador = resumen['Llamados 108'].replace(0, 1)
    
    # Calculamos % y Strings combinados "50% (10)"
    for cat in ['Se contacta', 'No se contacta', 'Sin cubrir']:
        val = resumen[cat]
        pct = (val / denominador * 100).round(0)
        resumen[f'% {cat}'] = combinar(pct, val)

    # 5. Agregar Acumulados (Cruzar por Comuna)
    # Obtenemos la foto acumulada por comuna
    df_acumulados_ref = calcular_acumulados_por_comuna(df)
    
    # Como el resumen es MultiIndex (Semana, Comuna), reseteamos para facilitar el merge
    resumen = resumen.reset_index()
    
    # Hacemos el merge con los acumulados usando 'comuna_calculada'
    resumen = pd.merge(resumen, df_acumulados_ref, on='comuna_calculada', how='left').fillna(0)
    
    # 6. Formatear Columnas de Acumulado "50% (10)"
    denominador_acum = resumen['acum_llamados'].replace(0, 1)
    
    # Nombres de columnas del df_acumulados_ref que son conteos raw
    mapa_acum = {
        'Se contacta': 'Se contacta', # El merge trajo colapsos de nombre si coinciden, cuidado
        'No se contacta': 'No se contacta',
        'Sin cubrir': 'Sin cubrir'
    }
    
    # Nota: Al hacer merge, si las columnas se llaman igual ('Se contacta'), pandas agrega sufijos (_x, _y).
    # En el paso 4 ya creamos columnas '% Se contacta', pero las originales 'Se contacta' siguen ahí.
    # El df_acumulados_ref trae también 'Se contacta'.
    # Para evitar líos, renombramos antes del merge o recalculamos.
    # Mejor estrategia: Recalcular strings de acumulado fila por fila.
    
    col_acumuladas_finales = []
    
    # Totales simples
    # (Estas columnas vienen del merge, pandas les habrá puesto sufijo _y si chocaban, pero 'acum_total' es único)
    
    # Generamos la lista de strings para la columna "Acumulado" (que puede ser una sola con saltos de línea o varias)
    # Tu formato anterior era una columna multivalor o varias columnas. 
    # Para Looker es mejor tener columnas separadas: "Acumulado Total", "Acumulado CIS", etc.
    
    resumen['Acumulado Total'] = resumen['acum_total'].astype(int)
    resumen['Acumulado CIS'] = resumen['acum_cis'].astype(int)
    resumen['Acumulado 108'] = resumen['acum_llamados'].astype(int)
    
    # Para los porcentajes acumulados, tenemos que usar los valores raw que vinieron del merge
    # Como df_acumulados_ref tenía columnas 'Se contacta', etc., al hacer merge con resumen (que ya las tenía),
    # las del acumulado serán 'Se contacta_y'
    
    for cat in ['Se contacta', 'No se contacta', 'Sin cubrir']:
        col_raw_acum = f"{cat}_y" # Viene del acumulado
        val_acum = resumen[col_raw_acum]
        pct_acum = (val_acum / denominador_acum * 100).round(0)
        resumen[f'Acumulado % {cat}'] = combinar(pct_acum, val_acum)

    # 7. Limpieza Final de Columnas
    # Seleccionamos y renombramos para que quede bonito en Looker
    resumen['Semana'] = resumen['Semana'].dt.strftime('%Y-%m-%d')
    resumen.rename(columns={'comuna_calculada': 'Comuna'}, inplace=True)
    
    # Prefijo "Comuna " para que se vea bien
    resumen['Comuna'] = 'Comuna ' + resumen['Comuna'].astype(str)
    
    cols_finales = [
        'Semana', 'Comuna',
        'Intervenciones totales', 'Derivaciones CIS', 'Llamados 108',
        '% Se contacta', '% No se contacta', '% Sin cubrir',
        'Acumulado Total', 'Acumulado CIS', 'Acumulado 108',
        'Acumulado % Se contacta', 'Acumulado % No se contacta', 'Acumulado % Sin cubrir'
    ]
    
    return resumen[cols_finales]

# --- FUNCIÓN PRINCIPAL ---

def ejecutar_reportes_looker(df_limpio):
    print("📈 Iniciando generación de reporte UNIFICADO POR COMUNA...")
    gc = get_gspread_client()
    
    # Asegurar tipos
    df_limpio['Fecha Inicio'] = pd.to_datetime(df_limpio['Fecha Inicio'])
    
    # Procesar
    print("Procesando datos...")
    df_final = procesar_datos_unificados(df_limpio)
    
    # Ordenar: Primero por Semana (desc), luego por Comuna (asc)
    # Para que las semanas recientes salgan arriba y las comunas ordenadas 1-15
    # Extraemos el número de comuna para ordenar correctamente (1, 2, 10... no 1, 10, 2)
    df_final['temp_comuna_num'] = df_final['Comuna'].str.extract('(\d+)').astype(int)
    df_final = df_final.sort_values(by=['Semana', 'temp_comuna_num'], ascending=[False, True])
    df_final = df_final.drop(columns=['temp_comuna_num'])
    
    # Subir
    update_sheet(gc, SHEET_ID_LOOKER, "Data_Por_Comuna_Looker", df_final)
    
    print("✅ Reporte unificado actualizado correctamente.")