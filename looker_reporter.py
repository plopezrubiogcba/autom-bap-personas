import pandas as pd
import numpy as np
import gspread
from google.oauth2 import service_account
from datetime import datetime, timedelta

# Configuración
KEY_FILE = 'credentials.json'
# ID de tu Google Sheet Maestro
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
            ws = sh.add_worksheet(title=worksheet_name, rows=100, cols=20)
        
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
# LÓGICA DE NEGOCIO (Tus funciones de análisis)
# ============================================================

def clasificar_contacto(row):
    """Clasifica el contacto según Resultado y Estado."""
    no_contacta = [
        '12–No se contacta y no se observan pertenencias',
        '11-No se contacta y se observan pertenencias',
        '16-Desestimado (cartas 911 u otras áreas)'
    ]
    
    # Lógica prioritaria
    if row.get('Estado') == 'PENDIENTE':
        return 'Sin cubrir'
    
    resultado = str(row.get('Resultado', ''))
    if resultado in no_contacta:
        return 'No se contacta'
    elif resultado == '15-Sin cubrir':
        return 'Sin cubrir'
    else:
        # Por descarte asumimos contacto si no es ninguno de los anteriores
        return 'Se contacta'

def combinar(pct, abs_):
    """Formato: 50% (10)"""
    # Manejo seguro de series vacías o nulos
    pct = pct.fillna(0)
    abs_ = abs_.fillna(0)
    return pct.astype(int).astype(str) + '% (' + abs_.astype(int).astype(str) + ')'

def generar_tabla_dashboard(df_base, es_comuna_2=True):
    """
    Genera la tabla resumen con formato 'X% (Y)' y acumulados.
    Sirve tanto para Comuna 2 como para el resto.
    """
    # 1. Filtrar Comuna
    if es_comuna_2:
        df = df_base[df_base['comuna_calculada'] == 2].copy()
    else:
        df = df_base[df_base['comuna_calculada'] != 2].copy()

    if df.empty: return pd.DataFrame()

    # 2. Preparar datos
    df['Semana'] = df['Fecha Inicio'].dt.to_period('W-SUN').dt.start_time
    df['Categoria_contacto'] = df.apply(clasificar_contacto, axis=1)

    # 3. Calcular Totales Semanales (Tail 8)
    df_total_sem = df.groupby('Semana').size()
    
    # 4. Métricas de Automáticas (108)
    df_auto = df[df['Tipo Carta'] == 'AUTOMATICA']
    df_auto_sem = df_auto.groupby('Semana').size()
    
    # Distribución categorías
    df_auto_conteo = df_auto.groupby(['Semana', 'Categoria_contacto']).size().unstack(fill_value=0)
    
    # Asegurar que existan las 3 columnas siempre
    for cat in ['Se contacta', 'No se contacta', 'Sin cubrir']:
        if cat not in df_auto_conteo.columns:
            df_auto_conteo[cat] = 0
            
    # Filtrar últimas 8 semanas disponibles
    df_auto_conteo = df_auto_conteo.tail(8)
    weeks_index = df_auto_conteo.index
    
    # Calcular porcentajes
    totales_auto = df_auto_conteo.sum(axis=1)
    # Evitar división por cero
    df_porcentajes = df_auto_conteo.div(totales_auto.replace(0, 1), axis=0) * 100
    df_porcentajes = df_porcentajes.round(0)

    # 5. Derivaciones CIS (Usando categoria_final que es más limpia)
    # Nota: Usamos 'traslado efectivo a cis' del proceso de limpieza previo
    df_cis = df[df['categoria_final'] == 'traslado efectivo a cis']
    df_cis_sem = df_cis.groupby('Semana').size().reindex(weeks_index, fill_value=0)

    # 6. Construir DataFrame Final Transpuesto
    df_final = pd.DataFrame({
        'Intervenciones totales': df_total_sem.reindex(weeks_index, fill_value=0).values,
        'Derivaciones CIS': df_cis_sem.values,
        'Llamados 108': df_auto_sem.reindex(weeks_index, fill_value=0).values,
        '% Contacta': combinar(df_porcentajes['Se contacta'], df_auto_conteo['Se contacta']).values,
        '% No se contacta': combinar(df_porcentajes['No se contacta'], df_auto_conteo['No se contacta']).values,
        '% Sin cubrir': combinar(df_porcentajes['Sin cubrir'], df_auto_conteo['Sin cubrir']).values
    }).T

    # 7. Etiquetas de columnas (Sem 27 Oct)
    # Nota: locale puede fallar en linux minimizado, usamos formateo manual simple en inglés o diccionario si es crítico
    # Usamos strftime simple para evitar errores de locale 'es_AR' no instalado en Cloud Run
    try:
        import locale
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8') # Intento configurar español
    except:
        pass # Si falla, saldrá en inglés o default
        
    cols = []
    for semana in weeks_index:
        cols.append('Sem ' + semana.strftime('%d %b').replace('.', '').title())
    df_final.columns = cols

    # 8. Columna Acumulado (Solo si se pide, por defecto SI para Comuna 2)
    # Fecha corte definida en tu código: 1 de Septiembre 2025
    fecha_corte = '2025-09-01'
    
    if es_comuna_2: # O si quieres para ambos, quita el if
        df_acum = df[df['Fecha Inicio'] >= fecha_corte]
        
        # Cálculos acumulados
        total_acum = len(df_acum)
        cis_acum = len(df_acum[df_acum['categoria_final'] == 'traslado efectivo a cis'])
        
        df_acum_auto = df_acum[df_acum['Tipo Carta'] == 'AUTOMATICA']
        llamados_acum = len(df_acum_auto)
        
        conteo_acum = df_acum_auto['Categoria_contacto'].value_counts()
        
        def fmt_acum(cat):
            val = conteo_acum.get(cat, 0)
            pct = (val / llamados_acum * 100) if llamados_acum > 0 else 0
            return f"{int(pct)}% ({int(val)})"

        columna_acumulada = [
            total_acum,
            cis_acum,
            llamados_acum,
            fmt_acum('Se contacta'),
            fmt_acum('No se contacta'),
            fmt_acum('Sin cubrir')
        ]
        
        df_final['Acumulado (desde 1/9)'] = columna_acumulada

    # Reseteamos index para que el nombre de las métricas sea una columna en Sheets
    return df_final.reset_index().rename(columns={'index': 'Métrica'})

# --- FUNCIONES ANTIGUAS (Mantenidas por si acaso, o puedes borrarlas si ya no las usas) ---
# ... (Puedes dejar generar_reporte_comuna2_metricas etc, si Looker las sigue usando) ...
# Para mantener tu sheet limpio, solo voy a llamar a las nuevas en la función principal abajo.

# --- FUNCIÓN PRINCIPAL ---

def ejecutar_reportes_looker(df_limpio):
    print("📈 Iniciando generación de reportes para Looker...")
    gc = get_gspread_client()
    
    # Asegurar tipos de datos
    df_limpio['Fecha Inicio'] = pd.to_datetime(df_limpio['Fecha Inicio'])
    
    # 1. Tabla Dashboard Comuna 2 (Nueva)
    print("Generando Dashboard Comuna 2...")
    df_dash_c2 = generar_tabla_dashboard(df_limpio, es_comuna_2=True)
    update_sheet(gc, SHEET_ID_LOOKER, "Dashboard Intervenciones C2", df_dash_c2)
    
    # 2. Tabla Dashboard Sin Comuna 2 (Nueva)
    print("Generando Dashboard Resto CABA...")
    df_dash_sin_c2 = generar_tabla_dashboard(df_limpio, es_comuna_2=False)
    update_sheet(gc, SHEET_ID_LOOKER, "Dashboard Intervenciones Sin C2", df_dash_sin_c2)

    # -----------------------------------------------------------
    # (Opcional) Si quieres mantener las tablas viejas también:
    # -----------------------------------------------------------
    # df_dni = generar_reporte_seguimiento_dni(df_limpio)
    # update_sheet(gc, SHEET_ID_LOOKER, "EvolucionSemanalDNIRecoleta", df_dni)
    
    print("✅ Todos los reportes de Looker han sido actualizados.")