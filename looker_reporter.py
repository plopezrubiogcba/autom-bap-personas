import pandas as pd
import numpy as np
import gspread
from google.oauth2 import service_account
from datetime import datetime

# Configuración
KEY_FILE = 'credentials.json'
SHEET_ID_LOOKER = '1EsLO-upDBrHupXnKYvfLvQWIRaiH0kdEVToreAGmuOg' 

# =====================================================================
# GSHEET
# =====================================================================

def get_gspread_client():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = service_account.Credentials.from_service_account_file(KEY_FILE, scopes=scopes)
    return gspread.authorize(creds)

def update_sheet(gc, sheet_id, worksheet_name, df):
    try:
        sh = gc.open_by_key(sheet_id)
        try:
            ws = sh.worksheet(worksheet_name)
            ws.clear()
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=worksheet_name, rows=200, cols=30)
        
        df_str = df.astype(str).replace({'nan': '', 'NaT': '', 'None': '', '<NA>': ''})
        data = [df_str.columns.tolist()] + df_str.values.tolist()
        
        ws.update('A1', data)
        print(f"📊 Hoja '{worksheet_name}' actualizada ({len(df)} filas).")

    except Exception as e:
        print(f"❌ Error actualizando hoja '{worksheet_name}': {type(e).__name__} - {str(e)}")

# =====================================================================
# LÓGICA
# =====================================================================

def clasificar_contacto(row):
    no_contacta = [
        '12–No se contacta y no se observan pertenencias',
        '11-No se contacta y se observan pertenencias',
        '16-Desestimado (cartas 911 u otras áreas)'
    ]
    
    if row.get('Estado') == 'PENDIENTE':
        return 'Sin cubrir'
    
    if row.get('Resultado') in no_contacta:
        return 'No se contacta'
    
    if row.get('Resultado') == '15-Sin cubrir':
        return 'Sin cubrir'

    return 'Se contacta'

def combinar(pct, abs_):
    pct = pct.fillna(0)
    abs_ = abs_.fillna(0)
    return pct.astype(int).astype(str) + '% (' + abs_.astype(int).astype(str) + ')'

# =====================================================================
# NUEVA FUNCIÓN → genera tablero para una comuna
# =====================================================================

def generar_tablero_comuna(df, comuna):

    # ================================
    # LINEA DE BASE POR COMUNA
    # ================================
    if comuna == 2:
        base = ["341", "26", "175", "38%", "53%", "9%"]
    elif comuna == 14:
        base = ["47", "2", "31", "98%", "6%", "3%"]
    else:
        base = ["4767", "517", "2972", "48%", "45%", "7%"]

    df = df[df['comuna_calculada'] == comuna]
    if df.empty:
        return pd.DataFrame()

    df['Fecha Inicio'] = pd.to_datetime(df['Fecha Inicio'])
    df['Categoria_contacto'] = df.apply(clasificar_contacto, axis=1)
    df['Semana'] = df['Fecha Inicio'].dt.to_period('W-SUN').dt.start_time

    # --- Totales ---
    total_sem = df.groupby('Semana').size().tail(8)

    # --- CIS ---
    cis_sem = df[df['Resultado'] == '01-Traslado efectivo a CIS'] \
                .groupby('Semana').size().reindex(total_sem.index, fill_value=0)

    # --- Automáticas ---
    df_auto = df[df['Tipo Carta'] == 'AUTOMATICA']
    auto_sem = df_auto.groupby('Semana').size().reindex(total_sem.index, fill_value=0)

    conteo = df_auto.groupby(['Semana', 'Categoria_contacto']) \
                    .size().unstack(fill_value=0).reindex(total_sem.index, fill_value=0)

    tot_auto = conteo.sum(axis=1).replace(0, 1)
    pct = (conteo.div(tot_auto, axis=0) * 100).round(0)

    # semanas formateadas
    cols = ['Sem ' + s.strftime('%d %b').replace('.', '').title() for s in total_sem.index]

    # Construcción final
    data = {
        'Indicador': [
            'Intervenciones totales',
            'Derivaciones CIS',
            'Llamados 108',
            '% Contacta',
            '% No se contacta',
            '% Sin cubrir'
        ],
        'Linea base': base
    }

    for i, col in enumerate(cols):
        data[col] = [
            total_sem.values[i],
            cis_sem.values[i],
            auto_sem.values[i],
            f"{int(pct['Se contacta'].values[i])}% ({conteo['Se contacta'].values[i]})",
            f"{int(pct['No se contacta'].values[i])}% ({conteo['No se contacta'].values[i]})",
            f"{int(pct['Sin cubrir'].values[i])}% ({conteo['Sin cubrir'].values[i]})"
        ]

    return pd.DataFrame(data)

# =====================================================================
# PROCESAMIENTO ORIGINAL POR COMUNA (NO SE TOCA)
# =====================================================================

def calcular_acumulados_por_comuna(df_base, fecha_corte='2025-09-01'):
    df = df_base[df_base['Fecha Inicio'] >= fecha_corte].copy()
    
    total_comuna = df.groupby('comuna_calculada').size().rename('acum_total')
    cis_comuna = df[df['categoria_final'] == 'traslado efectivo a cis'] \
                  .groupby('comuna_calculada').size().rename('acum_cis')
    df_auto = df[df['Tipo Carta'] == 'AUTOMATICA']
    llamados_comuna = df_auto.groupby('comuna_calculada').size().rename('acum_llamados')
    
    conteo = df_auto.groupby(['comuna_calculada', 'Categoria_contacto']) \
                    .size().unstack(fill_value=0)

    for cat in ['Se contacta', 'No se contacta', 'Sin cubrir']:
        if cat not in conteo.columns:
            conteo[cat] = 0
    
    df_acum = pd.concat([total_comuna, cis_comuna, llamados_comuna, conteo], axis=1).fillna(0)
    
    return df_acum

def procesar_datos_unificados(df_base):

    if df_base.empty:
        return pd.DataFrame()

    df = df_base.copy()
    df['Semana'] = df['Fecha Inicio'].dt.to_period('W-SUN').dt.start_time
    df['Categoria_contacto'] = df.apply(clasificar_contacto, axis=1)

    df = df.dropna(subset=['comuna_calculada'])
    df['comuna_calculada'] = df['comuna_calculada'].astype(int)

    grp = df.groupby(['Semana', 'comuna_calculada'])
    df_total = grp.size().rename('Intervenciones totales')

    df_cis = df[df['categoria_final'] == 'traslado efectivo a cis'] \
              .groupby(['Semana', 'comuna_calculada']).size().rename('Derivaciones CIS')

    df_auto = df[df['Tipo Carta'] == 'AUTOMATICA']
    grp_auto = df_auto.groupby(['Semana', 'comuna_calculada'])
    df_llamados = grp_auto.size().rename('Llamados 108')

    df_conteo = df_auto.groupby(['Semana', 'comuna_calculada', 'Categoria_contacto']) \
                       .size().unstack(fill_value=0)

    for cat in ['Se contacta', 'No se contacta', 'Sin cubrir']:
        if cat not in df_conteo.columns:
            df_conteo[cat] = 0

    resumen = pd.concat([df_total, df_cis, df_llamados, df_conteo], axis=1).fillna(0)

    denominador = resumen['Llamados 108'].replace(0, 1)
    for cat in ['Se contacta', 'No se contacta', 'Sin cubrir']:
        pct = (resumen[cat] / denominador * 100).round(0)
        resumen[f'% {cat}'] = combinar(pct, resumen[cat])

    df_acum = calcular_acumulados_por_comuna(df)
    resumen = resumen.reset_index()
    resumen = pd.merge(resumen, df_acum, on='comuna_calculada', how='left').fillna(0)

    resumen['Semana'] = resumen['Semana'].dt.strftime('%Y-%m-%d')
    resumen.rename(columns={'comuna_calculada': 'Comuna'}, inplace=True)
    resumen['Comuna'] = 'Comuna ' + resumen['Comuna'].astype(str)

    cols = [
        'Semana', 'Comuna',
        'Intervenciones totales', 'Derivaciones CIS', 'Llamados 108',
        '% Se contacta', '% No se contacta', '% Sin cubrir',
        'acum_total', 'acum_cis', 'acum_llamados',
        'Se contacta_y', 'No se contacta_y', 'Sin cubrir_y'
    ]

    return resumen[cols]

# =====================================================================
# FUNCIÓN PRINCIPAL
# =====================================================================

def ejecutar_reportes_looker(df_limpio):

    print("📈 Iniciando procesamiento...")
    gc = get_gspread_client()

    df_limpio['Fecha Inicio'] = pd.to_datetime(df_limpio['Fecha Inicio'])

    # =========================================================
    # ORIGINAL: DATA POR COMUNA
    # =========================================================
    df_final = procesar_datos_unificados(df_limpio)

    df_final['temp'] = df_final['Comuna'].str.extract('(\d+)').astype(int)
    df_final = df_final.sort_values(['Semana', 'temp'], ascending=[False, True])
    df_final = df_final.drop(columns=['temp'])

    update_sheet(gc, SHEET_ID_LOOKER, "Data_Por_Comuna_Looker", df_final)

    # =========================================================
    # NUEVO → TABLERO POR COMUNA 
    # =========================================================
    comunas = sorted(df_limpio['comuna_calculada'].dropna().unique())

    for c in comunas:
        df_tab = generar_tablero_comuna(df_limpio, c)
        if not df_tab.empty:
            update_sheet(gc, SHEET_ID_LOOKER, f"Tablero_C{c}", df_tab)

    print("✅ Reportes generados correctamente.")
