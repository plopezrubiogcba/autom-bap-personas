import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import re
import gc
import unicodedata
import pyarrow
import adjustText
from adjustText import adjust_text



file_dir = 'C:/Users/patol/OneDrive/Red de Atencion/'
archivo_nombre = 'Intervenciones/1_database/2025_historico_limpio.parquet'


# Grafico Resto de la CIUDAD

# ============================================================
# Convertir fecha si no está en datetime
# ============================================================
df = pd.read_parquet(file_dir + archivo_nombre)
df['Fecha Inicio'] = pd.to_datetime(df['Fecha Inicio'])

# ============================================================
# 2️⃣ Filtrado de comuna y clasificación de contacto
# ============================================================
df = df[df['comuna_calculada'] != 2].copy()
#df = df[~df['comuna_calculada'].isin([2, 14])]

def clasificar_contacto(row):
    no_contacta = [
        '12–No se contacta y no se observan pertenencias',
        '11-No se contacta y se observan pertenencias',
        '16-Desestimado (cartas 911 u otras áreas)'
    ]
    
    # 🔸 Nueva condición: si Estado == 'PENDIENTE' → "Sin cubrir"
    if row['Estado'] == 'PENDIENTE':
        return 'Sin cubrir'
    
    elif row['Resultado'] in no_contacta:
        return 'No se contacta'
    
    elif row['Resultado'] == '15-Sin cubrir':
        return 'Sin cubrir'
    
    else:
        return 'Se contacta'

df['Categoria_contacto'] = df.apply(clasificar_contacto, axis=1)

# ============================================================
# 3️⃣ Definir semana (inicio lunes) y agrupar
# ============================================================
df['Semana'] = df['Fecha Inicio'].dt.to_period('W-SUN').dt.start_time

# Totales de intervenciones (todas)
df_total_sem = df.groupby('Semana').size()

# Totales de intervenciones automáticas
df_auto = df[df['Tipo Carta'] == 'AUTOMATICA']
df_auto_sem = df_auto.groupby('Semana').size()

# Distribución de categorías dentro de automáticas
df_auto_conteo = df_auto.groupby(['Semana', 'Categoria_contacto']).size().unstack(fill_value=0)
df_auto_conteo = df_auto_conteo.tail(8)

totales_auto = df_auto_conteo.sum(axis=1)
df_porcentajes = (df_auto_conteo.div(totales_auto, axis=0) * 100).round(0)

# ============================================================
# 🔹 Nueva fila: Derivaciones CIS
# ============================================================
df_cis = df[df['Resultado'] == '01-Traslado efectivo a CIS']
df_cis_sem = df_cis.groupby('Semana').size()
df_cis_sem = df_cis_sem.reindex(df_auto_conteo.index, fill_value=0)

# ============================================================
# 4️⃣ Formatear columnas (porcentaje + número absoluto)
# ============================================================
def combinar(pct, abs_):
    return pct.astype(int).astype(str) + '% (' + abs_.astype(int).astype(str) + ')'

df_final = pd.DataFrame({
    'Intervenciones totales': df_total_sem.tail(8).values,
    'Derivaciones CIS': df_cis_sem.values,
    'Llamados 108': df_auto_sem.reindex(df_auto_conteo.index, fill_value=0).values,
    '% Contacta': combinar(df_porcentajes['Se contacta'], df_auto_conteo['Se contacta']),
    '% No se contacta': combinar(df_porcentajes['No se contacta'], df_auto_conteo['No se contacta']),
    '% Sin cubrir': combinar(df_porcentajes['Sin cubrir'], df_auto_conteo['Sin cubrir'])
}).T

# ============================================================
# 5️⃣ Etiquetas de columnas (tipo “Sem 27 Oct”)
# ============================================================
df_final.columns = [
    'Sem ' + semana.strftime('%d %b').replace('.', '').title()
    for semana in df_auto_conteo.index
]

# ============================================================
# 6️⃣ Estilo visual tipo dashboard
# ============================================================
estilo = (
    df_final.style
    .set_table_styles([
        {'selector': 'thead th', 'props': 'background-color: #5DD5C4; color: black; font-weight: bold; text-align: center;'},
        {'selector': 'th.row_heading', 'props': 'background-color: #003341; color: white; text-align: left; font-weight: bold;'},
        {'selector': 'td', 'props': 'text-align: center; font-size: 13px; border: 1px solid #ccc;'},
        {'selector': 'tbody tr:hover td', 'props': 'background-color: #f0f0f0;'}
    ])
    .set_properties(**{'text-align': 'center', 'padding': '6px'})
    .set_caption("Distribución semanal de intervenciones 108 – Comuna Resto de la ciudad")
)

display(estilo)




# Grafico comuna 14

# ============================================================
# Convertir fecha si no está en datetime
# ============================================================

df['Fecha Inicio'] = pd.to_datetime(df['Fecha Inicio'])

# ============================================================
# 2️⃣ Filtrado de comuna y clasificación de contacto
# ============================================================
df = df[df['comuna_calculada'] == 14].copy()

def clasificar_contacto(row):
    no_contacta = [
        '12–No se contacta y no se observan pertenencias',
        '11-No se contacta y se observan pertenencias',
        '16-Desestimado (cartas 911 u otras áreas)'
    ]
    
    # 🔸 Nueva condición: si Estado == 'PENDIENTE' → "Sin cubrir"
    if row['Estado'] == 'PENDIENTE':
        return 'Sin cubrir'
    
    elif row['Resultado'] in no_contacta:
        return 'No se contacta'
    
    elif row['Resultado'] == '15-Sin cubrir':
        return 'Sin cubrir'
    
    else:
        return 'Se contacta'

df['Categoria_contacto'] = df.apply(clasificar_contacto, axis=1)

# ============================================================
# 3️⃣ Definir semana (inicio lunes) y agrupar
# ============================================================
df = pd.read_parquet(file_dir + archivo_nombre)

df = df[df['comuna_calculada'] == 14].copy()

df['Semana'] = df['Fecha Inicio'].dt.to_period('W-SUN').dt.start_time

# Totales de intervenciones (todas)
df_total_sem = df.groupby('Semana').size()

# Totales de intervenciones automáticas
df_auto = df[df['Tipo Carta'] == 'AUTOMATICA']
df_auto_sem = df_auto.groupby('Semana').size()

# Distribución de categorías dentro de automáticas
df_auto_conteo = df_auto.groupby(['Semana', 'Categoria_contacto']).size().unstack(fill_value=0)
df_auto_conteo = df_auto_conteo.tail(8)

totales_auto = df_auto_conteo.sum(axis=1)
df_porcentajes = (df_auto_conteo.div(totales_auto, axis=0) * 100).round(0)

# ============================================================
# 🔹 Nueva fila: Derivaciones CIS
# ============================================================
df_cis = df[df['Resultado'] == '01-Traslado efectivo a CIS']
df_cis_sem = df_cis.groupby('Semana').size()
df_cis_sem = df_cis_sem.reindex(df_auto_conteo.index, fill_value=0)

# ============================================================
# 4️⃣ Formatear columnas (porcentaje + número absoluto)
# ============================================================
def combinar(pct, abs_):
    return pct.astype(int).astype(str) + '% (' + abs_.astype(int).astype(str) + ')'

df_final = pd.DataFrame({
    'Intervenciones totales': df_total_sem.tail(8).values,
    'Derivaciones CIS': df_cis_sem.values,
    'Llamados 108': df_auto_sem.reindex(df_auto_conteo.index, fill_value=0).values,
    '% Contacta': combinar(df_porcentajes['Se contacta'], df_auto_conteo['Se contacta']),
    '% No se contacta': combinar(df_porcentajes['No se contacta'], df_auto_conteo['No se contacta']),
    '% Sin cubrir': combinar(df_porcentajes['Sin cubrir'], df_auto_conteo['Sin cubrir'])
}).T

# ============================================================
# 5️⃣ Etiquetas de columnas (tipo “Sem 27 Oct”)
# ============================================================
df_final.columns = [
    'Sem ' + semana.strftime('%d %b').replace('.', '').title()
    for semana in df_auto_conteo.index
]

# ============================================================
# 6️⃣ Estilo visual tipo dashboard
# ============================================================
estilo = (
    df_final.style
    .set_table_styles([
        {'selector': 'thead th', 'props': 'background-color: #5DD5C4; color: black; font-weight: bold; text-align: center;'},
        {'selector': 'th.row_heading', 'props': 'background-color: #003341; color: white; text-align: left; font-weight: bold;'},
        {'selector': 'td', 'props': 'text-align: center; font-size: 13px; border: 1px solid #ccc;'},
        {'selector': 'tbody tr:hover td', 'props': 'background-color: #f0f0f0;'}
    ])
    .set_properties(**{'text-align': 'center', 'padding': '6px'})
    .set_caption("Distribución semanal de intervenciones 108 – Comuna 14")
)

display(estilo)



# Grafico Comuna 2 

df = pd.read_parquet(file_dir + archivo_nombre)
df = df[df['comuna_calculada'] == 2]


# ============================================================
# 1️⃣ Carga y filtrado base
# ============================================================
df = pd.read_parquet(file_dir + archivo_nombre)

# Convertir fecha si no está en datetime
df['Fecha Inicio'] = pd.to_datetime(df['Fecha Inicio'])

# ============================================================
# 2️⃣ Filtrado de comuna y clasificación de contacto
# ============================================================
df = df[df['comuna_calculada'] == 2].copy()

def clasificar_contacto(resultado):
    no_contacta = [
        '12–No se contacta y no se observan pertenencias',
        '11-No se contacta y se observan pertenencias'
    ]
    if resultado in no_contacta:
        return 'No se contacta'
    elif resultado == '15-Sin cubrir':
        return 'Sin cubrir'
    else:
        return 'Se contacta'

df['Categoria_contacto'] = df['Resultado'].apply(clasificar_contacto)

# ============================================================
# 3️⃣ Definir semana (inicio lunes) y agrupar
# ============================================================
df['Semana'] = df['Fecha Inicio'].dt.to_period('W-SUN').dt.start_time

# Totales de intervenciones (todas)
df_total_sem = df.groupby('Semana').size()

# Totales de intervenciones automáticas
df_auto = df[df['Tipo Carta'] == 'AUTOMATICA']
df_auto_sem = df_auto.groupby('Semana').size()

# Distribución de categorías dentro de automáticas
df_auto_conteo = df_auto.groupby(['Semana', 'Categoria_contacto']).size().unstack(fill_value=0)
df_auto_conteo = df_auto_conteo.tail(8)

totales_auto = df_auto_conteo.sum(axis=1)
df_porcentajes = (df_auto_conteo.div(totales_auto, axis=0) * 100).round(0)

# ============================================================
# 🔹 Nueva fila: Derivaciones CIS
# ============================================================
df_cis = df[df['categoria_final'] == 'traslado efectivo a cis']
df_cis_sem = df_cis.groupby('Semana').size()
df_cis_sem = df_cis_sem.reindex(df_auto_conteo.index, fill_value=0)

# ============================================================
# 4️⃣ Formatear columnas (porcentaje + número absoluto)
# ============================================================
def combinar(pct, abs_):
    return pct.astype(int).astype(str) + '% (' + abs_.astype(int).astype(str) + ')'

df_final = pd.DataFrame({
    'Intervenciones totales': df_total_sem.tail(8).values,
    'Derivaciones CIS': df_cis_sem.values,
    'Llamados 108': df_auto_sem.reindex(df_auto_conteo.index, fill_value=0).values,
    '% Contacta': combinar(df_porcentajes['Se contacta'], df_auto_conteo['Se contacta']),
    '% No se contacta': combinar(df_porcentajes['No se contacta'], df_auto_conteo['No se contacta']),
    '% Sin cubrir': combinar(df_porcentajes['Sin cubrir'], df_auto_conteo['Sin cubrir'])
}).T

# ============================================================
# 5️⃣ Etiquetas de columnas (tipo “Sem 27 Oct”)
# ============================================================
df_final.columns = [
    'Sem ' + semana.strftime('%d %b').replace('.', '').title()
    for semana in df_auto_conteo.index
]
# ============================================================
# 5.5️⃣ 🔹 Agregar columna Acumulado (desde 01-09-25)
# ============================================================
fecha_corte = '2025-09-01'

# 1. Filtrar dataframe desde la fecha de corte
df_acum = df[df['Fecha Inicio'] >= fecha_corte]

# 2. Calcular métricas absolutas
total_acum = len(df_acum)
cis_acum = len(df_acum[df_acum['categoria_final'] == 'traslado efectivo a cis'])

# 3. Métricas de automáticas (108)
df_acum_auto = df_acum[df_acum['Tipo Carta'] == 'AUTOMATICA']
llamados_acum = len(df_acum_auto)

# 4. Distribución de contactos acumulada
conteo_acum = df_acum_auto['Categoria_contacto'].value_counts()

# Función auxiliar para formatear acumulados (reutilizando lógica)
def fmt_acum(cat):
    val = conteo_acum.get(cat, 0)
    pct = (val / llamados_acum * 100) if llamados_acum > 0 else 0
    return f"{int(pct)}% ({int(val)})"

# 5. Crear la columna de datos
columna_acumulada = [
    total_acum,       # Intervenciones totales
    cis_acum,         # Derivaciones CIS
    llamados_acum,    # Llamados 108
    fmt_acum('Se contacta'),
    fmt_acum('No se contacta'),
    fmt_acum('Sin cubrir')
]

# 6. Insertar en el DataFrame final
df_final['Acumulado\n(desde 1/9)'] = columna_acumulada
# ============================================================
# 6️⃣ Estilo visual tipo dashboard
# ============================================================
estilo = (
    df_final.style
    .set_table_styles([
        {'selector': 'thead th', 'props': 'background-color: #5DD5C4; color: black; font-weight: bold; text-align: center;'},
        {'selector': 'th.row_heading', 'props': 'background-color: #003341; color: white; text-align: left; font-weight: bold;'},
        {'selector': 'td', 'props': 'text-align: center; font-size: 13px; border: 1px solid #ccc;'},
        {'selector': 'tbody tr:hover td', 'props': 'background-color: #f0f0f0;'}
    ])
    .set_properties(**{'text-align': 'center', 'padding': '6px'})
    .set_caption("Distribución semanal de intervenciones 108 – Comuna 2")
)

display(estilo)



# Grafico recurrentes-migratorios-nuevos comuna 2

# --------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------
N_SEMANAS = 8
COL_FECHA = "Fecha Inicio"
COL_DNI = "DNI_Categorizado"
COL_COMUNA = "comuna_calculada"

# --------------------------------------------------------------------
# 1) Preparación de columna Semana
# --------------------------------------------------------------------
df[COL_FECHA] = pd.to_datetime(df[COL_FECHA], errors="coerce")
df["Semana"] = df[COL_FECHA].dt.to_period("W-SUN").apply(lambda r: r.start_time)
df = df.sort_values(COL_FECHA)
df_sem = df.drop_duplicates(subset=["Semana", COL_DNI]).copy()

# --------------------------------------------------------------------
# 2) Helper para detectar Comuna 2
# --------------------------------------------------------------------
def is_recoleta_val(x):
    if pd.isna(x):
        return False
    if isinstance(x, (int, float)):
        return x == 2.0
    sx = str(x).upper().replace(" ", "")
    return sx in {"2", "2.0", "COMUNA2"}

# --------------------------------------------------------------------
# 3) Iteración cronológica semana a semana
# --------------------------------------------------------------------
semanas = sorted(df_sem["Semana"].unique())
dni_last_comuna = {}
dni_seen = set()
resultados = []

for semana in semanas:
    rows_sem = df_sem[df_sem["Semana"] == semana]
    rows_recoleta = rows_sem[rows_sem[COL_COMUNA].apply(is_recoleta_val)]
    dnis_recoleta = rows_recoleta[COL_DNI].unique()

    rec_count = 0
    mig_count = 0
    nue_count = 0

    for dni in dnis_recoleta:
        prior_comuna = dni_last_comuna.get(dni, None)
        if prior_comuna is None and dni not in dni_seen:
            nue_count += 1
        else:
            if prior_comuna is not None and is_recoleta_val(prior_comuna):
                rec_count += 1
            else:
                mig_count += 1

    resultados.append({
        "Semana": semana,
        "Recurrentes": rec_count,
        "Migratorios": mig_count,
        "Nuevos": nue_count,
        "TotalEnRecoleta": len(dnis_recoleta)
    })

    for _, r in rows_sem.iterrows():
        dni = r[COL_DNI]
        comuna_actual = r[COL_COMUNA]
        dni_last_comuna[dni] = comuna_actual
        dni_seen.add(dni)

# --------------------------------------------------------------------
# 4) DataFrame final (últimas N semanas)
# --------------------------------------------------------------------
df_evolucion = pd.DataFrame(resultados).sort_values("Semana")
df_evolucion = df_evolucion.tail(N_SEMANAS).reset_index(drop=True)
df_evolucion["Semana_str"] = df_evolucion["Semana"].dt.strftime("%d %b")

# --------------------------------------------------------------------
# 5) Gráfico apilado con totales arriba
# --------------------------------------------------------------------
plt.figure(figsize=(11,6))

plt.bar(df_evolucion["Semana_str"], df_evolucion["Recurrentes"], label="Recurrentes", color="#4C72B0", width=0.6)
plt.bar(df_evolucion["Semana_str"], df_evolucion["Migratorios"], bottom=df_evolucion["Recurrentes"], label="Migratorios", color="#FF7B00", width=0.6)
plt.bar(df_evolucion["Semana_str"], df_evolucion["Nuevos"], bottom=df_evolucion["Recurrentes"] + df_evolucion["Migratorios"], label="Nuevos", color="#55A868", width=0.6)

# Etiquetas numéricas
for i, row in df_evolucion.iterrows():
    rec = int(row["Recurrentes"])
    mig = int(row["Migratorios"])
    nue = int(row["Nuevos"])
    total = rec + mig + nue
    x = i

    if rec > 0:
        plt.text(x, rec / 2, str(rec), ha="center", va="center", color="white", fontsize=9, fontweight="bold")
    if mig > 0:
        plt.text(x, rec + mig / 2, str(mig), ha="center", va="center", color="black", fontsize=9, fontweight="bold")
    if nue > 0:
        plt.text(x, rec + mig + nue / 2, str(nue), ha="center", va="center", color="black", fontsize=9, fontweight="bold")

    # 👉 Total arriba de la barra
    plt.text(x, total + 2, str(total), ha="center", va="bottom", color="black", fontsize=10, fontweight="bold")

plt.title("Evolución semanal de DNIs en Recoleta (Recurrentes / Migratorios / Nuevos)")
plt.xlabel("Semana (inicio lunes)")
plt.ylabel("Cantidad de personas")
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

