import pandas as pd
import numpy as np
import datetime
import json
import re
import os
from data_processor import get_drive_service, download_parquet_as_df

# --- CONFIGURACION ---
# ID de la carpeta DB (tomado de main.py)
FOLDER_ID_DB = '1q7rGJjb3qCTNcyDUYzpn9v4JveLjsk6t'
FILE_NAME_PARQUET = '2025_historico_limpio.parquet'
TEMPLATE_HTML_PATH = 'reporte_tablero.html'
OUTPUT_HTML_PATH = 'reporte_autom_bap.html'

# =============================================================================
# LOGICA DE NEGOCIO
# =============================================================================

def clasificar_contacto(row):
    """Clasificación estricta de contactos."""
    no_contacta = [
        '12–No se contacta y no se observan pertenencias',
        '11-No se contacta y se observan pertenencias',
        '16-Desestimado (cartas 911 u otras áreas)'
    ]
    
    if row.get('Estado') == 'PENDIENTE':
        return 'Sin cubrir'
    
    resultado = row.get('Resultado')
    if resultado in no_contacta:
        return 'No se contacta'
    elif resultado == '15-Sin cubrir':
        return 'Sin cubrir'
    else:
        return 'Se contacta'

def calculate_dni_evolution(df_base, target_comuna_id=2):
    """
    Calcula evolución de DNIs para una Comuna dada (Nuevos/Recurrentes/Migratorios).
    target_comuna_id puede ser int (2, 14, etc).
    """
    COL_FECHA = "Fecha Inicio"
    COL_DNI = "DNI_Categorizado"
    COL_COMUNA = "comuna_calculada"
    
    df = df_base.copy()
    if COL_DNI not in df.columns:
        df[COL_DNI] = df['Persona DNI']

    df = df.sort_values(COL_FECHA)
    df["Semana"] = df[COL_FECHA].dt.to_period("W-SUN").apply(lambda r: r.start_time)
    df_sem = df.drop_duplicates(subset=["Semana", COL_DNI]).copy()

    def is_target_val(x):
        if pd.isna(x): return False
        if isinstance(x, (int, float)): return x == float(target_comuna_id)
        
        sx = str(x).upper().replace(" ", "")
        
        # Variaciones comunes
        if target_comuna_id == 2:
            return sx in {"2", "2.0", "COMUNA2"}
        elif target_comuna_id == 14:
            return sx in {"14", "14.0", "COMUNA14"}
        else:
            # Fallback generico
            return sx in {str(target_comuna_id), f"{target_comuna_id}.0", f"COMUNA{target_comuna_id}"}

    semanas = sorted(df_sem["Semana"].unique())
    dni_last_comuna = {}
    dni_seen = set()
    resultados = []

    for semana in semanas:
        rows_sem = df_sem[df_sem["Semana"] == semana]
        rows_target = rows_sem[rows_sem[COL_COMUNA].apply(is_target_val)]
        dnis_target = rows_target[COL_DNI].unique()

        rec_count = 0
        mig_count = 0
        nue_count = 0

        for dni in dnis_target:
            prior_comuna = dni_last_comuna.get(dni, None)
            if prior_comuna is None and dni not in dni_seen:
                nue_count += 1
            else:
                # Si existia su ultima comuna registrada y ERA la target => Recurrente
                if prior_comuna is not None and is_target_val(prior_comuna):
                    rec_count += 1
                else:
                    # Si existia pero NO era la target => Migratorio (viene de otro lado)
                    # O si no tenia prior_comuna pero YA FUE visto (caso borde) => Migratorio interno (??)
                    # La logica original dice: "if prior_comuna is None and dni not in dni_seen: Nuevo"
                    # "else: ..." -> aqui entra si tiene prior_comuna OR dni in dni_seen.
                    mig_count += 1
        
        resultados.append({
            "Semana": semana,
            "recurrentes": int(rec_count),
            "migratorios": int(mig_count),
            "nuevos": int(nue_count)
        })

        for _, r in rows_sem.iterrows():
            dni_last_comuna[r[COL_DNI]] = r[COL_COMUNA]
            dni_seen.add(r[COL_DNI])

    return resultados[-8:]

# =============================================================================
# GENERACION DE HTML INTERACTIVO Y CALCULOS GLOBALES
# =============================================================================

def get_stats_data_raw(df_base, comuna_filter_func, base_vals):
    """
    Devuelve un diccionario con los datos crudos para el frontend.
    """
    df = comuna_filter_func(df_base).copy()
    
    # 8 Semanas fijas
    all_weeks = sorted(df_base['Fecha Inicio'].dt.to_period('W-SUN').dt.start_time.unique())[-8:]
    weeks_str = [w.strftime('%d %b').replace('.', '').title() for w in all_weeks]

    if df.empty:
        return {
            'weeks': weeks_str,
            'rows': [
                {'label': 'Intervenciones totales', 'base': base_vals[0], 'vals': [0]*8},
                {'label': 'Derivaciones CIS', 'base': base_vals[1], 'vals': [0]*8},
                {'label': 'Llamados 108', 'base': base_vals[2], 'vals': [0]*8},
                {'label': '% Se contacta', 'base': base_vals[3], 'vals': ["0% (0)"]*8},
                {'label': '% No se contacta', 'base': base_vals[4], 'vals': ["0% (0)"]*8},
                {'label': '% Sin cubrir', 'base': base_vals[5], 'vals': ["0% (0)"]*8},
            ]
        }

    df['Semana'] = df['Fecha Inicio'].dt.to_period('W-SUN').dt.start_time
    df['Categoria_contacto'] = df.apply(clasificar_contacto, axis=1)

    df_total_sem = df.groupby('Semana').size().reindex(all_weeks, fill_value=0)
    
    df_cis = df[df['Resultado'] == '01-Traslado efectivo a CIS']
    df_cis_sem = df_cis.groupby('Semana').size().reindex(all_weeks, fill_value=0)

    df_auto = df[df['Tipo Carta'] == 'AUTOMATICA']
    df_auto_sem = df_auto.groupby('Semana').size().reindex(all_weeks, fill_value=0)

    df_auto_conteo = df_auto.groupby(['Semana', 'Categoria_contacto']).size().unstack(fill_value=0)
    for col in ['Se contacta', 'No se contacta', 'Sin cubrir']:
        if col not in df_auto_conteo.columns:
            df_auto_conteo[col] = 0
    df_auto_conteo = df_auto_conteo.reindex(all_weeks, fill_value=0)

    totales_auto = df_auto_conteo.sum(axis=1).replace(0, 1)
    df_pct = (df_auto_conteo.div(totales_auto, axis=0) * 100).round(0)

    rows = []
    
    def get_vals(series): return series.values.tolist()
    def get_comb(cat):
        return [f"{int(p)}% ({int(a)})" for p, a in zip(df_pct[cat].values, df_auto_conteo[cat].values)]

    rows.append({'label': 'Intervenciones totales', 'base': base_vals[0], 'vals': get_vals(df_total_sem)})
    rows.append({'label': 'Derivaciones CIS', 'base': base_vals[1], 'vals': get_vals(df_cis_sem)})
    rows.append({'label': 'Llamados 108', 'base': base_vals[2], 'vals': get_vals(df_auto_sem)})
    rows.append({'label': '% Se contacta', 'base': base_vals[3], 'vals': get_comb('Se contacta')})
    rows.append({'label': '% No se contacta', 'base': base_vals[4], 'vals': get_comb('No se contacta')})
    rows.append({'label': '% Sin cubrir', 'base': base_vals[5], 'vals': get_comb('Sin cubrir')})

    return {'weeks': weeks_str, 'rows': rows}

def main():
    print("🚀 Iniciando Generador de Dashboard Interactivo V2 (Fixed)...")
    
    service = get_drive_service()
    print(f"⬇️ Descargando {FILE_NAME_PARQUET}...")
    df = download_parquet_as_df(service, FILE_NAME_PARQUET, FOLDER_ID_DB)
    
    if df.empty: return

    df['Fecha Inicio'] = pd.to_datetime(df['Fecha Inicio'])
    last_update = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")

    print("📊 Calculando datos para TODAS las comunas...")
    
    all_data = {}
    
    # Bases HARDCODEADAS
    # Usare valores vacios "-" para las comunas que no son la 2, la 14 o el Resto
    base_dummy = ["-", "-", "-", "-", "-", "-"]
    base_c2 = ["1364", "92", "247", "151", "90", "6"]
    
    # Comuna 14
    # Valores: 366, 7, 245, 23% (58), 31% (76), 45% (111)
    base_c14 = ["366", "7", "245", "23% (58)", "31% (76)", "45% (111)"]
    
    # Base Total (Antiguamente Resto - Solicitado usar esta base para Total)
    base_total = ["4344", "341", "2798", "782", "717", "1299"]

    for c in range(1, 16):
        if c == 2:
            base = base_c2
        elif c == 14:
            base = base_c14
        else:
            base = base_dummy
            
        all_data[f'c{c}'] = get_stats_data_raw(
            df, 
            lambda d, com=c: d[d['comuna_calculada'] == com], 
            base
        )
    
    # Total Ciudad (Usando base_total)
    all_data['total'] = get_stats_data_raw(df, lambda d: d, base_total)

    def prepare_chart_json(dni_data_list):
        return {
            "labels": [d["Semana"].strftime("%d %b") for d in dni_data_list],
            "datasets": [
               {"label": "Nuevos", "data": [d["nuevos"] for d in dni_data_list], "backgroundColor": "#10B981"},
               {"label": "Recurrentes", "data": [d["recurrentes"] for d in dni_data_list], "backgroundColor": "#3B82F6"},
               {"label": "Migratorios", "data": [d["migratorios"] for d in dni_data_list], "backgroundColor": "#F97316"}
            ]
        }

    print("📈 Calculando evolución DNI Comuna 2...")
    dni_data_c2 = calculate_dni_evolution(df, target_comuna_id=2)
    chart_json_c2 = prepare_chart_json(dni_data_c2)

    print("📈 Calculando evolución DNI Comuna 14...")
    dni_data_c14 = calculate_dni_evolution(df, target_comuna_id=14)
    chart_json_c14 = prepare_chart_json(dni_data_c14)

    print(f"📝 Generando HTML Interactivo...")
    
    with open(TEMPLATE_HTML_PATH, 'r', encoding='utf-8') as f:
        html = f.read()

    # --- CAMBIOS DE NOMBRE (Refinamiento Final) ---
    html = html.replace("Red BAP", "Red de Atención")
    html = html.replace("BAP Personas", "Red de Atención")

    # --- LOGO (Base64) ---
    import base64
    logo_b64 = ""
    logo_path = "logoba-removebg-preview.png"
    
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as image_file:
            logo_b64 = base64.b64encode(image_file.read()).decode('utf-8')
            img_tag = f'<img src="data:image/png;base64,{logo_b64}" alt="BA Logo" class="h-16 w-auto object-contain" />'
    else:
        # Fallback si no encuentra la imagen
        img_tag = '<span class="text-white font-bold text-xl">BA</span>'

    # --- NUEVO HEADER (Diseño Visual) ---
    # Reemplazamos todo el bloque <header>...</header> del template original
    new_header = f'''
    <header class="sticky top-0 z-50 flex w-full h-24 bg-[#1E2B37] font-sans shadow-md">
        <!-- Teal Bar Wrapper -->
        <div class="flex-grow bg-gradient-to-r from-[#8BE3D9] to-[#80E0D6] rounded-tr-[3rem] flex mr-4 relative items-center">
            
            <!-- Yellow Section (Tab) -->
            <div class="bg-ba-yellow h-full w-full lg:w-1/2 rounded-tr-[3rem] px-8 flex items-center justify-between sm:justify-start sm:space-x-8 relative z-10 shadow-sm">
                 <h1 class="text-xl md:text-2xl font-bold text-ba-grey uppercase tracking-wider leading-tight">
                    INDICADORES CLAVE - RED DE ATENCIÓN
                 </h1>
                 
                 <!-- Vertical Divider & Date -->
                 <div class="hidden sm:flex items-center space-x-4 border-l border-gray-400 pl-4 h-1/2">
                     <div class="flex flex-col text-xs font-semibold text-gray-800">
                          <!-- Placeholders que el regex reemplazará abajo -->
                          <div>Actualizado: 01/01/2000 00:00</div>
                          <div class="text-gray-600">Semana: 01 Jan</div>
                     </div>
                 </div>
            </div>

            <!-- Teal Decoration (Empty space to the right of yellow acts as the teal bar) -->
        </div>

        <!-- Logo Area -->
        <div class="w-24 md:w-32 flex items-center justify-center shrink-0 pr-4">
             {img_tag}
        </div>
    </header>
    '''
    
    html = re.sub(r'<header.*?</header>', new_header, html, flags=re.DOTALL)


    # --- INYECCIONES ---
    
    # 1. CDN Compatibles (Chart.js 3.9.1 + Datalabels 2.2.0)
    head_libs = '''
    <script src="https://cdn.jsdelivr.net/npm/chart.js@3.9.1/dist/chart.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0"></script>
    '''
    
    html = re.sub(r'<script src=".*?chart\.js.*?"></script>', '', html)
    html = re.sub(r'<script src=".*?chartjs-plugin-datalabels.*?"></script>', '', html)
    html = html.replace('<head>', f'<head>{head_libs}')

    # 2. Contenedores de Tablas (Multi-Select)
    def build_container_html(container_id, title, default_key):
        # Generamos las opciones del dropdown (Checkboxes)
        # Checkeamos el default
        opts = ""
        keys = []
        labels = []
        
        # Opciones Comunas 1-15
        for i in range(1, 16):
            key = f"c{i}"
            is_checked = "checked" if key == default_key else ""
            keys.append(key)
            labels.append(f"Comuna {i}")
            opts += f'''
            <label class="flex items-center px-4 py-2 hover:bg-gray-100 cursor-pointer">
                <input type="checkbox" id="{container_id}_chk_{key}"
                       class="form-checkbox h-4 w-4 text-teal-600 transition duration-150 ease-in-out" 
                       {is_checked} 
                       onchange="updateSelection('{container_id}', '{key}', this.checked)">
                <span class="ml-2 text-gray-700">Comuna {i}</span>
            </label>
            '''

        # Opcion Total
        key_total = "total"
        is_checked_total = "checked" if key_total == default_key else ""
        opts += f'''
            <div class="border-t border-gray-200 my-1"></div>
            <label class="flex items-center px-4 py-2 hover:bg-gray-100 cursor-pointer bg-gray-50">
                <input type="checkbox" id="{container_id}_chk_{key_total}"
                       class="form-checkbox h-4 w-4 text-teal-600 transition duration-150 ease-in-out" 
                       {is_checked_total} 
                       onchange="updateSelection('{container_id}', '{key_total}', this.checked)">
                <span class="ml-2 text-gray-800 font-bold">Total Ciudad</span>
            </label>
        '''
        
        # Nombre inicial del boton
        default_label = f"Comuna {default_key.replace('c','')}" if default_key != 'total' else "Total Ciudad"

        return f'''
            <div class="bg-white rounded-xl shadow-lg overflow-visible border border-gray-200 z-10 relative">
                <div class="bg-teal-600 p-4 text-white font-bold text-lg flex justify-between items-center rounded-t-xl">
                    <span>{title}</span>
                    
                    <!-- Custom Dropdown Trigger -->
                    <div class="relative inline-block text-left w-48">
                        <div>
                            <button type="button" 
                                    onclick="toggleDropdown('dropdown_{container_id}')"
                                    class="inline-flex justify-between w-full rounded-md border border-teal-500 shadow-sm px-4 py-2 bg-white text-sm font-medium text-gray-700 hover:bg-gray-50 focus:outline-none" 
                                    id="btn_{container_id}">
                                <span id="label_{container_id}" class="truncate">{default_label}</span>
                                <svg class="-mr-1 ml-2 h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                                    <path fill-rule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clip-rule="evenodd" />
                                </svg>
                            </button>
                        </div>

                        <!-- Dropdown Menu -->
                        <div id="dropdown_{container_id}" 
                             class="hidden absolute right-0 mt-2 w-56 rounded-md shadow-lg bg-white ring-1 ring-black ring-opacity-5 z-50 overflow-y-auto max-h-60 origin-top-right">
                            <div class="py-1" role="menu">
                                {opts}
                            </div>
                        </div>
                    </div>

                </div>
                <div class="overflow-x-auto rounded-b-xl" id="{container_id}"></div>
            </div>
        '''

    new_section_content = f'''
        <section class="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
            {build_container_html('table1', 'Panel Izquierdo', 'c2')}
            {build_container_html('table2', 'Panel Derecho', 'total')}
        </section>
    '''
    
    html = re.sub(
        r'<!-- SECCION 1: TABLAS -->\s*<section.*?>(.*?)</section>', 
        f'<!-- SECCION 1: TABLAS -->\n{new_section_content}', 
        html, 
        flags=re.DOTALL
    )

    # 3. Gráficos Duales (Comuna 2 y Comuna 14)
    # Helper para crear seccion de grafico
    def build_chart_section(id_canvas, title):
        return f'''
        <section class="bg-white rounded-xl shadow-lg p-6 border border-gray-200">
            <h2 class="text-xl font-bold text-gray-800 mb-6 border-b pb-2">{title}</h2>
            <div class="relative h-96 w-full">
                <canvas id="{id_canvas}"></canvas>
            </div>
        </section>
        '''
    
    charts_html = f'''
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {build_chart_section('dniChart', "Evolución Semanal de DNI's (Operación Comuna 2)")}
        {build_chart_section('dniChart14', "Evolución Semanal de DNI's (Operación Comuna 14)")}
    </div>
    '''

    html = re.sub(
        r'<!-- SECCION 2: GRAFICOS -->\s*<section.*?</section>',
        f'<!-- SECCION 2: GRAFICOS -->\n{charts_html}',
        html,
        flags=re.DOTALL
    )

    # 4. Lógica JS
    json_all = json.dumps(all_data)
    json_chart_c2 = json.dumps(chart_json_c2)
    json_chart_c14 = json.dumps(chart_json_c14)

    js_logic = f'''
    <script>
        // DATOS GLOBALES
        const allComunaData = {json_all};
        const chartDataC2 = {json_chart_c2};
        const chartDataC14 = {json_chart_c14};
        
        // ESTADO DE SELECCION
        const appState = {{
            table1: new Set(['c2']),
            table2: new Set(['total'])
        }};

        // LOGICA DE AGREGACION
        function aggregateData(keys) {{
            if (keys.length === 0) return null;
            
            // Si es solo una, retornamos directo
            if (keys.length === 1) {{
                const k = keys[0];
                const d = allComunaData[k];
                // Ajuste Linea Base: mostrar solo si es C2, C14 o Total
                const showBase = ['c2', 'c14', 'total'].includes(k);
                
                // Clonamos para no mutar el original
                const rows = d.rows.map(r => ({{
                    ...r,
                    base: showBase ? r.base : "-"
                }}));
                return {{ weeks: d.weeks, rows: rows }};
            }}

            // MULTI SELECCION (AGREGAR)
            const weeks = allComunaData['total'].weeks; // Todas tienen las mismas semanas
            
            // Estructura para sumarizar
            // Indices: 0=Interv Total, 1=Deriv CIS, 2=Llamados, 3=%Contacta, 4=%NoContacta, 5=%SinCubrir
            // Pero labels pueden variar, usamos indices fijos asumiendo orden constante
            
            // Inicializar acumuladores por semana (8 semanas)
            const acc = {{
                interv: new Array(8).fill(0),
                deriv: new Array(8).fill(0),
                llamados: new Array(8).fill(0), // Total Automatica
                contacta_count: new Array(8).fill(0),
                no_contacta_count: new Array(8).fill(0),
                sin_cubrir_count: new Array(8).fill(0)
            }};

            keys.forEach(k => {{
                const d = allComunaData[k];
                if (!d) return;

                d.rows[0].vals.forEach((v, i) => acc.interv[i] += parseInt(v)); // Interv Total
                d.rows[1].vals.forEach((v, i) => acc.deriv[i] += parseInt(v));  // Deriv CIS
                d.rows[2].vals.forEach((v, i) => acc.llamados[i] += parseInt(v)); // Llamados 108 (Total Autom)
                
                // Parsers para porcentajes: "XX% (YYY)" -> extact YYY
                const parseCount = (str) => {{
                    const m = str.match(/\\((\\d+)\\)/);
                    return m ? parseInt(m[1]) : 0;
                }};

                d.rows[3].vals.forEach((v, i) => acc.contacta_count[i] += parseCount(v));
                d.rows[4].vals.forEach((v, i) => acc.no_contacta_count[i] += parseCount(v));
                d.rows[5].vals.forEach((v, i) => acc.sin_cubrir_count[i] += parseCount(v));
            }});

            // Reconstruir Rows
            const fmtPct = (count, total) => {{
                if (total === 0) return "0% (0)";
                const pct = Math.round((count / total) * 100);
                return `${{pct}}% (${{count}})`;
            }};

            const rows = [
                {{ label: 'Intervenciones totales', base: '-', vals: acc.interv }},
                {{ label: 'Derivaciones CIS', base: '-', vals: acc.deriv }},
                {{ label: 'Llamados 108', base: '-', vals: acc.llamados }},
                {{ label: '% Se contacta', base: '-', vals: acc.contacta_count.map((c, i) => fmtPct(c, acc.llamados[i])) }},
                {{ label: '% No se contacta', base: '-', vals: acc.no_contacta_count.map((c, i) => fmtPct(c, acc.llamados[i])) }},
                {{ label: '% Sin cubrir', base: '-', vals: acc.sin_cubrir_count.map((c, i) => fmtPct(c, acc.llamados[i])) }}
            ];

            return {{ weeks: weeks, rows: rows }};
        }}

        // UI HELPERS
        function toggleDropdown(id) {{
            const el = document.getElementById(id);
            if (el.classList.contains('hidden')) {{
                // Close others if needed, but simple toggle is enough
                el.classList.remove('hidden');
            }} else {{
                el.classList.add('hidden');
            }}
        }}

        // Close dropdowns when clicking outside
        window.onclick = function(event) {{
            if (!event.target.closest('.relative.inline-block')) {{
                document.querySelectorAll('[id^="dropdown_"]').forEach(el => {{
                    el.classList.add('hidden');
                }});
            }}
        }}

        function updateSelection(containerId, key, isChecked) {{
            const set = appState[containerId];
            if (isChecked) {{
                set.add(key);
            }} else {{
                set.delete(key);
            }}
            
            // Actualizar Label Boton
            const btnLabel = document.getElementById(`label_${{containerId}}`);
            if (set.size === 0) {{
                btnLabel.textContent = "Ninguna";
            }} else if (set.size === 1) {{
                const k = Array.from(set)[0];
                btnLabel.textContent = k === 'total' ? 'Total Ciudad' : `Comuna ${{k.replace('c','')}}`;
            }} else {{
                btnLabel.textContent = `${{set.size}} Seleccionadas`;
            }}

            renderTable(containerId);
        }}

        // RENDER TABLA
        function renderTable(containerId) {{
            const keys = Array.from(appState[containerId]);
            const data = aggregateData(keys);
            const container = document.getElementById(containerId);
            
            if (!data) {{
                container.innerHTML = '<div class="p-8 text-center text-gray-400">Seleccione al menos una opción</div>';
                return;
            }}
            
            let ths = '<th class="p-3 text-left">Indicadores</th><th class="p-3 w-20 bg-teal-800">Línea Base</th>';
            data.weeks.forEach(w => ths += `<th class="p-3 w-24">${{w}}</th>`);
            
            let trs = '';
            data.rows.forEach((r, idx) => {{
                let tds = '';
                r.vals.forEach(v => tds += `<td class="p-3 text-gray-800">${{v}}</td>`);
                trs += `
                    <tr class="hover:bg-yellow-50 transition-colors">
                        <td class="p-3 text-left font-semibold text-gray-700 bg-gray-50 sticky left-0">${{r.label}}</td>
                        <td class="p-3 font-bold text-gray-600 bg-gray-100 border-r border-gray-300">${{r.base}}</td>
                        ${{tds}}
                    </tr>`;
            }});

            container.innerHTML = `<table class="w-full text-sm text-center"><thead><tr class="bg-teal-700 text-white">${{ths}}</tr></thead><tbody class="divide-y divide-gray-200">${{trs}}</tbody></table>`;
        }}

        // INIT
        renderTable('table1');
        renderTable('table2');

        // FUNCIÓN CHART GENERICA
        function initChart(canvasId, dataJson) {{
            const ctx = document.getElementById(canvasId).getContext('2d');
            if (typeof ChartDataLabels !== 'undefined') {{
                Chart.register(ChartDataLabels);
            }}

            new Chart(ctx, {{
                type: 'bar',
                data: dataJson,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {{
                        x: {{ stacked: true, grid: {{ display: false }} }},
                        y: {{ stacked: true, beginAtZero: true }}
                    }},
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{ mode: 'index', intersect: false }},
                        datalabels: {{
                            color: 'white',
                            font: {{ weight: 'bold', size: 10 }},
                            formatter: (value) => value > 0 ? value : ''
                        }}
                    }}
                }},
                plugins: [{{
                    id: 'totalLabels',
                    afterDatasetsDraw: (chart) => {{
                        const ctx = chart.ctx;
                        chart.data.labels.forEach((label, index) => {{
                            let total = 0;
                            chart.data.datasets.forEach(ds => total += ds.data[index]);
                            if (total > 0) {{
                                const meta = chart.getDatasetMeta(chart.data.datasets.length - 1);
                                const x = meta.data[index].x;
                                const y = meta.data[index].y;
                                ctx.fillStyle = 'black';
                                ctx.font = 'bold 11px Inter';
                                ctx.textAlign = 'center';
                                ctx.fillText(total, x, y - 5);
                            }}
                        }});
                    }}
                }}]
            }});
        }}

        initChart('dniChart', chartDataC2);
        initChart('dniChart14', chartDataC14);

    </script>
    '''

    # Usamos replace    # Reemplazamos el script
    # Usamos lambda para evitar que re.sub interprete los backslashes del JS como escapes
    html = re.sub(
        r'<script>\s*// Datos inyectados desde Python.*?</script>', 
        lambda _: js_logic, 
        html, 
        flags=re.DOTALL
    )  # Info Header
    html = re.sub(r'Actualizado: .*?</div>', f'Actualizado: {last_update}</div>', html)
    if chart_json_c2['labels']:
        last_week_label = chart_json_c2['labels'][-1]
        html = re.sub(r'Semana: .*?</div>', f'Semana: {last_week_label}</div>', html)

    with open(OUTPUT_HTML_PATH, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print("✅ Dashboard Interactivo generado.")

if __name__ == '__main__':
    main()
