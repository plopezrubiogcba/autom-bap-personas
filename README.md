# Autom BAP Personas - Red de Atención

Este repositorio contiene el sistema de automatización para el procesamiento, análisis y visualización de datos de intervenciones de la **Red de Atención** de la Ciudad de Buenos Aires.

El objetivo principal es transformar planillas operativas semanales en un **Dashboard Interactivo HTML** que permite monitorear indicadores clave por Comuna y la evolución de la población asistida.

## 🚀 Funcionalidades Principales

1.  **Ingesta Automática**: Descarga y procesa archivos Excel desde Google Drive.
2.  **ETL & Normalización**: Limpieza de datos, normalización de textos y georreferenciación (asignación de Comunas).
3.  **Base Histórica**: Mantiene un archivo incremental `parquet` ("2025_historico_limpio.parquet") para análisis longitudinal.
4.  **Generación de Dashboard**: Crea un reporte web autónomo (`.html`) con:
    *   Tablas comparativas dinámicas por Comuna vs. Total Ciudad.
    *   Cálculo de líneas de base y porcentajes de efectividad (Se contacta / No se contacta / Sin cubrir).
    *   Gráficos de evolución de población (Nuevos vs. Recurrentes vs. Migratorios).
    *   Diseño *responsive* con Tailwind CSS e interactividad con Chart.js.

## 📂 Estructura del Proyecto

### Scripts Principales

*   **`main.py`**:
    *   Punto de entrada (Cloud Function). Recibe un evento (ej. webhook o cron), descarga el archivo entrante y orquesta la ejecución del procesador y los reportes.
*   **`data_processor.py`**:
    *   Motor ETL. Se encarga de conectar con Google Drive API, descargar los datos, limpiar el dataset (fase "CLEAN"), asignar coordenadas geográficas y guardar el histórico.
*   **`dashboard_generator.py`**:
    *   Script encargado de la capa visual.
    *   Lee el histórico procesado.
    *   Calcula KPIs semanales y métricas de evolución de DNI (lógica de recurrentes/nuevos).
    *   Inyecta los datos en el template HTML (`reporte_tablero.html`) y genera el archivo final `reporte_autom_bap.html`.
    *   Maneja la lógica de visualización (colores, logos, fechas).
*   **`indicadores.py`**:
    *   Librería de cálculo de métricas específicas (derivaciones a CIS, llamados 108, clasificación estricta de resultados de intervención).
*   **`looker_reporter.py`**:
    *   Módulo auxiliar para conectar y actualizar fuentes de datos para dashboards legacy en Looker Studio (si aplica).

### Archivos de Recursos

*   **`reporte_tablero.html`**: Plantilla base HTML/Tailwind para el dashboard.
*   **`reporte_autom_bap.html`**: El producto final generado. Un archivo HTML autocontenido listo para compartir o hostear.
*   **`credentials.json`**: (Ignorado en git) Credenciales de servicio para acceso a Google Cloud/Drive.
*   **`2025_historico_limpio.parquet`**: Base de datos columnar optimizada con todo el historial de intervenciones.

## 🛠️ Flujo de Trabajo (Workflow)


graph TD
    A[Excel Semanal (Drive/Mail)] -->|Trigger| B(main.py)
    B -->|Descarga| C(data_processor.py)
    C -->|ETL + Geocode| D[(Historico .parquet)]
    D -->|Lectura| E(dashboard_generator.py)
    E -->|Calculo KPIs| F{Generación HTML}
    F -->|Inyección JSON+JS| G[reporte_autom_bap.html]
    G -->|Deploy| H[GitHub Pages / Web]


## 📊 Lógica de Negocio Destacada

### Clasificación de Contacto
El sistema aplica reglas estrictas para determinar el resultado de una intervención:
*   **Se contacta**: Interacción efectiva con la persona.
*   **No se contacta**: La persona rechaza o no se logra establecer vínculo.
*   **Sin cubrir**: Casos pendientes o cancelados operativamente.

### Evolución de Población (Comuna 2)
Algoritmo cronológico que analiza el historial de cada DNI para clasificarlo semanalmente como:
*   **Nuevo**: Primera vez visto en la red.
*   **Recurrente**: Visto previamente en la misma comuna recientemente.
*   **Migratorio**: Visto previamente pero en otra comuna.

## 💻 Instalación y Ejecución Local

1.  **Requisitos**: Python 3.9+
2.  **Instalar dependencias**:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Configuración**:
    *   Colocar `credentials.json` en la raíz.
    *   Asegurar acceso a la carpeta de Drive definida en `FOLDER_ID_DB`.
4.  **Ejecutar Generador**:
    ```bash
    python dashboard_generator.py
    ```
    Esto actualizará el archivo `reporte_autom_bap.html` localmente.

## 🚀 Despliegue

El proyecto está configurado para desplegarse automáticamente mediante **GitHub Actions** en **GitHub Pages**, permitiendo acceso público o restringido al tablero actualizado semanalmente.
