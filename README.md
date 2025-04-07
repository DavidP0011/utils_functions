# utils_functions

**utils_functions** es un repositorio de funciones utilitarias que provee soporte para tareas comunes en procesos ETL y otras operaciones de integración y análisis. El paquete incluye módulos para:

- Inicialización y autenticación en Google Cloud Platform (GCP).
- Manipulación y procesamiento de archivos PDF.
- Transformación y análisis de datos tabulares.

## Características

- **GCP Initialization Utilities**  
  El módulo `dpm_GCP_ini_utils.py` contiene funciones para facilitar la autenticación en GCP, utilizando credenciales locales o accediendo a Secret Manager.

- **Funciones Legadas**  
  El módulo `dpm_old.py` agrupa funciones de versiones anteriores, que pueden servir como referencia o respaldo.

- **PDF Utilities**  
  En `dpm_pdf_utils.py` encontrarás funciones para extraer, fusionar o manipular archivos PDF, utilizando la biblioteca PyPDF2.

- **Table Processing**  
  El módulo `dpm_tables.py` ofrece herramientas para trabajar con datos tabulares, facilitando la transformación y análisis de DataFrames con pandas.

## Instalación

Puedes instalar el paquete directamente desde GitHub utilizando pip:

```bash
pip install -q git+https://github.com/DavidP0011/utils_functions.git
