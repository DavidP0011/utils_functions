# __________________________________________________________________________________________________________________________________________________________
# Repositorio de funciones
# __________________________________________________________________________________________________________________________________________________________
from google.cloud import bigquery
import pandas as pd
import pandas_gbq

from dpm_common_functions import _ini_authenticate_API

import unicodedata
import re
import time
import os
import io
import json
from typing import Dict



# ----------------------------------------------------------------------------
# fields_name_format()
# ----------------------------------------------------------------------------
def fields_name_format(config):
    """
    Formatea nombres de campos de datos según configuraciones específicas.
    
    Parámetros en config:
      - fields_name_raw_list (list): Lista de nombres de campos.
      - formato_final (str, opcional): 'CamelCase', 'snake_case', 'Sentence case', o None.
      - reemplazos (dict, opcional): Diccionario de términos a reemplazar.
      - siglas (list, opcional): Lista de siglas que deben mantenerse en mayúsculas.
    
    Retorna:
        pd.DataFrame: DataFrame con columnas 'Campo Original' y 'Campo Formateado'.
    """
    print("[START 🚀] Iniciando formateo de nombres de campos...", flush=True)
    
    def aplicar_reemplazos(field, reemplazos):
        for key, value in sorted(reemplazos.items(), key=lambda x: -len(x[0])):
            if key in field:
                field = field.replace(key, value)
        return field

    def formatear_campo(field, formato, siglas):
        if formato is None or formato is False:
            return field
        words = [w for w in re.split(r'[_\-\s]+', field) if w]
        if formato == 'CamelCase':
            return ''.join(
                word.upper() if word.upper() in siglas
                else word.capitalize() if idx == 0
                else word.lower()
                for idx, word in enumerate(words)
            )
        elif formato == 'snake_case':
            return '_'.join(
                word.upper() if word.upper() in siglas
                else word.lower() for word in words
            )
        elif formato == 'Sentence case':
            return ' '.join(
                word.upper() if word.upper() in siglas
                else word.capitalize() if idx == 0
                else word.lower()
                for idx, word in enumerate(words)
            )
        else:
            raise ValueError(f"Formato '{formato}' no soportado.")
    
    resultado = []
    for field in config.get('fields_name_raw_list', []):
        original_field = field
        field = aplicar_reemplazos(field, config.get('reemplazos', {}))
        formatted_field = formatear_campo(field, config.get('formato_final', 'CamelCase'), [sig.upper() for sig in config.get('siglas', [])])
        resultado.append({'Campo Original': original_field, 'Campo Formateado': formatted_field})
    
    df_result = pd.DataFrame(resultado)
    print("[END [FINISHED 🏁]] Formateo de nombres completado.\n", flush=True)
    return df_result









# ----------------------------------------------------------------------------
# tables_consolidate_duplicates_df()
# ----------------------------------------------------------------------------
def tables_consolidate_duplicates_df(config_dic: dict):
    """Consolida dos ``pandas.DataFrame`` resolviendo duplicados.

    La función aplica la política indicada en *config_dic* y devuelve un
    ``pd.DataFrame`` limpio (y metadatos opcionales) siguiendo el Manual de
    Estilo DPM.

    Args:
        config_dic (dict):
            validate_df_schemas_match (bool, opcional): Valida coincidencia de
                columnas entre DataFrames (default ``True``).
            df_initial (pd.DataFrame): DataFrame fuente prioritario.
            df_to_merge (pd.DataFrame): DataFrame a fusionar.
            id_fields (list[str]): Campos clave para identificar registros.
            duplicate_policy (str): ``keep_newest`` | ``keep_oldest`` |
                ``keep_df_initial`` | ``keep_df_to_merge``.
            duplicate_date_field (str, opcional): Campo fecha para políticas
                basadas en tiempo.
            duplicate_date_field_format_str (str, opcional): Formato
                ``datetime.strptime`` de *duplicate_date_field*.
            return_metadata (bool, opcional): Si ``True`` devuelve metadatos.

    Returns:
        pd.DataFrame | tuple[pd.DataFrame, dict]: DataFrame consolidado y,
        opcionalmente, metadatos del proceso.

    Raises:
        ValueError: Parámetros erróneos o esquemas distintos.
        TypeError: *df_initial* o *df_to_merge* no son ``pd.DataFrame``.
    """

    # ──────────────────────────
    # Imports locales mínimos
    # ──────────────────────────
    import pandas as pd  # type: ignore
    from datetime import datetime

    # ──────────────────────────
    # 1️⃣ Extracción y validación de parámetros
    # ──────────────────────────
    allowed_policies_set = {
        "keep_newest",
        "keep_oldest",
        "keep_df_initial",
        "keep_df_to_merge",
    }

    validate_schema_bool: bool = config_dic.get("validate_df_schemas_match", True)
    df_initial_df = config_dic.get("df_initial")
    df_to_merge_df = config_dic.get("df_to_merge")
    id_fields_list: list[str] = config_dic.get("id_fields", [])
    policy_str: str = config_dic.get("duplicate_policy", "keep_newest")
    date_col_str: str | None = config_dic.get("duplicate_date_field")
    date_fmt_str: str | None = config_dic.get("duplicate_date_field_format_str")
    return_meta_bool: bool = config_dic.get("return_metadata", False)

    print(f"[CONSOLIDATION START ▶️] {datetime.now().isoformat(timespec='seconds')}", flush=True)
    print(
        f"INFO ℹ️ id_fields={id_fields_list} | policy={policy_str}", flush=True
    )

    # Validaciones básicas
    if policy_str not in allowed_policies_set:
        raise ValueError(f"duplicate_policy debe ser uno de {allowed_policies_set}")

    if not isinstance(df_initial_df, pd.DataFrame) or not isinstance(
        df_to_merge_df, pd.DataFrame
    ):
        raise TypeError("df_initial y df_to_merge deben ser DataFrame")

    if not id_fields_list:
        raise ValueError("id_fields no puede ser vacío")

    for col_str in id_fields_list:
        if col_str not in df_initial_df.columns or col_str not in df_to_merge_df.columns:
            raise ValueError(
                f"Columna clave '{col_str}' ausente en alguno de los DataFrames"
            )

    if policy_str in ("keep_newest", "keep_oldest"):
        if not date_col_str:
            raise ValueError(
                "duplicate_date_field es obligatorio para la política basada en fecha"
            )
        if (
            date_col_str not in df_initial_df.columns
            or date_col_str not in df_to_merge_df.columns
        ):
            raise ValueError(f"Campo fecha '{date_col_str}' inexistente en ambos DataFrames")

    # ──────────────────────────
    # 2️⃣ Validación opcional de esquemas
    # ──────────────────────────
    if validate_schema_bool and set(df_initial_df.columns) != set(df_to_merge_df.columns):
        diff_left_set = set(df_initial_df.columns) - set(df_to_merge_df.columns)
        diff_right_set = set(df_to_merge_df.columns) - set(df_initial_df.columns)
        print(
            f"[VALIDATION [ERROR ❌]] Schemas difieren – izquierda: {diff_left_set} | derecha: {diff_right_set}",
            flush=True,
        )
        raise ValueError("Esquemas distintos entre DataFrames")

    # ──────────────────────────
    # 3️⃣ Concatenar y resolver duplicados
    #     • keep_newest / keep_oldest  → usa fecha
    #     • keep_df_initial / keep_df_to_merge → orden de concat
    # ──────────────────────────
    df_all_df = pd.concat([df_initial_df, df_to_merge_df], ignore_index=True)
    duplicates_before_int: int = len(df_all_df)

    # Helper para convertir fecha
    def _parse_date_dt(series, fmt):
        return pd.to_datetime(series, format=fmt, errors="coerce")

    if policy_str in ("keep_newest", "keep_oldest"):
        df_all_df[date_col_str] = _parse_date_dt(df_all_df[date_col_str], date_fmt_str)

        # Rellena NaT para que idxmax/min tomen decisiones coherentes
        if policy_str == "keep_newest":
            df_all_df["_temp_sort"] = df_all_df[date_col_str].fillna(pd.Timestamp.min)
            idx = (
                df_all_df.groupby(id_fields_list, sort=False)["_temp_sort"].idxmax()
            )
        else:  # keep_oldest
            df_all_df["_temp_sort"] = df_all_df[date_col_str].fillna(pd.Timestamp.max)
            idx = (
                df_all_df.groupby(id_fields_list, sort=False)["_temp_sort"].idxmin()
            )

        result_df = df_all_df.loc[idx].copy()
        result_df = result_df.drop(columns=["_temp_sort"])  # limpia helper
    else:
        # Mantener orden de concatenación; "first" preserva df_initial_df
        keep_flag = "first" if policy_str == "keep_df_initial" else "last"
        result_df = (
            df_all_df.drop_duplicates(subset=id_fields_list, keep=keep_flag)
            .copy()
        )

    result_df.reset_index(drop=True, inplace=True)

    # ──────────────────────────
    # 4️⃣ Alinear dtypes al esquema original
    # ──────────────────────────
    for col_str, dtype in df_initial_df.dtypes.items():
        try:
            result_df[col_str] = result_df[col_str].astype(dtype, copy=False)
        except (ValueError, TypeError):
            print(
                f"[TYPE WARN ⚠️] No se pudo convertir '{col_str}' a {dtype}", flush=True
            )

    # ──────────────────────────
    # 5️⃣ Metadatos y salida
    # ──────────────────────────
    duplicates_resolved_int = duplicates_before_int - len(result_df)
    print(
        f"FINISHED ✅ registros finales={len(result_df)} | duplicados_resueltos={duplicates_resolved_int}",
        flush=True,
    )

    if return_meta_bool:
        metadata_dic: dict = {
            "timestamp": datetime.now(),
            "initial_records": len(df_initial_df),
            "merge_records": len(df_to_merge_df),
            "final_records": len(result_df),
            "duplicates_resolved": duplicates_resolved_int,
            "records_added": len(result_df) - len(df_initial_df),
        }
        return result_df, metadata_dic

    return result_df














# ----------------------------------------------------------------------------
# DType_df_to_df()
# ----------------------------------------------------------------------------
def DType_df_to_df(config: Dict[str, Any]):
    """Copia *dtypes* entre DataFrames con coerción robusta.

    Claves flexibles admitidas en el `config`:
    - ``reference_dtype_df`` / ``source_df`` → DataFrame cuya *signature* de `dtypes` actúa de referencia.
    - ``target_dtype_df`` / ``targete_dtype_df`` / ``target_df`` → DataFrame al que se le aplicarán los dtypes.

    Parámetros opcionales:
    ---------------------
    inplace : bool  (default ``True``)
        Si *True*, muta el ``target`` *in‑place*; si *False* trabaja con una copia.
    return_metadata : bool  (default ``True``)
        Devuelve un segundo objeto con información de columnas casteadas / fallidas / omitidas.
    decimal_comma : bool  (default ``True``)
        Pre‑procesa strings reemplazando `"," → "."` antes de la coerción numérica.

    Returns
    -------
    pd.DataFrame | Tuple[pd.DataFrame, dict]
        El DataFrame transformado y, opcionalmente, un diccionario con metadatos.
    """
    import pandas as pd
    import numpy as np
    from typing import Dict, Any, List, Tuple

    # --------------------- VALIDACIÓN DE ENTRADA ---------------------
    source_df = config.get("reference_dtype_df", config.get("source_df"))
    target_df = config.get("target_dtype_df", config.get("targete_dtype_df", config.get("target_df")))

    if source_df is None or target_df is None:
        raise ValueError("[VALIDATION ❌] Debes proporcionar 'reference_dtype_df/source_df' y 'target_dtype_df/target_df'.")
    if not isinstance(source_df, pd.DataFrame):
        raise ValueError("[VALIDATION ❌] 'reference_dtype_df' no es DataFrame.")
    if not isinstance(target_df, pd.DataFrame):
        raise ValueError("[VALIDATION ❌] 'target_dtype_df' no es DataFrame.")

    inplace: bool = config.get("inplace", True)
    return_metadata: bool = config.get("return_metadata", True)
    decimal_comma: bool = config.get("decimal_comma", True)

    if not inplace:
        target_df = target_df.copy()

    print("🔹🔹🔹 [START ▶️] DTYPE COPY", flush=True)

    # ------------------------ LÓGICA PRINCIPAL ------------------------
    common_cols: List[str] = [c for c in source_df.columns if c in target_df.columns]
    if not common_cols:
        print("[DTYPE COPY ⚠️] No hay columnas coincidentes.", flush=True)
        empty_meta = {"cols_casted": [], "cols_failed": [], "cols_skipped": []}
        return (target_df, empty_meta) if return_metadata else target_df

    cols_casted, cols_failed, cols_skipped = [], [], []

    def _safe_cast(col: pd.Series, tgt_dtype) -> Tuple[pd.Series, bool]:
        """Intenta castear la *Series* a `tgt_dtype`.

        Estrategia:
        1. Intento directo ``astype``.
        2. Si falla y `tgt_dtype` es numérico → ``pd.to_numeric`` con coerción, gestionando
           *nullable integer* cuando hay *NaN*.
        3. Si falla y es fecha → ``pd.to_datetime``.
        4. *Fallback* → string; marca la conversión como fallida.
        """
        # ── Paso 0: pre‑procesar comas decimales ────────────────────
        series_proc = col.astype(str).str.replace(",", ".", regex=False) if decimal_comma else col

        # ── Paso 1: intento directo ─────────────────────────────────
        try:
            return col.astype(tgt_dtype), True
        except Exception:
            pass  # continuará con coerciones especializadas

        # ── Paso 2: coerción numérica ───────────────────────────────
        if pd.api.types.is_numeric_dtype(tgt_dtype):
            coerced = pd.to_numeric(series_proc, errors="coerce")
            if pd.api.types.is_integer_dtype(tgt_dtype):
                # Si existen NaN y el dtype destino es entero ⇒ usar Int64 (nullable)
                if coerced.isna().any():
                    return coerced.astype("Int64"), not coerced.isna().all()
                # sin NaNs: redondea y castea al entero exacto
                return coerced.round().astype(tgt_dtype), True
            # destino float
            return coerced.astype("float64"), not coerced.isna().all()

        # ── Paso 3: coerción de fechas ───────────────────────────────
        try:
            if np.issubdtype(tgt_dtype, np.datetime64):
                coerced_dt = pd.to_datetime(series_proc, errors="coerce")
                return coerced_dt, not coerced_dt.isna().all()
        except Exception:
            pass

        # ── Paso 4: fallback a string ───────────────────────────────
        return series_proc.astype(str), False

    total = len(common_cols)
    for idx, col in enumerate(common_cols, start=1):
        tgt_dtype = source_df[col].dtype
        if target_df[col].dtype == tgt_dtype:
            print(f"[DTYPE COPY ℹ️] ({idx}/{total}) '{col}' ya es {tgt_dtype}. Skipped.", flush=True)
            cols_skipped.append(col)
            continue

        target_df[col], ok = _safe_cast(target_df[col], tgt_dtype)
        if ok:
            print(f"[DTYPE COPY ✅] ({idx}/{total}) '{col}' → {target_df[col].dtype}.", flush=True)
            cols_casted.append(col)
        else:
            print(f"[DTYPE COPY ⚠️] ({idx}/{total}) No se pudo castear '{col}' → {tgt_dtype}.", flush=True)
            cols_failed.append(col)

    print(
        f"[DTYPE COPY ✔️] Cast fin — ok: {len(cols_casted)}/{total} | "
        f"fail: {len(cols_failed)} | skipped: {len(cols_skipped)}",
        flush=True,
    )

    meta = {
        "cols_casted": cols_casted,
        "cols_failed": cols_failed,
        "cols_skipped": cols_skipped,
    }

    return (target_df, meta) if return_metadata else target_df



















# ----------------------------------------------------------------------------
# table_various_sources_to_DF()
# ----------------------------------------------------------------------------
def table_various_sources_to_DF(params: dict) -> pd.DataFrame:
    """
    Extrae datos desde distintos orígenes (archivo, Google Sheets, BigQuery o GCS) y los convierte en un DataFrame.
    
    Parámetros en params:
      - (ver docstring completo en la versión original)
    
    Retorna:
      pd.DataFrame: DataFrame con los datos extraídos y procesados.
    
    Raises:
      RuntimeError: Si ocurre un error al extraer o procesar los datos.
      ValueError: Si faltan parámetros obligatorios para identificar el origen de datos.
    """
    import re
    import io
    import time
    import pandas as pd

    # Para Google Sheets y otros servicios de Google
    import gspread
    try:
        from google.colab import files
    except ImportError:
        pass

    # ────────────────────────────── Utilidades Comunes ──────────────────────────────
    def _imprimir_encabezado(mensaje: str) -> None:
        print(f"\n🔹🔹🔹 {mensaje} 🔹🔹🔹\n", flush=True)

    def _validar_comun(params: dict) -> None:
        if not (params.get('json_keyfile_GCP_secret_id') or params.get('json_keyfile_colab')):
            raise ValueError("[VALIDATION [ERROR ❌]] Falta el parámetro obligatorio 'json_keyfile_GCP_secret_id' o 'json_keyfile_colab' para autenticación.")

    def _apply_common_filters(df: pd.DataFrame, params: dict) -> pd.DataFrame:
        # Filtrado de filas (si corresponde)
        row_start = params.get('source_table_row_start', 0)
        row_end = params.get('source_table_row_end', None)
        if row_end is not None:
            df = df.iloc[row_start:row_end]
        else:
            df = df.iloc[row_start:]
        
        # Filtrado de columnas por posición
        col_start = params.get('source_table_col_start', 0)
        col_end = params.get('source_table_col_end', None)
        if col_end is not None:
            df = df.iloc[:, col_start:col_end]
        else:
            df = df.iloc[:, col_start:]
        
        # Selección de campos específicos si se indica
        if 'source_table_fields_list' in params:
            fields = params['source_table_fields_list']
            fields = [f for f in fields if f in df.columns]
            if fields:
                df = df[fields]
        return df

    def _auto_convert(df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            col_lower = col.lower()
            if "fecha" in col_lower or col_lower == "valor":
                try:
                    df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
                except Exception as e:
                    print(f"[TRANSFORMATION [WARNING ⚠️]] Error al convertir la columna '{col}' a datetime: {e}", flush=True)
            elif col_lower in ['importe', 'saldo']:
                try:
                    df[col] = df[col].apply(lambda x: float(x.replace('.', '').replace(',', '.')) if isinstance(x, str) and x.strip() != '' else x)
                except Exception as e:
                    print(f"[TRANSFORMATION [WARNING ⚠️]] Error al convertir la columna '{col}' a float: {e}", flush=True)
        return df

    # ────────────────────────────── Detección del Origen ──────────────────────────────
    def _es_fuente_archivo(params: dict) -> bool:
        return bool(params.get('file_source_table_path', '').strip())

    def _es_fuente_gsheet(params: dict) -> bool:
        return (not _es_fuente_archivo(params)) and (
            bool(params.get('spreadsheet_source_table_id', '').strip()) and 
            bool(params.get('spreadsheet_source_table_worksheet_name', '').strip())
        )

    def _es_fuente_gbq(params: dict) -> bool:
        return bool(params.get('GBQ_source_table_name', '').strip())

    def _es_fuente_gcs(params: dict) -> bool:
        return bool(params.get('GCS_source_table_bucket_name', '').strip()) and bool(params.get('GCS_source_table_file_path', '').strip())

    # ────────────────────────────── Fuente – Archivo Local ──────────────────────────────
    def _leer_archivo(params: dict) -> pd.DataFrame:
        _imprimir_encabezado("[START 🚀] Iniciando carga del archivo")
        file_path = params.get('file_source_table_path')
        row_skip_empty = params.get('source_table_filter_skip_row_empty_use', True)
        row_start = params.get('source_table_row_start', 0)
        row_end = params.get('source_table_row_end', None)
        nrows = (row_end - row_start) if row_end is not None else None
        col_start = params.get('source_table_col_start', 0)
        col_end = params.get('source_table_col_end', None)

        if not file_path:
            print("[EXTRACTION [WARNING ⚠️]] No se proporcionó 'file_source_table_path'. Suba un archivo desde su ordenador:", flush=True)
            uploaded = files.upload()
            file_path = list(uploaded.keys())[0]
            file_input = io.BytesIO(uploaded[file_path])
            print(f"[EXTRACTION [SUCCESS ✅]] Archivo '{file_path}' subido exitosamente.", flush=True)
        else:
            file_input = file_path

        _, ext = os.path.splitext(file_path)
        ext = ext.lower()

        try:
            print(f"[EXTRACTION [START ⏳]] Leyendo archivo '{file_path}'...", flush=True)
            if ext in ['.xls', '.xlsx']:
                engine = 'xlrd' if ext == '.xls' else 'openpyxl'
                df = pd.read_excel(file_input, engine=engine, skiprows=row_start, nrows=nrows)
            elif ext == '.csv':
                df = pd.read_csv(file_input, skiprows=row_start, nrows=nrows, sep=',')
            elif ext == '.tsv':
                df = pd.read_csv(file_input, skiprows=row_start, nrows=nrows, sep='\t')
            else:
                raise RuntimeError(f"[EXTRACTION [ERROR ❌]] Extensión de archivo '{ext}' no soportada.")
            
            if col_end is not None:
                df = df.iloc[:, col_start:col_end]
            else:
                df = df.iloc[:, col_start:]
            
            if row_skip_empty:
                initial_rows = len(df)
                df.dropna(how='all', inplace=True)
                removed_rows = initial_rows - len(df)
                print(f"[TRANSFORMATION [SUCCESS ✅]] Se eliminaron {removed_rows} filas vacías.", flush=True)
            
            df = df.convert_dtypes()
            df = _auto_convert(df)

            print("\n[METRICS [INFO 📊]] INFORME ESTADÍSTICO DEL DATAFRAME:")
            print(f"  - Total filas: {df.shape[0]}")
            print(f"  - Total columnas: {df.shape[1]}")
            print("  - Tipos de datos por columna:")
            print(df.dtypes)
            print("  - Resumen estadístico (numérico):")
            print(df.describe())
            print("  - Resumen estadístico (incluyendo variables categóricas):")
            print(df.describe(include='all'))
            print(f"\n[END [FINISHED 🏁]] Archivo '{file_path}' cargado correctamente. Filas: {df.shape[0]}, Columnas: {df.shape[1]}", flush=True)
            return df

        except Exception as e:
            error_message = f"[EXTRACTION [ERROR ❌]] Error al leer el archivo '{file_path}': {e}"
            print(error_message, flush=True)
            raise RuntimeError(error_message)

    # ────────────────────────────── Fuente – Google Sheets ──────────────────────────────
    def _leer_google_sheet(params: dict) -> pd.DataFrame:
        from googleapiclient.discovery import build

        spreadsheet_id_raw = params.get("spreadsheet_source_table_id")
        if "spreadsheets/d/" in spreadsheet_id_raw:
            match = re.search(r"/d/([a-zA-Z0-9-_]+)", spreadsheet_id_raw)
            if match:
                spreadsheet_id = match.group(1)
            else:
                raise ValueError("[VALIDATION [ERROR ❌]] No se pudo extraer el ID de la hoja de cálculo desde la URL proporcionada.")
        else:
            spreadsheet_id = spreadsheet_id_raw

        worksheet_name = params.get("spreadsheet_source_table_worksheet_name")
        if not spreadsheet_id or not worksheet_name:
            raise ValueError("[VALIDATION [ERROR ❌]] Faltan 'spreadsheet_source_table_id' o 'spreadsheet_source_table_worksheet_name'.")

        try:
            scope_list = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            # Determinación del project_id según el entorno
            ini_env = params.get("ini_environment_identificated")
            if not ini_env:
                raise ValueError("[VALIDATION [ERROR ❌]] Falta la key 'ini_environment_identificated' en params.")
            project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") if ini_env == "COLAB_ENTERPRISE" else ini_env

            # Uso de _ini_authenticate_API para la autenticación
            creds = _ini_authenticate_API(params, project_id)
            creds = creds.with_scopes(scope_list)
            
            service = build('sheets', 'v4', credentials=creds)
            range_name = f"{worksheet_name}"
            print("[EXTRACTION [START ⏳]] Extrayendo datos de Google Sheets...", flush=True)
            result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
            data = result.get('values', [])
            if not data:
                print("[EXTRACTION [WARNING ⚠️]] No se encontraron datos en la hoja especificada.", flush=True)
                return pd.DataFrame()
            
            # Se obtiene el encabezado y se ajustan las filas para que todas tengan la misma longitud.
            header = data[0]
            n_columns = len(header)
            data_fixed = []
            for row in data[1:]:
                if len(row) < n_columns:
                    row = row + [None] * (n_columns - len(row))
                elif len(row) > n_columns:
                    row = row[:n_columns]
                data_fixed.append(row)

            df = pd.DataFrame(data_fixed, columns=header)
            print(f"[EXTRACTION [SUCCESS ✅]] Datos extraídos con éxito de la hoja '{worksheet_name}'.", flush=True)
            return df

        except Exception as e:
            raise ValueError(f"[EXTRACTION [ERROR ❌]] Error al extraer datos de Google Sheets: {e}")

    # ────────────────────────────── Fuente – BigQuery ──────────────────────────────
    def _leer_gbq(params: dict) -> pd.DataFrame:
        scope_list = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive"]
        ini_env = params.get("ini_environment_identificated")
        if not ini_env:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta la key 'ini_environment_identificated' en params.")
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") if ini_env == "COLAB_ENTERPRISE" else ini_env

        # Uso de _ini_authenticate_API para la autenticación en BigQuery
        from google.oauth2.service_account import Credentials
        creds = _ini_authenticate_API(params, project_id)
        creds = creds.with_scopes(scope_list)
    
        client_bq = bigquery.Client(credentials=creds, project=project_id)
        gbq_table = params.get("GBQ_source_table_name")
        if not gbq_table:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta el parámetro 'GBQ_source_table_name' para BigQuery.")
        try:
            query = f"SELECT * FROM `{gbq_table}`"
            print(f"[EXTRACTION [START ⏳]] Ejecutando consulta en BigQuery: {query}", flush=True)
            df = client_bq.query(query).to_dataframe()
            print("[EXTRACTION [SUCCESS ✅]] Datos extraídos con éxito de BigQuery.", flush=True)
            return df
        except Exception as e:
            raise RuntimeError(f"[EXTRACTION [ERROR ❌]] Error al extraer datos de BigQuery: {e}")

    # ────────────────────────────── Fuente – Google Cloud Storage (GCS) ──────────────────────────────
    def _leer_gcs(params: dict) -> pd.DataFrame:
        scope_list = ["https://www.googleapis.com/auth/devstorage.read_only"]
        ini_env = params.get("ini_environment_identificated")
        if not ini_env:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta la key 'ini_environment_identificated' en params.")
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") if ini_env == "COLAB_ENTERPRISE" else ini_env

        # Uso de _ini_authenticate_API para la autenticación en GCS
        from google.oauth2.service_account import Credentials
        creds = _ini_authenticate_API(params, project_id)
        creds = creds.with_scopes(scope_list)
    
        try:
            bucket_name = params.get("GCS_source_table_bucket_name")
            file_path = params.get("GCS_source_table_file_path")
            if not bucket_name or not file_path:
                raise ValueError("[VALIDATION [ERROR ❌]] Faltan 'GCS_source_table_bucket_name' o 'GCS_source_table_file_path'.")
            from google.cloud import storage
            client_storage = storage.Client(credentials=creds, project=project_id)
            bucket = client_storage.bucket(bucket_name)
            blob = bucket.blob(file_path)
            print(f"[EXTRACTION [START ⏳]] Descargando archivo '{file_path}' del bucket '{bucket_name}'...", flush=True)
            file_bytes = blob.download_as_bytes()
            _, ext = os.path.splitext(file_path)
            ext = ext.lower()
            if ext in ['.xls', '.xlsx']:
                engine = 'xlrd' if ext == '.xls' else 'openpyxl'
                df = pd.read_excel(io.BytesIO(file_bytes), engine=engine)
            elif ext == '.csv':
                df = pd.read_csv(io.BytesIO(file_bytes), sep=',')
            elif ext == '.tsv':
                df = pd.read_csv(io.BytesIO(file_bytes), sep='\t')
            else:
                raise RuntimeError(f"[EXTRACTION [ERROR ❌]] Extensión de archivo '{ext}' no soportada en GCS.")
            print("[EXTRACTION [SUCCESS ✅]] Archivo descargado y leído desde GCS.", flush=True)
            return df
        except Exception as e:
            raise RuntimeError(f"[EXTRACTION [ERROR ❌]] Error al leer archivo desde GCS: {e}")

    # ────────────────────────────── PROCESO PRINCIPAL ──────────────────────────────
    _validar_comun(params)
    if _es_fuente_archivo(params):
        df = _leer_archivo(params)
    elif _es_fuente_gsheet(params):
        df = _leer_google_sheet(params)
    elif _es_fuente_gbq(params):
        df = _leer_gbq(params)
    elif _es_fuente_gcs(params):
        df = _leer_gcs(params)
    else:
        raise ValueError(
            "[VALIDATION [ERROR ❌]] No se han proporcionado parámetros válidos para identificar el origen de datos. "
            "Defina 'file_source_table_path', 'spreadsheet_source_table_id' y 'spreadsheet_source_table_worksheet_name', "
            "'GBQ_source_table_name' o 'GCS_source_table_bucket_name' y 'GCS_source_table_file_path'."
        )

    df = _apply_common_filters(df, params)
    return df




















def table_DF_to_various_targets(params: dict) -> None:
    """
    Escribe un DataFrame en distintos destinos (archivo local, Google Sheets,
    BigQuery o GCS) según la configuración definida en el diccionario de
    entrada, permitiendo especificar el modo de escritura: sobrescribir
    ('overwrite') o agregar ('append').

    Args
    ----
      params (dict):
        - df (pd.DataFrame): DataFrame a exportar.
        - ini_environment_identificated (str): 'LOCAL', 'COLAB', 'COLAB_ENTERPRISE'
          o el ID del proyecto GCP.
        - Claves de autenticación (una de ellas según el entorno):
            · json_keyfile_local
            · json_keyfile_colab
            · json_keyfile_GCP_secret_id
        - Parámetros de destino (uno de los grupos):
            · file_target_table_path, file_target_table_overwrite_or_append
            · spreadsheet_target_table_id,
              spreadsheet_target_table_worksheet_name,
              spreadsheet_target_table_overwrite_or_append
            · GBQ_target_table_name, GBQ_target_table_overwrite_or_append
            · GCS_target_table_bucket_name, GCS_target_table_file_path,
              GCS_target_table_overwrite_or_append

    Raises
    ------
      ValueError  : Si faltan parámetros obligatorios o son inválidos.
      RuntimeError: Si ocurre un error durante la escritura.
    """
    # ─────────────────────────────── IMPORTS BÁSICOS ────────────────────────────────
    import os, io, math
    import pandas as pd, numpy as np
    from google.cloud import bigquery
    from google.oauth2.service_account import Credentials

    # ──────────────────────────────── VALIDACIONES ────────────────────────────────
    print("\n🔹🔹🔹 [START ▶️] Iniciando escritura de DataFrame en destino configurado 🔹🔹🔹\n", flush=True)

    df = params.get("df")
    if df is None or not isinstance(df, pd.DataFrame):
        raise ValueError("[VALIDATION [ERROR ❌]] La clave 'df' debe contener un DataFrame válido.")
    print(f"[METRICS [INFO ℹ️]] DataFrame recibido: {df.shape[0]} filas × {df.shape[1]} columnas.", flush=True)

    if not any(params.get(k) for k in ("json_keyfile_GCP_secret_id", "json_keyfile_colab", "json_keyfile_local")):
        raise ValueError("[VALIDATION [ERROR ❌]] Falta un parámetro de keyfile para autenticación.")

    # ────────────────────────── DETECCIÓN DEL DESTINO ──────────────────────────
    _es_target_archivo = lambda p: bool(p.get('file_target_table_path', '').strip())
    _es_target_gsheet  = lambda p: not _es_target_archivo(p) and \
                                   bool(p.get('spreadsheet_target_table_id', '').strip()) and \
                                   bool(p.get('spreadsheet_target_table_worksheet_name', '').strip())
    _es_target_gbq     = lambda p: bool(p.get('GBQ_target_table_name', '').strip())
    _es_target_gcs     = lambda p: bool(p.get('GCS_target_table_bucket_name', '').strip()) and \
                                   bool(p.get('GCS_target_table_file_path', '').strip())

    # ──────────────────────────── SUB-FUNCIONES DE ESCRITURA ────────────────────────────
    def _escribir_archivo(p: dict, d: pd.DataFrame) -> None:
        import os
        print("\n[LOAD [START ▶️]] Iniciando escritura en archivo local…", flush=True)
        mode  = p.get("file_target_table_overwrite_or_append", "overwrite").lower()
        fpath = p.get("file_target_table_path")
        if not fpath:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta 'file_target_table_path'.")
        _, ext = os.path.splitext(fpath); ext = ext.lower()

        try:
            if ext in {'.xls', '.xlsx'}:
                engine = 'openpyxl'
                if mode == "append" and os.path.exists(fpath):
                    d = pd.concat([pd.read_excel(fpath, engine=engine), d], ignore_index=True)
                d.to_excel(fpath, index=False, engine=engine)

            elif ext in {'.csv', '.tsv'}:
                sep    = '\t' if ext == '.tsv' else ','
                header = not (mode == "append" and os.path.exists(fpath))
                d.to_csv(fpath, sep=sep, index=False,
                         mode=('a' if mode == "append" else 'w'),
                         header=header)
            else:
                raise RuntimeError(f"Extensión '{ext}' no soportada para archivos locales.")

            print(f"[LOAD [SUCCESS ✅]] DataFrame escrito en '{fpath}'.", flush=True)
            print(f"[METRICS [INFO ℹ️]] Destino final: file://{fpath}", flush=True)
        except Exception as e:
            raise RuntimeError(f"[LOAD [ERROR ❌]] Error al escribir en archivo local: {e}")

    def _escribir_google_sheet(p: dict, d: pd.DataFrame) -> None:
        """
        Envía el DataFrame a Google Sheets respetando los formatos de fecha-hora
        españoles (coma decimal). Se sobrescribe o añade según `mode`.
        """
        import re
        from googleapiclient.discovery import build
        print("\n[LOAD [START ▶️]] Iniciando escritura en Google Sheets…", flush=True)

        mode = p.get("spreadsheet_target_table_overwrite_or_append", "overwrite").lower()
        raw_id   = p.get("spreadsheet_target_table_id")
        ws_name  = p.get("spreadsheet_target_table_worksheet_name")
        if not raw_id or not ws_name:
            raise ValueError("[VALIDATION [ERROR ❌]] Faltan 'spreadsheet_target_table_id' o 'spreadsheet_target_table_worksheet_name'.")

        # Normalizar el ID (por si viene una URL completa)
        m = re.search(r"/d/([A-Za-z0-9-_]+)", raw_id)
        sheet_id = m.group(1) if m else raw_id

        scopes = ["https://www.googleapis.com/auth/spreadsheets",
                  "https://www.googleapis.com/auth/drive"]
        env = p.get("ini_environment_identificated")
        project = os.getenv("GOOGLE_CLOUD_PROJECT") if env == "COLAB_ENTERPRISE" else env
        creds   = _ini_authenticate_API(params, project).with_scopes(scopes)
        service = build('sheets', 'v4', credentials=creds)

        # ── NUEVO: convertir columnas datetime a texto con coma decimal ──────────
        datetime_cols = d.select_dtypes(include=["datetime64[ns]", "datetime64[ns, utc]"]).columns
        if len(datetime_cols) > 0:
            d = d.copy()  # evitamos SettingWithCopyWarning
            for col in datetime_cols:
                # Ej.: 2021-02-09 08:52:29,217577  (coma decimal)
                d[col] = d[col].dt.strftime("%Y-%m-%d %H:%M:%S,%f")
        # ─────────────────────────────────────────────────────────────────────────

        # ── Conversión de cada celda ────────────────────────────────────────────
        from decimal import Decimal, InvalidOperation
        def _cast(value):
            """
            Devuelve un número nativo, None o str.
            - Timestamps ya vienen formateados como str con coma.
            - Si prefieres coma decimal en todos los floats, cambia aquí `float(value)`
              por `str(value).replace('.', ',')`.
            """
            if pd.isna(value):
                return None
            if isinstance(value, (float, np.floating, Decimal)):
                try:
                    return float(value)
                except (ValueError, InvalidOperation):
                    return str(value)
            if isinstance(value, (int, np.integer)):
                return int(value)
            return str(value)

        rows = [[_cast(val) for val in row] for row in d.itertuples(index=False, name=None)]
        values = ([d.columns.tolist()] + rows) if mode == "overwrite" else rows

        # Limpiar rango si es overwrite
        if mode == "overwrite":
            service.spreadsheets().values().clear(
                spreadsheetId=sheet_id,
                range=ws_name,
                body={}
            ).execute()

        request = (service.spreadsheets().values().update if mode == "overwrite"
                   else service.spreadsheets().values().append)
        result = request(
            spreadsheetId   = sheet_id,
            range           = ws_name,
            valueInputOption= "RAW",
            body            = {"values": values},
            **({"insertDataOption": "INSERT_ROWS"} if mode == "append" else {})
        ).execute()

        affected = result.get('updates', {}).get('updatedCells', 'N/A')
        print(f"[LOAD [SUCCESS ✅]] Se actualizaron {affected} celdas en Google Sheets.", flush=True)
        print(f"[METRICS [INFO ℹ️]] Destino final: https://docs.google.com/spreadsheets/d/{sheet_id}", flush=True)

    def _escribir_gbq(p: dict, d: pd.DataFrame) -> None:
        from google.cloud.bigquery import LoadJobConfig, WriteDisposition
        print("\n[LOAD [START ▶️]] Iniciando carga en BigQuery…", flush=True)

        mode  = p.get("GBQ_target_table_overwrite_or_append", "overwrite").lower()
        table = p.get("GBQ_target_table_name")
        if not table:
            raise ValueError("[VALIDATION [ERROR ❌]] Falta 'GBQ_target_table_name'.")

        env = p.get("ini_environment_identificated")
        project = os.getenv("GOOGLE_CLOUD_PROJECT") if env == "COLAB_ENTERPRISE" else env
        creds = _ini_authenticate_API(params, project).with_scopes(
            ["https://www.googleapis.com/auth/bigquery",
             "https://www.googleapis.com/auth/drive"])
        client = bigquery.Client(credentials=creds, project=project)

        job_cfg = LoadJobConfig(
            write_disposition=(WriteDisposition.WRITE_TRUNCATE if mode == "overwrite"
                               else WriteDisposition.WRITE_APPEND)
        )
        try:
            client.load_table_from_dataframe(d, table, job_config=job_cfg).result()
            print("[LOAD [SUCCESS ✅]] DataFrame cargado exitosamente en BigQuery.", flush=True)
            print(f"[METRICS [INFO ℹ️]] Destino final: https://console.cloud.google.com/bigquery?project={project}&ws=!1m5!1m4!4m3!1s{table}", flush=True)
        except Exception as e:
            raise RuntimeError(f"[LOAD [ERROR ❌]] Error al escribir en BigQuery: {e}")

    def _escribir_gcs(p: dict, d: pd.DataFrame) -> None:
        from google.cloud import storage
        print("\n[LOAD [START ▶️]] Iniciando subida a Google Cloud Storage…", flush=True)

        bucket = p.get("GCS_target_table_bucket_name")
        path   = p.get("GCS_target_table_file_path")
        if not bucket or not path:
            raise ValueError("[VALIDATION [ERROR ❌]] Faltan 'GCS_target_table_bucket_name' o 'GCS_target_table_file_path'.")

        mode = p.get("GCS_target_table_overwrite_or_append", "overwrite").lower()
        env  = p.get("ini_environment_identificated")
        project = os.getenv("GOOGLE_CLOUD_PROJECT") if env == "COLAB_ENTERPRISE" else env
        creds   = _ini_authenticate_API(params, project).with_scopes(
            ["https://www.googleapis.com/auth/devstorage.read_write"])
        client  = storage.Client(credentials=creds, project=project)
        blob    = client.bucket(bucket).blob(path)
        _, ext  = os.path.splitext(path); ext = ext.lower()

        try:
            # Si append, descargar y concatenar
            if mode == "append" and blob.exists(client):
                data = blob.download_as_bytes()
                if ext == '.csv':
                    d = pd.concat([pd.read_csv(io.BytesIO(data)), d], ignore_index=True)
                elif ext == '.tsv':
                    d = pd.concat([pd.read_csv(io.BytesIO(data), sep='\t'), d], ignore_index=True)
                elif ext in {'.xls', '.xlsx'}:
                    d = pd.concat([pd.read_excel(io.BytesIO(data)), d], ignore_index=True)
                else:
                    raise RuntimeError(f"Extensión '{ext}' no soportada en modo append.")

            # Serializar y subir
            if ext in {'.xls', '.xlsx'}:
                buf = io.BytesIO(); d.to_excel(buf, index=False, engine='openpyxl')
                blob.upload_from_string(buf.getvalue(),
                                        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            elif ext == '.csv':
                blob.upload_from_string(d.to_csv(index=False).encode('utf-8'), content_type='text/csv')
            elif ext == '.tsv':
                blob.upload_from_string(d.to_csv(sep='\t', index=False).encode('utf-8'),
                                        content_type='text/tab-separated-values')
            else:
                raise RuntimeError(f"Extensión '{ext}' no soportada para GCS.")

            print("[LOAD [SUCCESS ✅]] Archivo subido exitosamente a GCS.", flush=True)
            print(f"[METRICS [INFO ℹ️]] Destino final: https://console.cloud.google.com/storage/browser/{bucket}?project={project}", flush=True)
        except Exception as e:
            raise RuntimeError(f"[LOAD [ERROR ❌]] Error al escribir en GCS: {e}")

    # ─────────────────────────────── DESPACHADOR ───────────────────────────────
    try:
        if _es_target_archivo(params):
            _escribir_archivo(params, df)
        elif _es_target_gsheet(params):
            _escribir_google_sheet(params, df)
        elif _es_target_gbq(params):
            _escribir_gbq(params, df)
        elif _es_target_gcs(params):
            _escribir_gcs(params, df)
        else:
            raise ValueError(
                "[VALIDATION [ERROR ❌]] No se detectó un destino válido. "
                "Defina 'file_target_table_path', "
                "o ('spreadsheet_target_table_id' y 'spreadsheet_target_table_worksheet_name'), "
                "o 'GBQ_target_table_name', "
                "o ('GCS_target_table_bucket_name' y 'GCS_target_table_file_path')."
            )
    except Exception as e:
        print(f"\n🔹🔹🔹 [END [FAILED ❌]] Proceso finalizado con errores: {e} 🔹🔹🔹\n", flush=True)
        raise

    print("\n🔹🔹🔹 [END [FINISHED ✅]] Escritura completada exitosamente. 🔹🔹🔹\n", flush=True)
