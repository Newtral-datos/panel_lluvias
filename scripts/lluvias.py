# aemet_pipeline.py
from __future__ import annotations

import json
import time
from io import StringIO
from pathlib import Path
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import JSONDecodeError, HTTPError, Timeout, ConnectionError
from babel.dates import format_date
import matplotlib.pyplot as plt

from api_keys import api_keys

# --- Google Sheets ---
import math, re
from datetime import datetime as _dt
from pandas.api.types import is_datetime64_any_dtype, is_datetime64tz_dtype
try:
    import httplib2
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from google_auth_httplib2 import AuthorizedHttp
    from google.oauth2.service_account import Credentials
    _GSHEETS_DISPONIBLE = True
except Exception:
    _GSHEETS_DISPONIBLE = False

# =========================
# Configuración
# =========================
BASE = "https://opendata.aemet.es/opendata/api"
_TZ_LOCAL = ZoneInfo("Europe/Madrid")

# Rutas
RUTA_INDICATIVOS       = "/Users/miguel.ros/Desktop/PANEL_LLUVIAS/complementarios_lluvias/ids_estaciones.xlsx"
RUTA_MAESTRO           = "/Users/miguel.ros/Desktop/PANEL_LLUVIAS/complementarios_lluvias/datos_mapa.xlsx"
RUTA_BASE              = "/Users/miguel.ros/Desktop/PANEL_LLUVIAS/"
RUTA_COMPLEMENTARIOS   = "/Users/miguel.ros/Desktop/PANEL_LLUVIAS/complementarios_lluvias/"

# Sheets
SUBIR_A_SHEETS   = True
ID_HOJA_CALCULO  = "1o0DICxbYpq_OqgwTqU9-8GaQzjYj14cdureHGN-uLQA"
NOMBRE_PESTANA   = "precipitaciones"
INICIO_A1        = f"{NOMBRE_PESTANA}!A1"
RUTA_CREDENCIALES = "/Users/miguel.ros/Desktop/PANEL_LLUVIAS/credenciales_google_sheet.json"
ALCANCES_SHEETS  = ["https://www.googleapis.com/auth/spreadsheets"]

# Comportamiento rápido
ESPERAR_SI_429 = False
PAUSA_ENTRE_ESTACIONES = 0.0

# =========================
# Descarga y helpers
# =========================
def sesion_reintentos() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "aemet-downloader/1.2", "Connection": "close", "Accept": "application/json"})
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def _decode_json_with_bom(resp: requests.Response):
    raw = resp.content.decode("utf-8-sig", errors="replace").strip()
    return json.loads(raw)

def _iter_api_keys(keys):
    if isinstance(keys, str):
        k = keys.strip()
        if k:
            yield k
        return
    if isinstance(keys, (list, tuple)):
        for k in keys:
            if isinstance(k, str) and k.strip():
                yield k.strip()

def aemet_descargar(endpoint: str, params_extra: dict | None = None, timeout=(5, 20)) -> str:
    s = sesion_reintentos()
    url = f"{BASE}/{endpoint.lstrip('/')}"
    if "?" in url and "api_key=" in url:
        raise ValueError("No incluyas ?api_key= en el endpoint")

    errores = []
    for idx, key in enumerate(_iter_api_keys(api_keys), start=1):
        params = {"api_key": key}
        if params_extra:
            params.update(params_extra)
        try:
            r = s.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            try:
                meta = _decode_json_with_bom(r)
            except JSONDecodeError:
                ct = r.headers.get("Content-Type", "")
                snippet = r.content[:120].decode("utf-8", "replace")
                errores.append(f"[key#{idx}] No-JSON (CT={ct}). Cuerpo≈ {snippet!r}")
                continue
            datos_url = meta.get("datos")
            if not datos_url:
                errores.append(f"[key#{idx}] Sin 'datos': {meta}")
                continue
            r2 = s.get(datos_url, timeout=(5, 25))
            r2.raise_for_status()
            return r2.text
        except HTTPError as e:
            code = getattr(e.response, "status_code", "¿?")
            ct = getattr(e.response, "headers", {}).get("Content-Type", "")
            body = (getattr(e.response, "text", "") or "")[:160]
            errores.append(f"[key#{idx}] HTTP {code} (CT={ct}) {body!r}")
            if code == 429 and not ESPERAR_SI_429:
                continue
            if code == 429 and ESPERAR_SI_429:
                time.sleep(65)
        except (Timeout, ConnectionError) as e:
            errores.append(f"[key#{idx}] Red: {type(e).__name__}: {e}")
        except Exception as e:
            errores.append(f"[key#{idx}] Excepción: {type(e).__name__}: {e}")

    resumen = "\n - ".join(errores) if errores else "Sin detalles."
    raise RuntimeError(f"No se pudo descargar con ninguna API key. Detalles:\n - {resumen}")

def a_texto_a_df(texto: str, content_hint: str | None = None) -> pd.DataFrame:
    if content_hint == "csv":
        return pd.read_csv(StringIO(texto), sep=";", engine="python")
    try:
        obj = json.loads(texto)
        if isinstance(obj, list):
            return pd.DataFrame(obj)
        if isinstance(obj, dict):
            return pd.json_normalize(obj)
    except Exception:
        pass
    try:
        return pd.read_csv(StringIO(texto), sep=None, engine="python")
    except Exception:
        return pd.DataFrame()

def guardar_xlsx(df: pd.DataFrame, ruta_salida: Path) -> Path:
    ruta_salida.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(ruta_salida, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="datos")
    return ruta_salida

def tratamiento(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    columnas_deseadas = ["fecha", "indicativo", "prec"]
    cols = [c for c in columnas_deseadas if c in df.columns]
    if cols:
        df = df.loc[:, cols]
    if "fecha" in df.columns:
        fechas = pd.to_datetime(df["fecha"], errors="coerce")
        df["fecha_txt"] = fechas.apply(
            lambda x: format_date(x, format="d 'de' MMMM", locale="es") if pd.notnull(x) else None
        )
    return df

# =========================
# Fechas objetivo / Rango últimos 5 días
# =========================
def rango_ultimos_ndias(n: int = 5) -> tuple[str, str]:
    """Devuelve fechaini/fechafin (UTC) para los últimos n días excluyendo hoy: hoy-n → ayer."""
    hoy = datetime.now(_TZ_LOCAL).date()
    fin = hoy - timedelta(days=1)          # ayer
    ini = hoy - timedelta(days=n)          # hace n días
    fechaini = f"{ini:%Y-%m-%d}T00:00:00UTC"
    fechafin = f"{fin:%Y-%m-%d}T23:59:00UTC"
    return fechaini, fechafin

# =========================
# Descarga por indicativos (rango de fechas)
# =========================
def descargar_por_indicativos_rango(
    ruta_indicativos: str | Path,
    fechaini: str,
    fechafin: str,
    hoja: int | str = 0,
    columna: str = "indicativo",
    pausa_seg: float = PAUSA_ENTRE_ESTACIONES,
) -> pd.DataFrame:
    tabla = pd.read_excel(ruta_indicativos, sheet_name=hoja)
    if columna not in tabla.columns:
        raise ValueError(f"No se encuentra la columna '{columna}' en {ruta_indicativos}")
    indicativos = (
        tabla[columna].dropna().astype(str).str.strip().str.upper()
        .replace("", pd.NA).dropna().unique().tolist()
    )

    print(f"Descargando datos del RANGO: {fechaini} → {fechafin}")

    dfs: list[pd.DataFrame] = []
    total = len(indicativos)

    for i, ind in enumerate(indicativos, start=1):
        try:
            endpoint = f"/valores/climatologicos/diarios/datos/fechaini/{fechaini}/fechafin/{fechafin}/estacion/{ind}"
            texto = aemet_descargar(endpoint, params_extra=None, timeout=(4, 18))
            if not texto or len(texto) < 3:
                print(f"[{i}/{total}] {ind}: sin datos (respuesta vacía) → salto")
                continue
            df_raw = a_texto_a_df(texto)
            if df_raw is None or df_raw.empty:
                print(f"[{i}/{total}] {ind}: vacío tras parseo → salto")
                continue
            df = tratamiento(df_raw)
            if df is None or df.empty:
                print(f"[{i}/{total}] {ind}: vacío tras tratamiento → salto")
                continue
            if "indicativo" not in df.columns:
                df = df.copy()
                df["indicativo"] = ind
            dfs.append(df)
            print(f"[{i}/{total}] {ind}: OK ({len(df)} filas)")
        except Exception as e:
            msg = str(e)
            if "429" in msg and not ESPERAR_SI_429:
                print(f"[{i}/{total}] {ind}: 429 → siguiente key/estación")
            else:
                print(f"[{i}/{total}] {ind}: ERROR -> {e} → salto")
        finally:
            if i < total and pausa_seg and pausa_seg > 0:
                time.sleep(pausa_seg)

    return pd.concat(dfs, ignore_index=True, sort=False) if dfs else pd.DataFrame()

def combinar_con_maestro(
    df_descargas: pd.DataFrame,
    ruta_maestro: str | Path,
    hoja: int | str = 0,
    clave: str = "indicativo",
) -> pd.DataFrame:
    maestro = pd.read_excel(ruta_maestro, sheet_name=hoja)
    if clave not in maestro.columns:
        raise ValueError(f"El maestro no tiene la columna '{clave}'")
    if df_descargas.empty:
        return maestro
    # Unir metadatos del maestro a CADA fila descargada (filtraremos antes a una sola fecha)
    combinado = df_descargas.merge(maestro, on=clave, how="left")
    return combinado

# =========================
# Limpieza / adaptación
# =========================
def invertir_coma(texto: str):
    if not isinstance(texto, str) or "," not in texto:
        return texto
    partes = [p.strip() for p in texto.split(",")]
    return ", ".join(partes[::-1])

def num_a_texto(n):
    try:
        return f"{float(n):.1f}".replace(".", ",")
    except Exception:
        return ""

def transformar_maestro(maestro: pd.DataFrame) -> pd.DataFrame:
    if "Unnamed: 0" in maestro.columns:
        maestro = maestro.drop(columns=["Unnamed: 0"])

    if "nombre" in maestro.columns:
        maestro["nombre"] = maestro["nombre"].apply(invertir_coma).str.title()
    if "provincia" in maestro.columns:
        maestro["provincia"] = maestro["provincia"].str.title()

    if "prec" in maestro.columns:
        maestro["prec"] = (
            maestro["prec"].astype(str).str.replace(",", ".", regex=False)
            .apply(pd.to_numeric, errors="coerce")
        )
        maestro = maestro.dropna(subset=["prec"])

    if "precip_media_mensual_historica" in maestro.columns:
        maestro["prec_txt"] = maestro["prec"].apply(num_a_texto)
        maestro["prec_historica_txt"] = maestro["precip_media_mensual_historica"].apply(num_a_texto)
        maestro["prec_historica_diaria"] = maestro["precip_media_mensual_historica"] / 30
        maestro["prec_historica_diaria_txt"] = maestro["prec_historica_diaria"].apply(num_a_texto)
        maestro["diferencia"] = maestro["prec"] - maestro["prec_historica_diaria"]
        maestro["diferencia_txt"] = maestro["diferencia"].apply(num_a_texto)

    orden = ["indicativo", "nombre", "provincia", "altitud", "año_inicio", "año_fin",
             "mes_historico", "precip_media_mensual_historica", "prec_historica_txt",
             "prec_historica_diaria", "prec_historica_diaria_txt", "fecha", "fecha_txt",
             "prec", "prec_txt", "diferencia", "diferencia_txt", "latitud", "longitud"]
    cols_finales = [c for c in orden if c in maestro.columns] + [c for c in maestro.columns if c not in orden]
    maestro = maestro.loc[:, cols_finales]
    return maestro

def categorizar_y_plot(maestro: pd.DataFrame) -> pd.DataFrame:
    if "diferencia" in maestro.columns:
        maestro["diferencia"].hist(bins=30, edgecolor="black")
        plt.xlabel("diferencia"); plt.ylabel("Frecuencia"); plt.title("Distribución de la variable diferencia")
        print(maestro["diferencia"].describe())

        bins = [-float("inf"), -10, -5, 5, 10, float("inf")]
        labels = ["Mucho menos", "Menos", "Similar", "Más", "Mucho más"]
        maestro["categoria"] = pd.cut(maestro["diferencia"], bins=bins, labels=labels, include_lowest=True)

        cat_dtype = pd.api.types.CategoricalDtype(categories=labels, ordered=True)
        maestro["categoria"] = maestro["categoria"].astype(cat_dtype)

        maestro["diferencia_txt"] = maestro["diferencia"].apply(
            lambda x: f"+{x:.1f}".replace(".", ",") if pd.notna(x) and x > 0
            else (f"-{abs(x):.1f}".replace(".", ",") if pd.notna(x) and x < 0
                  else ("0,0" if pd.notna(x) else pd.NA))
        )

        presentes = set(maestro["categoria"].dropna().astype(str).unique())
        faltantes = [lab for lab in labels if lab not in presentes]

        if faltantes:
            base = {col: pd.NA for col in maestro.columns}
            if len(maestro) > 0:
                last_row = maestro.iloc[-1]
                if "latitud" in maestro.columns:
                    base["latitud"] = last_row.get("latitud", pd.NA)
                if "longitud" in maestro.columns:
                    base["longitud"] = last_row.get("longitud", pd.NA)

            filas = []
            for lab in faltantes:
                fila = base.copy()
                fila["categoria"] = lab
                filas.append(fila)

            if filas:
                falt_df = pd.DataFrame(filas, columns=maestro.columns)
                falt_df["categoria"] = falt_df["categoria"].astype(cat_dtype)
                maestro = pd.concat([maestro, falt_df], ignore_index=True)

    print(maestro.head()); print(f"Número de filas: {len(maestro)}")
    return maestro

# =========================
# Main
# =========================
if __name__ == "__main__":
    print("Descargando por indicativos para los últimos 5 días (excluyendo hoy)…")

    # 1) Rango de 5 días hacia atrás respecto al día actual (hoy-5 → hoy-1)
    fechaini, fechafin = rango_ultimos_ndias(5)

    # 2) Descarga para todas las estaciones del Excel de indicativos (puede traer varias fechas)
    df_rango = descargar_por_indicativos_rango(
        ruta_indicativos=RUTA_INDICATIVOS,
        fechaini=fechaini,
        fechafin=fechafin,
        hoja=0,
        columna="indicativo",
        pausa_seg=PAUSA_ENTRE_ESTACIONES,
    )

    if df_rango.empty:
        raise RuntimeError("No se encontraron datos para el rango solicitado (últimos 5 días excluyendo hoy).")

    # 3) Elegir UNA única fecha: la más reciente con datos
    if "fecha" not in df_rango.columns:
        raise RuntimeError("La descarga no contiene columna 'fecha'.")

    df_rango = df_rango.copy()
    df_rango["__fecha_dt"] = pd.to_datetime(df_rango["fecha"], errors="coerce")
    df_rango = df_rango.dropna(subset=["__fecha_dt"])
    if df_rango.empty:
        raise RuntimeError("No hay fechas válidas en los datos descargados.")

    df_rango["__fecha_dia"] = df_rango["__fecha_dt"].dt.date
    fechas_disponibles = sorted(df_rango["__fecha_dia"].unique())
    fecha_objetivo = max(fechas_disponibles)  # <- LA MÁS RECIENTE CON CUALQUIER DATO

    # (Opcional) log de cobertura por fecha para trazabilidad
    cobertura = df_rango.groupby("__fecha_dia")["indicativo"].nunique().sort_index()
    print("Cobertura por fecha (nº estaciones con dato):")
    for f, n in cobertura.items():
        print(f"  {f}: {n} estaciones")
    print(f"→ Fecha objetivo seleccionada: {fecha_objetivo}")

    # 4) Filtrar TODOS los registros a esa ÚNICA fecha
    df_todas = df_rango.loc[df_rango["__fecha_dia"] == fecha_objetivo].drop(columns=["__fecha_dt", "__fecha_dia"])

    # 5) Continuar el pipeline
    print("Combinando con maestro…")
    df_maestro = combinar_con_maestro(df_todas, RUTA_MAESTRO)

    ruta_complementarios = Path(RUTA_COMPLEMENTARIOS)
    ruta_complementarios.mkdir(parents=True, exist_ok=True)
    ruta_df_maestro = ruta_complementarios / "df_maestro.xlsx"
    guardar_xlsx(df_maestro, ruta_df_maestro)
    print("Guardado df_maestro en:", ruta_df_maestro)

    print("Aplicando transformaciones…")
    maestro = transformar_maestro(df_maestro)
    maestro = categorizar_y_plot(maestro)

    if "categoria" in maestro.columns:
        maestro["categoria"] = maestro["categoria"].astype(object).where(maestro["categoria"].notna(), "")

    # Export final
    ruta_final = Path(RUTA_BASE) / "MAPA_LLUVIAS.xlsx"
    with pd.ExcelWriter(ruta_final, engine="openpyxl") as writer:
        maestro.to_excel(writer, index=False)
    print("Exportado:", ruta_final)

    # Subida a Google Sheets
    if SUBIR_A_SHEETS:
        if not _GSHEETS_DISPONIBLE:
            print("AVISO: faltan dependencias de Google Sheets (pip install google-api-python-client google-auth-httplib2 google-auth httplib2)")
        elif not ID_HOJA_CALCULO:
            print("AVISO: configura ID_HOJA_CALCULO.")
        elif not Path(RUTA_CREDENCIALES).exists():
            print(f"AVISO: no se encontró el fichero de credenciales en {RUTA_CREDENCIALES}.")
        else:
            try:
                subir_df_a_sheet(
                    df=maestro,
                    spreadsheet_id=ID_HOJA_CALCULO,
                    rango_inicial=INICIO_A1,
                    pestana=NOMBRE_PESTANA,
                    ruta_credenciales=RUTA_CREDENCIALES,
                    alcances=ALCANCES_SHEETS,
                    filas_bloque=2000,
                )
                print("Subida a Google Sheets completada.")
            except Exception as e:
                print(f"ERROR subiendo a Google Sheets: {e}")
