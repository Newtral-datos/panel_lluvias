from __future__ import annotations

import json
import time
from io import StringIO
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import JSONDecodeError, HTTPError, Timeout, ConnectionError
from babel.dates import format_date

from api_keys import api_keys

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

# --- Configuración.
BASE = "https://opendata.aemet.es/opendata/api"
_TZ_LOCAL = ZoneInfo("Europe/Madrid")

RUTA_BASE            = "/Users/miguel.ros/Desktop/PANEL_LLUVIAS/"
RUTA_INDICATIVOS     = f"{RUTA_BASE}complementarios_temperaturas/ids_estaciones_reducido.xlsx"
RUTA_MAESTRO         = f"{RUTA_BASE}complementarios_temperaturas/datos_mapa.xlsx"
RUTA_SALIDAS         = Path(RUTA_BASE)

NOMBRE_XLSX_INTERMEDIO = "df_maestro.xlsx"
NOMBRE_XLSX_FINAL      = "MAPA_TEMPERATURAS.xlsx"

# --- Subir a Google Sheets.
SUBIR_A_SHEETS    = True
ID_HOJA_CALCULO   = "1o0DICxbYpq_OqgwTqU9-8GaQzjYj14cdureHGN-uLQA"
NOMBRE_PESTANA    = "temperaturas"
INICIO_A1         = f"{NOMBRE_PESTANA}!A1"
RUTA_CREDENCIALES = f"{RUTA_BASE}credenciales_google_sheet.json"
ALCANCES_SHEETS   = ["https://www.googleapis.com/auth/spreadsheets"]

# =========================
# Descarga y helpers AEMET
# =========================
def sesion_reintentos() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "aemet-downloader/1.1", "Connection": "close", "Accept": "application/json"})
    retry = Retry(total=5, backoff_factor=0.7, status_forcelist=[500, 502, 503, 504, 524],
                  allowed_methods=["GET"], respect_retry_after_header=True)
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

def aemet_descargar(endpoint: str, params_extra: dict | None = None) -> str:
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
            r = s.get(url, params=params, timeout=(5, 45))
            r.raise_for_status()
            try:
                meta = _decode_json_with_bom(r)
            except JSONDecodeError:
                ct = r.headers.get("Content-Type", "")
                snippet = r.content[:120].decode("utf-8", "replace")
                errores.append(f"[key#{idx}] No-JSON (CT={ct}). Cuerpo≈ {snippet!r}")
                continue
            if "datos" not in meta:
                errores.append(f"[key#{idx}] Sin 'datos': {meta}")
                continue
            r2 = s.get(meta["datos"], timeout=(5, 60))
            r2.raise_for_status()
            return r2.text
        except HTTPError as e:
            code = getattr(e.response, "status_code", "¿?")
            ct = getattr(e.response, "headers", {}).get("Content-Type", "")
            body = (getattr(e.response, "text", "") or "")[:160]
            errores.append(f"[key#{idx}] HTTP {code} (CT={ct}) {body!r}")
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
        return pd.read_csv(StringIO(texto), sep=";", engine="python")
    except Exception:
        return pd.DataFrame({"contenido": [texto]})

def guardar_xlsx(df: pd.DataFrame, ruta_salida: Path) -> Path:
    ruta_salida.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(ruta_salida, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="datos")
    return ruta_salida

def tratamiento_temperaturas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    columnas_deseadas = ["fecha", "indicativo", "tmax"]
    cols = [c for c in columnas_deseadas if c in df.columns]
    if cols:
        df = df.loc[:, cols]
    if "fecha" in df.columns:
        fechas = pd.to_datetime(df["fecha"], errors="coerce")
        df["fecha_txt"] = fechas.apply(
            lambda x: format_date(x, format="d 'de' MMMM", locale="es") if pd.notnull(x) else None
        )
    return df

def _quizas_esperar_por_429(err: Exception) -> bool:
    s = str(err)
    if " 429" in s or 'estado" : 429' in s or "estado': 429" in s:
        print("   → 429 recibido: esperando 65s para reintentar…")
        time.sleep(65)
        return True
    return False

def _probe_aemet_rapido(indicativo: str, fecha: datetime.date, api_key: str) -> bool:
    fechaini = f"{fecha:%Y-%m-%d}T00:00:00UTC"
    fechafin = f"{fecha:%Y-%m-%d}T23:59:00UTC"
    url_meta = f"{BASE}/valores/climatologicos/diarios/datos/fechaini/{fechaini}/fechafin/{fechafin}/estacion/{indicativo}"
    try:
        r = requests.get(url_meta, params={"api_key": api_key}, timeout=(3, 8))
        r.raise_for_status()
        meta = json.loads(r.content.decode("utf-8-sig", errors="replace").strip())
        datos_url = meta.get("datos")
        if not datos_url:
            return False
        r2 = requests.get(datos_url, timeout=(3, 10))
        r2.raise_for_status()
        txt = r2.text
        if not txt or len(txt) < 5:
            return False
        df = a_texto_a_df(txt)
        return df is not None and not df.empty
    except Exception:
        return False

def _fecha_aemet_mas_reciente(indicativo: str, max_retraso: int = 5, deadline_seg: int = 40) -> tuple[str, str]:
    import time as _time
    t0 = _time.monotonic()
    hoy_local = datetime.now(_TZ_LOCAL).date()
    primera_key = next(_iter_api_keys(api_keys), None)
    if not primera_key:
        raise RuntimeError("No hay API key configurada.")
    for delta in range(1, max_retraso + 1):
        if _time.monotonic() - t0 > deadline_seg:
            break
        candidato = hoy_local - timedelta(days=delta)
        if _probe_aemet_rapido(indicativo, candidato, primera_key):
            fechaini = f"{candidato:%Y-%m-%d}T00:00:00UTC"
            fechafin = f"{candidato:%Y-%m-%d}T23:59:00UTC"
            return fechaini, fechafin
    candidato = hoy_local - timedelta(days=max_retraso)
    return (f"{candidato:%Y-%m-%d}T00:00:00UTC", f"{candidato:%Y-%m-%d}T23:59:00UTC")

def descargar_por_indicativos_xlsx(
    ruta_indicativos: str | Path,
    hoja: int | str = 0,
    columna: str = "indicativo",
    pausa_seg: float = 2,
) -> pd.DataFrame:
    tabla = pd.read_excel(ruta_indicativos, sheet_name=hoja)
    if columna not in tabla.columns:
        raise ValueError(f"No se encuentra la columna '{columna}' en {ruta_indicativos}")
    indicativos = (
        tabla[columna].dropna().astype(str).str.strip().str.upper()
        .replace("", pd.NA).dropna().unique().tolist()
    )

    print("Determinando día más reciente con datos (sonda rápida)…")
    fechaini = fechafin = None
    for probe in indicativos[:3]:
        print(f"  · probando {probe}…", end="", flush=True)
        try:
            fechaini, fechafin = _fecha_aemet_mas_reciente(probe, max_retraso=5, deadline_seg=40)
            print(f" OK → {fechaini} → {fechafin}")
            break
        except Exception as e:
            print(f" falló ({e})")
    if fechaini is None:
        candidato = datetime.now(_TZ_LOCAL).date() - timedelta(days=5)
        fechaini = f"{candidato:%Y-%m-%d}T00:00:00UTC"
        fechafin = f"{candidato:%Y-%m-%d}T23:59:00UTC"
        print(f"AVISO: usando fallback {fechaini} → {fechafin}")

    dfs: list[pd.DataFrame] = []
    total = len(indicativos)

    for i, ind in enumerate(indicativos, start=1):
        try:
            endpoint = (
                f"/valores/climatologicos/diarios/datos/fechaini/{fechaini}/fechafin/{fechafin}/estacion/{ind}"
            )
            try:
                texto = aemet_descargar(endpoint, params_extra=None)
            except Exception as e1:
                if _quizas_esperar_por_429(e1):
                    texto = aemet_descargar(endpoint, params_extra=None)
                else:
                    raise
            df_raw = a_texto_a_df(texto)
            if df_raw is None or df_raw.empty:
                print(f"[{i}/{total}] {ind}: vacío tras parseo")
                continue
            df = tratamiento_temperaturas(df_raw)
            if df is None or df.empty:
                print(f"[{i}/{total}] {ind}: vacío tras tratamiento")
                continue
            if "indicativo" not in df.columns:
                df = df.copy()
                df["indicativo"] = ind
            dfs.append(df)
            print(f"[{i}/{total}] {ind}: OK ({len(df)} filas)")
        except Exception as e:
            print(f"[{i}/{total}] {ind}: ERROR -> {e}")
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
    base = df_descargas.drop_duplicates(subset=[clave], keep="last")
    cols_aemet = [c for c in base.columns if c != clave]
    combinado = maestro.merge(base[[clave] + cols_aemet], on=clave, how="left")
    return combinado

# --- Limpieza.
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

def transformar_maestro_temperaturas(maestro: pd.DataFrame) -> pd.DataFrame:
    maestro = maestro.copy()

    cols_basura = [c for c in ["Unnamed: 0.1", "Unnamed: 0", "indsinop"] if c in maestro.columns]
    if cols_basura:
        maestro = maestro.drop(columns=cols_basura)

    if "tm_max_media" in maestro.columns:
        maestro = maestro.rename(columns={"tm_max_media": "media_maxima_historica"})

    if "nombre" in maestro.columns:
        maestro["nombre"] = maestro["nombre"].apply(invertir_coma).str.title()
    if "provincia" in maestro.columns:
        maestro["provincia"] = maestro["provincia"].str.title()

    if "tmax" in maestro.columns:
        maestro["tmax"] = (
            maestro["tmax"].astype(str).str.replace(",", ".", regex=False)
            .apply(pd.to_numeric, errors="coerce")
        )
        maestro = maestro.dropna(subset=["tmax"])

    if "media_maxima_historica" in maestro.columns and "tmax" in maestro.columns:
        maestro["diferencia"] = maestro["tmax"] - maestro["media_maxima_historica"]
        maestro["tmax_txt"] = maestro["tmax"].apply(num_a_texto)
        maestro["media_maxima_historica_txt"] = maestro["media_maxima_historica"].apply(num_a_texto)

        maestro["diferencia_txt"] = maestro["diferencia"].apply(
            lambda x: f"+{num_a_texto(x)}" if x > 0 
                    else f"-{num_a_texto(abs(x))}" if x < 0 
                    else num_a_texto(0)
        )

        bins = [-10, -6, -2, 2, 6, 10]
        labels = ["Muy baja", "Baja", "Similar", "Alta", "Muy alta"]
        maestro["categoria"] = pd.cut(maestro["diferencia"], bins=bins, labels=labels)

        cat_dtype = pd.api.types.CategoricalDtype(categories=labels, ordered=True)
        maestro["categoria"] = maestro["categoria"].astype(cat_dtype)

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

    orden = [
        "indicativo", "nombre", "provincia", "altitud", "año_inicio", "año_fin",
        "media_maxima_historica", "fecha", "fecha_txt", "tmax",
        "latitud", "longitud", "tmax_txt", "media_maxima_historica_txt",
        "diferencia", "diferencia_txt", "categoria",
    ]
    cols_finales = [c for c in orden if c in maestro.columns] + [c for c in maestro.columns if c not in orden]
    maestro = maestro.loc[:, cols_finales]
    return maestro


# --- Subir a Google Sheets.
def hora() -> str:
    return _dt.now().strftime("[%Y-%m-%d %H:%M:%S] ")

def _parse_a1(celda: str):
    m = re.match(r"^([A-Za-z]+)(\d+)?$", celda)
    if not m:
        return "A", 1
    col, fila = m.group(1).upper(), int(m.group(2) or 1)
    return col, fila

def _exec_reintentado(req, intentos=5, espera_base=1.5):
    for i in range(intentos):
        try:
            return req.execute(num_retries=5)
        except Exception as e:
            transitorio = isinstance(e, TimeoutError) or isinstance(e, HttpError)
            if (i == intentos - 1) or not transitorio:
                raise
            time.sleep(espera_base * (2 ** i))

def _construir_servicio_sheets(ruta_credenciales: str, alcances: list[str]):
    if not _GSHEETS_DISPONIBLE:
        raise RuntimeError("Faltan dependencias de Google Sheets. Instala: google-api-python-client google-auth-httplib2 google-auth httplib2")
    cred = Credentials.from_service_account_file(ruta_credenciales, scopes=alcances)
    _http = httplib2.Http(timeout=500)
    _authed_http = AuthorizedHttp(cred, http=_http)
    return build("sheets", "v4", http=_authed_http, cache_discovery=False)

def subir_df_a_sheet(
    df: pd.DataFrame,
    spreadsheet_id: str,
    rango_inicial: str,
    pestana: str,
    ruta_credenciales: str,
    alcances: list[str] = ALCANCES_SHEETS,
    filas_bloque: int = 2000,
):
    servicio = _construir_servicio_sheets(ruta_credenciales=ruta_credenciales, alcances=alcances)

    df = df.copy()
    for c in ["LATITUD", "LONGITUD", "LATITUDE", "LONGITUDE", "latitud", "longitud"]:
        if c in df.columns:
            df[c] = df[c].astype(str)

    for col in df.columns:
        if is_datetime64_any_dtype(df[col]) or is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    def _a_texto(x):
        if isinstance(x, (pd.Timestamp, _dt)):
            return x.strftime("%Y-%m-%d %H:%M:%S")
        return x

    df = df.applymap(_a_texto).where(pd.notnull(df), None)

    print(f"{hora()}Limpiando hoja '{pestana}' …")
    _exec_reintentado(servicio.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=f"{pestana}!A1:ZZ"))

    cabecera = list(map(str, df.columns.tolist()))
    filas = [[("" if v is None else str(v)) for v in fila] for fila in df.to_numpy().tolist()]

    celda_a1 = rango_inicial.replace(f"{pestana}!", "")
    col_inicio, fila_inicio = _parse_a1(celda_a1)

    rango_cabecera = f"{pestana}!{col_inicio}{fila_inicio}"
    _exec_reintentado(
        servicio.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=rango_cabecera, valueInputOption="RAW", body={"values": [cabecera]}
        )
    )

    if not filas:
        print(f"{hora()}No hay filas para subir en '{pestana}'."); return

    fila_datos_inicio = fila_inicio + 1
    total = len(filas)
    bloques = math.ceil(total / filas_bloque)
    print(f"{hora()}Subiendo datos a '{pestana}' en {bloques} bloque(s) de hasta {filas_bloque} fila(s)…")

    for i in range(bloques):
        i0, i1 = i * filas_bloque, min((i + 1) * filas_bloque, total)
        bloque = filas[i0:i1]
        rango_escritura = f"{pestana}!{col_inicio}{fila_datos_inicio + i0}"
        _exec_reintentado(
            servicio.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, range=rango_escritura, valueInputOption="RAW", body={"values": bloque}
            )
        )
        print(f"{hora()}  · Bloque {i+1}/{bloques} ({i1 - i0} filas) OK")

# --- Ejecutar la función.
if __name__ == "__main__":
    RUTA_SALIDAS.mkdir(parents=True, exist_ok=True)

    print("Descargando por indicativos del Excel…")
    df_todas = descargar_por_indicativos_xlsx(RUTA_INDICATIVOS)

    print("Combinando con maestro…")
    df_maestro = combinar_con_maestro(df_todas, RUTA_MAESTRO)

    ruta_df_maestro = RUTA_SALIDAS / NOMBRE_XLSX_INTERMEDIO
    guardar_xlsx(df_maestro, ruta_df_maestro)
    print("Guardado df_maestro en:", ruta_df_maestro)

    print("Aplicando transformaciones (temperaturas)…")
    maestro = transformar_maestro_temperaturas(df_maestro)

    if "categoria" in maestro.columns:
        maestro["categoria"] = maestro["categoria"].astype(object).where(maestro["categoria"].notna(), "")

    ruta_final = RUTA_SALIDAS / NOMBRE_XLSX_FINAL
    with pd.ExcelWriter(ruta_final, engine="openpyxl") as writer:
        maestro.to_excel(writer, index=False)
    print("Exportado:", ruta_final)

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
