# -*- coding: utf-8 -*-

import geopandas as gpd
import pandas as pd
from pathlib import Path
from datetime import datetime

# =========================
# Configuración de paths
# =========================
DIR = Path("/Users/miguel.ros/Desktop/PANEL_LLUVIAS/complementarios_mar/")

# =========================
# Opciones de subida a Google Sheets
# =========================
SUBIR_A_SHEETS    = True
ID_HOJA_CALCULO   = "1o0DICxbYpq_OqgwTqU9-8GaQzjYj14cdureHGN-uLQA"
NOMBRE_PESTANA    = "temperatura_mar"
INICIO_A1         = f"{NOMBRE_PESTANA}!A1"
RUTA_CREDENCIALES = "/Users/miguel.ros/Desktop/PANEL_LLUVIAS/credenciales_google_sheet.json"
ALCANCES_SHEETS   = ["https://www.googleapis.com/auth/spreadsheets"]

# =========================
# Dependencias y helpers Google Sheets
# =========================
import re, time, math
from datetime import datetime as _dt
from pandas.api.types import is_datetime64_any_dtype, is_datetime64tz_dtype

try:
    import httplib2
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from google_auth_httplib2 import AuthorizedHttp
    _GSHEETS_DISPONIBLE = True
except Exception:
    _GSHEETS_DISPONIBLE = False

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
        raise RuntimeError(
            "Faltan dependencias de Google Sheets. Instala: "
            "google-api-python-client google-auth-httplib2 google-auth httplib2"
        )
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

    # Asegurar que lat/lon como texto si existiesen con estos nombres habituales
    for c in ["LATITUD", "LONGITUD", "LATITUDE", "LONGITUDE", "latitud", "longitud"]:
        if c in df.columns:
            df[c] = df[c].astype(str)

    # Formatear columnas de fecha/hora
    for col in df.columns:
        if is_datetime64_any_dtype(df[col]) or is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    def _a_texto(x):
        if isinstance(x, (pd.Timestamp, _dt)):
            return x.strftime("%Y-%m-%d %H:%M:%S")
        return x

    df = df.applymap(_a_texto).where(pd.notnull(df), None)

    print(f"{hora()}Limpiando hoja '{pestana}' …")
    _exec_reintentado(
        servicio.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=f"{pestana}!A1:ZZ"
        )
    )

    cabecera = list(map(str, df.columns.tolist()))
    filas = [[("" if v is None else str(v)) for v in fila] for fila in df.to_numpy().tolist()]

    celda_a1 = rango_inicial.replace(f"{pestana}!", "")
    col_inicio, fila_inicio = _parse_a1(celda_a1)

    rango_cabecera = f"{pestana}!{col_inicio}{fila_inicio}"
    _exec_reintentado(
        servicio.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=rango_cabecera,
            valueInputOption="RAW",
            body={"values": [cabecera]},
        )
    )

    if not filas:
        print(f"{hora()}No hay filas para subir en '{pestana}'.")
        return

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
                spreadsheetId=spreadsheet_id,
                range=rango_escritura,
                valueInputOption="RAW",
                body={"values": bloque},
            )
        )
        print(f"{hora()}  · Bloque {i+1}/{bloques} ({i1 - i0} filas) OK")

# =========================
# Lógica original
# =========================

# Carácter invisible para forzar tipo texto en parsers (Flourish, etc.)
INVISIBLE_PREFIX = "\u200B"  # zero-width space

hoy_utc = pd.Timestamp(datetime.utcnow().date())   # fecha de hoy (UTC) sin hora
ayer_utc = hoy_utc - pd.Timedelta(days=1)          # día anterior
fecha = ayer_utc + pd.Timedelta(hours=12)          # 12:00Z del día anterior
stamp = fecha.strftime("%Y%m%d_12utc")

# Leer datos
gdf_actual = gpd.read_file(DIR / f"temperatura_mar_{stamp}.geojson")[["lon", "lat", "sst_c"]]
gdf_actual = gdf_actual.rename(columns={"sst_c": "sst_actual"})

gdf_hist = gpd.read_file(DIR / "ssc_septiembre_historico.geojson")[["lon", "lat", "sst_media_sep_c"]]

# Asegurar que lon y lat son numéricas float
for col in ["lon", "lat"]:
    gdf_actual[col] = pd.to_numeric(gdf_actual[col], errors="coerce").astype(float)
    gdf_hist[col] = pd.to_numeric(gdf_hist[col], errors="coerce").astype(float)

# Usar valor histórico tal cual (sin calcular medias)
df_hist_media = gdf_hist.rename(columns={"sst_media_sep_c": "sst_hist_media"})

# Unir y calcular diferencia
df_comp = gdf_actual.merge(df_hist_media, on=["lon", "lat"], how="inner")
df_comp["diferencia"] = df_comp["sst_actual"] - df_comp["sst_hist_media"]

# Redondear temperaturas a 1 decimal
temp_cols = ["sst_actual", "sst_hist_media", "diferencia"]
df_comp[temp_cols] = df_comp[temp_cols].astype(float).round(1)

# Categorización de sst_actual
bins = list(range(5, 45, 5))
labels = [f"{bins[i]}–{bins[i+1]}" for i in range(len(bins)-1)]
df_comp["sst_actual_cat"] = pd.cut(df_comp["sst_actual"], bins=bins, labels=labels, include_lowest=True)

# Formateadores (coma decimal)
def fmt_es(x):
    if pd.isna(x):
        return ""
    s = f"{x:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return INVISIBLE_PREFIX + s  # fuerza texto

def fmt_es_signed(x):
    if pd.isna(x):
        return ""
    s = f"{x:+,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return INVISIBLE_PREFIX + s  # fuerza texto y mantiene el +

# Crear columnas de texto
for c in temp_cols:
    df_comp[c + "_txt"] = df_comp[c].apply(fmt_es)
# Sobrescribir diferencia_txt con formato con signo (+ también en 0)
df_comp["diferencia_txt"] = df_comp["diferencia"].apply(fmt_es_signed)

# Asegurar tipos: numéricos en lon/lat y temp_cols
for c in ["lon", "lat"] + temp_cols:
    df_comp[c] = df_comp[c].astype(float)

# Asegurar que *_txt son texto
for c in [col + "_txt" for col in temp_cols]:
    df_comp[c] = df_comp[c].astype(str)

# Orden columnas
cols = ["lon", "lat", "sst_actual", "sst_actual_cat", "sst_hist_media", "diferencia"] \
       + [c + "_txt" for c in temp_cols]
df_comp = df_comp[cols]
ayer_str = ayer_utc.strftime("%d/%m/%Y")
FECHA_ACTUALIZADO = ayer_str
df_comp["fecha_actualizado"] = FECHA_ACTUALIZADO

# Exportar a XLSX
out_xlsx = DIR / "comparacion_actual_vs_hist.xlsx"
df_comp.to_excel(out_xlsx, index=False, sheet_name="comparacion")

print("Guardado:", out_xlsx)
print(df_comp.dtypes)

# =========================
# Subida a Google Sheets (opcional)
# =========================
if SUBIR_A_SHEETS:
    print(f"{hora()}Subiendo DataFrame a Google Sheets…")
    try:
        subir_df_a_sheet(
            df=df_comp,
            spreadsheet_id=ID_HOJA_CALCULO,
            rango_inicial=INICIO_A1,
            pestana=NOMBRE_PESTANA,
            ruta_credenciales=RUTA_CREDENCIALES,
            alcances=ALCANCES_SHEETS,
        )
        print(f"{hora()}Subida completada en la hoja '{NOMBRE_PESTANA}'.")
    except Exception as e:
        print(f"{hora()}ERROR subiendo a Google Sheets: {e}")
