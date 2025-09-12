# --- Importaciones necesarias.
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import geopandas as gpd
from shapely.ops import transform

# --- URL de origen de los datos.
URL = "https://www.aemet.es/es/eltiempo/prediccion/avisos?r=1"

# --- Configuración del navegador en modo headless.
opts = webdriver.ChromeOptions()
opts.add_argument("--headless=new")
opts.add_argument("--lang=es-ES")
opts.add_argument("--user-agent=Mozilla/5.0")
driver = webdriver.Chrome(options=opts)

# --- Descarga y parseo de la tabla HTML.
data = []
headers = []
try:
    driver.get(URL)
    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".table")))
    table = driver.find_element(By.CSS_SELECTOR, ".table")
    ths = table.find_elements(By.CSS_SELECTOR, "thead tr th")
    headers = [th.text.strip() for th in ths] if ths else []
    for tr in table.find_elements(By.CSS_SELECTOR, "tbody tr"):
        tds = tr.find_elements(By.CSS_SELECTOR, "td")
        data.append([td.text.strip() for td in tds])
finally:
    driver.quit()

# --- Construcción del DataFrame con cabeceras seguras.
ncols = max((len(r) for r in data), default=0)
if not headers or len(headers) != ncols:
    headers = [f"col_{i+1}" for i in range(ncols)]
data = [r + [""] * (ncols - len(r)) for r in data]
df = pd.DataFrame(data, columns=headers)

# --- Eliminación de columnas no necesarias.
drop_cols = [c for c in df.columns if c.strip().lower() in ("probabilidad", "comentario")]
df = df.drop(columns=drop_cols, errors="ignore")

# --- Separación de 'zona' y 'ccaa' desde la columna de zona original.
src = next((c for c in df.columns if c.strip().lower() in ("zona de avisos", "zona de aviso", "zona avisos", "zona")), None)
if src is not None:
    s = df[src].astype(str).str.replace("–", "-", regex=False)
    parts = s.str.extract(r"^(?P<zona>.+?)\s*-\s*(?P<ccaa>.+)$")
    df["zona"] = parts["zona"].fillna(s).str.strip()
    df["ccaa"] = parts["ccaa"].fillna("").str.strip()
    df = df.drop(columns=[src])
    df = df[["zona", "ccaa"] + [c for c in df.columns if c not in ("zona", "ccaa")]]

# --- Función de normalización simple de texto.
def norm(x: str) -> str:
    x = x.lower()
    x = x.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u")
    return " ".join(x.split())

# --- Conversión de series de texto a horas HH:MM cuando sea posible.
def only_time_series(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    out = dt.dt.strftime("%H:%M")
    mask = dt.isna()
    if mask.any():
        out.loc[mask] = s.loc[mask].str.extract(r"(\d{1,2}:\d{2})", expand=False)
    return out.fillna("").str.strip()

# --- Detección de columnas de inicio y fin de aviso.
norm_cols = {c: norm(c) for c in df.columns}
col_inicio = next((c for c,nc in norm_cols.items() if nc in ("hora de comienzo","hora comienzo","inicio")), None)
col_fin = next((c for c,nc in norm_cols.items() if nc in ("hora de finalizacion","hora finalizacion","fin","hora de finalización")), None)

# --- Normalización de horas si existen.
if col_inicio:
    df[col_inicio] = only_time_series(df[col_inicio].astype(str))
if col_fin:
    df[col_fin] = only_time_series(df[col_fin].astype(str))

# --- Carga de delimitaciones AEMET y cruce por 'zona'.
zonas_aemet = gpd.read_file("/Users/miguel.ros/Desktop/PANEL_LLUVIAS/complementarios_avisos/delimitaciones_aemet.geojson")
df_geo = df.merge(zonas_aemet, left_on="zona", right_on="zona", how="left")
df_geo = gpd.GeoDataFrame(df_geo, geometry="geometry", crs=zonas_aemet.crs)

# --- Filtrado de filas sin geometría y reparación de geometrías inválidas.
df_geo = df_geo[~df_geo.geometry.isna()].copy()
invalidas = ~df_geo.geometry.is_valid
if invalidas.any():
    df_geo.loc[invalidas, "geometry"] = df_geo.loc[invalidas, "geometry"].buffer(0)

# --- Función para forzar geometrías 2D.
def a_2d(geom):
    if geom is None or geom.is_empty:
        return geom
    return transform(lambda x, y, z=None: (x, y), geom)

# --- Aplicación de 2D y reproyección a WGS84.
df_geo["geometry"] = df_geo["geometry"].apply(a_2d)
df_geo = df_geo.to_crs(4326)

# --- Mantener solo una fila por zona priorizando el mayor nivel de riesgo.
# --- Mapa de prioridades del nivel de riesgo.
prioridad_nivel = {
    "Riesgo importante": 2,
    "Riesgo": 1
}
# --- Columna auxiliar de prioridad.
df_geo["_prioridad"] = df_geo["Nivel de riesgo"].map(prioridad_nivel).fillna(0)
# --- Ordenar por mayor prioridad y eliminar duplicados por zona.
df_geo = (
    df_geo.sort_values("_prioridad", ascending=False)
          .drop_duplicates(subset="zona", keep="first")
          .drop(columns="_prioridad")
)

# --- Visualización rápida por consola.
print(df_geo)

# --- Exportación a GeoJSON en WGS84.
salida = "/Users/miguel.ros/Desktop/PANEL_LLUVIAS/MAPA_AVISOS_AEMET.geojson"
try:
    df_geo.to_file(salida, driver="GeoJSON", engine="pyogrio", write_options={"RFC7946": "YES"})
except Exception:
    df_geo.to_file(salida, driver="GeoJSON")

# --- Opciones de subida a Google Sheets.
SUBIR_A_SHEETS    = True
ID_HOJA_CALCULO   = "1o0DICxbYpq_OqgwTqU9-8GaQzjYj14cdureHGN-uLQA" 
NOMBRE_PESTANA    = "avisos_aemet"
INICIO_A1         = f"{NOMBRE_PESTANA}!A1"
RUTA_CREDENCIALES = "/Users/miguel.ros/Desktop/PANEL_LLUVIAS/credenciales_google_sheet.json"
ALCANCES_SHEETS   = ["https://www.googleapis.com/auth/spreadsheets"]

# --- Dependencias y helpers Google Sheets.
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

# --- Construcción del servicio de Google Sheets.
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

df_sin_geom = df_geo.drop(columns=df_geo.geometry.name)

if SUBIR_A_SHEETS:
    print(f"{hora()}Subiendo DataFrame a Google Sheets…")
    try:
        subir_df_a_sheet(
            df=df_sin_geom,
            spreadsheet_id=ID_HOJA_CALCULO,
            rango_inicial=INICIO_A1,
            pestana=NOMBRE_PESTANA,
            ruta_credenciales=RUTA_CREDENCIALES,
            alcances=ALCANCES_SHEETS,
        )
        print(f"{hora()}Subida completada en la hoja '{NOMBRE_PESTANA}'.")
    except Exception as e:
        print(f"{hora()}ERROR subiendo a Google Sheets: {e}")
