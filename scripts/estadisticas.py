import pandas as pd
from datetime import datetime
import math, time, re
from datetime import datetime as _dt
from pandas.api.types import is_datetime64_any_dtype, is_datetime64tz_dtype
from pathlib import Path

directorio = "/Users/miguel.ros/Desktop/PANEL_LLUVIAS/"

# --- Estadísticas de las lluvias.
ruta_historico_lluvias = f"{directorio}complementarios_lluvias/"

historico_lluvias = pd.read_excel(f"{ruta_historico_lluvias}datos_historicos.xlsx")
lluvias_media_agosto_historico_mensual = historico_lluvias["precip_media_mensual_historica"].mean()
lluvias_media_agosto_historico_diario = lluvias_media_agosto_historico_mensual / 30

datos_ultimas_lluvias = pd.read_excel(f"{directorio}MAPA_LLUVIAS.xlsx")
ultimas_lluvias = datos_ultimas_lluvias["prec"].sum()
print(f"Ha llovido {ultimas_lluvias} mm en las últimas 24 horas.")
lluvia_ultimas_media = datos_ultimas_lluvias["prec"].mean()

lluvias_variacion_pct = ((lluvia_ultimas_media - lluvias_media_agosto_historico_diario) / lluvias_media_agosto_historico_diario) * 100
print(f"La media reciente de lluvias ha variado un {lluvias_variacion_pct:.2f}% respecto al histórico diario de agosto.")

# --- Crear DataFrame con los resultados.
fecha_actual = datetime.now().strftime("%d/%m/%Y a las %H:%M")

def num_es(n, dec=1):
    s = f"{float(n):,.{dec}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

resultados = pd.DataFrame([{
    "actualizacion": fecha_actual,
    "precipitaciones": num_es(ultimas_lluvias, 1),
    "diferencia": num_es(lluvias_variacion_pct, 1)
}])

# --- Subir a Google Sheet.
SUBIR_A_SHEETS    = True
ID_HOJA_CALCULO   = "1o0DICxbYpq_OqgwTqU9-8GaQzjYj14cdureHGN-uLQA"
NOMBRE_PESTANA    = "datos"
INICIO_A1         = f"{NOMBRE_PESTANA}!A1"
RUTA_CREDENCIALES = f"{directorio}credenciales_google_sheet.json"
ALCANCES_SHEETS   = ["https://www.googleapis.com/auth/spreadsheets"]

try:
    import httplib2
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from google_auth_httplib2 import AuthorizedHttp
    from google.oauth2.service_account import Credentials
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
    for col in df.columns:
        if is_datetime64_any_dtype(df[col]) or is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df.applymap(lambda x: x if pd.notnull(x) else None)

    print(f"{hora()}Limpiando hoja '{pestana}' …")
    _exec_reintentado(servicio.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{pestana}!A1:ZZ"
    ))

    cabecera = list(map(str, df.columns.tolist()))
    celda_a1 = rango_inicial.replace(f"{pestana}!", "")
    col_inicio, fila_inicio = _parse_a1(celda_a1)
    rango_cabecera = f"{pestana}!{col_inicio}{fila_inicio}"

    _exec_reintentado(servicio.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=rango_cabecera,
        valueInputOption="RAW",
        body={"values": [cabecera]}
    ))

    filas = [[("" if v is None else str(v)) for v in fila] for fila in df.to_numpy().tolist()]
    if not filas:
        print(f"{hora()}No hay filas para subir en '{pestana}'."); return

    fila_datos_inicio = fila_inicio + 1
    total = len(filas)
    bloques = math.ceil(total / filas_bloque)
    print(f"{hora()}Subiendo datos a '{pestana}' en {bloques} bloque(s)…")

    for i in range(bloques):
        i0, i1 = i * filas_bloque, min((i + 1) * filas_bloque, total)
        bloque = filas[i0:i1]
        rango_escritura = f"{pestana}!{col_inicio}{fila_datos_inicio + i0}"
        _exec_reintentado(servicio.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=rango_escritura,
            valueInputOption="RAW",
            body={"values": bloque}
        ))
        print(f"{hora()}  · Bloque {i+1}/{bloques} ({i1 - i0} filas) OK")

# --- Llamada para subir los datos.
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
                df=resultados,
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
