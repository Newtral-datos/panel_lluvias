from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import geopandas as gpd
from shapely.ops import transform

URL = "https://www.aemet.es/es/eltiempo/prediccion/avisos?r=1"

opts = webdriver.ChromeOptions()
opts.add_argument("--headless=new")
opts.add_argument("--lang=es-ES")
opts.add_argument("--user-agent=Mozilla/5.0")
driver = webdriver.Chrome(options=opts)

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

ncols = max((len(r) for r in data), default=0)
if not headers or len(headers) != ncols:
    headers = [f"col_{i+1}" for i in range(ncols)]
data = [r + [""] * (ncols - len(r)) for r in data]
df = pd.DataFrame(data, columns=headers)

drop_cols = [c for c in df.columns if c.strip().lower() in ("probabilidad", "comentario")]
df = df.drop(columns=drop_cols, errors="ignore")

src = next((c for c in df.columns if c.strip().lower() in ("zona de avisos", "zona de aviso", "zona avisos", "zona")), None)
if src is not None:
    s = df[src].astype(str).str.replace("–", "-", regex=False)
    parts = s.str.extract(r"^(?P<zona>.+?)\s*-\s*(?P<ccaa>.+)$")
    df["zona"] = parts["zona"].fillna(s).str.strip()
    df["ccaa"] = parts["ccaa"].fillna("").str.strip()
    df = df.drop(columns=[src])
    df = df[["zona", "ccaa"] + [c for c in df.columns if c not in ("zona", "ccaa")]]

def norm(x: str) -> str:
    x = x.lower()
    x = x.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u")
    return " ".join(x.split())

def only_time_series(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    out = dt.dt.strftime("%H:%M")
    mask = dt.isna()
    if mask.any():
        out.loc[mask] = s.loc[mask].str.extract(r"(\d{1,2}:\d{2})", expand=False)
    return out.fillna("").str.strip()

norm_cols = {c: norm(c) for c in df.columns}
col_inicio = next((c for c,nc in norm_cols.items() if nc in ("hora de comienzo","hora comienzo","inicio")), None)
col_fin = next((c for c,nc in norm_cols.items() if nc in ("hora de finalizacion","hora finalizacion","fin","hora de finalización")), None)

if col_inicio:
    df[col_inicio] = only_time_series(df[col_inicio].astype(str))
if col_fin:
    df[col_fin] = only_time_series(df[col_fin].astype(str))

zonas_aemet = gpd.read_file("/Users/miguel.ros/Desktop/PANEL_LLUVIAS/avisos/delimitaciones_aemet.geojson")
df_geo = df.merge(zonas_aemet, left_on="zona", right_on="zona", how="left")
df_geo = gpd.GeoDataFrame(df_geo, geometry="geometry", crs=zonas_aemet.crs)

df_geo = df_geo[~df_geo.geometry.isna()].copy()
invalid = ~df_geo.geometry.is_valid
if invalid.any():
    df_geo.loc[invalid, "geometry"] = df_geo.loc[invalid, "geometry"].buffer(0)

def to_2d(geom):
    if geom is None or geom.is_empty:
        return geom
    return transform(lambda x, y, z=None: (x, y), geom)

df_geo["geometry"] = df_geo["geometry"].apply(to_2d)
df_geo = df_geo.to_crs(4326)

print(df_geo)

salida = "/Users/miguel.ros/Desktop/PANEL_LLUVIAS/avisos/avisos.geojson"
try:
    df_geo.to_file(salida, driver="GeoJSON", engine="pyogrio", write_options={"RFC7946": "YES"})
except Exception:
    df_geo.to_file(salida, driver="GeoJSON")
