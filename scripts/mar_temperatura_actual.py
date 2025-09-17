import re
import math
import requests
import rasterio
import numpy as np
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from pathlib import Path
from datetime import datetime

START_LAT_MIN, START_LAT_MAX = 0.0, 60.0
START_LON_MIN, START_LON_MAX = -40.0, 40.0
SHRINK = 0.2
PASO_CELDA = 2

URL_WCS = "https://view.eumetsat.int/geoserver/ows"
URL_WMS = "https://view.eumetsat.int/geoserver/wms"
COVERAGE_ID = "eps__osisaf_avhrr_l3_sst"     # WCS
WMS_LAYERS_CANDIDATES = ["eps:osisaf_avhrr_l3_sst", "eps__osisaf_avhrr_l3_sst"]  # WMS nombres posibles

DIR_SALIDA = Path("/Users/miguel.ros/Desktop/PANEL_LLUVIAS/complementarios_mar/")
DIR_SALIDA.mkdir(parents=True, exist_ok=True)

HDRS = {"User-Agent": "Mozilla/5.0 (compatible; panel-lluvias/1.0)"}

def shrink_bbox(lat_min, lat_max, lon_min, lon_max, shrink=0.2):
    lat_c, lon_c = (lat_min + lat_max) / 2, (lon_min + lon_max) / 2
    lat_half, lon_half = (lat_max - lat_min) * (1 - shrink) / 2, (lon_max - lon_min) * (1 - shrink) / 2
    lat_min2, lat_max2 = max(-90.0, lat_c - lat_half), min(90.0, lat_c + lat_half)
    lon_min2, lon_max2 = max(-180.0, lon_c - lon_half), min(180.0, lon_c + lon_half)
    return lat_min2, lat_max2, lon_min2, lon_max2

def _parse_iso_duration(d):
    m = re.fullmatch(r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?", d)
    if not m: return pd.Timedelta(0)
    days = int(m.group(1) or 0)
    hours = int(m.group(2) or 0)
    minutes = int(m.group(3) or 0)
    seconds = int(m.group(4) or 0)
    return pd.Timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

def _expand_time_dimension(txt):
    txt = txt.strip()
    if "/" in txt:  # start/end/period
        parts = txt.split(",")  # algunos servidores mezclan varios rangos separados por coma
        out = []
        for p in parts:
            p = p.strip()
            if not p: continue
            s, e, step = p.split("/")
            start = pd.to_datetime(s, utc=True)
            end = pd.to_datetime(e, utc=True)
            period = _parse_iso_duration(step)
            if period <= pd.Timedelta(0):  # seguridad
                period = pd.Timedelta(hours=1)
            t = start
            # proteger contra bucles infinitos
            max_iter = 100000
            while t <= end and max_iter > 0:
                out.append(t)
                t = t + period
                max_iter -= 1
        return sorted(set(out))
    else:  # lista separada por comas
        vals = [v.strip() for v in txt.split(",") if v.strip()]
        return sorted(set(pd.to_datetime(vals, utc=True)))
    
def tiempos_por_wms(layer_candidates):
    r = requests.get(URL_WMS, params={
        "service":"WMS","version":"1.3.0","request":"GetCapabilities"
    }, timeout=90, headers=HDRS)
    r.raise_for_status()
    xml = r.text
    for name in layer_candidates:
        # busca <Layer>…<Name>name</Name>…<Dimension name="time">…</Dimension>
        m_layer = re.search(rf"<Layer[^>]*>\s*<Name>\s*{re.escape(name)}\s*</Name>(.*?)</Layer>", xml, re.S|re.I)
        if not m_layer: continue
        block = m_layer.group(1)
        m_dim = re.search(r'<Dimension[^>]+name\s*=\s*"(?:time|TIME)"[^>]*>(.*?)</Dimension>', block, re.S|re.I)
        if not m_dim: continue
        times_txt = m_dim.group(1)
        lst = _expand_time_dimension(times_txt)
        if lst: return lst
    return []

def elige_timestamp(lista, objetivo):
    if not lista: return None
    cand = [t for t in lista if t <= objetivo]
    return cand[-1] if cand else lista[-1]

def descarga_wcs(fecha_iso, bbox, salida):
    lat_min, lat_max, lon_min, lon_max = bbox
    params = {
        "service":"WCS","version":"2.0.1","request":"GetCoverage",
        "coverageId":COVERAGE_ID,"format":"image/geotiff",
    }
    query = [(k,v) for k,v in params.items()]
    query += [("subset", f'time("{fecha_iso}")')]
    query += [("subset", f"Lat({lat_min},{lat_max})")]
    query += [("subset", f"Long({lon_min},{lon_max})")]
    r = requests.get(URL_WCS, params=query, timeout=240, headers=HDRS)
    r.raise_for_status()
    with open(salida, "wb") as f: f.write(r.content)

def procesar_tif(tif_path, geojson_path, paso):
    with rasterio.open(tif_path) as ds:
        arr = ds.read(1).astype("float64"); nodata = ds.nodata
        tags = ds.tags(1) if ds.count >= 1 else {}; transform = ds.transform
    if nodata is not None: arr[arr == nodata] = np.nan
    arr = arr * float(tags.get("scale_factor",1)) + float(tags.get("add_offset",0))
    try:
        if float(np.nanmin(arr)) > 150: arr = arr - 273.15
    except: pass
    filas, cols = arr.shape; geoms, vals, lons, lats = [], [], [], []
    for r in range(0, filas, paso):
        for c in range(0, cols, paso):
            v = arr[r,c]
            if np.isfinite(v):
                x,y = rasterio.transform.xy(transform,r,c)
                geoms.append(Point(x,y)); vals.append(float(v)); lons.append(float(x)); lats.append(float(y))
    gdf = gpd.GeoDataFrame({"sst_c":vals,"lon":lons,"lat":lats}, geometry=geoms, crs="EPSG:4326")
    bins = list(range(5,45,5)); etiquetas=[f"{bins[i]}–{bins[i+1]}" for i in range(len(bins)-1)]
    if len(gdf):
        gdf["categoria"] = pd.cut(gdf["sst_c"], bins=bins, labels=etiquetas, include_lowest=True)
        gdf["sst_txt"] = gdf["sst_c"].apply(lambda x:f"{x:.1f}".replace(".",","))
        for col in gdf.select_dtypes(include="category").columns: gdf[col] = gdf[col].astype(str).fillna("")
    else:
        gdf["categoria"] = ""; gdf["sst_txt"] = ""
    try:
        gdf.to_file(geojson_path, driver="GeoJSON", engine="pyogrio", write_options={"RFC7946":"YES"})
    except: gdf.to_file(geojson_path, driver="GeoJSON")
    return len(gdf)

def main():
    LAT_MIN,LAT_MAX,LON_MIN,LON_MAX = shrink_bbox(START_LAT_MIN,START_LAT_MAX,START_LON_MIN,START_LON_MAX,SHRINK)
    hoy_utc = pd.Timestamp(datetime.utcnow().date(), tz="UTC")
    objetivo = (hoy_utc - pd.Timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)

    lista_ts = tiempos_por_wms(WMS_LAYERS_CANDIDATES)
    if not lista_ts: raise RuntimeError("No hay tiempos disponibles en WMS para la capa.")
    ts = elige_timestamp(lista_ts, objetivo)
    fecha_iso = ts.strftime("%Y-%m-%dT%H:%M:%SZ")

    stamp = ts.strftime("%Y%m%d_%Hutc")
    tif = DIR_SALIDA / f"temperatura_mar_{stamp}.tif"
    geojson = DIR_SALIDA / f"temperatura_mar_{stamp}.geojson"

    try:
        descarga_wcs(fecha_iso, (LAT_MIN,LAT_MAX,LON_MIN,LON_MAX), tif)
    except requests.HTTPError:
        # fallback: último disponible absoluto
        fecha_iso_fallback = lista_ts[-1].strftime("%Y-%m-%dT%H:%M:%SZ")
        descarga_wcs(fecha_iso_fallback, (LAT_MIN,LAT_MAX,LON_MIN,LON_MAX), tif)

    n = procesar_tif(tif, geojson, PASO_CELDA)
    print("GeoJSON guardado en:", geojson); print("Total de puntos:", n)

if __name__ == "__main__": main()
