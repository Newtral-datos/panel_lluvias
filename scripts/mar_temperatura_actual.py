import re
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
URL_WFS = "https://view.eumetsat.int/geoserver/wfs"
CAPA = "eps__osisaf_avhrr_l3_sst"
HISTO_TYPENAME = "eps:osisaf_avhrr_l3_sst_histogram"
DIR_SALIDA = Path("/Users/miguel.ros/Desktop/PANEL_LLUVIAS/complementarios_mar/")
DIR_SALIDA.mkdir(parents=True, exist_ok=True)

def shrink_bbox(lat_min, lat_max, lon_min, lon_max, shrink=0.2):
    lat_c, lon_c = (lat_min + lat_max) / 2, (lon_min + lon_max) / 2
    lat_half, lon_half = (lat_max - lat_min) * (1 - shrink) / 2, (lon_max - lon_min) * (1 - shrink) / 2
    lat_min2, lat_max2 = max(-90.0, lat_c - lat_half), min(90.0, lat_c + lat_half)
    lon_min2, lon_max2 = max(-180.0, lon_c - lon_half), min(180.0, lon_c + lon_half)
    return lat_min2, lat_max2, lon_min2, lon_max2

def tiempos_disponibles():
    r = requests.get(URL_WFS, params={
        "service":"WFS","version":"2.0.0","request":"GetPropertyValue",
        "typeName": HISTO_TYPENAME,"valueReference":"time"
    }, timeout=90)
    r.raise_for_status()
    ts = re.findall(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", r.text)
    return sorted(set(pd.to_datetime(ts, utc=True)))

def elige_timestamp(lista, objetivo):
    candidatos = [t for t in lista if t <= objetivo]
    return candidatos[-1] if candidatos else lista[-1]

def descarga_wcs(fecha_iso, bbox, salida):
    lat_min, lat_max, lon_min, lon_max = bbox
    params = {
        "service": "WCS","version": "2.0.1","request": "GetCoverage",
        "coverageId": CAPA,"format": "image/geotiff",
        "subset": [
            f'time("{fecha_iso}")',
            f"Lat({lat_min},{lat_max})",
            f"Long({lon_min},{lon_max})",
        ],
    }
    query = [(k,v) for k,v in params.items() if k!="subset"]
    for s in params["subset"]: query.append(("subset", s))
    r = requests.get(URL_WCS, params=query, timeout=240); r.raise_for_status()
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
    gdf = gpd.GeoDataFrame({"sst_c":vals,"lon":lons,"lat":lats},geometry=geoms,crs="EPSG:4326")
    bins = list(range(5,45,5)); etiquetas=[f"{bins[i]}–{bins[i+1]}" for i in range(len(bins)-1)]
    if len(gdf):
        gdf["categoria"] = pd.cut(gdf["sst_c"],bins=bins,labels=etiquetas,include_lowest=True)
        gdf["sst_txt"] = gdf["sst_c"].apply(lambda x:f"{x:.1f}".replace(".",","))
        for col in gdf.select_dtypes(include="category").columns: gdf[col] = gdf[col].astype(str).fillna("")
    else:
        gdf["categoria"] = ""; gdf["sst_txt"] = ""
    try:
        gdf.to_file(geojson_path,driver="GeoJSON",engine="pyogrio",write_options={"RFC7946":"YES"})
    except: gdf.to_file(geojson_path,driver="GeoJSON")
    return len(gdf)

def main():
    LAT_MIN,LAT_MAX,LON_MIN,LON_MAX = shrink_bbox(START_LAT_MIN,START_LAT_MAX,START_LON_MIN,START_LON_MAX,SHRINK)
    hoy_utc = pd.Timestamp(datetime.utcnow().date(),tz="UTC")
    objetivo = (hoy_utc - pd.Timedelta(days=1)).replace(hour=12,minute=0,second=0,microsecond=0)
    lista_ts = tiempos_disponibles()
    ts_elegido = elige_timestamp(lista_ts,objetivo)
    fecha_iso = ts_elegido.strftime("%Y-%m-%dT%H:%M:%SZ")
    stamp = ts_elegido.strftime("%Y%m%d_%Hutc")
    tif = DIR_SALIDA / f"temperatura_mar_{stamp}.tif"
    geojson = DIR_SALIDA / f"temperatura_mar_{stamp}.geojson"
    try: descarga_wcs(fecha_iso,(LAT_MIN,LAT_MAX,LON_MIN,LON_MAX),tif)
    except: descarga_wcs(lista_ts[-1].strftime("%Y-%m-%dT%H:%M:%SZ"),(LAT_MIN,LAT_MAX,LON_MIN,LON_MAX),tif)
    n = procesar_tif(tif,geojson,PASO_CELDA)
    print("GeoJSON guardado en:",geojson); print("Total de puntos:",n)

if __name__ == "__main__": main()
