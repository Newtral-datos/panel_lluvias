# sst_actual_12utc.py
# pip install requests rasterio numpy shapely geopandas pandas python-dateutil

import requests, rasterio, numpy as np, geopandas as gpd, pandas as pd
from shapely.geometry import Point
from pathlib import Path
from datetime import datetime

# ===========================
# ⟵ PARÁMETROS AJUSTABLES
# ===========================
START_LAT_MIN, START_LAT_MAX = 0.0, 60.0
START_LON_MIN, START_LON_MAX = -40.0, 40.0
SHRINK = 0.2
# ===========================

URL_WCS = "https://view.eumetsat.int/geoserver/ows"
CAPA = "eps__osisaf_avhrr_l3_sst"
DIR_SALIDA = Path("/Users/miguel.ros/Desktop/PANEL_LLUVIAS/complementarios_mar/")
DIR_SALIDA.mkdir(parents=True, exist_ok=True)
PASO_CELDA = 2

def shrink_bbox(lat_min, lat_max, lon_min, lon_max, shrink=0.2):
    if not (0 <= shrink < 1):
        raise ValueError("SHRINK debe estar en [0, 1).")
    lat_c = (lat_min + lat_max) / 2.0
    lon_c = (lon_min + lon_max) / 2.0
    lat_half = (lat_max - lat_min) * (1 - shrink) / 2.0
    lon_half = (lon_max - lon_min) * (1 - shrink) / 2.0
    lat_min2, lat_max2 = lat_c - lat_half, lat_c + lat_half
    lon_min2, lon_max2 = lon_c - lon_half, lon_c + lon_half
    lat_min2, lat_max2 = max(-90.0, lat_min2), min(90.0, lat_max2)
    lon_min2, lon_max2 = max(-180.0, lon_min2), min(180.0, lon_max2)
    return lat_min2, lat_max2, lon_min2, lon_max2

LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = shrink_bbox(
    START_LAT_MIN, START_LAT_MAX, START_LON_MIN, START_LON_MAX, shrink=SHRINK
)
print(f"BBox original:  lat=({START_LAT_MIN}, {START_LAT_MAX}), lon=({START_LON_MIN}, {START_LON_MAX})")
print(f"BBox reducido:  lat=({LAT_MIN:.3f}, {LAT_MAX:.3f}), lon=({LON_MIN:.3f}, {LON_MAX:.3f})")

hoy_utc = pd.Timestamp(datetime.utcnow().date())
ayer_utc = hoy_utc - pd.Timedelta(days=1)
fecha = ayer_utc + pd.Timedelta(hours=12)
fecha_iso = fecha.strftime("%Y-%m-%dT%H:%M:%SZ")
print("Descargando datos para:", fecha_iso)

stamp = fecha.strftime("%Y%m%d_12utc")
TIF_SALIDA = DIR_SALIDA / f"temperatura_mar_{stamp}.tif"
GEOJSON_SALIDA = DIR_SALIDA / f"temperatura_mar_{stamp}.geojson"

params = {
    "service": "WCS","version": "2.0.1","request": "GetCoverage","coverageId": CAPA,
    "format": "image/tiff",
    "subset": [f'time("{fecha_iso}")', f"Lat({LAT_MIN},{LAT_MAX})", f"Long({LON_MIN},{LON_MAX})"]
}
query = [(k, v) for k, v in params.items() if k != "subset"]
for s in params["subset"]:
    query.append(("subset", s))
resp = requests.get(URL_WCS, params=query, timeout=240)
resp.raise_for_status()
with open(TIF_SALIDA, "wb") as f:
    f.write(resp.content)

with rasterio.open(TIF_SALIDA) as ds:
    arr = ds.read(1).astype("float64")
    nodata, tags, transform = ds.nodata, ds.tags(1), ds.transform
if nodata is not None:
    arr[arr == nodata] = np.nan
arr = arr * float(tags.get("scale_factor", 1.0)) + float(tags.get("add_offset", 0.0))
if np.nanmin(arr) > 150:
    arr = arr - 273.15

filas, cols = arr.shape
geoms, vals, lons, lats = [], [], [], []
for r in range(0, filas, PASO_CELDA):
    for c in range(0, cols, PASO_CELDA):
        v = arr[r, c]
        if np.isfinite(v):
            x, y = rasterio.transform.xy(transform, r, c)
            geoms.append(Point(x, y))
            vals.append(float(v))
            lons.append(x)
            lats.append(y)

gdf = gpd.GeoDataFrame({"sst_c": vals, "lon": lons, "lat": lats}, geometry=geoms, crs="EPSG:4326")

bins = list(range(5, 45, 5))
etiquetas = [f"{bins[i]}–{bins[i+1]}" for i in range(len(bins)-1)]
gdf["categoria"] = pd.cut(gdf["sst_c"], bins=bins, labels=etiquetas, include_lowest=True)
gdf["sst_txt"] = gdf["sst_c"].apply(lambda x: f"{x:.1f}".replace(".", ","))

gdf.to_file(GEOJSON_SALIDA, driver="GeoJSON")
print("GeoJSON guardado en:", GEOJSON_SALIDA)
print("Total de puntos:", len(gdf))
