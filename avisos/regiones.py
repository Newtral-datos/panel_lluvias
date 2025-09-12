import geopandas as gpd
import pandas as pd
from shapely.ops import transform

carpeta = "/Users/miguel.ros/Desktop/PANEL_LLUVIAS/delimitacion_zonas/"

zonas = gpd.read_file(f"{carpeta}zonas.geojson")
costeras = gpd.read_file(f"{carpeta}zonas_costeras.geojson")

target_crs = zonas.crs or costeras.crs or "EPSG:4326"
if zonas.crs != target_crs:
    zonas = zonas.to_crs(target_crs)
if costeras.crs != target_crs:
    costeras = costeras.to_crs(target_crs)

zonas_aemet = gpd.GeoDataFrame(
    pd.concat(
        [zonas.assign(origen="zonas"),
         costeras.assign(origen="zonas_costeras")],
        ignore_index=True,
        sort=False
    ),
    crs=target_crs
).reset_index(drop=True)

columnas = ["NOM_Z", "NOM_PROV", "NOM_CCAA", "geometry"]
zonas_aemet = zonas_aemet[columnas]
zonas_aemet = zonas_aemet.rename(columns={"NOM_Z": "zona", "NOM_PROV": "PROVINCIA", "NOM_CCAA": "CCAA"})

zonas_aemet = zonas_aemet[~zonas_aemet.geometry.isna()].copy()
invalid = ~zonas_aemet.geometry.is_valid
if invalid.any():
    zonas_aemet.loc[invalid, "geometry"] = zonas_aemet.loc[invalid, "geometry"].buffer(0)

def to_2d(geom):
    if geom is None or geom.is_empty:
        return geom
    return transform(lambda x, y, z=None: (x, y), geom)

zonas_aemet["geometry"] = zonas_aemet["geometry"].apply(to_2d)
zonas_aemet = zonas_aemet.to_crs(4326)

salida = "/Users/miguel.ros/Desktop/PANEL_LLUVIAS/avisos/delimitaciones_aemet.geojson"
try:
    zonas_aemet.to_file(salida, driver="GeoJSON", engine="pyogrio", write_options={"RFC7946": "YES"})
except Exception:
    zonas_aemet.to_file(salida, driver="GeoJSON")
