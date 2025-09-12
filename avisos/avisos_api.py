import io, json, zipfile, tarfile, requests, xml.etree.ElementTree as ET
from shapely.geometry import Polygon, mapping
from datetime import datetime
from zoneinfo import ZoneInfo
from api_key import api_key

url=f"https://opendata.aemet.es/opendata/api/avisos_cap/ultimoelaborado/area/esp?api_key={api_key}"
salida="/Users/miguel.ros/Desktop/PANEL_LLUVIAS/avisos/avisos_esp.geojson"

s=requests.Session()
s.headers.update({"Accept":"application/json","User-Agent":"aemet-client/1.0","Accept-Language":"es-ES,es;q=0.9"})

def ns(t): return t.split("}",1)[1] if "}" in t else t

def is_spanish(text: str) -> bool:
    if not text: return False
    t=text.lower()
    if any(ch in t for ch in "áéíóúñ"):
        return True
    dic_es=("lluv","viento","torment","costa","interior","noroeste","aviso",
            "amarillo","naranja","rojo","nieve","granizo","marejad","oleaje",
            "precipit","nev","rachas","temperatura","máxima","mínima")
    return any(w in t for w in dic_es)

def parse_cap_polys(b):
    rows=[]; root=ET.fromstring(b)
    alerts=[el for el in root.iter() if ns(el.tag)=="alert"] or ([root] if ns(root.tag)=="alert" else [])
    for a in alerts:
        identifier=next((e.text or "" for e in a if ns(e.tag)=="identifier"),"")
        sent=next((e.text or "" for e in a if ns(e.tag)=="sent"),"")
        for info in a.iter():
            if ns(info.tag)!="info": continue
            language=next((e.text or "" for e in info if ns(e.tag)=="language"),"")
            event=next((e.text or "" for e in info if ns(e.tag)=="event"),"")
            headline=next((e.text or "" for e in info if ns(e.tag)=="headline"),"")
            severity=next((e.text or "" for e in info if ns(e.tag)=="severity"),"")
            effective=next((e.text or "" for e in info if ns(e.tag)=="effective"),"")
            expires=next((e.text or "" for e in info if ns(e.tag)=="expires"),"")
            for el in info.iter():
                if ns(el.tag)=="polygon" and (el.text or "").strip():
                    pts=[]
                    for pair in el.text.strip().split():
                        lat,lon=pair.split(","); pts.append((float(lon),float(lat)))
                    if pts[0]!=pts[-1]: pts.append(pts[0])
                    try:
                        geom=Polygon(pts)
                        if geom.is_valid:
                            rows.append({
                                "properties":{
                                    "identifier":identifier,
                                    "sent":sent,
                                    "event":event,
                                    "headline":headline or event,
                                    "severity":severity,
                                    "effective":effective,
                                    "expires":expires,
                                    "language":language
                                },
                                "geometry":geom
                            })
                    except:
                        pass
    return rows

def get(url, timeout):
    for _ in range(5):
        try:
            r=s.get(url,timeout=timeout); r.raise_for_status(); return r
        except requests.exceptions.RequestException:
            continue
    r=s.get(url,timeout=timeout); r.raise_for_status(); return r

# Descarga
m=get(url,30).json()
r=get(m["datos"],120)
b=r.content; bio=io.BytesIO(b)

rows=[]
if tarfile.is_tarfile(bio):
    bio.seek(0)
    with tarfile.open(fileobj=bio,mode="r:*") as t:
        for mb in t.getmembers():
            if mb.isfile() and mb.name.lower().endswith(".xml"):
                rows+=parse_cap_polys(t.extractfile(mb).read())
else:
    bio.seek(0)
    if zipfile.is_zipfile(bio):
        with zipfile.ZipFile(bio) as z:
            for n in z.namelist():
                if n.lower().endswith(".xml"):
                    rows+=parse_cap_polys(z.read(n))
    else:
        rows+=parse_cap_polys(b)

# --- Filtro por día actual en Europe/Madrid ---
today_madrid = datetime.now(ZoneInfo("Europe/Madrid")).date()

def is_today_madrid(iso_text: str) -> bool:
    if not iso_text: return False
    try:
        dt = datetime.fromisoformat(iso_text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        dt_mad = dt.astimezone(ZoneInfo("Europe/Madrid"))
        return dt_mad.date() == today_madrid
    except Exception:
        return False

rows_today = [r for r in rows if is_today_madrid(r["properties"].get("sent",""))]

# --- Filtro por idioma: solo español ---
def is_es_lang(p):
    lang=(p.get("language","") or "").lower()
    if lang.startswith("es"):  # es, es-ES, etc.
        return True
    # fallback: detectar por contenido
    return is_spanish(p.get("headline","")) or is_spanish(p.get("event",""))

rows_es = [r for r in rows_today if is_es_lang(r["properties"])]

# Export GeoJSON
fc={
    "type":"FeatureCollection",
    "features":[
        {"type":"Feature","properties":r["properties"],"geometry":mapping(r["geometry"])}
        for r in rows_es
    ]
}
with open(salida,"w",encoding="utf-8") as f:
    json.dump(fc,f,ensure_ascii=False)
print(salida)
