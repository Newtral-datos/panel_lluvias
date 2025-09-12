from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import re
from datetime import timedelta

URL = "https://www.aemet.es/es/eltiempo/prediccion/avisos?r=1"

opts = webdriver.ChromeOptions()
opts.add_argument("--headless=new")
opts.add_argument("--lang=es-ES")
opts.add_argument("--user-agent=Mozilla/5.0")

driver = webdriver.Chrome(options=opts)

rows = []
try:
    driver.get(URL)
    WebDriverWait(driver, 30).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#resumen-avisos .rs-dia-zona[data-zona-id]"))
    )
    for dia in driver.find_elements(By.CSS_SELECTOR, "#resumen-avisos .rs-dia-zona[data-zona-id]"):
        nombre_el = dia.find_elements(By.CSS_SELECTOR, ".rs-zona .rs-nombre-zona")
        if not nombre_el:
            continue
        nombre = nombre_el[0].text.strip()
        for h in dia.find_elements(By.CSS_SELECTOR, ".rs-horas .rs-hora[data-rs-fecha]"):
            fecha = h.get_attribute("data-rs-fecha")
            clases = (h.get_attribute("class") or "").split()
            nivel = next((c for c in clases if c.startswith("rs-nivel-")), "sin-nivel")
            rows.append({
                "zona": nombre,
                "fecha_hora": fecha,
                "nivel": nivel
            })
finally:
    driver.quit()

df = pd.DataFrame(rows).sort_values(["zona", "fecha_hora"]).reset_index(drop=True)

df_ok = df[df["nivel"] != "sin-nivel"].copy()
df_ok["dt"] = pd.to_datetime(df_ok["fecha_hora"], errors="coerce")
df_ok["fecha"] = df_ok["dt"].dt.date
df_ok["hora"] = df_ok["dt"].dt.strftime("%H:%M")

def pretty_nivel(nivel_raw: str) -> str:
    m = re.search(r"rs-nivel-(\d+)", nivel_raw or "")
    return f"Nivel {m.group(1)}" if m else nivel_raw

def rangos_consecutivos(horas_sorted):
    if not horas_sorted:
        return []
    inicio = fin = horas_sorted[0]
    out = []
    for t in horas_sorted[1:]:
        if t - fin == timedelta(hours=1):
            fin = t
        else:
            out.append((inicio, fin))
            inicio = fin = t
    out.append((inicio, fin))
    return out

def resumen_por_zona(g: pd.DataFrame) -> str:
    partes = []
    g = g.sort_values("dt")
    for nivel_raw, g_nivel in g.groupby("nivel"):
        nivel_txt = pretty_nivel(nivel_raw)
        for fecha, g_dia in g_nivel.groupby("fecha"):
            horas = sorted(g_dia["dt"].tolist())
            for ini, fin in rangos_consecutivos(horas):
                partes.append(f"{nivel_txt} de {ini.strftime('%H:%M')} a {fin.strftime('%H:%M')}")
    return " | ".join(partes)

resumen = (
    df_ok.groupby("zona")
         .apply(resumen_por_zona)
         .reset_index(name="nivel_y_tramos")
)

print(resumen)

resumen.to_csv("/Users/miguel.ros/Desktop/PANEL_LLUVIAS/avisos/avisos_aemet_niveles_por_ccaa.csv",
               index=False, encoding="utf-8")
