from pathlib import Path
import subprocess
import sys
from datetime import datetime

BASE = Path(__file__).resolve().parent
SCRIPTS = BASE / "scripts"
PY = sys.executable

PIPELINE = [
    (SCRIPTS / "lluvias.py", [], None),
    (SCRIPTS / "temperaturas.py", [], None),
    (SCRIPTS / "avisos_aemet.py", [], None),
    (SCRIPTS / "estadisticas.py", [], None),
    (SCRIPTS / "mar_temperatura_actual.py", [], None),
    (SCRIPTS / "mar_comparacion.py", [], None)
]

def run_step(script: Path, args: list[str], timeout: int | None):
    if not script.exists():
        raise FileNotFoundError(f"No existe: {script}")
    cmd = [PY, str(script), *args]
    hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n🕒 {hora} ▶ Ejecutando: {' '.join(cmd)}")
    subprocess.run(cmd, check=True, timeout=timeout, cwd=script.parent)

def main():
    for script, args, to in PIPELINE:
        run_step(script, args, to)
    print("\n✅ TODO HA SALIDO A PEDIR DE MILHOUSE.")

if __name__ == "__main__":
    try:
        main()
    except subprocess.TimeoutExpired as e:
        print(f"\n⏰ Timeout en: {e.cmd}")
        sys.exit(124)
    except subprocess.CalledProcessError as e:
        print(f"\n❌ El código ha FALLECIDO. Mira esto, anda: {e.returncode}: {e.cmd}")
        sys.exit(e.returncode)
    except FileNotFoundError as e:
        print(f"\n🗂️ {e}")
        sys.exit(2)
