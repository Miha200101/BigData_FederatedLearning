"""
run_pipeline.py
---------------
Orchestrator COMPLET - ruleaza TOTI pasii de la zero:

  FAZA 0: Descarcare date (optional, doar daca lipsesc)
  FAZA 1: Procesare Spark (ETL + ML) - 7 pasi
  FAZA 2: Flower Federated Learning
  FAZA 3: Raport Final
  FAZA 4: Upload Google Cloud Storage

Rulare completa:
  python src/run_pipeline.py

Rulare fara download (datele exista deja):
  python src/run_pipeline.py --skip-download

Rulare fara upload GCS:
  python src/run_pipeline.py --skip-upload

Rulare fara download si fara upload:
  python src/run_pipeline.py --skip-download --skip-upload
"""

import subprocess
import sys
import os
import time
import argparse
from pathlib import Path

# ======================================================================
# CONFIGURATIE
# ======================================================================

SPARK_STEPS = [
    ("src/ingest.py",                    "O1 - Ingestie CSV -> Parquet"),
    ("src/clean.py",                     "O1 - Curatare date"),
    ("src/validate.py",                  "O1 - Validare calitate"),
    ("src/o2_build_analysis_dataset.py", "O2 - Agregare SQL (join + profil student)"),
    ("src/o3_eda.py",                    "O3 - Analiza Exploratorie (EDA)"),
    ("src/o3_ml.py",                     "O3 - ML Centralizat (6 features, stratificat)"),
    ("src/o3_federated_sim.py",          "O3 - FL Simulat (PySpark)"),
]

FLOWER_STEP = ("src/o3_flower_federated.py", "O3/O4 - Flower Federated Learning (FedAvg)")
REPORT_STEP = ("src/generate_final_report.py", "Generare Raport Final")

# Date necesare pentru a considera ca download-ul e deja facut
REQUIRED_RAW_FILES = [
    "data/raw/oulad/studentVle.csv",
    "data/raw/oulad/studentInfo.csv",
    "data/raw/oulad/studentAssessment.csv",
    "data/raw/oulad/assessments.csv",
]

# ======================================================================
# FILTRARE OUTPUT
# ======================================================================

IGNORE_PATTERNS = [
    "ShutdownHookManager", "java.nio.file.NoSuchFileException",
    "winutils", "NativeCodeLoader", "SparkContext",
    "NativeMethodAccessorImpl", "SUCCESS: The process with PID",
    "Setting default log level", "WARN SparkConf", "WARN MemoryManager",
    "RowBasedKeyValueBatch", "To adjust logging level", "For SparkR",
    "DEPRECATED FEATURE", "entirely in future versions",
    "INFO :", "DEBUG :",
]


def clean_output(output_str: str) -> list:
    if not output_str:
        return []
    return [
        line for line in output_str.split("\n")
        if line.strip() and not any(p in line for p in IGNORE_PATTERNS)
    ]


# ======================================================================
# RULARE PAS
# ======================================================================

def run_step(script_path: str, description: str,
             timeout: int = 600, extra_args: list = None) -> bool:
    print(f"\n{'─' * 60}")
    print(f"  Rulare: {description}")
    print(f"  Script: {script_path}")
    print(f"{'─' * 60}")

    start  = time.time()
    cmd    = [sys.executable, script_path] + (extra_args or [])
    result = subprocess.run(
        cmd, capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=timeout,
    )
    elapsed = time.time() - start

    if result.returncode == 0:
        print(f"  [OK] Finalizat in {elapsed:.1f}s")
        for line in clean_output(result.stdout):
            print(f"    {line}")
        for line in clean_output(result.stderr):
            print(f"    {line}")
        return True
    else:
        print(f"  [EROARE] Esuat dupa {elapsed:.1f}s")
        print("\n  --- STDOUT ---")
        print(result.stdout[-3000:] if len(result.stdout) > 3000 else result.stdout)
        print("\n  --- STDERR ---")
        print(result.stderr[-3000:] if len(result.stderr) > 3000 else result.stderr)
        return False


# ======================================================================
# FAZA 0: DOWNLOAD DATE
# ======================================================================

def check_data_exists() -> bool:
    """Verifica daca fisierele OULAD principale exista si sunt reale (>1MB)."""
    for fpath in REQUIRED_RAW_FILES:
        p = Path(fpath)
        if not p.exists():
            return False
        if p.stat().st_size < 1024 * 1024:  # mai mic de 1MB = probabil sintetic/incomplet
            return False
    vle = Path("data/raw/oulad/studentVle.csv")
    if vle.exists() and vle.stat().st_size > 50 * 1024 * 1024:  # >50MB = date reale
        return True
    return False


def run_download():
    """Ruleaza reset_and_download.py cu --yes pentru a nu cere confirmare."""
    print(f"\n{'─' * 60}")
    print("  Rulare: Descarcare date (OULAD + UCI + xAPI)")
    print("  Script: reset_and_download.py")
    print(f"{'─' * 60}")
    print("  (Aceasta operatie poate dura cateva minute...)")

    start  = time.time()
    result = subprocess.run(
        [sys.executable, "reset_and_download.py", "--yes"],
        capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=600,
    )
    elapsed = time.time() - start

    if result.returncode == 0:
        print(f"  [OK] Download finalizat in {elapsed:.1f}s")
        for line in clean_output(result.stdout):
            print(f"    {line}")
        return True
    else:
        print(f"  [ATENTIE] Download cu erori dupa {elapsed:.1f}s")
        print(result.stdout[-2000:])
        print(result.stderr[-1000:])
        # Nu oprim pipeline-ul - poate s-au creat date sintetice
        return True


# ======================================================================
# FAZA 4: UPLOAD GCS
# ======================================================================

def run_gcs_upload():
    """Uploadeaza toate datele pe Google Cloud Storage."""
    print(f"\n{'─' * 60}")
    print("  Rulare: Upload Google Cloud Storage")
    print("  Script: setup_gcs.py --upload")
    print(f"{'─' * 60}")

    start  = time.time()
    result = subprocess.run(
        [sys.executable, "setup_gcs.py", "--upload"],
        capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=1800,
    )
    elapsed = time.time() - start

    if result.returncode == 0:
        print(f"  [OK] Upload finalizat in {elapsed:.1f}s")
        for line in clean_output(result.stdout):
            print(f"    {line}")
        return True
    else:
        print(f"  [ATENTIE] Upload GCS a esuat dupa {elapsed:.1f}s")
        print("  Verifica: gcloud auth application-default login")
        print(result.stderr[-1000:])
        return False


# ======================================================================
# MAIN
# ======================================================================

def main():
    parser = argparse.ArgumentParser(description="Pipeline complet Federated Learning")
    parser.add_argument("--skip-download", action="store_true",
                        help="Sare peste descarcare date (foloseste ce exista)")
    parser.add_argument("--skip-upload",   action="store_true",
                        help="Sare peste upload-ul pe GCS")
    parser.add_argument("--only-flower",   action="store_true",
                        help="Ruleaza doar Flower FL + Raport (date deja procesate)")
    args = parser.parse_args()

    total_start = time.time()
    n_total = 2 + len(SPARK_STEPS) + 2  # download + spark + flower + report + upload
    if args.skip_download: n_total -= 1
    if args.skip_upload:   n_total -= 1

    print("=" * 60)
    print("  PIPELINE COMPLET - ANALIZA FEDERATA DATE EDUCATIONALE")
    print("=" * 60)
    print(f"  Python:    {sys.executable}")
    print(f"  Director:  {os.getcwd()}")
    print(f"  Total pasi: {n_total}")
    if args.skip_download: print("  [--skip-download] Sarim peste descarcare")
    if args.skip_upload:   print("  [--skip-upload]   Sarim peste GCS upload")
    if args.only_flower:   print("  [--only-flower]   Doar Flower FL + Raport")
    print("=" * 60)

    # ------------------------------------------------------------------
    # FAZA 0: DOWNLOAD DATE
    # ------------------------------------------------------------------
    if not args.skip_download and not args.only_flower:
        print("\n[FAZA 0/4] DESCARCARE DATE")

        if check_data_exists():
            print("\n  Date OULAD reale deja existente (>50MB), sarim peste download.")
            print("  (foloseste --skip-download explicit pentru a suprima acest mesaj)")
        else:
            print("\n  Date lipsa sau incomplete, descarcare automata...")
            run_download()
    else:
        print("\n[FAZA 0/4] DESCARCARE DATE -> SARITA")

    # ------------------------------------------------------------------
    # FAZA 1: SPARK
    # ------------------------------------------------------------------
    if not args.only_flower:
        print(f"\n[FAZA 1/4] PROCESARE SPARK (ETL + ML) - {len(SPARK_STEPS)} pasi")

        for i, (script, desc) in enumerate(SPARK_STEPS, 1):
            print(f"\n  Pas {i}/{len(SPARK_STEPS)}")
            TIMEOUTS = {"src/o3_ml.py": 1800, "src/o3_federated_sim.py": 900}
            timeout = TIMEOUTS.get(script, 600)
            ok = run_step(script, desc, timeout=timeout)
            if not ok:
                print(f"\n  Pipeline oprit la pasul Spark {i}: {script}")
                print("  Verifica eroarea si reincearca.")
                sys.exit(1)

        print(f"\n  [FAZA 1 COMPLETA] Toti pasii Spark au rulat cu succes!")
    else:
        print("\n[FAZA 1/4] PROCESARE SPARK -> SARITA (--only-flower)")

    # ------------------------------------------------------------------
    # FAZA 2: FLOWER FL
    # ------------------------------------------------------------------
    print("\n[FAZA 2/4] FLOWER FEDERATED LEARNING")
    print("  (Poate dura 5-15 minute...)")

    script, desc = FLOWER_STEP
    ok = run_step(script, desc, timeout=1800)

    if ok:
        print("\n  [FAZA 2 COMPLETA] Flower FL finalizat!")
    else:
        print("\n  [ATENTIE] Flower FL a esuat, continuam cu raportul partial.")

    # ------------------------------------------------------------------
    # FAZA 3: RAPORT FINAL
    # ------------------------------------------------------------------
    print("\n[FAZA 3/4] GENERARE RAPORT FINAL")

    script, desc = REPORT_STEP
    ok = run_step(script, desc, timeout=300)
    if not ok:
        print("\n  [EROARE] Nu s-a putut genera raportul final.")
        sys.exit(1)

    # ------------------------------------------------------------------
    # FAZA 4: UPLOAD GCS
    # ------------------------------------------------------------------
    if not args.skip_upload:
        print("\n[FAZA 4/4] UPLOAD GOOGLE CLOUD STORAGE")
        gcs_ok = run_gcs_upload()
        if gcs_ok:
            print("\n  [FAZA 4 COMPLETA] Datele sunt pe GCS!")
        else:
            print("\n  [ATENTIE] Upload GCS esuat. Ruleaza manual:")
            print("    python setup_gcs.py --upload")
    else:
        print("\n[FAZA 4/4] UPLOAD GCS -> SARIT (--skip-upload)")

    # ------------------------------------------------------------------
    # SUMAR FINAL
    # ------------------------------------------------------------------
    total_elapsed = time.time() - total_start
    minutes = int(total_elapsed // 60)
    seconds = int(total_elapsed % 60)

    print("\n" + "=" * 60)
    print("  PIPELINE FINALIZAT CU SUCCES!")
    print(f"  Timp total: {minutes}m {seconds}s")
    print("=" * 60)
    print("\n  Fisiere generate:")
    print("    reports/Raport_Final.txt         <- raport complet disertatie")
    print("    reports/O3_ml_report.txt         <- ML centralizat + matrice confuzie")
    print("    reports/O3_flower_report.txt     <- Flower FL comparativ")
    print("    reports/flower_results.json      <- date brute FL")
    print("    reports/O3_eda_report.txt        <- analiza exploratorie")
    print("    data/processed/analysis_dataset/ <- date agregate Parquet")
    if not args.skip_upload:
        print(f"\n  GCS bucket:")
        print(f"    https://console.cloud.google.com/storage/browser/{get_bucket_name()}")
    print("=" * 60)


def get_bucket_name():
    """Citeste numele bucket-ului din setup_gcs.py."""
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location("setup_gcs", "setup_gcs.py")
        mod  = importlib.util.load_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.GCS_BUCKET_NAME
    except Exception:
        return "edu-federated-mihaela"


if __name__ == "__main__":
    main()
