"""
run_pipeline.py
---------------
Orchestrator pipeline Federated Learning.

Optiuni:
  --skip-download   Sare peste descarcare date
  --skip-upload     Sare peste upload GCS
  --only-flower     Ruleaza doar FL + grafice + raport (Spark deja rulat)

Exemple:
  python src/run_pipeline.py --only-flower --skip-upload
  python src/run_pipeline.py --skip-download --skip-upload
  python src/run_pipeline.py
"""

import subprocess
import sys
import os
import time
import argparse
from pathlib import Path

# ============================================================
# Configuratie
# ============================================================

GCS_BUCKET_NAME = "edu-federated-mihaela"

SPARK_STEPS = [
    ("src/ingest.py",                    "Ingestie CSV -> Parquet"),
    ("src/clean.py",                     "Curatare date"),
    ("src/validate.py",                  "Validare calitate"),
    ("src/o2_build_analysis_dataset.py", "Agregare SQL (9 features)"),
    ("src/o3_eda.py",                    "Analiza exploratorie (EDA)"),
    ("src/o3_ml.py",                     "ML centralizat (LR, RF, GBT)"),
]

FLOWER_STEP   = ("src/o3_flower_federated.py", "Flower FL (4 strategii, 10 runde)")
REPORTS_STEP  = ("src/generate_reports.py",    "Rapoarte finale + Grafice + CSV")

REQUIRED_RAW = [
    "data/raw/oulad/studentVle.csv",
    "data/raw/oulad/studentInfo.csv",
    "data/raw/oulad/studentAssessment.csv",
    "data/raw/oulad/assessments.csv",
]

TIMEOUTS = {
    "src/ingest.py":               900,
    "src/clean.py":                900,
    "src/o2_build_analysis_dataset.py": 900,
    "src/o3_ml.py":               1800,

    "src/o3_flower_federated.py": 3600,
    "src/generate_reports.py":      300,
}

IGNORE_PATTERNS = [
    "ShutdownHookManager", "NativeCodeLoader", "winutils",
    "NativeMethodAccessorImpl", "Setting default log level",
    "WARN SparkConf", "WARN MemoryManager", "RowBasedKeyValueBatch",
    "To adjust logging level", "DEPRECATED FEATURE",
    "INFO :", "DEBUG :", "WARNING :",
]


# ============================================================
# Utilitare
# ============================================================

def clean_output(text: str) -> list:
    if not text:
        return []
    return [
        line for line in text.split("\n")
        if line.strip() and not any(p in line for p in IGNORE_PATTERNS)
    ]


def run_step(script: str, desc: str, extra_args: list = None) -> bool:
    timeout = TIMEOUTS.get(script, 600)
    start   = time.time()

    print(f"\n  [{desc}]")

    result = subprocess.run(
        [sys.executable, script] + (extra_args or []),
        capture_output=True, text=True,
        encoding="utf-8", errors="replace",
        timeout=timeout,
    )
    elapsed = time.time() - start

    if result.returncode == 0:
        print(f"  OK  ({elapsed:.0f}s)")
        for line in clean_output(result.stdout):
            print(f"      {line}")
        return True
    else:
        print(f"  EROARE  ({elapsed:.0f}s)")
        tail = lambda s: s[-2000:] if len(s) > 2000 else s
        if result.stdout.strip():
            print(tail(result.stdout))
        if result.stderr.strip():
            print(tail(result.stderr))
        return False


def check_data_exists() -> bool:
    for f in REQUIRED_RAW:
        p = Path(f)
        if not p.exists() or p.stat().st_size < 1_000_000:
            return False
    vle = Path("data/raw/oulad/studentVle.csv")
    return vle.exists() and vle.stat().st_size > 50_000_000


def run_download() -> bool:
    print("\n  [Descarcare date OULAD + UCI + xAPI]")
    result = subprocess.run(
        [sys.executable, "reset_and_download.py", "--yes"],
        capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=600,
    )
    if result.returncode == 0:
        print("  OK")
        for line in clean_output(result.stdout):
            print(f"      {line}")
    else:
        print("  Atentie: download cu erori, continuam cu datele existente.")
    return True


def run_upload() -> bool:
    print("\n  [Upload Google Cloud Storage]")
    result = subprocess.run(
        [sys.executable, "setup_gcs.py", "--upload"],
        capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=1800,
    )
    elapsed_ok = result.returncode == 0
    if elapsed_ok:
        print("  OK")
        for line in clean_output(result.stdout):
            print(f"      {line}")
    else:
        print("  Atentie: upload GCS esuat.")
        print("  Ruleaza manual: python setup_gcs.py --upload")
        print("  Autentificare: gcloud auth application-default login")
    return elapsed_ok


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline FL — Ciobanu Mihaela Lavinia, UPT 2026"
    )
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload",   action="store_true")
    parser.add_argument("--only-flower",   action="store_true")
    args = parser.parse_args()

    t0 = time.time()

    print("=" * 55)
    print("  Analiza Federata Date Educationale")
    print("  Ciobanu Mihaela Lavinia | UPT 2026")
    print("=" * 55)
    print(f"  Python:   {sys.executable}")
    print(f"  Director: {os.getcwd()}")
    print("=" * 55)

    # Faza 0 — Download
    print("\n--- Faza 0: Date ---")
    if args.skip_download or args.only_flower:
        print("  Sarita.")
    elif check_data_exists():
        print("  Date existente, descarcare sarita.")
    else:
        run_download()

    # Faza 1 — Spark
    print("\n--- Faza 1: Procesare Spark ---")
    if args.only_flower:
        print("  Sarita.")
    else:
        for i, (script, desc) in enumerate(SPARK_STEPS, 1):
            print(f"\n  Pas {i}/{len(SPARK_STEPS)}")
            if not run_step(script, desc):
                print(f"\n  Pipeline oprit la: {script}")
                sys.exit(1)
        print("\n  Faza 1 completa.")

    # Faza 2 — Flower FL
    print("\n--- Faza 2: Federated Learning ---")
    fl_ok = run_step(*FLOWER_STEP)
    if not fl_ok:
        print("  Atentie: FL a esuat, continuam cu raportul partial.")

    # Faza 3 — Rapoarte finale (raport + grafice + CSV intr-un singur pas)
    print("\n--- Faza 3: Rapoarte finale ---")
    if not run_step(*REPORTS_STEP):
        print("  Rapoartele finale nu au putut fi generate.")
        sys.exit(1)

    # Faza 4 — GCS
    print("\n--- Faza 4: GCS Upload ---")
    if args.skip_upload:
        print("  Sarita.")
    else:
        if run_upload():
            print(f"  https://console.cloud.google.com/storage/browser/{GCS_BUCKET_NAME}")

    # Sumar
    elapsed = time.time() - t0
    print(f"\n{'=' * 55}")
    print(f"  Finalizat in {int(elapsed // 60)}m {int(elapsed % 60)}s")
    print(f"{'=' * 55}")
    print("""
  reports/Raport_Final.txt
  reports/flower_results.json
  reports/rezultate.csv
  reports/figures/fig1-fig4.png
""")


if __name__ == "__main__":
    main()