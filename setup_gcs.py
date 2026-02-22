"""
setup_gcs.py
------------
Incarca datele pe Google Cloud Storage.

Proiect GCP: disertatie-488118
Bucket: edu-federated-mihaela

RULARE:
  python setup_gcs.py --upload         <- incarca TOTUL (raw + processed + reports)
  python setup_gcs.py --upload-raw     <- doar data/raw/
  python setup_gcs.py --upload-results <- doar processed/ + reports/
  python setup_gcs.py --verify         <- verifica ce e in cloud
  python setup_gcs.py --structure      <- afiseaza structura bucket
"""

import argparse
import os
import sys
from pathlib import Path

GCS_PROJECT_ID  = "disertatie-488118"
GCS_BUCKET_NAME = "edu-federated-mihaela"
GCS_REGION      = "europe-west1"

# Ce uploadam si unde
UPLOAD_FOLDERS = [
    # (folder_local,          prefix_gcs,    descriere)
    ("data/raw",              "data/raw",    "Date brute CSV"),
    ("data/interim",          "data/interim","Date interim Parquet"),
    ("data/processed",        "data/processed", "Date procesate Parquet"),
    ("reports",               "reports",     "Rapoarte finale"),
]

# Extensii de ignorat (fisiere temporare Spark)
IGNORE_EXTENSIONS = {".crc", ".tmp"}
IGNORE_NAMES      = {"_SUCCESS", "_committed", "_started"}


def get_client():
    try:
        from google.cloud import storage
        return storage.Client(project=GCS_PROJECT_ID)
    except ImportError:
        print("EROARE: pip install google-cloud-storage")
        sys.exit(1)
    except Exception as e:
        print(f"EROARE autentificare GCP: {e}")
        print("Ruleaza: gcloud auth application-default login")
        sys.exit(1)


def should_skip(file_path: Path) -> bool:
    """Returneaza True daca fisierul trebuie ignorat."""
    if file_path.suffix in IGNORE_EXTENSIONS:
        return True
    if file_path.name in IGNORE_NAMES:
        return True
    if file_path.name.startswith("._"):
        return True
    return False


def create_bucket_if_needed(client):
    try:
        client.get_bucket(GCS_BUCKET_NAME)
        print(f"  Bucket existent: gs://{GCS_BUCKET_NAME}")
    except Exception:
        print(f"  Creare bucket gs://{GCS_BUCKET_NAME} in {GCS_REGION}...")
        client.create_bucket(GCS_BUCKET_NAME, location=GCS_REGION)
        print("  Bucket creat!")


def upload_folder(client, local_folder: str, gcs_prefix: str, label: str):
    """Uploadeaza un folder local catre GCS cu prefixul dat."""
    bucket    = client.bucket(GCS_BUCKET_NAME)
    local_path = Path(local_folder)

    if not local_path.exists():
        print(f"  [SKIP] {local_folder} nu exista local.")
        return 0, 0

    files = [f for f in local_path.rglob("*")
             if f.is_file() and not should_skip(f)]

    if not files:
        print(f"  [SKIP] {local_folder} e gol.")
        return 0, 0

    total_bytes = sum(f.stat().st_size for f in files)
    print(f"\n  [{label}] {len(files)} fisiere, {total_bytes/1024/1024:.1f} MB")

    uploaded = 0
    for lf in sorted(files):
        # Construim calea GCS: prefix_gcs + calea relativa din folderul local
        rel_path = lf.relative_to(local_path)
        gcs_path = f"{gcs_prefix}/{rel_path}".replace("\\", "/")

        blob = bucket.blob(gcs_path)
        mb   = lf.stat().st_size / (1024 * 1024)

        print(f"    {str(rel_path):<55} {mb:>7.2f} MB", end="", flush=True)
        try:
            blob.upload_from_filename(str(lf))
            print("  OK")
            uploaded += 1
        except Exception as e:
            print(f"  EROARE: {e}")

    return uploaded, len(files)


def upload_all(raw_only=False, results_only=False):
    """Uploadeaza folderele selectate."""
    client = get_client()
    create_bucket_if_needed(client)

    print(f"\nUpload catre: gs://{GCS_BUCKET_NAME}/")
    print("=" * 65)

    total_uploaded = 0
    total_files    = 0

    for local_folder, gcs_prefix, label in UPLOAD_FOLDERS:
        # Filtrare in functie de optiune
        if raw_only and "raw" not in local_folder:
            continue
        if results_only and local_folder == "data/raw":
            continue

        uploaded, total = upload_folder(client, local_folder, gcs_prefix, label)
        total_uploaded += uploaded
        total_files    += total

    print(f"\n{'='*65}")
    print(f"UPLOAD COMPLET: {total_uploaded}/{total_files} fisiere")
    print(f"URL: https://console.cloud.google.com/storage/browser/{GCS_BUCKET_NAME}")


def verify_gcs():
    """Listeaza toate fisierele din bucket grupate pe folder."""
    client = get_client()
    try:
        blobs = list(client.list_blobs(GCS_BUCKET_NAME))
    except Exception as e:
        print(f"Eroare: {e}")
        return

    if not blobs:
        print(f"Bucket gs://{GCS_BUCKET_NAME} este GOL.")
        return

    # Grupeaza pe prefix (primul nivel de folder)
    folders = {}
    for b in blobs:
        parts  = b.name.split("/")
        folder = parts[0] if len(parts) > 1 else "(root)"
        folders.setdefault(folder, []).append(b)

    total_size = sum(b.size for b in blobs)

    print(f"\nStructura bucket gs://{GCS_BUCKET_NAME}:")
    print("=" * 60)
    for folder in sorted(folders):
        folder_blobs = folders[folder]
        folder_size  = sum(b.size for b in folder_blobs)
        print(f"\n  {folder}/  ({len(folder_blobs)} fisiere, {folder_size/1024/1024:.1f} MB)")
        # Afiseaza primele 10 fisiere per folder
        for b in sorted(folder_blobs, key=lambda x: x.name)[:10]:
            print(f"    {b.name:<55} {b.size/1024/1024:>7.2f} MB")
        if len(folder_blobs) > 10:
            print(f"    ... si inca {len(folder_blobs)-10} fisiere")

    print(f"\n{'='*60}")
    print(f"TOTAL: {len(blobs)} fisiere, {total_size/1024/1024:.1f} MB")


def show_local_structure():
    """Afiseaza ce exista local si ce ar urma sa fie uploadat."""
    print("\nStructura locala (ce va fi uploadat):")
    print("=" * 60)
    for local_folder, gcs_prefix, label in UPLOAD_FOLDERS:
        path = Path(local_folder)
        if not path.exists():
            print(f"\n  {local_folder}/ -> LIPSA LOCAL")
            continue
        files = [f for f in path.rglob("*")
                 if f.is_file() and not should_skip(f)]
        size  = sum(f.stat().st_size for f in files) / 1024 / 1024
        print(f"\n  {local_folder}/ -> gs://{GCS_BUCKET_NAME}/{gcs_prefix}/")
        print(f"    [{label}] {len(files)} fisiere, {size:.1f} MB")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Google Cloud Storage manager pentru proiect FL")
    p.add_argument("--upload",         action="store_true", help="Uploadeaza TOTUL (raw+processed+reports)")
    p.add_argument("--upload-raw",     action="store_true", help="Uploadeaza doar data/raw/")
    p.add_argument("--upload-results", action="store_true", help="Uploadeaza processed/ + reports/")
    p.add_argument("--verify",         action="store_true", help="Listeaza fisierele din bucket")
    p.add_argument("--structure",      action="store_true", help="Afiseaza structura locala")
    args = p.parse_args()

    if args.upload:
        upload_all()
    elif args.upload_raw:
        upload_all(raw_only=True)
    elif args.upload_results:
        upload_all(results_only=True)
    elif args.verify:
        verify_gcs()
    elif args.structure:
        show_local_structure()
    else:
        print("Folosire:")
        print("  python setup_gcs.py --upload          # incarca TOTUL")
        print("  python setup_gcs.py --upload-raw      # doar data/raw/")
        print("  python setup_gcs.py --upload-results  # processed/ + reports/")
        print("  python setup_gcs.py --verify          # verifica bucket")
        print("  python setup_gcs.py --structure       # afiseaza structura locala")
