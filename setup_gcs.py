"""
setup_gcs.py
------------
Script pentru configurarea Google Cloud Storage ca Data Lake.

Acest script:
1. Creeaza bucket-ul GCS pentru proiectul de disertatie
2. Incarca datele locale (data/raw/) in GCS
3. Verifica conexiunea

RULARE:
  pip install google-cloud-storage
  
  # Autentificare (o singura data):
  gcloud auth application-default login
  
  # Apoi:
  python setup_gcs.py --upload     # Incarca datele pe GCS
  python setup_gcs.py --verify     # Verifica ce e pe GCS
  python setup_gcs.py --download   # Descarca inapoi local
"""

import argparse
import os
import sys
from pathlib import Path

# ======================================================================
# CONFIGURARE - MODIFICA ACESTE VALORI
# ======================================================================
GCS_PROJECT_ID  = "disertatie-federated-learning"   # ID-ul proiectului nou din GCP
GCS_BUCKET_NAME = "edu-federated-data"              # Numele bucket-ului (unic global!)
GCS_REGION      = "europe-west1"                    # Closest to Romania
LOCAL_DATA_DIR  = "data/"                           # Folderul local cu date


def get_client():
    """Initializeaza clientul Google Cloud Storage."""
    try:
        from google.cloud import storage
        client = storage.Client(project=GCS_PROJECT_ID)
        return client
    except ImportError:
        print("EROARE: Instaleaza: pip install google-cloud-storage")
        sys.exit(1)
    except Exception as e:
        print(f"EROARE autentificare GCS: {e}")
        print("\nRuleaza mai intai:")
        print("  gcloud auth application-default login")
        sys.exit(1)


def create_bucket():
    """Creeaza bucket-ul GCS daca nu exista."""
    client = get_client()
    from google.cloud import storage
    
    bucket_name = GCS_BUCKET_NAME
    
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"Bucket existent: gs://{bucket_name}")
    except Exception:
        print(f"Creare bucket: gs://{bucket_name} in {GCS_REGION}...")
        bucket = client.create_bucket(
            bucket_name,
            location=GCS_REGION,
        )
        print(f"Bucket creat: gs://{bucket_name}")
    
    return bucket


def upload_data():
    """
    Incarca datele din data/raw/ pe Google Cloud Storage.
    Structura GCS va fi identica cu cea locala:
      gs://edu-federated-data/raw/studentVle.csv
      gs://edu-federated-data/raw/studentInfo.csv
      ...etc
    """
    client = get_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    
    local_path = Path(LOCAL_DATA_DIR) / "raw"
    
    if not local_path.exists():
        print(f"EROARE: Folderul {local_path} nu exista!")
        return
    
    files = list(local_path.rglob("*"))
    csv_files = [f for f in files if f.is_file()]
    
    print(f"\nIncarcare {len(csv_files)} fisiere pe GCS...")
    print(f"Destinatie: gs://{GCS_BUCKET_NAME}/raw/\n")
    
    for local_file in csv_files:
        # Calea relativa in GCS
        relative = local_file.relative_to(LOCAL_DATA_DIR)
        gcs_path = str(relative).replace("\\", "/")
        
        blob = bucket.blob(gcs_path)
        
        file_size_mb = local_file.stat().st_size / (1024 * 1024)
        print(f"  Incarcare {local_file.name} ({file_size_mb:.1f} MB)...")
        
        blob.upload_from_filename(str(local_file))
        print(f"  -> gs://{GCS_BUCKET_NAME}/{gcs_path}")
    
    print(f"\nFinalizat! {len(csv_files)} fisiere incarcate pe GCS.")
    print(f"\nURL bucket: https://console.cloud.google.com/storage/browser/{GCS_BUCKET_NAME}")


def verify_gcs():
    """Listeaza fisierele de pe GCS si afiseaza dimensiunile."""
    client = get_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    
    print(f"\nFisiere pe gs://{GCS_BUCKET_NAME}:")
    print("-" * 60)
    
    blobs = list(client.list_blobs(GCS_BUCKET_NAME))
    
    if not blobs:
        print("Bucket-ul este gol!")
        return
    
    total_size = 0
    for blob in blobs:
        size_mb = blob.size / (1024 * 1024)
        total_size += blob.size
        print(f"  {blob.name:<50} {size_mb:>8.2f} MB")
    
    print("-" * 60)
    print(f"  Total: {total_size / (1024*1024):.2f} MB in {len(blobs)} fisiere")


def download_data():
    """Descarca datele de pe GCS inapoi local (pentru rulare locala)."""
    client = get_client()
    
    blobs = list(client.list_blobs(GCS_BUCKET_NAME, prefix="raw/"))
    
    print(f"\nDescarcare {len(blobs)} fisiere de pe GCS...")
    
    for blob in blobs:
        local_path = Path(LOCAL_DATA_DIR) / blob.name
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        print(f"  {blob.name} -> {local_path}")
        blob.download_to_filename(str(local_path))
    
    print("Finalizat!")


def generate_gcs_config():
    """
    Genereaza config.yaml actualizat pentru a citi direct de pe GCS cu Spark.
    Spark suporta nativ gs:// paths cu conectorul GCS.
    """
    config_content = f"""# config_gcs.yaml
# Versiunea pentru Google Cloud Storage
# Spark citeste direct de pe GCS cu gs:// paths

paths:
  raw: "gs://{GCS_BUCKET_NAME}/raw/"
  interim: "gs://{GCS_BUCKET_NAME}/interim/"
  processed: "gs://{GCS_BUCKET_NAME}/processed/"

spark:
  shuffle_partitions: 8
  coalesce_out: 4
  # Conector GCS pentru Spark (necesar pentru gs:// paths)
  gcs_connector: true
  gcs_project: "{GCS_PROJECT_ID}"

analysis:
  output_dir: "analysis_dataset/"

quality_rules:
  sum_click_min: 1
  date_min: -100
  date_max: 300
  score_min: 0
  score_max: 100

datasets:
  student_vle:
    input_file: "studentVle.csv"
    interim_dir: "student_vle/"
    processed_dir: "student_vle/"
    required_cols: ["code_module", "code_presentation", "id_student", "id_site", "date", "sum_click"]
    dedup_keys: ["code_module", "code_presentation", "id_student", "id_site", "date"]
  student_info:
    input_file: "studentInfo.csv"
    interim_dir: "student_info/"
    processed_dir: "student_info/"
    required_cols: ["code_module", "code_presentation", "id_student", "final_result"]
    dedup_keys: ["code_module", "code_presentation", "id_student"]
  student_assessment:
    input_file: "studentAssessment.csv"
    interim_dir: "student_assessment/"
    processed_dir: "student_assessment/"
    required_cols: ["id_assessment", "id_student", "score"]
    dedup_keys: ["id_assessment", "id_student"]
  assessments:
    input_file: "assessments.csv"
    interim_dir: "assessments/"
    processed_dir: "assessments/"
    required_cols: ["id_assessment", "code_module", "code_presentation"]
    dedup_keys: ["id_assessment"]
"""
    with open("configs/config_gcs.yaml", "w") as f:
        f.write(config_content)
    
    print("Generat: configs/config_gcs.yaml")
    print("Foloseste-l cu: python src/run_pipeline.py --config configs/config_gcs.yaml")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup Google Cloud Storage pentru proiectul FL")
    parser.add_argument("--create-bucket", action="store_true", help="Creeaza bucket-ul GCS")
    parser.add_argument("--upload",        action="store_true", help="Incarca datele pe GCS")
    parser.add_argument("--verify",        action="store_true", help="Listeaza fisierele de pe GCS")
    parser.add_argument("--download",      action="store_true", help="Descarca datele de pe GCS")
    parser.add_argument("--gen-config",    action="store_true", help="Genereaza config.yaml pentru GCS")
    
    args = parser.parse_args()
    
    if args.create_bucket:
        create_bucket()
    elif args.upload:
        create_bucket()
        upload_data()
    elif args.verify:
        verify_gcs()
    elif args.download:
        download_data()
    elif args.gen_config:
        generate_gcs_config()
    else:
        print(__doc__)
        print("\nExemplu rapid:")
        print("  python setup_gcs.py --upload    # Incarca tot data/raw/ pe GCS")
        print("  python setup_gcs.py --verify    # Verifica ce e pe cloud")
        print("  python setup_gcs.py --gen-config # Genereaza config pentru GCS")
