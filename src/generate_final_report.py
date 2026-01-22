import os
import glob
from datetime import datetime
from utils import load_config, make_spark, ensure_dir

def read_file_content(path: str) -> str:
    """Citește conținutul unui fișier text."""
    if not os.path.exists(path):
        return f"[LIPSEȘTE] Fișierul {path} nu a fost găsit."
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()

def cleanup_intermediate_reports():
    """Șterge rapoartele parțiale pentru a lăsa folderul curat."""
    patterns = [
        "reports/validate_*.txt",
        "reports/O3_*.txt"
    ]
    for pattern in patterns:
        for f in glob.glob(pattern):
            try:
                os.remove(f)
            except:
                pass

def main():
    """
    Generează Raportul Final și curăță fișierele temporare.
    """
    cfg = load_config("configs/config.yaml")
    ensure_dir("reports")

    # Inițializăm Spark doar pentru numărare
    spark = make_spark("Generare raport final", cfg)

    path_vle = cfg["paths"]["processed"] + "student_vle/"
    path_analysis = cfg["paths"]["processed"] + cfg["analysis"]["output_dir"]

    try:
        count_vle = spark.read.parquet(path_vle).count()
        count_analysis = spark.read.parquet(path_analysis).count()
    except Exception as e:
        count_vle = "N/A"
        count_analysis = "N/A"

    spark.stop()

    #Construim Raportul
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report_content = []

    report_content.append("=" * 70)
    report_content.append(f"RAPORT FINAL PROIECT")
    report_content.append(f"Tema: Analiza Federată a Datelor Educaționale")
    report_content.append(f"Data generării: {timestamp}")
    report_content.append("=" * 70 + "\n")

    report_content.append("1. DATE PROCESATE")
    report_content.append("-" * 50)
    report_content.append(f"Rânduri în dataset-ul principal (student_vle): {count_vle}")
    report_content.append(f"Rânduri în dataset-ul final de analiză dupa join: {count_analysis}")
    report_content.append("-" * 50 + "\n")

    # VALIDARE
    report_content.append("2. VALIDARE CALITATE DATE")
    report_content.append("-" * 50)
    for ds_name in cfg["datasets"].keys():
        report_content.append(f"\n[Dataset: {ds_name}]")
        report_content.append(read_file_content(f"reports/validate_{ds_name}.txt"))
    report_content.append("\n" + "-" * 50 + "\n")

    # EDA
    report_content.append("3. ANALIZA DATELOR")
    report_content.append("-" * 50)
    report_content.append(read_file_content("reports/O3_eda_report.txt"))
    report_content.append("\n" + "-" * 50 + "\n")

    # ML
    report_content.append("4. REZULTATE MACHINE LEARNING CENTRALIZAT")
    report_content.append("-" * 50)
    report_content.append(read_file_content("reports/O3_ml_report.txt"))
    report_content.append("\n" + "-" * 50 + "\n")

    # FL
    report_content.append("5. SIMULARE FEDERATED LEARNING")
    report_content.append("-" * 50)
    report_content.append(read_file_content("reports/O3_federated_report.txt"))
    report_content.append("=" * 70 + "\n")

    # Scriere Finală
    out_file = "reports/Raport_Final.txt"
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(report_content))

    cleanup_intermediate_reports()

    print(f"\n RAPORT FINAL GENERAT: {out_file}")

if __name__ == "__main__":
    main()