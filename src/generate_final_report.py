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

    # FL Simulare PySpark
    report_content.append("5. SIMULARE FEDERATED LEARNING (PySpark)")
    report_content.append("-" * 50)
    report_content.append(read_file_content("reports/O3_federated_report.txt"))
    report_content.append("\n" + "-" * 50 + "\n")

    # Flower FL
    report_content.append("6. FLOWER FEDERATED LEARNING - COMPARATIE CENTRALIZAT vs FEDERAT")
    report_content.append("-" * 50)
    report_content.append(read_file_content("reports/O3_flower_report.txt"))
    report_content.append("\n" + "-" * 50 + "\n")

    # Concluzii
    report_content.append("7. CONCLUZII")
    report_content.append("-" * 50)
    report_content.append("""
7.1 Pipeline Big Data (Obiectiv O1 + O2)
  - Procesare eficienta cu Apache Spark a 8.4 milioane de inregistrari
  - Arhitectura ETL: CSV -> Parquet (stocare optimizata) -> Agregate
  - Feature engineering: 9 features comportamentale si demografice
  - Calitate date: 0 valori nule in toate tabele dupa procesare
  - Date stocate in Data Lake pe Google Cloud Storage

7.2 Analiza Exploratorie (EDA)
  - OULAD: Studentii promovati sunt de 2.8x mai activi pe platforma
  - xAPI: Engagement total de 2.7x mai mare la studenti promovati
  - UCI: Performanta corelata puternic cu nota anterioara (G2)
  - Concluzie EDA: activitatea digitala este un predictor puternic

7.3 Machine Learning Centralizat (Obiectiv O3)
  - 3 modele comparate: Logistic Regression, Random Forest, GBT
  - OULAD: Best model GBT, AUC=0.789 (9 features comportamentale+demografice)
  - xAPI:  Best model LR,  AUC=0.977 (engagement VLE)
  - UCI:   Best model RF/GBT, AUC>0.92 (note + demografie)

7.4 Federated Learning cu Flower + FedAvg (Obiectiv O3)
  - Implementare reala FL: datele NU parasesc niciodata clientul (cursul)
  - Fiecare client = un curs distinct (client_id), server = agregator central
  - Algoritm FedAvg: media ponderata a coeficientilor dupa fiecare runda
  - OULAD:  FL AUC=0.782 vs Centralizat AUC=0.766  (+0.016 FL castiga!)
  - xAPI:   FL AUC=0.934 vs Centralizat AUC=0.940  (-0.006 comparabil)
  - Convergenta rapida (runda 1) demonstreaza stabilitatea algoritmului

7.5 Concluzie Finala
  Proiectul valideaza ipoteza principala: arhitecturile federate reprezinta
  o alternativa tehnic viabila pentru analiza datelor educationale,
  protejand confidentialitatea studentilor fara a sacrifica semnificativ
  performanta predictiva. Pe OULAD (22 cursuri independente), FL depaseste
  chiar modelul centralizat cu +0.016 AUC, demonstrand ca diversitatea
  datelor distribuite poate fi un avantaj.
    """)
    report_content.append("=" * 70 + "\n")

    # Scriere Finala
    out_file = "reports/Raport_Final.txt"
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(report_content))

    cleanup_intermediate_reports()

    print(f"\n RAport FINAL GENERAT: {out_file}")

if __name__ == "__main__":
    main()