# Analiza Federata a Datelor Educationale cu Apache Spark si Flower

Pipeline complet de procesare Big Data pentru **predicția promovabilității studenților**, comparând machine learning centralizat cu **Federated Learning** (FL) pentru protecția datelor.

**Întrebarea de cercetare:** Poate Federated Learning atinge o performanță comparabilă cu modelele centralizate, menținând confidențialitatea completă a datelor studenților?

---

## Rezultate Principale

| Dataset | Studenti | AUC Centralizat | AUC Federat | Diferenta |
|---------|----------|-----------------|-------------|-----------|
| OULAD   | 28.174   | 0.8055 (GBT)    | 0.7934 (FL) | -1.50%    |
| xAPI    | 480      | 0.9766 (LR)     | 0.9537 (FL) | -2.29%    |
| UCI     | 649      | 0.9647 (LR)     | N/A*        | -         |

*UCI nu a generat suficienți clienți cu min. 30 samples pentru simularea FL.

---

## Arhitectura

```
data/raw/          ← CSV brute (OULAD 443MB + UCI + xAPI)
    ↓  ingest.py (Spark)
data/interim/      ← Parquet (30.5MB, compresie 93%)
    ↓  clean.py (Spark)
data/processed/    ← Date curatate + client_id FL
    ↓  o2_build_analysis_dataset.py (Spark SQL)
data/processed/analysis_dataset/  ← 28.174 profiluri, 9 features
    ↓  o3_ml.py + o3_flower_federated.py
reports/           ← Rapoarte finale
    ↓  setup_gcs.py
gs://edu-federated-mihaela  ← Google Cloud Storage Data Lake
```

**Reducere date:** 8.459.320 înregistrări → 28.174 profiluri unice (reducere 99.67%)

---

## Features Utilizate (OULAD, 9 total)

| Feature | Tip | Descriere |
|---------|-----|-----------|
| total_clicks | Comportamental | Suma interacțiunilor pe VLE |
| days_active | Comportamental | Zile distincte de activitate |
| avg_score | Comportamental | Media notelor la evaluări |
| clicks_per_day | Derivat | Intensitate medie zilnică |
| engagement | Derivat | total_clicks × days_active |
| num_prev_attempts | Demografic | Încercări anterioare |
| studied_credits | Demografic | Credite înscrise |
| imd_band_num | Demografic | Status socio-economic (0-9) |
| edu_level | Demografic | Nivel educație (0-4) |

---

## Federated Learning — Cum Funcționează

```
Server (agregator)
    │
    ├─► trimite modelul global
    │
Client 1 (AAA_2013J)   Client 2 (BBB_2013J)  ...  Client 22
    │ antrenează local        │ antrenează local
    │ datele NU parasesc!     │
    └── trimite coeficienți ──┘
                │
        FedAvg (medie ponderată)
                │
        model global îmbunătățit
```

5 runde de comunicare, convergență după runda 1 (variație AUC < 0.0001 în rundele 2-5).

---

## Structura Proiectului

```
.
├── src/
│   ├── ingest.py                    # O1: CSV → Parquet
│   ├── clean.py                     # O1: Curățare + feature engineering
│   ├── validate.py                  # O1: Validare calitate date
│   ├── o2_build_analysis_dataset.py # O2: JOIN + agregare SQL (9 features)
│   ├── o3_eda.py                    # O3: Analiză exploratorie (3 datasets)
│   ├── o3_ml.py                     # O3: ML centralizat (LR, RF, GBT)
│   ├── o3_federated_sim.py          # O3: FL simulat cu PySpark
│   ├── o3_flower_federated.py       # O3: FL real cu Flower + FedAvg
│   ├── flower_client.py             # Client Flower (NumPyClient)
│   ├── generate_final_report.py     # Generare raport consolidat
│   ├── run_pipeline.py              # Orchestrator complet
│   └── utils.py                     # SparkSession + helpers
├── configs/
│   └── config.yaml                  # Căi și parametri
├── setup_gcs.py                     # Upload Google Cloud Storage
├── reset_and_download.py            # Download date (OULAD + UCI + xAPI)
├── requirements.txt
└── README.md
```

---

## Instalare și Rulare

### Cerințe
- Python 3.8+
- Java 8, 11 sau 17 (pentru Apache Spark)
- Windows / Linux / macOS

### 1. Clonare și instalare dependențe

```bash
git clone https://github.com/Miha200101/BigData_FederatedLearning.git
cd BigData_FederatedLearning
python -m venv .venv
.venv\Scripts\activate      # Windows
# source .venv/bin/activate  # Linux/macOS
pip install -r requirements.txt
pip install openpyxl
```

### 2. Descărcare date

```bash
python reset_and_download.py
```

Descarcă automat OULAD (~443MB), UCI Student Performance și xAPI-Edu-Data de pe Kaggle.
Dacă nu există cont Kaggle, se generează date sintetice pentru testare.

### 3. Rulare pipeline complet

```bash
python src/run_pipeline.py
```

Opțiuni disponibile:
```bash
python src/run_pipeline.py --skip-download   # datele există deja
python src/run_pipeline.py --skip-upload     # fără upload GCS
python src/run_pipeline.py --only-flower     # doar Flower FL + raport
```

**Timp estimat:** ~10 minute pe o mașină locală (i7, 16GB RAM)

### 4. Rezultate generate

```
reports/Raport_Final.txt       ← raport complet
reports/O3_ml_report.txt       ← ML centralizat + matrice confuzie
reports/O3_flower_report.txt   ← Flower FL comparativ
reports/flower_results.json    ← date brute JSON
reports/O3_eda_report.txt      ← analiză exploratorie
```

---

## Google Cloud Storage

```bash
python setup_gcs.py --upload    # upload toate datele
python setup_gcs.py --verify    # verifică bucket-ul
```

**Bucket:** `gs://edu-federated-mihaela` (regiune: `europe-west1`)

---

## Tehnologii

| Tehnologie | Versiune | Rol |
|------------|----------|-----|
| Apache Spark (PySpark) | 3.4.4 | ETL, ML centralizat |
| Flower (flwr) | 1.26.1 | Federated Learning |
| Ray | 2.51.1 | Backend simulare Flower |
| scikit-learn | 1.8.0 | ML pentru UCI și xAPI |
| PyArrow | 23.0.1 | Citire/scriere Parquet |
| Google Cloud Storage | - | Data Lake |

---

## Dataset-uri

- **OULAD** – [Open University Learning Analytics Dataset](https://www.kaggle.com/datasets/anlgrbz/student-demographics-online-education-dataoulad)
- **UCI Student Performance** – [Kaggle](https://www.kaggle.com/datasets/whenamancodes/student-performance)
- **xAPI-Edu-Data** – [Kaggle](https://www.kaggle.com/datasets/aljarah/xAPI-Edu-Data)
