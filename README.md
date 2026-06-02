# Analiza Federata a Datelor Educationale cu Apache Spark si Flower

Pipeline complet de procesare Big Data pentru **predictia promovabilitatii studentilor**, comparand machine learning centralizat cu **Federated Learning** (FL) pentru protectia datelor.

**Intrebarea de cercetare:** Poate Federated Learning atinge o performanta comparabila cu modelele centralizate, mentinand confidentialitatea completa a datelor studentilor?

---

## Rezultate Principale

| Dataset | Studenti | Clienti FL | AUC Centralizat (LR) | AUC FL (cel mai bun) | Diferenta |
|---------|----------|------------|----------------------|---------------|-----------|
| OULAD | 28.174 | 22 cursuri | 0.7750 | 0.7934 (FedMedian) | +1.84% |
| xAPI | 480 | 4 nationalitati | 0.9910 | 0.9155 (FedProx) | -7.6% |
| UCI | 649 | exclus* | 0.9647 (LR) | N/A | — |

*UCI exclus din FL — justificare completa in `configs/experiment_config.yaml`.

> **Nota metodologica:** Comparatia corecta este LR centralizat vs. LR federat (aceeasi arhitectura). Comparatia LR vs. GBT/RF este suplimentara.

### Comparatie completa strategii FL — OULAD (22 clienti, 10 runde)

| Strategie | AUC | vs. LR Central (0.7750) | Implementare |
|-----------|-----|-------------------------|--------------|
| FedMedian | 0.7917 | +0.67% | Mediana coeficientilor |
| **FedProx** | **0.7875** | **+0.25%** | Regularizare L2 locala (C=1/(1+mu)) |
| FedAdam | 0.7954 | +0.54% | Adam server-side |
| FedAvg | 0.7693 | −0.57% | Medie ponderata |

> **Nota FedProx:** Termenul proximal a fost implementat prin cresterea regularizarii locale (C=1/(1+mu)), echivalent matematic pentru Logistic Regression. Implementarea directa a termenului ||w−w_global||^2 ar necesita un optimizer custom.

### Analiza parametrului mu — FedProx pe OULAD

| mu | AUC Final | Obs. |
|----|-----------|------|
| 0 (FedAvg) | 0.7693 | fara termen proximal |
| 0.01 | 0.7830 | regularizare slaba |
| **0.1** | **0.7974** | echilibru optim |
| 1.0 | 0.7995 | regularizare puternica |

### xAPI (4 clienti, 10 runde)

| Strategie | AUC | Obs. |
|-----------|-----|------|
| FedMedian | 0.9137 | cel mai bun |
| FedAvg | 0.9089 | stabil |
| FedProx | 0.6359 | regularizare prea puternica cu 4 clienti |
| FedAdam | 0.0000 | model degenerat — Adam instabil cu 4 clienti |

---

## Arhitectura

```
data/raw/          <- CSV brute (OULAD 443MB + UCI + xAPI)
    |  ingest.py (Spark)
data/interim/      <- Parquet (30.5MB, compresie 93%)
    |  clean.py (Spark)
data/processed/    <- Date curatate + client_id FL
    |  o2_build_analysis_dataset.py (Spark SQL)
data/processed/analysis_dataset/  <- 28.174 profiluri, 9 features
    |  o3_ml.py + o3_flower_federated.py
reports/           <- Rapoarte finale + grafice + CSV-uri
    |  setup_gcs.py
gs://edu-federated-mihaela  <- Google Cloud Storage Data Lake
```

**Reducere date:** 8.459.320 inregistrari -> 28.174 profiluri unice (reducere 99.67%)

---

## Configuratia Experimentului

Toti parametrii experimentali sunt centralizati in `configs/experiment_config.yaml`:
- Numar runde FL: 10
- Min samples/client: 20
- Split local: stratificat 80/20, `random_state=42`
- Strategii: FedAvg, FedProx (mu=0.1), FedAdam, FedMedian
- Analiza mu: {0.01, 0.1, 1.0} pe OULAD
- UCI: exclus din FL (justificare in config)

---

## Features Utilizate (OULAD, 9 total)

| Feature | Tip | Descriere |
|---------|-----|-----------|
| total_clicks | Comportamental | Suma interactiunilor pe VLE |
| days_active | Comportamental | Zile distincte de activitate |
| avg_score | Comportamental | Media notelor la evaluari |
| clicks_per_day | Derivat | Intensitate medie zilnica |
| engagement | Derivat | log(1 + total_clicks) |
| num_prev_attempts | Demografic | Incercari anterioare |
| studied_credits | Demografic | Credite inscrise |
| imd_band_num | Demografic | Status socio-economic (0-9) |
| edu_level | Demografic | Nivel educatie (0-4) |

---

## Federated Learning — Cum Functioneaza

```
Server (agregator)
    |
    |-> trimite modelul global
    |
Client 1 (AAA_2013J)   Client 2 (BBB_2013J)  ...  Client 22
    | antreneaza local        | antreneaza local
    | datele NU parasesc!     | split local stratificat 80/20
    |-- trimite coeficienti --|
                |
        FedAvg (medie ponderata dupa n_samples)
                |
        model global imbunatatit
```

- Convergenta din runda 1-2 pe OULAD (|delta AUC| < 0.0002 in rundele 2-15)
- Split local: stratificat 80/20, `random_state=42`, reproductibil
- Datele de test raman la client — nu se agrega centralizat

---

## Structura Proiectului

```
.
|-- src/
|   |-- ingest.py                    # O1: CSV -> Parquet
|   |-- clean.py                     # O1: Curatare + label binar + client_id
|   |-- validate.py                  # O1: Validare calitate date
|   |-- o2_build_analysis_dataset.py # O2: JOIN SQL + agregare (9 features)
|   |-- o3_eda.py                    # O3: Analiza exploratorie (3 datasets)
|   |-- o3_ml.py                     # O3: ML centralizat (LR, RF, GBT)
|   |-- o3_federated_sim.py          # O3: Validare locala PySpark (precursor FL)
|   |-- o3_flower_federated.py       # O3/O4: FL real (4 strategii, 10 runde)
|   |-- flower_client.py             # Client Flower (NumPyClient, split stratificat)
|   |-- generate_reports.py          # Rapoarte + Grafice + CSV (un singur script)
|   |-- run_pipeline.py              # Orchestrator complet
|   `-- utils.py                     # SparkSession + helpers
|-- configs/
|   |-- config.yaml                  # Cai si parametri Spark
|   `-- experiment_config.yaml       # Parametrii experimentului FL
|-- setup_gcs.py                     # Upload Google Cloud Storage
|-- reset_and_download.py            # Download date (OULAD + UCI + xAPI)
|-- requirements.txt
`-- README.md
```

---

## Instalare si Rulare

### Cerinte
- Python 3.8+
- Java 8, 11 sau 17 (pentru Apache Spark)
- Windows / Linux / macOS

### 1. Clonare si instalare dependente

```bash
git clone https://github.com/Miha200101/BigData_FederatedLearning.git
cd BigData_FederatedLearning
python -m venv .venv
.venv\Scripts\activate      # Windows
# source .venv/bin/activate  # Linux/macOS
pip install -r requirements.txt
pip install openpyxl matplotlib
```

### 2. Descarcare date

```bash
python reset_and_download.py
```

### 3. Rulare pipeline

```bash
# Recomandat -- date Spark deja procesate, fara upload GCS
python src/run_pipeline.py --skip-download --skip-upload

# Doar FL + grafice + raport (Spark deja rulat)
python src/run_pipeline.py --only-flower --skip-upload

# Pipeline complet de la zero
python src/run_pipeline.py
```

**Timp estimat:** ~10 minute (FL cu analiza mu + grafice + raport)

### 4. Rezultate generate

```
reports/Raport_Final.txt              <- raport complet disertatie (5 sectiuni)
reports/flower_results.json           <- rezultate FL brute (JSON)
reports/rezultate.csv                 <- toate rezultatele (FL + centralizat + mu)
reports/figures/fig1_*.png            <- convergenta AUC pe runde (OULAD)
reports/figures/fig2_*.png            <- comparatie centralizat vs FL
reports/figures/fig3_*.png            <- analiza mu FedProx
reports/figures/fig4_*.png            <- strategii per dataset
```

---

## Google Cloud Storage

```bash
python setup_gcs.py --upload    # upload toate datele
python setup_gcs.py --verify    # verifica bucket-ul
```

**Bucket:** `gs://edu-federated-mihaela` (regiune: `europe-west1`)

---

## Tehnologii

| Tehnologie | Versiune | Rol |
|------------|----------|-----|
| Apache Spark (PySpark) | 3.4.4 | ETL, ML centralizat |
| Flower (flwr) | 1.26.1 | Federated Learning (4 strategii) |
| Ray | 2.51.1 | Backend simulare Flower |
| scikit-learn | 1.8.0 | LogisticRegression, RF, GBT, metrici |
| PyArrow | 23.0.1 | Citire/scriere Parquet |
| Google Cloud Storage | — | Data Lake |
| matplotlib | — | Grafice disertatie |

---

## Dataset-uri

- **OULAD** — Open University Learning Analytics Dataset: 28.174 studenti, 22 cursuri
- - **xAPI-Edu-Data** — 480 studenti, 4 nationalitati (clienti FL simulati)
- **UCI Student Performance** — 649 studenti, 2 materii (exclus din FL, inclus in ML centralizat)
