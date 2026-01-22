# Analiza Federata a Datelor Educationale cu Apache Spark

Acest proiect implementeaza un pipeline complet de procesare Big Data (ETL si Machine Learning) pentru analiza performantei studentilor, utilizand setul de date OULAD (Open University Learning Analytics Dataset).

Obiectivul principal este predictia promovabilitatii pe baza comportamentului digital, comparand o abordare clasica (centralizata) cu o simulare de Invatare Federata (Federated Learning) pentru protectia datelor.

## Obiectivele Proiectului

Proiectul atinge trei obiective majore de Inginerie a Datelor:

1.  **O1 - Colectare si Pregatire:** Ingestia datelor brute (CSV), curatarea lor si optimizarea stocarii folosind formatul Parquet.
2.  **O2 - Stocare si Integrare:** Transformarea datelor din format tranzactional (milioane de interactiuni) in format analitic (profil de student), prin agregarea datelor din surse multiple.
3.  **O3 - Analiza si Modelare:**
    * Analiza Exploratorie (EDA) pentru identificarea corelatiilor.
    * Machine Learning Centralizat (Regresie Logistica) pentru stabilirea performantei de baza.
    * Simulare Federated Learning: Antrenarea modelelor locale pe 22 de cursuri distincte, fara centralizarea datelor.

## Descriere Tehnica

Am implementat o arhitectura bazata pe Apache Spark (PySpark) pentru a procesa eficient volume mari de date. Fluxul de date este urmatorul:

1.  **Ingestie:** Conversie CSV in Parquet (Zona Raw in Interim).
2.  **Curatare:** Eliminare valori nule, deduplicare si feature engineering (crearea client_id pentru federare).
3.  **Agregare:** Procesare SQL complexa pentru a comprima 8.4 milioane de randuri de activitate in profiluri unice de studenti.
4.  **Analiza:** Antrenare modele ML (Logistic Regression) si generare rapoarte automate.

## Structura Proiectului

Codul este organizat in folderul src:

* `src/ingest.py`: Scriptul de ingestie care citeste datele raw si le salveaza in zona interim.
* `src/clean.py`: Scriptul de procesare care aplica regulile de calitate si salveaza datele in zona processed.
* `src/validate.py`: Verificarea integritatii datelor (numarare valori nule).
* `src/o2_build_analysis_dataset.py`: Realizeaza join-urile intre tabele si agregarea datelor.
* `src/o3_eda.py`: Calculeaza statistici descriptive (Analiza Exploratorie).
* `src/o3_ml.py`: Antreneaza modelul de Machine Learning in regim centralizat.
* `src/o3_federated_sim.py`: Simuleaza antrenarea distribuita pe 22 de clienti (cursuri).
* `src/generate_final_report.py`: Consolideaza toate rezultatele intr-un singur fisier text.
* `src/run_pipeline.py`: Orchestrator care ruleaza secvential toti pasii de mai sus.
* `src/utils.py`: Functii utilitare pentru configurarea sesiunii Spark.
* `configs/config.yaml`: Fisier de configurare pentru cai si parametri.

## Instructiuni de Instalare si Rulare

### 1. Cerinte Preliminare
* Python 3.8 sau mai nou.
* Java 8 sau 11 (necesar pentru Apache Spark).
* Sistem de operare: Windows, Linux sau macOS.

### 2. Instalarea Dependentelor
Deschideti un terminal in folderul radacina al proiectului si rulati comanda:

```bash
pip install -r requirements.txt