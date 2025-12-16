# Proiect  Big Data pentru Analiza Federată a Datelor Educaționale (Etapa 1)

Proiectul se concentreaza pe Obiectivul 1 (Colectare si Pregatire) si foloseste setul de date OULAD (Open University Learning Analytics Dataset).

## Descriere Tehnica

Am implementat un pipeline de date folosind Apache Spark (PySpark) care transforma datele brute in date pregatite pentru analiza, trecand prin urmatoarele etape:

1. Ingestie: Preluarea datelor din format CSV si salvarea lor optimizata in format Parquet.
2. Curatare: Eliminarea valorilor nule, filtrarea datelor invalide si conversia tipurilor de date.
3. Feature Engineering: Crearea identificatorului unic client_id (combinatie intre modul si prezentare) necesar pentru scenariul de invatare federata.
4. Validare: Generarea unui raport automat pentru verificarea integritatii datelor.

## Structura Proiectului

Codul este organizat in folderul src:

- src/ingest.py: Scriptul de ingestie care citeste datele raw si le salveaza in zona interim.
- src/clean.py: Scriptul de procesare care aplica regulile de calitate si salveaza datele in zona processed.
- src/validate.py: Scriptul care compara datele si genereaza raportul final.
- src/utils.py: Functii utilitare pentru configurarea sesiunii Spark.
- configs/config.yaml: Fisier de configurare pentru cai si parametri.

## Instructiuni de Instalare si Rulare

1. Instalarea dependentelor:
pip install -r requirements.txt

2. Rularea fluxului de date (comenzile se executa din folderul radacina al proiectului):

Pasul 1 - Ingestia datelor:
python src/ingest.py

Pasul 2 - Curatarea si procesarea:
python src/clean.py

Pasul 3 - Validarea si generarea raportului:
python src/validate.py

## Rezultate

Dupa rularea scripturilor:
- Datele sunt convertite in format Parquet (pentru performanta).
- Raportul de calitate este generat in folderul reports/raport_pregatire_date.txt.
- Setul de date final contine coloana client_id si este pregatit pentru stocare (Obiectivul 2).
