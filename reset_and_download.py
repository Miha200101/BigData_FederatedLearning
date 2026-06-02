"""
reset_and_download.py (VERSIUNEA NOUA)
---------------------------------------
Sterge datele existente si descarca totul de la zero.
Structura noua:
  data/raw/oulad/   <- OULAD dataset
  data/raw/uci/     <- UCI Student Performance
  data/raw/xapi/    <- xAPI-Edu-Data

Rulare:
  python reset_and_download.py
  python reset_and_download.py --yes   (fara confirmare)
"""

import os
import shutil
import zipfile
import urllib.request
import csv
import random
from pathlib import Path

RAW_DIR   = Path("data/raw")
INT_DIR   = Path("data/interim")
PROC_DIR  = Path("data/processed")

OULAD_DIR        = RAW_DIR / "oulad"
UCI_DIR          = RAW_DIR / "uci"
XAPI_DIR         = RAW_DIR / "xapi"


def print_progress(block_num, block_size, total_size):
    if total_size > 0:
        pct = min(block_num * block_size * 100 / total_size, 100)
        mb  = block_num * block_size / (1024 * 1024)
        print(f"\r  {pct:.0f}% ({mb:.1f} MB)", end="", flush=True)


# ======================================================================
# STEP 1: CURATARE
# ======================================================================
def clean_data():
    print("\n[CURATARE] Sterg datele existente...")
    for folder in [INT_DIR, PROC_DIR, OULAD_DIR, UCI_DIR, XAPI_DIR]:
        if folder.exists():
            shutil.rmtree(folder)
            print(f"  Sters: {folder}/")
    # Sterge si eventuale CSV-uri vechi direct in raw/
    for f in ["studentVle.csv", "studentInfo.csv", "studentAssessment.csv",
              "assessments.csv", "courses.csv", "studentRegistration.csv", "vle.csv"]:
        p = RAW_DIR / f
        if p.exists():
            p.unlink()
            print(f"  Sters CSV vechi: {f}")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    OULAD_DIR.mkdir(parents=True, exist_ok=True)
    UCI_DIR.mkdir(parents=True, exist_ok=True)
    XAPI_DIR.mkdir(parents=True, exist_ok=True)
    print("  Curatare finalizata!")


# ======================================================================
# STEP 2: OULAD
# ======================================================================
def download_oulad():
    print("\n[1/3] OULAD - Open University Learning Analytics Dataset")
    needed = ["studentVle.csv", "studentInfo.csv", "studentAssessment.csv", "assessments.csv"]

    # Verifica daca exista deja fisierele reale (>50 MB = real, nu sintetic)
    if all((OULAD_DIR / f).exists() for f in needed):
        vle_size = (OULAD_DIR / "studentVle.csv").stat().st_size / (1024 * 1024)
        if vle_size > 50:
            print(f"  [OULAD] Fisiere reale deja existente (studentVle: {vle_size:.0f} MB)!")
            return True

    try:
        import kagglehub
        print("  Descarcare OULAD via kagglehub...")
        dataset_path = kagglehub.dataset_download(
            "anlgrbz/student-demographics-online-education-dataoulad"
        )
        print(f"\n  Descarcat in: {dataset_path}")

        src = Path(dataset_path)
        copied = []
        for csv_file in src.rglob("*.csv"):
            dest = OULAD_DIR / csv_file.name
            shutil.copy2(csv_file, dest)
            size_mb = dest.stat().st_size / (1024 * 1024)
            print(f"  Copiat: {csv_file.name} ({size_mb:.1f} MB) -> oulad/")
            copied.append(csv_file.name)

        if copied:
            print(f"  [OULAD] OK! {len(copied)} fisiere in data/raw/oulad/")
            return True

    except ImportError:
        print("  kagglehub nu e instalat. Ruleaza: pip install kagglehub")
    except Exception as e:
        print(f"  [OULAD] kagglehub eroare: {e}")

    # Fallback sintetic
    print("\n  Creare date sintetice OULAD (pentru testare)...")
    create_synthetic_oulad()
    return True


def create_synthetic_oulad():
    """Creeaza fisiere sintetice OULAD in data/raw/oulad/"""
    random.seed(42)
    modules       = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG"]
    presentations = ["2013J", "2014J", "2013B", "2014B"]
    n_students    = 500

    # studentInfo.csv
    if not (OULAD_DIR / "studentInfo.csv").exists():
        rows = []
        for i in range(n_students):
            result = random.choices(
                ["Pass", "Fail", "Distinction", "Withdrawn"],
                weights=[0.45, 0.25, 0.20, 0.10]
            )[0]
            rows.append({
                "code_module": random.choice(modules),
                "code_presentation": random.choice(presentations),
                "id_student": 10000 + i,
                "gender": random.choice(["M", "F"]),
                "region": random.choice(["London", "South East Region", "Scotland"]),
                "highest_education": random.choice(["HE Qualification", "A Level or Equivalent"]),
                "imd_band": random.choice(["0-10%", "10-20%", "40-50%", "80-90%"]),
                "age_band": random.choice(["0-35", "35-55", "55<="]),
                "num_of_prev_attempts": random.randint(0, 3),
                "studied_credits": random.choice([60, 90, 120]),
                "disability": random.choice(["Y", "N"]),
                "final_result": result,
            })
        with open(OULAD_DIR / "studentInfo.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"    studentInfo.csv: {n_students} randuri -> oulad/")

    # assessments.csv
    if not (OULAD_DIR / "assessments.csv").exists():
        rows, aid = [], 1
        for mod in modules:
            for pres in presentations[:2]:
                for atype, n in [("TMA", 3), ("CMA", 2), ("Exam", 1)]:
                    for _ in range(n):
                        rows.append({
                            "code_module": mod, "code_presentation": pres,
                            "id_assessment": aid, "assessment_type": atype,
                            "date": random.randint(30, 260) if atype != "Exam" else 269,
                            "weight": 10 if atype == "TMA" else (5 if atype == "CMA" else 100)
                        })
                        aid += 1
        with open(OULAD_DIR / "assessments.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"    assessments.csv: {len(rows)} randuri -> oulad/")

    # studentAssessment.csv
    if not (OULAD_DIR / "studentAssessment.csv").exists():
        rows = []
        for i in range(n_students):
            for aid in random.sample(range(1, 50), k=random.randint(3, 8)):
                rows.append({
                    "id_assessment": aid,
                    "id_student": 10000 + i,
                    "date_submitted": random.randint(1, 269),
                    "is_banked": 0,
                    "score": random.randint(30, 100)
                })
        with open(OULAD_DIR / "studentAssessment.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"    studentAssessment.csv: {len(rows)} randuri -> oulad/")

    # studentVle.csv - generam mai mare pentru a simula volumul real
    if not (OULAD_DIR / "studentVle.csv").exists():
        print("    Creare studentVle.csv (100K randuri)...")
        rows = []
        for _ in range(100000):
            rows.append({
                "code_module": random.choice(modules),
                "code_presentation": random.choice(presentations),
                "id_student": 10000 + random.randint(0, n_students - 1),
                "id_site": random.randint(1, 200),
                "date": random.randint(-10, 269),
                "sum_click": random.randint(1, 20)
            })
        with open(OULAD_DIR / "studentVle.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"    studentVle.csv: 100.000 randuri (sintetic) -> oulad/")

    print("  [OULAD] Sintetic creat in data/raw/oulad/")


# ======================================================================
# STEP 3: UCI
# ======================================================================
def download_uci():
    print("\n[2/3] UCI Student Performance Dataset")

    if (UCI_DIR / "student-mat.csv").exists() and (UCI_DIR / "student-por.csv").exists():
        print("  [UCI] Deja exista in data/raw/uci/!")
        return True

    try:
        import kagglehub
        print("  Descarcare UCI via kagglehub...")
        path = kagglehub.dataset_download("whenamancodes/student-performance")
        src  = Path(path)
        copied = 0
        for csv_file in src.rglob("*.csv"):
            name = csv_file.name.lower()
            if "mat" in name:
                shutil.copy2(csv_file, UCI_DIR / "student-mat.csv")
                print(f"  Copiat: student-mat.csv -> uci/")
                copied += 1
            elif "por" in name:
                shutil.copy2(csv_file, UCI_DIR / "student-por.csv")
                print(f"  Copiat: student-por.csv -> uci/")
                copied += 1
        if (UCI_DIR / "student-mat.csv").exists():
            print("  [UCI] OK via kagglehub!")
            return True
    except Exception as e:
        print(f"  kagglehub UCI: {e}")

    # Fallback ZIP oficial
    url      = "https://archive.ics.uci.edu/static/public/320/student+performance.zip"
    zip_path = RAW_DIR / "uci_temp.zip"
    try:
        print("  Descarcare ZIP UCI...")
        urllib.request.urlretrieve(url, zip_path, reporthook=print_progress)
        print()
        with zipfile.ZipFile(zip_path, "r") as z:
            for name in z.namelist():
                bn = os.path.basename(name)
                if bn in ("student-mat.csv", "student-por.csv"):
                    with z.open(name) as src, open(UCI_DIR / bn, "wb") as dst:
                        dst.write(src.read())
                    print(f"  Extras: {bn} -> uci/")
        zip_path.unlink()
        if (UCI_DIR / "student-mat.csv").exists():
            print("  [UCI] OK!")
            return True
    except Exception as e:
        print(f"  UCI ZIP: {e}")
        if zip_path.exists():
            zip_path.unlink()

    print("  Creare UCI sintetic...")
    return create_synthetic_uci()


def create_synthetic_uci():
    random.seed(42)
    for fname, school_counts in [
        ("student-mat.csv", [("GP", 349), ("MS", 46)]),
        ("student-por.csv", [("GP", 423), ("MS", 226)])
    ]:
        rows = []
        for school, n in school_counts:
            for _ in range(n):
                g1 = random.randint(6, 19)
                g2 = random.randint(max(0, g1 - 3), min(20, g1 + 3))
                g3 = random.randint(max(0, g2 - 3), min(20, g2 + 3))
                rows.append({
                    "school": school, "sex": random.choice(["F", "M"]),
                    "age": random.randint(15, 22), "address": random.choice(["U", "R"]),
                    "famsize": random.choice(["LE3", "GT3"]),
                    "Pstatus": random.choice(["T", "A"]),
                    "Medu": random.randint(0, 4), "Fedu": random.randint(0, 4),
                    "Mjob": random.choice(["teacher", "health", "services", "at_home", "other"]),
                    "Fjob": random.choice(["teacher", "health", "services", "at_home", "other"]),
                    "reason": random.choice(["home", "reputation", "course", "other"]),
                    "guardian": random.choice(["mother", "father", "other"]),
                    "traveltime": random.randint(1, 4), "studytime": random.randint(1, 4),
                    "failures": random.choices([0, 1, 2, 3], weights=[70, 15, 10, 5])[0],
                    "schoolsup": random.choice(["yes", "no"]),
                    "famsup": random.choice(["yes", "no"]),
                    "paid": random.choice(["yes", "no"]),
                    "activities": random.choice(["yes", "no"]),
                    "nursery": random.choice(["yes", "no"]),
                    "higher": random.choice(["yes", "no"]),
                    "internet": random.choice(["yes", "no"]),
                    "romantic": random.choice(["yes", "no"]),
                    "famrel": random.randint(1, 5), "freetime": random.randint(1, 5),
                    "goout": random.randint(1, 5), "Dalc": random.randint(1, 5),
                    "Walc": random.randint(1, 5), "health": random.randint(1, 5),
                    "absences": random.randint(0, 28), "G1": g1, "G2": g2, "G3": g3
                })
        with open(UCI_DIR / fname, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()), delimiter=";")
            w.writeheader()
            w.writerows(rows)
        print(f"  [UCI] Sintetic: {fname} -> uci/")
    return True


# ======================================================================
# STEP 4: xAPI
# ======================================================================
def download_xapi():
    print("\n[3/3] xAPI-Edu-Data Dataset")
    dest = XAPI_DIR / "xAPI-Edu-Data.csv"

    if dest.exists():
        print("  [xAPI] Deja exista in data/raw/xapi/!")
        return True

    try:
        import kagglehub
        print("  Descarcare xAPI via kagglehub...")
        path = kagglehub.dataset_download("aljarah/xAPI-Edu-Data")
        for csv_file in Path(path).rglob("*.csv"):
            shutil.copy2(csv_file, dest)
            print(f"  Copiat: {csv_file.name} ({dest.stat().st_size / 1024:.0f} KB) -> xapi/")
            print("  [xAPI] OK!")
            return True
    except Exception as e:
        print(f"  kagglehub xAPI: {e}")

    print("  Creare xAPI sintetic...")
    return create_synthetic_xapi(dest)


def create_synthetic_xapi(dest):
    random.seed(42)
    nats = ["Kuwait", "Jordan", "Palestine", "Iraq", "Lebanon", "Tunis", "Saudi",
            "Egypt", "Syria", "USA", "Iran", "Morocco", "Venezuela", "Libya"]
    rows = []
    for _ in range(480):
        lbl = random.choices(["H", "M", "L"], weights=[0.45, 0.33, 0.22])[0]
        if lbl == "H":
            r, v, a, d = (random.randint(50, 100), random.randint(60, 99),
                          random.randint(40, 98), random.randint(25, 99))
        elif lbl == "M":
            r, v, a, d = (random.randint(20, 70), random.randint(25, 75),
                          random.randint(15, 70), random.randint(10, 60))
        else:
            r, v, a, d = (random.randint(0, 40), random.randint(0, 40),
                          random.randint(0, 30), random.randint(0, 30))
        rows.append({
            "gender": random.choice(["M", "F"]),
            "NationalITy": random.choice(nats),
            "PlaceofBirth": random.choice(nats),
            "StageID": random.choice(["lowerlevel", "MiddleSchool", "HighSchool"]),
            "GradeID": random.choice(["G-04", "G-06", "G-08", "G-10", "G-12"]),
            "SectionID": random.choice(["A", "B", "C"]),
            "Topic": random.choice(["IT", "Math", "Arabic", "Science", "English"]),
            "Semester": random.choice(["First", "Second"]),
            "Relation": random.choice(["Father", "Mum"]),
            "raisedhands": r, "VisITedResources": v,
            "AnnouncementsView": a, "Discussion": d,
            "ParentAnsweringSurvey": random.choice(["Yes", "No"]),
            "ParentschoolSatisfaction": random.choice(["Good", "Bad"]),
            "StudentAbsenceDays": random.choice(["Under-7", "Above-7"]),
            "Class": lbl
        })
    with open(dest, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"  [xAPI] Sintetic: 480 randuri -> xapi/")
    return True


# ======================================================================

def main():
    import sys
    print("=" * 65)
    print("RESET COMPLET SI DESCARCARE DATE DE LA ZERO")
    print("Structura tinta:")
    print("  data/raw/oulad/        <- OULAD (432MB)")
    print("  data/raw/uci/          <- UCI Student Performance")
    print("  data/raw/xapi/         <- xAPI-Edu-Data")
    print("=" * 65)

    if "--yes" not in sys.argv:
        resp = input("\nSterge interim/ si processed/ si re-descarca totul.\nContinui? (y/n): ")
        if resp.lower() not in ["y", "yes", "da"]:
            print("Anulat.")
            return

    clean_data()
    oulad_ok       = download_oulad()
    uci_ok         = download_uci()
    xapi_ok        = download_xapi()

    print("\n" + "=" * 65)
    print("SUMAR:")
    print(f"  {'[OK]' if oulad_ok       else '[EROARE]'} OULAD        -> data/raw/oulad/")
    print(f"  {'[OK]' if uci_ok         else '[EROARE]'} UCI          -> data/raw/uci/")
    print(f"  {'[OK]' if xapi_ok        else '[EROARE]'} xAPI         -> data/raw/xapi/")
    print("\nPASII URMATORI:")
    print("  1. Verifica Ray:    python -c \"import ray; print('Ray OK')\"")
    print("  2. Daca lipseste:   pip install ray[default]")
    print("  3. Pipeline Spark:  python src/run_pipeline.py")
    print("  4. Flower FL:       python src/o3_flower_federated.py")
    print("  5. Upload GCS:      python setup_gcs.py --upload")
    print("=" * 65)


if __name__ == "__main__":
    main()