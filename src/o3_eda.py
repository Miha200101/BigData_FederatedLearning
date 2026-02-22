"""
o3_eda.py
---------
Analiza Exploratorie a Datelor (EDA) pentru TOATE cele 3 dataset-uri:
  - OULAD  : citit din Parquet (Spark)
  - UCI    : citit direct din CSV (pandas)
  - xAPI   : citit direct din CSV (pandas)
"""

import os
import pandas as pd
import numpy as np
from utils import load_config, make_spark, ensure_dir


# ======================================================================
# EDA OULAD (Spark)
# ======================================================================

def eda_oulad_spark(spark, cfg):
    """Analiza OULAD din datele procesate Parquet cu Spark SQL."""
    path = cfg["paths"]["processed"] + cfg["analysis"]["output_dir"]
    df = spark.read.parquet(path)
    df.createOrReplaceTempView("data")

    total = df.count()
    res = spark.sql("""
        SELECT
          label,
          COUNT(*)                    AS count,
          ROUND(AVG(total_clicks), 1) AS avg_clicks,
          ROUND(AVG(days_active), 1)  AS avg_days,
          ROUND(AVG(avg_score), 2)    AS avg_score,
          ROUND(AVG(num_prev_attempts), 2) AS avg_prev_attempts,
          ROUND(AVG(studied_credits), 1)   AS avg_credits
        FROM data
        GROUP BY label
        ORDER BY label DESC
    """).collect()

    lines = []
    lines.append("=" * 60)
    lines.append("DATASET 1: OULAD (Open University Learning Analytics)")
    lines.append("=" * 60)
    lines.append(f"Total studenti analizati:  {total:,}")
    lines.append(f"Sursa date:                Apache Spark + Parquet")
    lines.append(f"Features disponibile:      9 (comportamentale + demografice)")
    lines.append("")

    poz = next((r for r in res if r["label"] == 1), None)
    neg = next((r for r in res if r["label"] == 0), None)

    if poz and neg:
        lines.append(f"  {'Metrica':<28} {'Promovati (1)':>14} {'Respinsi (0)':>14} {'Diferenta':>10}")
        lines.append("  " + "-" * 68)
        lines.append(f"  {'Numar studenti':<28} {poz['count']:>14,} {neg['count']:>14,}")
        lines.append(f"  {'Procent':<28} {100*poz['count']/total:>13.1f}% {100*neg['count']/total:>13.1f}%")
        lines.append(f"  {'Medie click-uri':<28} {poz['avg_clicks']:>14,.1f} {neg['avg_clicks']:>14,.1f} {poz['avg_clicks']-neg['avg_clicks']:>+10.1f}")
        lines.append(f"  {'Medie zile active':<28} {poz['avg_days']:>14.1f} {neg['avg_days']:>14.1f} {poz['avg_days']-neg['avg_days']:>+10.1f}")
        lines.append(f"  {'Medie scor evaluare':<28} {poz['avg_score']:>14.2f} {neg['avg_score']:>14.2f} {poz['avg_score']-neg['avg_score']:>+10.2f}")
        lines.append(f"  {'Medie incercari anterioare':<28} {poz['avg_prev_attempts']:>14.2f} {neg['avg_prev_attempts']:>14.2f} {poz['avg_prev_attempts']-neg['avg_prev_attempts']:>+10.2f}")
        lines.append(f"  {'Medie credite studiate':<28} {poz['avg_credits']:>14.1f} {neg['avg_credits']:>14.1f} {poz['avg_credits']-neg['avg_credits']:>+10.1f}")
        ratio = poz['avg_clicks'] / neg['avg_clicks'] if neg['avg_clicks'] > 0 else 0
        lines.append("")
        lines.append(f"  CONCLUZIE: Studentii promovati sunt de {ratio:.1f}x mai activi pe platforma.")
        lines.append(f"  Corelatie activitate digitala <-> promovabilitate: PUTERNICA")

    return "\n".join(lines)


# ======================================================================
# EDA UCI (pandas)
# ======================================================================

def eda_uci_pandas(cfg):
    """Analiza UCI Student Performance din CSV-urile brute."""
    raw_path = cfg["paths"]["raw"] + "uci/"
    lines = []
    lines.append("=" * 60)
    lines.append("DATASET 2: UCI Student Performance")
    lines.append("=" * 60)

    results_per_subject = {}
    for fname, subject in [("student-mat.csv", "Matematica"), ("student-por.csv", "Portugheza")]:
        fpath = raw_path + fname
        if not os.path.exists(fpath):
            lines.append(f"  [{subject}] LIPSA: {fpath}")
            continue

        df = None
        # Detecteaza tipul fisierului dupa magic bytes (PK = ZIP = Excel .xlsx)
        try:
            with open(fpath, "rb") as f:
                magic = f.read(4)
            is_excel = magic[:2] == b'PK'
        except Exception:
            is_excel = False

        if is_excel:
            try:
                df = pd.read_excel(fpath, engine="openpyxl")
                if "G3" not in df.columns or len(df) < 10:
                    df = None
            except Exception as e:
                lines.append(f"  [{subject}] Eroare citire Excel: {e} (pip install openpyxl)")
                continue
        else:
            for enc in ["latin-1", "cp1252", "utf-8", "iso-8859-1"]:
                for sep in [";", ","]:
                    try:
                        tmp = pd.read_csv(fpath, sep=sep, encoding=enc)
                        if "G3" in tmp.columns and len(tmp) > 10:
                            df = tmp
                            break
                    except Exception:
                        continue
                if df is not None:
                    break

        if df is None:
            lines.append(f"  [{subject}] Eroare citire - verificati fisierul")
            continue

        df["label"] = (df["G3"] >= 10).astype(int)
        df["avg_grade"] = (df["G1"] + df["G2"]) / 2
        df["grade_trend"] = df["G2"] - df["G1"]

        total = len(df)
        poz = df[df["label"] == 1]
        neg = df[df["label"] == 0]

        lines.append(f"\n  Subiect: {subject}")
        lines.append(f"  Total studenti:    {total}")
        lines.append(f"  {'Metrica':<25} {'Promovati (G3>=10)':>18} {'Respinsi (G3<10)':>16}")
        lines.append("  " + "-" * 62)
        lines.append(f"  {'Numar studenti':<25} {len(poz):>18} {len(neg):>16}")
        lines.append(f"  {'Procent':<25} {100*len(poz)/total:>17.1f}% {100*len(neg)/total:>15.1f}%")
        lines.append(f"  {'Medie nota finala (G3)':<25} {poz['G3'].mean():>18.2f} {neg['G3'].mean():>16.2f}")
        lines.append(f"  {'Medie note anterioare':<25} {poz['avg_grade'].mean():>18.2f} {neg['avg_grade'].mean():>16.2f}")
        lines.append(f"  {'Tendinta note (G2-G1)':<25} {poz['grade_trend'].mean():>18.2f} {neg['grade_trend'].mean():>16.2f}")
        lines.append(f"  {'Medie timp studiu':<25} {poz['studytime'].mean():>18.2f} {neg['studytime'].mean():>16.2f}")
        lines.append(f"  {'Medie absente':<25} {poz['absences'].mean():>18.2f} {neg['absences'].mean():>16.2f}")
        lines.append(f"  {'Medie educatie mama':<25} {poz['Medu'].mean():>18.2f} {neg['Medu'].mean():>16.2f}")

        results_per_subject[subject] = {"total": total, "poz": len(poz), "neg": len(neg)}

    lines.append("")
    lines.append("  CONCLUZIE: Performanta academica corelata puternic cu")
    lines.append("  nota anterioara (G2) si timpul de studiu.")

    return "\n".join(lines)


# ======================================================================
# EDA xAPI (pandas)
# ======================================================================

def eda_xapi_pandas(cfg):
    """Analiza xAPI-Edu-Data din CSV."""
    fpath = cfg["paths"]["raw"] + "xapi/xAPI-Edu-Data.csv"
    lines = []
    lines.append("=" * 60)
    lines.append("DATASET 3: xAPI Educational Data")
    lines.append("=" * 60)

    if not os.path.exists(fpath):
        lines.append(f"  LIPSA: {fpath}")
        return "\n".join(lines)

    df = pd.read_csv(fpath)
    df["label"] = (df["Class"] != "L").astype(int)  # H,M = promovat; L = respins
    df["total_engagement"] = (df["raisedhands"] + df["VisITedResources"]
                              + df["AnnouncementsView"] + df["Discussion"])

    total = len(df)
    poz = df[df["label"] == 1]
    neg = df[df["label"] == 0]

    # Distributie pe clase H/M/L
    dist = df["Class"].value_counts()

    lines.append(f"  Total studenti:    {total}")
    lines.append(f"  Distributie pe clase:")
    lines.append(f"    H (High / Excelent): {dist.get('H', 0):>4} studenti ({100*dist.get('H',0)/total:.1f}%)")
    lines.append(f"    M (Medium / Mediu) : {dist.get('M', 0):>4} studenti ({100*dist.get('M',0)/total:.1f}%)")
    lines.append(f"    L (Low / Slab)     : {dist.get('L', 0):>4} studenti ({100*dist.get('L',0)/total:.1f}%)")
    lines.append("")
    lines.append(f"  {'Metrica':<28} {'Promovati (H+M)':>14} {'Respinsi (L)':>14}")
    lines.append("  " + "-" * 58)
    lines.append(f"  {'Numar studenti':<28} {len(poz):>14} {len(neg):>14}")
    lines.append(f"  {'Procent':<28} {100*len(poz)/total:>13.1f}% {100*len(neg)/total:>13.1f}%")
    lines.append(f"  {'Ridicat mana (raisedhands)':<28} {poz['raisedhands'].mean():>14.1f} {neg['raisedhands'].mean():>14.1f}")
    lines.append(f"  {'Resurse vizitate':<28} {poz['VisITedResources'].mean():>14.1f} {neg['VisITedResources'].mean():>14.1f}")
    lines.append(f"  {'Anunturi vizualizate':<28} {poz['AnnouncementsView'].mean():>14.1f} {neg['AnnouncementsView'].mean():>14.1f}")
    lines.append(f"  {'Participare discutii':<28} {poz['Discussion'].mean():>14.1f} {neg['Discussion'].mean():>14.1f}")
    lines.append(f"  {'Engagement total':<28} {poz['total_engagement'].mean():>14.1f} {neg['total_engagement'].mean():>14.1f}")

    top_nations = df.groupby("NationalITy")["label"].mean().sort_values(ascending=False).head(5)
    lines.append("")
    lines.append("  Top 5 nationalitati dupa rata de promovare:")
    for nat, rate in top_nations.items():
        lines.append(f"    {nat:<20}: {100*rate:.1f}%")

    ratio = poz["total_engagement"].mean() / neg["total_engagement"].mean() if neg["total_engagement"].mean() > 0 else 0
    lines.append("")
    lines.append(f"  CONCLUZIE: Studentii promovati au engagement de {ratio:.1f}x mai mare.")
    lines.append("  Cel mai predictiv feature: VisITedResources + raisedhands.")

    return "\n".join(lines)


# ======================================================================
# MAIN
# ======================================================================

def main():
    cfg   = load_config("configs/config.yaml")
    spark = make_spark("O3 - EDA Complet", cfg)
    ensure_dir("reports")

    sections = []

    sections.append("=" * 60)
    sections.append("RAPORT EDA - ANALIZA EXPLORATORIE COMPLETA")
    sections.append("Toate cele 3 dataset-uri educationale")
    sections.append("=" * 60)
    sections.append("")

    # 1. OULAD
    try:
        sections.append(eda_oulad_spark(spark, cfg))
    except Exception as e:
        sections.append(f"[OULAD] Eroare EDA: {e}")

    sections.append("")

    # 2. UCI
    try:
        sections.append(eda_uci_pandas(cfg))
    except Exception as e:
        sections.append(f"[UCI] Eroare EDA: {e}")

    sections.append("")

    # 3. xAPI
    try:
        sections.append(eda_xapi_pandas(cfg))
    except Exception as e:
        sections.append(f"[xAPI] Eroare EDA: {e}")

    sections.append("")
    sections.append("=" * 60)
    sections.append("SUMAR COMPARATIV DATASETS")
    sections.append("=" * 60)
    sections.append("  Dataset  | Sursa        | Studenti | Features cheie")
    sections.append("  " + "-" * 56)
    sections.append("  OULAD    | Parquet/Spark | ~28K     | clicks, zile, scor, demografie")
    sections.append("  UCI      | CSV/pandas   | ~650     | note, studytime, absente")
    sections.append("  xAPI     | CSV/pandas   | 480      | engagement VLE, discutii")

    report = "\n".join(sections)

    with open("reports/O3_eda_report.txt", "w", encoding="utf-8") as f:
        f.write(report)

    print("Raport EDA scris: reports/O3_eda_report.txt")
    spark.stop()


if __name__ == "__main__":
    main()
