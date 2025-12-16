from utils import load_config, make_spark, ensure_dir
from pyspark.sql.functions import col, min as smin, max as smax

def main():
    cfg = load_config("configs/config.yaml")
    ensure_dir("reports")

    spark = make_spark("Validare date")

    raw = spark.read.parquet(cfg["paths"]["interim"] + cfg["datasets"]["student_vle"]["interim_dir"])
    clean = spark.read.parquet(cfg["paths"]["processed"] + cfg["datasets"]["student_vle"]["processed_dir"])

    total_raw = raw.count()
    total_clean = clean.count()
    removed = total_raw - total_clean
    pct_removed = (removed / total_raw) * 100

    report = []
    report.append("Colectarea si pregătirea datelor\n")
    report.append("Set date: student_vle (OULAD)\n")
    report.append(f"Randuri initiale: {total_raw}")
    report.append(f"Randuri finale: {total_clean}")
    report.append(f"Randuri eliminate: {removed} ({pct_removed:.2f}%)\n")

    for c in cfg["datasets"]["student_vle"]["required_cols"]:
        if c in clean.columns:
            report.append(f"Null {c}: {clean.filter(col(c).isNull()).count()}")

    if "sum_click" in clean.columns:
        stats = clean.select(smin("sum_click"), smax("sum_click")).first()
        report.append(f"sum_click: min={stats[0]}, max={stats[1]}")

    if "date" in clean.columns:
        stats = clean.select(smin("date"), smax("date")).first()
        report.append(f"date: min={stats[0]}, max={stats[1]}")

    with open("reports/raport_pregatire_date.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(report))

    spark.stop()

if __name__ == "__main__":
    main()