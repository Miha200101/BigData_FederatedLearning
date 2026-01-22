from utils import load_config, make_spark, ensure_dir

def main():
    """
    Analiză date (Spark SQL).
    Calculează statistici simple pe label și salvează rezultatele.
    """
    cfg = load_config("configs/config.yaml")
    spark = make_spark("O3 - EDA", cfg)
    ensure_dir("reports")

    df = spark.read.parquet(cfg["paths"]["processed"] + cfg["analysis"]["output_dir"])
    df.createOrReplaceTempView("data")

    res = spark.sql("""
        SELECT
          label,
          COUNT(*) as count,
          AVG(total_clicks) as avg_clicks,
          AVG(days_active) as avg_days,
          AVG(avg_score) as avg_score
        FROM data
        GROUP BY label
    """).collect()

    lines = ["EDA REPORT"]
    for r in res:
        lines.append(str(r))

    with open("reports/O3_eda_report.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("Raport EDA scris: reports/O3_eda_report.txt")
    spark.stop()

if __name__ == "__main__":
    main()