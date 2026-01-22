from utils import load_config, ensure_dir, make_spark

def main():
    """
    Construirea Dataset-ului de Analiză.
    Acest script integrează datele din surse diferite (activitate, note, demografie)
    și le agregă la nivel de student pentru a pregăti analiza ML.
    """
    cfg = load_config("configs/config.yaml")
    spark = make_spark("O2 - Build Dataset", cfg)
    spark.read.parquet(cfg["paths"]["processed"] + "student_vle/").createOrReplaceTempView("vle")
    spark.read.parquet(cfg["paths"]["processed"] + "student_info/").createOrReplaceTempView("info")
    spark.read.parquet(cfg["paths"]["processed"] + "student_assessment/").createOrReplaceTempView("sa")

    # Transformam datele TRANZACTIONALE (click-uri individuale) in date COMPORTAMENTALE (profil student).
    # Aici are loc reducerea numarului de randuri
    df = spark.sql("""
        SELECT v.client_id, v.id_student, SUM(v.sum_click) as total_clicks,
        COUNT(DISTINCT v.date) as days_active, i.label, AVG(s.score) as avg_score
        FROM vle v
        JOIN info i ON v.id_student = i.id_student AND v.client_id = i.client_id
        LEFT JOIN sa s ON v.id_student = s.id_student
        GROUP BY v.client_id, v.id_student, i.label
    """)
    out = cfg["paths"]["processed"] + cfg["analysis"]["output_dir"]
    ensure_dir(out)
    df.write.mode("overwrite").parquet(out)
    spark.stop()

if __name__ == "__main__":
    main()