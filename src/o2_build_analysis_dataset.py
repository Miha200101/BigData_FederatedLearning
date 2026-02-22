"""
o2_build_analysis_dataset.py
----------------------------
Construieste dataset-ul de analiza cu features comportamentale + demografice.
9 features finale:
  - Comportamentale: total_clicks, days_active, avg_score
  - Derivate:        clicks_per_day, engagement
  - Demografice:     num_prev_attempts, studied_credits, imd_band_num, edu_level
"""
from utils import load_config, ensure_dir, make_spark


def main():
    cfg   = load_config("configs/config.yaml")
    spark = make_spark("O2 - Build Dataset", cfg)

    spark.read.parquet(cfg["paths"]["processed"] + "student_vle/").createOrReplaceTempView("vle")
    spark.read.parquet(cfg["paths"]["processed"] + "student_info/").createOrReplaceTempView("info")
    spark.read.parquet(cfg["paths"]["processed"] + "student_assessment/").createOrReplaceTempView("sa")

    # Agregare: date tranzactionale (click-uri) -> profil student
    # Include si features demografice encodate ordinal direct in SQL
    df = spark.sql("""
        SELECT
            v.client_id,
            v.id_student,
            SUM(v.sum_click)          AS total_clicks,
            COUNT(DISTINCT v.date)    AS days_active,
            i.label,
            AVG(s.score)              AS avg_score,

            -- Features demografice numerice (direct)
            FIRST(i.num_of_prev_attempts)  AS num_prev_attempts,
            FIRST(i.studied_credits)       AS studied_credits,

            -- IMD Band: encodare ordinala 0-9 (socioeconomic status)
            FIRST(CASE i.imd_band
                WHEN '0-10%'    THEN 0
                WHEN '10-20%'   THEN 1
                WHEN '20-30%'   THEN 2
                WHEN '30-40%'   THEN 3
                WHEN '40-50%'   THEN 4
                WHEN '50-60%'   THEN 5
                WHEN '60-70%'   THEN 6
                WHEN '70-80%'   THEN 7
                WHEN '80-90%'   THEN 8
                WHEN '90-100%'  THEN 9
                ELSE 5
            END) AS imd_band_num,

            -- Nivel educatie: encodare ordinala 0-4
            FIRST(CASE i.highest_education
                WHEN 'No Formal quals'              THEN 0
                WHEN 'Lower Than A Level'           THEN 1
                WHEN 'A Level or Equivalent'        THEN 2
                WHEN 'HE Qualification'             THEN 3
                WHEN 'Post Graduate Qualification'  THEN 4
                ELSE 2
            END) AS edu_level

        FROM vle v
        JOIN info i
            ON v.id_student = i.id_student
           AND v.client_id  = i.client_id
        LEFT JOIN sa s
            ON v.id_student = s.id_student
        GROUP BY v.client_id, v.id_student, i.label
    """)

    out = cfg["paths"]["processed"] + cfg["analysis"]["output_dir"]
    ensure_dir(out)
    df.write.mode("overwrite").parquet(out)

    n = df.count()
    print(f"Dataset analiza: {n} profiluri de studenti, {df.columns}")
    spark.stop()


if __name__ == "__main__":
    main()
