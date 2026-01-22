from pyspark.sql.functions import col, concat_ws, when
from utils import load_config, ensure_dir, make_spark

def main():
    # Acest script procesează datele din zona 'interim', elimină valorile nule și pregătește feature-urile necesare pentru analiză.
    # Incarcare configuratie si initializare Spark
    cfg = load_config("configs/config.yaml")
    spark = make_spark("Curatare dataset", cfg)

    # Trecem prin fiecare dataset definit in config
    for name, ds in cfg["datasets"].items():

        df = spark.read.parquet(cfg["paths"]["interim"] + ds["interim_dir"])

        # Selectam doar coloanele relevante si eliminam randurile care au valori NULL
        cols = [c for c in ds["required_cols"] if c in df.columns]
        df = df.select(*cols).dropna(subset=cols)

        # Identifica unic o instanta de curs (ex: AAA_2013J) care va deveni un "client" federat
        if "code_module" in df.columns and "code_presentation" in df.columns:
            df = df.withColumn("client_id", concat_ws("_", col("code_module"), col("code_presentation")))

        # Convertim rezultatul text ("Pass", "Fail", "Distinction") in binar (0/1)
        # 0 = Fail, 1 = Pass sau Distinction
        if name == "student_info":
            df = df.withColumn("label", when(col("final_result") == "Fail", 0).otherwise(1))

        # Eliminam inregistrarile duplicate pentru a asigura calitatea datelor
        if "dedup_keys" in ds:
            df = df.dropDuplicates(ds["dedup_keys"])

        # Salvare in zona finala 'processed'
        out_dir = cfg["paths"]["processed"] + ds["processed_dir"]
        ensure_dir(out_dir)

        # Scriem datele in format Parquet
        df.write.mode("overwrite").parquet(out_dir)
    spark.stop()

if __name__ == "__main__":
    main()