from pyspark.sql.functions import col, concat_ws, when
from utils import load_config, ensure_dir, make_spark

def main():
    """Curata datele din zona interim, elimina valorile nule si invalide."""
    cfg   = load_config("configs/config.yaml")
    spark = make_spark("Curatare dataset", cfg)

    for name, ds in cfg["datasets"].items():
        df = spark.read.parquet(cfg["paths"]["interim"] + ds["interim_dir"])

        cols = [c for c in ds["required_cols"] if c in df.columns]
        df = df.select(*cols).dropna(subset=cols)

        if "code_module" in df.columns and "code_presentation" in df.columns:
            df = df.withColumn("client_id",
                               concat_ws("_", col("code_module"), col("code_presentation")))

        if name == "student_info":
            df = df.withColumn("label",
                               when(col("final_result") == "Fail", 0).otherwise(1))

        if "dedup_keys" in ds:
            df = df.dropDuplicates(ds["dedup_keys"])

        out_dir = cfg["paths"]["processed"] + ds["processed_dir"]
        ensure_dir(out_dir)
        df.write.mode("overwrite").parquet(out_dir)
        print(f"  [{name}] Curatare OK -> {out_dir}")

    spark.stop()

if __name__ == "__main__":
    main()
