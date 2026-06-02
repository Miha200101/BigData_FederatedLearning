from pyspark.sql.functions import current_timestamp
from utils import load_config, ensure_dir, make_spark

def main():
    """Preia datele brute (CSV) si le transforma in format Parquet."""
    cfg   = load_config("configs/config.yaml")
    spark = make_spark("Ingest", cfg)

    for name, ds in cfg["datasets"].items():
        in_path = cfg["paths"]["raw"] + ds["input_file"]
        out_dir = cfg["paths"]["interim"] + ds["interim_dir"]

        df = spark.read.csv(in_path, header=True, inferSchema=True)
        df = df.withColumn("ingest_timestamp", current_timestamp())

        ensure_dir(out_dir)
        df.write.mode("overwrite").parquet(out_dir)
        print(f"  [{name}] Ingest OK -> {out_dir}")

    spark.stop()

if __name__ == "__main__":
    main()
