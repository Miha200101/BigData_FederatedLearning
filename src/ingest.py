from pyspark.sql.functions import current_timestamp
from utils import load_config, ensure_dir, make_spark

def main():
    cfg = load_config("configs/config.yaml")

    # Cai fisiere
    raw_path = cfg["paths"]["raw"]
    interim_path = cfg["paths"]["interim"]
    ds = cfg["datasets"]["student_vle"]

    spark = make_spark("O1 - Ingestie Date")

    print(f"--> Citesc datele brute din: {ds['input_file']}...")
    df = spark.read.csv(raw_path + ds["input_file"], header=True, inferSchema=True)

    # Adaug timestamp-ul ingestiei
    df = df.withColumn("ingest_timestamp", current_timestamp())

    # Salvare in format Parquet (mult mai rapid decat CSV)
    out_dir = interim_path + ds["interim_dir"]
    ensure_dir(out_dir)
    df.write.mode("overwrite").parquet(out_dir)

    print(f"  Ingestie completa!")
    print(f"   Salvat in: {out_dir}")
    print(f"   Total randuri: {df.count()}")

    spark.stop()

if __name__ == "__main__":
    main()