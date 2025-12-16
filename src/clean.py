from pyspark.sql.functions import col, concat_ws
from utils import load_config, ensure_dir, make_spark

def main():
    cfg = load_config("configs/config.yaml")
    
    # Configurare cai
    interim_path = cfg["paths"]["interim"]
    processed_path = cfg["paths"]["processed"]
    ds = cfg["datasets"]["student_vle"]
    rules = cfg["quality_rules"]

    spark = make_spark("Curatare Date:")

    print("Procesare date...")
    df = spark.read.parquet(interim_path + ds["interim_dir"])
    before = df.count()

    # Pastram coloanele necesare
    cols = [c for c in ds["required_cols"] if c in df.columns]
    if "ingest_timestamp" in df.columns:
        cols.append("ingest_timestamp")
    df = df.select(*cols)

    # Eliminam datele incomplete (NULL)
    df = df.dropna(subset=ds["required_cols"])

    # Corectam tipurile de date
    df = df.withColumn("sum_click", col("sum_click").cast("int")) \
           .withColumn("date", col("date").cast("int")) \
           .withColumn("id_student", col("id_student").cast("string"))

    # Filtre de calitate (eliminam valori ilogice)
    df = df.filter(col("sum_click") >= rules["sum_click_min"])
    df = df.filter((col("date") >= rules["date_min"]) & (col("date") <= rules["date_max"]))

    # Eliminam duplicatele
    df = df.dropDuplicates(ds["dedup_keys"])

    # Cream ID-ul unic pentru Federated Learning
    # (Combina cursul cu sesiunea pentru a simula un "client" in retea)
    df = df.withColumn("client_id", concat_ws("_", col("code_module"), col("code_presentation")))

    after = df.count()

    # Salvare finala
    out_dir = processed_path + ds["processed_dir"]
    ensure_dir(out_dir)
    df.coalesce(4).write.mode("overwrite").parquet(out_dir)

    print("-" * 30)
    print(" Curatare finalizata")
    print(f"   Randuri initiale: {before}")
    print(f"   Randuri finale:   {after}")
    print(f"   Eliminate:        {before - after} ({(before-after)/before:.2%})")
    print("-" * 30)

    spark.stop()

if __name__ == "__main__":
    main()