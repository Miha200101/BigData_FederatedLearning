from pyspark.sql.functions import current_timestamp
from utils import load_config, ensure_dir, make_spark

def main():
    # Acest script preia datele brute (CSV) și le transformă în format Parquet pentru eficiență.

    # Incarcam configuratiile
    cfg = load_config("configs/config.yaml")

    # Initializam sesiunea Spark
    spark = make_spark("OULAD - Ingest", cfg)

    # Procesam fiecare set de date definit in configuratie
    for name, ds in cfg["datasets"].items():

        # Citim fisierul CSV original din folderul raw
        # inferSchema=True permite Spark sa detecteze automat daca o coloana e numar sau text
        df = spark.read.csv(cfg["paths"]["raw"] + ds["input_file"], header=True, inferSchema=True)

        # Adaugam o coloana cu data si ora procesarii (pentru trasabilitate)
        df = df.withColumn("ingest_timestamp", current_timestamp())

        # Definim calea de iesire catre zona 'interim'
        out_dir = cfg["paths"]["interim"] + ds["interim_dir"]

        #Salvam datele in format Parquet
        ensure_dir(out_dir)
        df.write.mode("overwrite").parquet(out_dir)
    spark.stop()

if __name__ == "__main__":
    main()