from utils import load_config, make_spark
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import col

def main():
    # Simulare Federated Learning pe unități de curs distincte.
    cfg = load_config("configs/config.yaml")
    spark = make_spark("Federated Sim", cfg)

    # Încărcare date
    path = cfg["paths"]["processed"] + cfg["analysis"]["output_dir"]
    print(f"Citire date din: {path}")
    df = spark.read.parquet(path)

    # Ignorăm rândurile care au valori NULL în coloanele de features
    assembler = VectorAssembler(
        inputCols=["total_clicks", "days_active", "avg_score"],
        outputCol="features",
        handleInvalid="skip"
    )

    clients = [r["client_id"] for r in df.select("client_id").distinct().collect()]
    print(f"Start simulare FL pentru {len(clients)} clienti (cursuri)...")

    processed = 0
    skipped = 0

    for client_id in clients:
        local_data = df.filter(col("client_id") == client_id)

        # Antrenăm model local doar dacă avem suficiente date (>50 rânduri)
        if local_data.count() > 50:
            try:
                # Transformarea datelor
                vec_data = assembler.transform(local_data)

                # Antrenare model Logistic Regression
                lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
                model = lr.fit(vec_data)
                processed += 1
            except Exception as e:
                print(f"! Eroare la clientul {client_id}: {e}")
        else:
            skipped += 1

    # Raportare finală
    report_path = "reports/O3_federated_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("REZULTATE SIMULARE FEDERATED LEARNING\n")
        f.write(f"Total clienti (cursuri): {len(clients)}\n")
        f.write(f"Modele antrenate cu succes: {processed}\n")
        f.write(f"Clienti ignorati (date insuficiente): {skipped}\n")

    print(f"Simulare completa. Raport generat: {report_path}")
    spark.stop()

if __name__ == "__main__":
    main()