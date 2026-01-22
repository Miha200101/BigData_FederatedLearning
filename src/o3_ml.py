from utils import load_config, make_spark, ensure_dir
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def main():
    """
    Model ML centralizat (SparkML).
    """
    cfg = load_config("configs/config.yaml")
    spark = make_spark("ML", cfg)
    ensure_dir("reports")

    df = spark.read.parquet(cfg["paths"]["processed"] + cfg["analysis"]["output_dir"])

    # Pregatire features
    assembler = VectorAssembler(
        inputCols=["total_clicks", "days_active", "avg_score"],
        outputCol="features",
        handleInvalid="skip"
    )

    data = assembler.transform(df).select("features", "label")
    train, test = data.randomSplit([0.8, 0.2], seed=42)

    # Antrenare
    lr = LogisticRegression(labelCol="label", featuresCol="features")
    model = lr.fit(train)
    pred = model.transform(test)

    # Evaluare
    evaluator = BinaryClassificationEvaluator(labelCol="label")
    auc = evaluator.evaluate(pred)

    with open("reports/O3_ml_report.txt", "w", encoding="utf-8") as f:
        f.write(f"Model: Logistic Regression\nAUC Score: {auc}")

    print(f"Raport ML scris (AUC={auc})")
    spark.stop()

if __name__ == "__main__":
    main()