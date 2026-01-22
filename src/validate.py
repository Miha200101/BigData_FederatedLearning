from utils import load_config, make_spark, ensure_dir
from pyspark.sql.functions import col

def main():
    """
    Validare minima a datelor din zona processed.
    - verifică NULL pe coloanele esențiale
    - generează un raport text per dataset
    """
    cfg = load_config("configs/config.yaml")
    ensure_dir("reports")
    spark = make_spark("Validate", cfg)

    for name, ds in cfg["datasets"].items():
        df = spark.read.parquet(cfg["paths"]["processed"] + ds["processed_dir"])

        lines = []
        lines.append(f"VALIDARE DATASET: {name}")
        lines.append(f"Randuri: {df.count()}")

        for c in ds["required_cols"]:
            if c in df.columns:
                lines.append(f"Null {c}: {df.filter(col(c).isNull()).count()}")

        out_path = f"reports/validate_{name}.txt"
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        print(f"Raport validare scris: {out_path}")

    spark.stop()

if __name__ == "__main__":
    main()
