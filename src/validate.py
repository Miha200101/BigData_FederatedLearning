from utils import load_config, make_spark, ensure_dir
from pyspark.sql.functions import col

def main():
    """
    Validare minima a datelor din zona processed.
    Verifica NULL pe coloanele esentiale si genereaza rapoarte text.
    """
    cfg = load_config("configs/config.yaml")
    ensure_dir("reports")
    spark = make_spark("Validate", cfg)

    for name, ds in cfg["datasets"].items():
        out_path = f"reports/validate_{name}.txt"
        lines    = [f"VALIDARE DATASET: {name}"]

        try:
            df = spark.read.parquet(cfg["paths"]["processed"] + ds["processed_dir"])
            lines.append(f"Randuri: {df.count():,}")
            cols_to_check = [c for c in ds["required_cols"] if c in df.columns]
            for c in cols_to_check[:6]:
                n_null = df.filter(col(c).isNull()).count()
                lines.append(f"Null {c}: {n_null}")
        except Exception as e:
            lines.append(f"Eroare citire: {e}")

        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        print(f"Raport validare scris: {out_path}")

    spark.stop()

if __name__ == "__main__":
    main()
