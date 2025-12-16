import os
import yaml
from pyspark.sql import SparkSession

def load_config(path: str) -> dict:
    """Citeste configurarea din YAML."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def ensure_dir(path: str) -> None:
    """Creeaza folderul daca nu exista."""
    os.makedirs(path, exist_ok=True)

def make_spark(app_name: str) -> SparkSession:
    """Porneste o sesiune Spark optimizata."""
    # Folder temporar local in proiect (stabil pe Windows)
    local_tmp = os.path.abspath("data/_tmp")
    os.makedirs(local_tmp, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.local.dir", local_tmp)
        .config("spark.sql.shuffle.partitions", "8") # Optimizare pentru setul tau de date
        .getOrCreate()
    )
    
    # Ascunde warning-urile inutile din consola (sa arate curat)
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark