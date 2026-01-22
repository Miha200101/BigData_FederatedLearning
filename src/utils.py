import os
import sys
import yaml
import shutil
from pyspark.sql import SparkSession

def load_config(path: str) -> dict:
    """Încarcă configurația din fișierul YAML."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def ensure_dir(path: str) -> None:
    """Creează folderul dacă nu există."""
    os.makedirs(path, exist_ok=True)

def make_spark(app_name: str, cfg: dict) -> SparkSession:
    """
    Inițializează sesiunea Spark într-un mediu izolat.
    Include configurări pentru a suprima erorile de shutdown pe Windows.
    """
    # 1. CURĂȚARE SELECTIVĂ
    # Ștergem variabilele care pot cauza conflicte, dar PĂSTRĂM HADOOP_HOME
    vars_to_kill = ['SPARK_HOME', 'PYTHONPATH']
    for var in vars_to_kill:
        if var in os.environ:
            # print(f"DEBUG: Se ignoră variabila de sistem {var}") # Comentat pentru liniște
            del os.environ[var]

    # 2. CONFIGURARE MEDIU LOCAL
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # 3. GESTIONARE DIRECTOARE TEMPORARE
    local_tmp = os.path.abspath("data/_tmp")
    if os.path.exists(local_tmp):
        try:
            shutil.rmtree(local_tmp, ignore_errors=True)
        except:
            pass
    ensure_dir(local_tmp)

    # 4. CONSTRUIRE SESIUNE
    spark = (SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.local.dir", local_tmp)
            .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
            .config("spark.sql.shuffle.partitions", str(cfg["spark"].get("shuffle_partitions", 8)))
            .config("spark.ui.showConsoleProgress", "false") # Ascunde bara de progres [Stage...]
            .getOrCreate())

    # 5. SUPRIMARE LOG-URI (The Silencer)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        log4j = spark.sparkContext._jvm.org.apache.log4j
        log4j.Logger.getLogger("org.apache.spark.util.ShutdownHookManager").setLevel(log4j.Level.OFF)
    except:
        pass

    return spark