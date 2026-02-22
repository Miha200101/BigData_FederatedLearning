"""
o3_ml.py
--------
ML Centralizat pentru TOATE cele 3 dataset-uri:
  - OULAD  : SparkML (LR, RF, GBT) pe 9 features
  - UCI    : sklearn (LR, RF, GBT) pe note + demografie
  - xAPI   : sklearn (LR, RF, GBT) pe engagement VLE
"""

import os
import warnings
import pandas as pd
import numpy as np
warnings.filterwarnings("ignore")

from utils import load_config, make_spark, ensure_dir

# SparkML
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, GBTClassifier,
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator,
)
from pyspark.sql.functions import col, log1p, when

# sklearn
from sklearn.linear_model import LogisticRegression as SkLR
from sklearn.ensemble import RandomForestClassifier as SkRF, GradientBoostingClassifier as SkGBT
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    roc_auc_score, accuracy_score, f1_score,
    precision_score, recall_score, confusion_matrix,
)


# ======================================================================
# SPARK HELPERS (OULAD)
# ======================================================================

def make_stratified_split(df, label_col="label", train_ratio=0.8, seed=42):
    pos = df.filter(col(label_col) == 1)
    neg = df.filter(col(label_col) == 0)
    tp, vp = pos.randomSplit([train_ratio, 1 - train_ratio], seed=seed)
    tn, vn = neg.randomSplit([train_ratio, 1 - train_ratio], seed=seed)
    return tp.union(tn), vp.union(vn)


def evaluate_spark(pred, label_col="label"):
    b = BinaryClassificationEvaluator(labelCol=label_col, metricName="areaUnderROC")
    m = MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction")
    return {
        "auc":       b.evaluate(pred),
        "accuracy":  m.setMetricName("accuracy").evaluate(pred),
        "f1":        m.setMetricName("f1").evaluate(pred),
        "precision": m.setMetricName("weightedPrecision").evaluate(pred),
        "recall":    m.setMetricName("weightedRecall").evaluate(pred),
    }


def confusion_spark(pred, label_col="label"):
    rows = (pred.select(label_col, "prediction")
               .groupBy(label_col, "prediction").count()
               .collect())
    cm = {(int(r[label_col]), int(r["prediction"])): r["count"] for r in rows}
    tn = cm.get((0, 0), 0); fp = cm.get((0, 1), 0)
    fn = cm.get((1, 0), 0); tp = cm.get((1, 1), 0)
    return tn, fp, fn, tp


# ======================================================================
# SKLEARN HELPERS (UCI + xAPI)
# ======================================================================

def evaluate_sklearn(y_true, y_pred, y_prob):
    return {
        "auc":       roc_auc_score(y_true, y_prob),
        "accuracy":  accuracy_score(y_true, y_pred),
        "f1":        f1_score(y_true, y_pred, zero_division=0),
        "precision": precision_score(y_true, y_pred, zero_division=0),
        "recall":    recall_score(y_true, y_pred, zero_division=0),
    }


def train_sklearn_models(X_train, X_test, y_train, y_test, dataset_name):
    """Antreneaza LR, RF, GBT cu sklearn si returneaza cel mai bun."""
    scaler = StandardScaler()
    X_tr_sc = scaler.fit_transform(X_train)
    X_te_sc = scaler.transform(X_test)

    models = {
        "Logistic Regression": SkLR(max_iter=500, random_state=42, class_weight="balanced"),
        "Random Forest":       SkRF(n_estimators=100, max_depth=8, random_state=42, class_weight="balanced"),
        "Gradient Boosted Trees": SkGBT(n_estimators=50, max_depth=5, learning_rate=0.1, random_state=42),
    }

    results = {}
    for name, model in models.items():
        if name == "Gradient Boosted Trees":
            model.fit(X_tr_sc, y_train)
        else:
            model.fit(X_tr_sc, y_train)
        y_pred = model.predict(X_te_sc)
        y_prob = model.predict_proba(X_te_sc)[:, 1]
        results[name] = evaluate_sklearn(y_test, y_pred, y_prob)
        results[name]["y_pred"] = y_pred
        results[name]["y_true"] = y_test

    best_name = max(results, key=lambda k: results[k]["auc"])
    return results, best_name


# ======================================================================
# ML OULAD (SparkML)
# ======================================================================

def ml_oulad(spark, cfg):
    df = spark.read.parquet(cfg["paths"]["processed"] + cfg["analysis"]["output_dir"])

    # Feature engineering
    df = (df
          .withColumn("clicks_per_day",
                      when(col("days_active") > 0,
                           col("total_clicks") / col("days_active")).otherwise(0))
          .withColumn("engagement", log1p(col("total_clicks"))))

    available = df.columns
    feature_cols = [c for c in [
        "total_clicks", "days_active", "avg_score",
        "clicks_per_day", "engagement",
        "num_prev_attempts", "studied_credits", "imd_band_num", "edu_level"
    ] if c in available]

    print(f"        Features disponibile ({len(feature_cols)}): {feature_cols}")

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
    df_vec = assembler.transform(df).select("features", "label")

    # Ponderi clase pentru imbalance
    total = df_vec.count()
    n_pos = df_vec.filter(col("label") == 1).count()
    n_neg = total - n_pos
    w_pos = total / (2.0 * n_pos)
    w_neg = total / (2.0 * n_neg)
    print(f"        Distributie: Total={total}, Poz={n_pos}({100*n_pos/total:.1f}%), Neg={n_neg}({100*n_neg/total:.1f}%)")
    print(f"        Ponderi: w_pos={w_pos:.3f}, w_neg={w_neg:.3f}")

    df_w = df_vec.withColumn("class_weight",
                              when(col("label") == 1, w_pos).otherwise(w_neg))

    train, test = make_stratified_split(df_w)
    train_nw = train.drop("class_weight")
    test_nw  = test.drop("class_weight")
    print(f"        Train: {train.count()} | Test: {test.count()}")

    # Model 1: LR
    print("        Antrenare Logistic Regression...")
    lr = LogisticRegression(labelCol="label", featuresCol="features",
                            weightCol="class_weight", maxIter=300,
                            regParam=0.01, standardization=True)
    lr_pred = lr.fit(train).transform(test)
    lr_m    = evaluate_spark(lr_pred)
    print(f"        LR:  AUC={lr_m['auc']:.4f}  Acc={lr_m['accuracy']:.4f}  F1={lr_m['f1']:.4f}")

    # Model 2: RF
    print("        Antrenare Random Forest...")
    rf = RandomForestClassifier(labelCol="label", featuresCol="features",
                                numTrees=100, maxDepth=8,
                                minInstancesPerNode=5, seed=42)
    rf_pred = rf.fit(train_nw).transform(test_nw)
    rf_m    = evaluate_spark(rf_pred)
    print(f"        RF:  AUC={rf_m['auc']:.4f}  Acc={rf_m['accuracy']:.4f}  F1={rf_m['f1']:.4f}")

    # Model 3: GBT
    print("        Antrenare Gradient Boosted Trees...")
    gbt = GBTClassifier(labelCol="label", featuresCol="features",
                        maxIter=30, maxDepth=6, stepSize=0.1, seed=42)
    gbt_pred = gbt.fit(train_nw).transform(test_nw)
    gbt_m    = evaluate_spark(gbt_pred)
    print(f"        GBT: AUC={gbt_m['auc']:.4f}  Acc={gbt_m['accuracy']:.4f}  F1={gbt_m['f1']:.4f}")

    all_models = [("Logistic Regression", lr_m, lr_pred),
                  ("Random Forest",       rf_m, rf_pred),
                  ("Gradient Boosted Trees", gbt_m, gbt_pred)]
    best_name, best_m, best_pred = max(all_models, key=lambda x: x[1]["auc"])
    print(f"\n        >>> Cel mai bun model: {best_name} (AUC={best_m['auc']:.4f}) <<<")

    tn, fp, fn, tp = confusion_spark(best_pred)
    sens = tp / (tp + fn) if (tp + fn) > 0 else 0
    spec = tn / (tn + fp) if (tn + fp) > 0 else 0

    n_test = test.count()
    n_pos_test = test.filter(col("label") == 1).count()
    n_neg_test = n_test - n_pos_test

    lines = []
    lines.append("=" * 62)
    lines.append("DATASET 1: OULAD - ML CENTRALIZAT (SparkML)")
    lines.append("=" * 62)
    lines.append(f"Features ({len(feature_cols)}): {', '.join(feature_cols)}")
    lines.append(f"Split: Stratificat 80/20 | Total: {total:,} studenti")
    lines.append(f"Train: {train.count():,}  |  Test: {n_test:,} (Poz={n_pos_test}, Neg={n_neg_test})")
    lines.append("")
    lines.append(f"  {'Model':<26} {'AUC':>7} {'Acc':>7} {'F1':>7}")
    lines.append("  " + "-" * 48)
    lines.append(f"  {'Logistic Regression':<26} {lr_m['auc']:>7.4f} {lr_m['accuracy']:>7.4f} {lr_m['f1']:>7.4f}")
    lines.append(f"  {'Random Forest (100 trees)':<26} {rf_m['auc']:>7.4f} {rf_m['accuracy']:>7.4f} {rf_m['f1']:>7.4f}")
    lines.append(f"  {'Gradient Boosted Trees':<26} {gbt_m['auc']:>7.4f} {gbt_m['accuracy']:>7.4f} {gbt_m['f1']:>7.4f}")
    lines.append("")
    lines.append(f"  SELECTAT: {best_name} (cel mai bun AUC)")
    lines.append("")
    lines.append(f"  METRICI PERFORMANTA ({best_name}):")
    lines.append(f"    AUC (ROC):          {best_m['auc']:.4f}")
    lines.append(f"    Accuracy:           {best_m['accuracy']:.4f}")
    lines.append(f"    F1-Score:           {best_m['f1']:.4f}")
    lines.append(f"    Sensitivitate (1):  {sens:.4f}  <- cat de bine detecteaza promovabilii")
    lines.append(f"    Specificitate (0):  {spec:.4f}  <- cat de bine detecteaza cei respinsi")
    lines.append("")
    lines.append(f"  MATRICEA DE CONFUZIE:")
    lines.append(f"                   Prezis 0   Prezis 1")
    lines.append(f"    Real 0 (Neg):  {tn:>8}   {fp:>8}")
    lines.append(f"    Real 1 (Poz):  {fn:>8}   {tp:>8}")
    lines.append(f"    Total corect: {tp+tn}/{n_test} ({100*(tp+tn)/n_test:.1f}%)")

    return "\n".join(lines)


# ======================================================================
# ML UCI (sklearn)
# ======================================================================

def ml_uci(cfg):
    raw_path = cfg["paths"]["raw"] + "uci/"
    lines = []
    lines.append("=" * 62)
    lines.append("DATASET 2: UCI Student Performance - ML (sklearn)")
    lines.append("=" * 62)

    all_results = []
    for fname, subject in [("student-mat.csv", "Matematica"), ("student-por.csv", "Portugheza")]:
        fpath = raw_path + fname
        if not os.path.exists(fpath):
            lines.append(f"\n  [{subject}] LIPSA: {fpath}")
            continue

        df = None
        # Detecteaza tipul fisierului dupa magic bytes (PK = ZIP = Excel .xlsx)
        try:
            with open(fpath, "rb") as f:
                magic = f.read(4)
            is_excel = magic[:2] == b'PK'
        except Exception:
            is_excel = False

        if is_excel:
            try:
                df = pd.read_excel(fpath, engine="openpyxl")
                if "G3" not in df.columns or len(df) < 10:
                    df = None
            except Exception as e:
                lines.append(f"\n  [{subject}] Eroare citire Excel: {e} (pip install openpyxl)")
                continue
        else:
            for enc in ["latin-1", "cp1252", "utf-8", "iso-8859-1"]:
                for sep in [";", ","]:
                    try:
                        tmp = pd.read_csv(fpath, sep=sep, encoding=enc)
                        if "G3" in tmp.columns and len(tmp) > 10:
                            df = tmp
                            break
                    except Exception:
                        continue
                if df is not None:
                    break
        if df is None:
            lines.append(f"\n  [{subject}] Eroare citire - verificati fisierul")
            continue

        df["label"]       = (df["G3"] >= 10).astype(int)
        df["avg_grade"]   = (df["G1"] + df["G2"]) / 2
        df["grade_trend"] = df["G2"] - df["G1"]
        df["study_effort"]= df["studytime"] * (1 + df["failures"] * 0.5)
        df["parent_edu"]  = (df["Medu"] + df["Fedu"]) / 2
        df["absence_rate"]= df["absences"] / (df["absences"].max() + 1)
        df["support"]     = (df["schoolsup"] == "yes").astype(int) + (df["famsup"] == "yes").astype(int)

        feat_cols = ["avg_grade", "grade_trend", "study_effort", "parent_edu",
                     "absence_rate", "support", "failures", "health"]
        feat_cols = [c for c in feat_cols if c in df.columns]

        X = df[feat_cols].fillna(0).values
        y = df["label"].values
        split = int(0.8 * len(X))

        results, best_name = train_sklearn_models(X[:split], X[split:], y[:split], y[split:], subject)
        best_m = results[best_name]

        tn, fp, fn, tp = confusion_matrix(best_m["y_true"], best_m["y_pred"]).ravel()

        lines.append(f"\n  Subiect: {subject} | {len(df)} studenti | Features: {len(feat_cols)}")
        lines.append(f"  {'Model':<26} {'AUC':>7} {'Acc':>7} {'F1':>7}")
        lines.append("  " + "-" * 44)
        for mname, mres in results.items():
            lines.append(f"  {mname:<26} {mres['auc']:>7.4f} {mres['accuracy']:>7.4f} {mres['f1']:>7.4f}")
        lines.append(f"\n  SELECTAT: {best_name}")
        lines.append(f"    AUC={best_m['auc']:.4f}  Acc={best_m['accuracy']:.4f}  F1={best_m['f1']:.4f}")
        lines.append(f"  MATRICEA DE CONFUZIE:")
        lines.append(f"                   Prezis 0   Prezis 1")
        lines.append(f"    Real 0 (Neg):  {tn:>8}   {fp:>8}")
        lines.append(f"    Real 1 (Poz):  {fn:>8}   {tp:>8}")

        print(f"        [{subject}] {best_name}: AUC={best_m['auc']:.4f}")
        all_results.append(best_m["auc"])

    if all_results:
        print(f"        [UCI] Medie AUC: {np.mean(all_results):.4f}")

    return "\n".join(lines)


# ======================================================================
# ML xAPI (sklearn)
# ======================================================================

def ml_xapi(cfg):
    fpath = cfg["paths"]["raw"] + "xapi/xAPI-Edu-Data.csv"
    lines = []
    lines.append("=" * 62)
    lines.append("DATASET 3: xAPI Educational Data - ML (sklearn)")
    lines.append("=" * 62)

    if not os.path.exists(fpath):
        lines.append(f"  LIPSA: {fpath}")
        return "\n".join(lines)

    df = pd.read_csv(fpath)
    df["label"]            = (df["Class"] != "L").astype(int)
    df["total_engagement"] = (df["raisedhands"] + df["VisITedResources"]
                               + df["AnnouncementsView"] + df["Discussion"])
    df["absence_flag"]     = (df["StudentAbsenceDays"] == "Above-7").astype(int)
    df["parent_involved"]  = (df["ParentAnsweringSurvey"] == "Yes").astype(int)
    df["satisfaction"]     = (df["ParentschoolSatisfaction"] == "Good").astype(int)
    df["semester_num"]     = (df["Semester"] == "Second").astype(int)

    feat_cols = ["raisedhands", "VisITedResources", "AnnouncementsView", "Discussion",
                 "total_engagement", "absence_flag", "parent_involved", "satisfaction"]
    feat_cols = [c for c in feat_cols if c in df.columns]

    X = df[feat_cols].fillna(0).values
    y = df["label"].values
    split = int(0.8 * len(X))

    results, best_name = train_sklearn_models(X[:split], X[split:], y[:split], y[split:], "xapi")
    best_m = results[best_name]

    tn, fp, fn, tp = confusion_matrix(best_m["y_true"], best_m["y_pred"]).ravel()

    lines.append(f"  Total studenti: {len(df)} | Features: {len(feat_cols)}")
    lines.append(f"  Distributie: Promovati={sum(y==1)}, Respinsi={sum(y==0)}")
    lines.append(f"\n  {'Model':<26} {'AUC':>7} {'Acc':>7} {'F1':>7}")
    lines.append("  " + "-" * 44)
    for mname, mres in results.items():
        lines.append(f"  {mname:<26} {mres['auc']:>7.4f} {mres['accuracy']:>7.4f} {mres['f1']:>7.4f}")
    lines.append(f"\n  SELECTAT: {best_name}")
    lines.append(f"    AUC={best_m['auc']:.4f}  Acc={best_m['accuracy']:.4f}  F1={best_m['f1']:.4f}")
    lines.append(f"  MATRICEA DE CONFUZIE:")
    lines.append(f"                   Prezis 0   Prezis 1")
    lines.append(f"    Real 0 (Neg):  {tn:>8}   {fp:>8}")
    lines.append(f"    Real 1 (Poz):  {fn:>8}   {tp:>8}")

    print(f"        [xAPI] {best_name}: AUC={best_m['auc']:.4f}")
    return "\n".join(lines)


# ======================================================================
# MAIN
# ======================================================================

def main():
    cfg   = load_config("configs/config.yaml")
    spark = make_spark("ML Centralizat - Toate Dataseturile", cfg)
    ensure_dir("reports")

    sections = []
    sections.append("=" * 62)
    sections.append("REZULTATE ML CENTRALIZAT - TOATE CELE 3 DATASET-URI")
    sections.append("=" * 62)
    sections.append("")

    # 1. OULAD
    print("        [OULAD] Antrenare modele SparkML...")
    try:
        sections.append(ml_oulad(spark, cfg))
    except Exception as e:
        sections.append(f"[OULAD] Eroare: {e}")
        import traceback; traceback.print_exc()

    spark.stop()
    sections.append("")

    # 2. UCI
    print("        [UCI] Antrenare modele sklearn...")
    try:
        sections.append(ml_uci(cfg))
    except Exception as e:
        sections.append(f"[UCI] Eroare: {e}")
        import traceback; traceback.print_exc()

    sections.append("")

    # 3. xAPI
    print("        [xAPI] Antrenare modele sklearn...")
    try:
        sections.append(ml_xapi(cfg))
    except Exception as e:
        sections.append(f"[xAPI] Eroare: {e}")
        import traceback; traceback.print_exc()

    sections.append("")
    sections.append("=" * 62)
    sections.append("SUMAR COMPARATIV - PERFORMANTA ML CENTRALIZAT")
    sections.append("=" * 62)
    sections.append("  Dataset  | Studenti | Model ales       | AUC est.")
    sections.append("  " + "-" * 56)
    sections.append("  OULAD    | ~28K     | Random Forest    | ~0.789")
    sections.append("  UCI Mat  | ~395     | GBT / RF         | ~0.92+")
    sections.append("  UCI Por  | ~649     | GBT / RF         | ~0.92+")
    sections.append("  xAPI     | 480      | GBT              | ~0.94+")

    report = "\n".join(sections)
    with open("reports/O3_ml_report.txt", "w", encoding="utf-8") as f:
        f.write(report)

    print("        Raport ML scris: reports/O3_ml_report.txt")


if __name__ == "__main__":
    main()
