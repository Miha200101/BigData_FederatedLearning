"""
o3_flower_federated.py
----------------------
Simulare Federated Learning reala cu framework-ul Flower (flwr).

Diferenta fata de o3_federated_sim.py (simularea manuala anterioara):
- Flower gestioneaza comunicarea client-server
- Serverul face FedAvg real (media ponderata a coeficientilor)
- Se produce un model GLOBAL care imbunatateste prin runde
- Putem testa strategii diferite (FedAvg, FedProx etc.)
- Metricile sunt colectate per runda si per client

Ruleaza: python src/o3_flower_federated.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import warnings
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import flwr as fl

from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    roc_auc_score, accuracy_score,
    f1_score, precision_score, recall_score,
)

from flower_client import EduFederatedClient
from utils import load_config, ensure_dir

warnings.filterwarnings("ignore")


# ======================================================================
# CONFIGURARE
# ======================================================================
NUM_ROUNDS        = 5     # Runde de comunicare global (cat de mult "converge" FL)
MIN_SAMPLES_CLIENT = 30   # Minim studenti per curs ca sa fie client eligibil
DATASET_KEY       = "oulad"  # Poate fi "oulad", "uci", "xapi"


# ======================================================================
# INCARCARE DATE
# ======================================================================

def load_oulad(cfg) -> pd.DataFrame:
    """Incarca dataset-ul OULAD procesat de pipeline-ul Spark."""
    path = cfg["paths"]["processed"] + cfg["analysis"]["output_dir"]
    table = pq.read_table(path)
    df = table.to_pandas()
    print(f"  OULAD: {len(df)} studenti, {df['client_id'].nunique()} cursuri")
    return df


def load_uci_student(cfg) -> pd.DataFrame:
    """
    Incarca UCI Student Performance Dataset.
    Download: https://archive.ics.uci.edu/static/public/320/student+performance.zip
    Pune fisierele student-mat.csv si student-por.csv in data/raw/uci/
    
    Clientii FL = scolile (GP = Gabriel Pereira, MS = Mousinho da Silveira)
    """
    raw_path = cfg["paths"]["raw"] + "uci/"
    
    dfs = []
    for fname, subject in [("student-mat.csv", "math"), ("student-por.csv", "portuguese")]:
        fpath = raw_path + fname
        if not os.path.exists(fpath):
            print(f"  [SKIP] UCI: fisierul {fpath} nu exista. Descarca de la:")
            print(f"         https://archive.ics.uci.edu/static/public/320/student+performance.zip")
            continue
        
        df = pd.read_csv(fpath, sep=";")
        
        # Target: promovat daca nota finala G3 >= 10
        df["label"] = (df["G3"] >= 10).astype(int)
        
        # Features comparabile cu OULAD
        # absente ~ days_active (inversat), G1+G2 ~ avg_score, studytime ~ total_clicks
        df["total_clicks"] = df["studytime"] * 100   # studytime: 1-4 -> 100-400
        df["days_active"]  = (30 - df["absences"]).clip(0, 30)
        df["avg_score"]    = (df["G1"] + df["G2"]) / 2 * 5  # scala 0-20 -> 0-100
        
        # client_id = scoala + materie (simuleaza institutii separate)
        df["client_id"] = df["school"] + "_" + subject
        df["id_student"] = range(len(df))
        
        dfs.append(df[["client_id", "id_student", "total_clicks", 
                        "days_active", "avg_score", "label"]])
    
    if not dfs:
        return pd.DataFrame()
    
    result = pd.concat(dfs, ignore_index=True)
    print(f"  UCI: {len(result)} studenti, {result['client_id'].nunique()} clienti")
    return result


def load_xapi(cfg) -> pd.DataFrame:
    """
    Incarca xAPI-Edu-Data Dataset.
    Download: https://www.kaggle.com/datasets/aljarah/xAPI-Edu-Data
    Pune fisierul xAPI-Edu-Data.csv in data/raw/xapi/
    
    Clientii FL = nationalitati (fiecare tara = un client/institutie)
    """
    fpath = cfg["paths"]["raw"] + "xapi/xAPI-Edu-Data.csv"
    
    if not os.path.exists(fpath):
        print(f"  [SKIP] xAPI: fisierul {fpath} nu exista. Descarca de la:")
        print(f"         https://www.kaggle.com/datasets/aljarah/xAPI-Edu-Data")
        return pd.DataFrame()
    
    df = pd.read_csv(fpath)
    
    # Target: H (High) si M (Medium) = promovat (1), L (Low) = respins (0)
    df["label"] = (df["Class"] != "L").astype(int)
    
    # Features comportamentale disponibile in xAPI
    df["total_clicks"] = df["raisedhands"] + df["VisITedResources"] + df["AnnouncementsView"]
    df["days_active"]  = df["Discussion"].clip(0, 100)
    df["avg_score"]    = df["raisedhands"]   # proxy pentru engagement
    
    # client_id = nationalitate (simuleaza scoli/tari diferite)
    df["client_id"] = "xapi_" + df["NationalITy"].str.replace(" ", "_")
    df["id_student"] = range(len(df))
    
    result = df[["client_id", "id_student", "total_clicks", 
                 "days_active", "avg_score", "label"]]
    print(f"  xAPI: {len(result)} studenti, {result['client_id'].nunique()} clienti")
    return result


def load_all_datasets(cfg) -> dict:
    """Incarca toate dataset-urile disponibile."""
    datasets = {}
    
    print("\nIncarcare dataset-uri:")
    
    oulad = load_oulad(cfg)
    if not oulad.empty:
        datasets["oulad"] = oulad
    
    uci = load_uci_student(cfg)
    if not uci.empty:
        datasets["uci"] = uci
    
    xapi = load_xapi(cfg)
    if not xapi.empty:
        datasets["xapi"] = xapi
    
    return datasets


# ======================================================================
# MODEL CENTRALIZAT (baseline pentru comparatie)
# ======================================================================

def train_centralized(df: pd.DataFrame, dataset_name: str) -> dict:
    """
    Antreneaza un model centralizat pe TOATE datele (ca si cum nu ar exista
    restrictii de confidentialitate). Acesta e BASELINE-ul cu care comparam FL.
    """
    df_clean = df.dropna(subset=["total_clicks", "days_active", "avg_score", "label"])
    
    X = df_clean[["total_clicks", "days_active", "avg_score"]].values
    y = df_clean["label"].values.astype(int)
    
    if len(np.unique(y)) < 2 or len(X) < 10:
        return {}
    
    scaler  = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    split = int(0.8 * len(X_scaled))
    X_train, X_test = X_scaled[:split], X_scaled[split:]
    y_train, y_test = y[:split], y[split:]
    
    model = LogisticRegression(max_iter=500, random_state=42)
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]
    
    metrics = {
        "dataset":   dataset_name,
        "auc":       float(roc_auc_score(y_test, y_prob)),
        "accuracy":  float(accuracy_score(y_test, y_pred)),
        "f1":        float(f1_score(y_test, y_pred, zero_division=0)),
        "precision": float(precision_score(y_test, y_pred, zero_division=0)),
        "recall":    float(recall_score(y_test, y_pred, zero_division=0)),
        "n_samples": len(X),
        "n_test":    len(X_test),
    }
    
    print(f"  Centralizat [{dataset_name}]: AUC={metrics['auc']:.4f}  "
          f"Acc={metrics['accuracy']:.4f}  F1={metrics['f1']:.4f}")
    return metrics


# ======================================================================
# SIMULARE FLOWER FL
# ======================================================================

def run_flower_on_dataset(df: pd.DataFrame, dataset_name: str) -> dict:
    """
    Ruleaza simularea Flower FL pe un dataset.
    Returneaza metricile per runda si per client.
    """
    
    # Filtreaza clientii eligibili (minim MIN_SAMPLES_CLIENT studenti)
    clients_data = {}
    for cid, group in df.groupby("client_id"):
        if len(group) >= MIN_SAMPLES_CLIENT:
            clients_data[cid] = group.reset_index(drop=True)
    
    client_ids = list(clients_data.keys())
    n_clients  = len(client_ids)
    
    if n_clients < 2:
        print(f"  [SKIP] {dataset_name}: prea putini clienti ({n_clients})")
        return {}
    
    print(f"\n  Flower FL [{dataset_name}]: {n_clients} clienti, {NUM_ROUNDS} runde")
    
    # Colectam metricile per runda
    round_metrics = []

    def evaluate_metrics_aggregation(server_round, results, failures):
        """Agrega metricile de la toti clientii dupa fiecare runda."""
        if not results:
            return None, {}
        
        total_samples = sum(n for _, n, _ in results)
        if total_samples == 0:
            return None, {}
        
        # Media ponderata (clientii cu mai multi studenti au influenta mai mare)
        aggregated = {"round": server_round}
        for metric in ["auc", "accuracy", "f1", "precision", "recall"]:
            weighted = sum(
                m.get(metric, 0) * n for _, n, m in results
            )
            aggregated[metric] = weighted / total_samples
        
        aggregated["n_clients_evaluated"] = len(results)
        round_metrics.append(aggregated)
        
        print(f"    Runda {server_round}/{NUM_ROUNDS} -> "
              f"AUC={aggregated['auc']:.4f}  "
              f"Acc={aggregated['accuracy']:.4f}  "
              f"F1={aggregated['f1']:.4f}  "
              f"({len(results)} clienti)")
        
        return aggregated["auc"], aggregated

    # Strategie FedAvg
    strategy = fl.server.strategy.FedAvg(
        fraction_fit=1.0,           # Foloseste 100% din clienti la antrenare
        fraction_evaluate=1.0,      # Evalueaza pe 100% din clienti
        min_fit_clients=min(2, n_clients),
        min_evaluate_clients=min(2, n_clients),
        min_available_clients=min(2, n_clients),
        evaluate_metrics_aggregation_fn=evaluate_metrics_aggregation,
    )

    # Functia client_fn (apelata de Flower pentru fiecare client)
    def client_fn(cid: str) -> fl.client.Client:
        client_id  = client_ids[int(cid)]
        local_df   = clients_data[client_id]
        return EduFederatedClient(client_id, local_df).to_client()

    # Pornire simulare Flower
    fl.simulation.start_simulation(
        client_fn=client_fn,
        num_clients=n_clients,
        config=fl.server.ServerConfig(num_rounds=NUM_ROUNDS),
        strategy=strategy,
        client_resources={"num_cpus": 1, "num_gpus": 0},
        ray_init_args={"ignore_reinit_error": True, "include_dashboard": False},
    )

    return {
        "dataset":       dataset_name,
        "n_clients":     n_clients,
        "num_rounds":    NUM_ROUNDS,
        "round_metrics": round_metrics,
        "final_metrics": round_metrics[-1] if round_metrics else {},
    }


# ======================================================================
# GENERARE RAPORT
# ======================================================================

def generate_report(all_results: dict, output_path: str):
    """Genereaza raportul comparativ centralizat vs FL per dataset."""
    
    lines = []
    lines.append("=" * 65)
    lines.append("REZULTATE FLOWER FEDERATED LEARNING")
    lines.append("=" * 65)
    lines.append(f"Runde FL: {NUM_ROUNDS}  |  Min samples/client: {MIN_SAMPLES_CLIENT}")
    lines.append("")

    for ds_name, res in all_results.items():
        if not res.get("centralized") and not res.get("federated"):
            continue
        
        lines.append(f"\n{'=' * 65}")
        lines.append(f"DATASET: {ds_name.upper()}")
        lines.append(f"{'=' * 65}")
        
        central = res.get("centralized", {})
        fed     = res.get("federated", {})
        final   = fed.get("final_metrics", {})
        
        lines.append(f"\nClienti FL: {fed.get('n_clients', 'N/A')}")
        lines.append(f"Sample-uri totale: {central.get('n_samples', 'N/A')}")
        lines.append("")
        lines.append(f"{'Metrica':<15} {'Centralizat':>12} {'Federat FL':>12} {'Diferenta':>12}")
        lines.append("-" * 55)
        
        for metric in ["auc", "accuracy", "f1", "precision", "recall"]:
            c_val = central.get(metric, 0)
            f_val = final.get(metric, 0)
            diff  = f_val - c_val
            sign  = "+" if diff >= 0 else ""
            lines.append(
                f"{metric.upper():<15} {c_val:>12.4f} {f_val:>12.4f} {sign + f'{diff:.4f}':>12}"
            )
        
        lines.append("")
        lines.append("Evolutie pe runde:")
        lines.append(f"{'Runda':<8} {'AUC':>8} {'Accuracy':>10} {'F1':>8} {'Precision':>10} {'Recall':>8}")
        lines.append("-" * 55)
        for r in fed.get("round_metrics", []):
            lines.append(
                f"{r.get('round', ''):<8} "
                f"{r.get('auc', 0):>8.4f} "
                f"{r.get('accuracy', 0):>10.4f} "
                f"{r.get('f1', 0):>8.4f} "
                f"{r.get('precision', 0):>10.4f} "
                f"{r.get('recall', 0):>8.4f}"
            )
    
    lines.append("\n" + "=" * 65)
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    
    print(f"\nRaport generat: {output_path}")


# ======================================================================
# MAIN
# ======================================================================

def main():
    cfg = load_config("configs/config.yaml")
    ensure_dir("reports")
    
    print("=" * 65)
    print("FLOWER FEDERATED LEARNING - START")
    print("=" * 65)
    
    # Incarca toate dataset-urile disponibile
    datasets = load_all_datasets(cfg)
    
    if not datasets:
        print("EROARE: Niciun dataset disponibil!")
        return
    
    all_results = {}
    
    for ds_name, df in datasets.items():
        print(f"\n{'─' * 65}")
        print(f"Procesare: {ds_name.upper()}")
        print(f"{'─' * 65}")
        
        all_results[ds_name] = {}
        
        # 1. Model centralizat (baseline)
        print("  Antrenare model centralizat (baseline):")
        central_metrics = train_centralized(df, ds_name)
        all_results[ds_name]["centralized"] = central_metrics
        
        # 2. Simulare Flower FL
        print("  Rulare simulare Flower FL:")
        fl_results = run_flower_on_dataset(df, ds_name)
        all_results[ds_name]["federated"] = fl_results
    
    # Salveaza rezultatele JSON (pentru vizualizari ulterioare)
    json_path = "reports/flower_results.json"
    with open(json_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\nDate JSON salvate: {json_path}")
    
    # Genereaza raportul text
    generate_report(all_results, "reports/O3_flower_report.txt")
    
    print("\nFINALIZAT!")


if __name__ == "__main__":
    main()
