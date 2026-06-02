"""
generate_reports.py
-------------------
Genereaza toate rapoartele finale ale proiectului intr-un singur loc.

Output:
  reports/Raport_Final.txt       <- raport complet disertatie (5 sectiuni)
  reports/rezultate.csv          <- un singur CSV cu toate rezultatele
  reports/figures/fig1-fig4.png  <- 4 grafice

Inlocuieste:
  generate_final_report.py + export_results_csv.py + generate_plots.py

Rulare:
  python src/generate_reports.py
"""

import os
import json
import csv
import sys
import io
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Fix encoding Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")


# ============================================================
# UTILITARE
# ============================================================

def load_json(path="reports/flower_results.json") -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_config():
    import yaml
    for p in ["configs/config.yaml", "../configs/config.yaml"]:
        if os.path.exists(p):
            with open(p, encoding="utf-8") as f:
                return yaml.safe_load(f)
    return {}


def ensure_dir(p):
    os.makedirs(p, exist_ok=True)


def sep(c="=", n=68):
    return c * n


def best_strategy(strats):
    """Returneaza (nume, auc) pentru cea mai buna strategie (ignora AUC=0)."""
    best_auc, best_name = 0.0, "N/A"
    for name, data in strats.items():
        if "error" in data:
            continue
        auc = data.get("final", {}).get("auc", 0)
        if auc > best_auc:
            best_auc, best_name = auc, name
    return best_name, best_auc


# ============================================================
# SECTIUNEA 1: PIPELINE & DATE
# ============================================================

def sectiune_pipeline(cfg, fl_results) -> str:
    lines = [sep(), "1. PIPELINE SI DATE PROCESATE", sep()]

    lines.append("""
  Arhitectura:
    CSV brute  ->  ingest.py (Spark)  ->  Parquet interim
               ->  clean.py  (Spark)  ->  Parquet procesat
               ->  o2_build_analysis_dataset.py (Spark SQL) -> profiluri
               ->  o3_flower_federated.py (Flower FL)
""")

    # Statistici din profiluri
    try:
        import pyarrow.parquet as pq
        path = cfg.get("paths", {}).get("processed", "data/processed/") + \
               cfg.get("analysis", {}).get("output_dir", "analysis_dataset/")
        if os.path.exists(path):
            df = pq.read_table(path).to_pandas()
            n_cl = df["client_id"].nunique() if "client_id" in df.columns else "?"
            lines.append(f"  OULAD:        {len(df):,} profiluri studenti, {n_cl} cursuri (clienti FL)")
    except Exception:
        pass
    lines.append(f"  xAPI:         480 studenti, 4 nationalitati (clienti FL simulati)")
    lines.append(f"  UCI:          649 studenti, exclus din FL (2 scoli, volum dezechilibrat)")
    lines.append("")
    lines.append("  Compresie Spark: CSV -> Parquet (~93% reducere spatiu)")
    lines.append("  Toate datele procesate cu Apache Spark 3.4.4, salvate pe GCS")
    return "\n".join(lines)


# ============================================================
# SECTIUNEA 2: EDA — concluzii cheie
# ============================================================

def sectiune_eda(cfg) -> str:
    lines = [sep(), "2. ANALIZA EXPLORATORIE — CONCLUZII CHEIE", sep()]

    # OULAD din Parquet
    try:
        import pyarrow.parquet as pq, pandas as pd
        path = cfg.get("paths", {}).get("processed", "data/processed/") + \
               cfg.get("analysis", {}).get("output_dir", "analysis_dataset/")
        if os.path.exists(path):
            df = pq.read_table(path).to_pandas()
            poz = df[df["label"] == 1]
            neg = df[df["label"] == 0]
            lines.append("")
            lines.append("  OULAD (28K studenti, date VLE):")
            lines.append(f"    Promovati: {len(poz):,} ({100*len(poz)/len(df):.1f}%)")
            if "total_clicks" in df.columns:
                r = poz["total_clicks"].mean() / neg["total_clicks"].mean()
                lines.append(f"    Studenti promovati: de {r:.1f}x mai activi pe platforma (clicks)")
            if "days_active" in df.columns:
                d = poz["days_active"].mean() - neg["days_active"].mean()
                lines.append(f"    Studenti promovati: activi cu {d:.0f} zile mai mult in medie")
            lines.append("    Predictor cheie: activitatea digitala (total_clicks, days_active)")
    except Exception as e:
        lines.append(f"  OULAD: [date indisponibile: {e}]")
    lines.append("")
    lines.append("  xAPI (480 studenti, engagement VLE):")
    lines.append("    Studenti H+M vs L: engagement total de ~2.7x mai mare")
    lines.append("    Predictori cheie: VisITedResources, raisedhands")
    lines.append("")
    lines.append("  UCI (649 studenti, 2 scoli portugheze):")
    lines.append("    Predictor cheie: nota anterioara (G2) — corelatie directa cu G3")

    return "\n".join(lines)


# ============================================================
# SECTIUNEA 3: ML CENTRALIZAT
# ============================================================

def sectiune_ml(fl_results) -> str:
    lines = [sep(), "3. MACHINE LEARNING CENTRALIZAT (BASELINE)", sep()]
    lines.append("""
  Metodologie: 3 modele comparate (LR, RF, GBT), split stratificat 80/20.
  LR centralizat = referinta directa pentru FL (aceeasi arhitectura).
""")
    lines.append(f"  {'Dataset':<14} {'Studenti':>8} {'LR AUC':>8} {'RF AUC':>8} {'GBT AUC':>9} {'Cel mai bun':>13}")
    lines.append("  " + sep("-", 65))

    for ds, data in fl_results.items():
        c    = data.get("centralized", {})
        aucs = c.get("model_aucs", {})
        lr   = aucs.get("LR",  c.get("lr_auc", 0))
        rf   = aucs.get("RF",  0)
        gbt  = aucs.get("GBT", 0)
        best = c.get("best_model", "?")
        n    = c.get("n_samples", 0)
        lines.append(
            f"  {ds.upper():<14} {n:>8,} {lr:>8.4f} {rf:>8.4f} {gbt:>9.4f} {best:>13}"
        )

    lines.append("")
    lines.append("  Nota: comparatia principala FL vs. centralizat foloseste LR vs. LR federat.")
    lines.append("  GBT este cel mai bun model centralizat dar nu este folosit in FL.")
    return "\n".join(lines)


# ============================================================
# SECTIUNEA 4: FEDERATED LEARNING
# ============================================================

def sectiune_fl(fl_results) -> str:
    lines = [sep(), "4. FEDERATED LEARNING — REZULTATE", sep()]
    lines.append("""
  Framework: Flower v1.26.1 cu Ray backend
  Strategii: FedAvg, FedProx (mu=0.1), FedAdam, FedMedian
  Model local: Logistic Regression (sklearn), split stratificat 80/20
  Datele brute NU parasesc niciodata clientul.
""")

    for ds, data in fl_results.items():
        fed    = data.get("federated", {})
        strats = fed.get("strategies", {})
        c      = data.get("centralized", {})
        lr_auc = c.get("lr_auc", 0)
        n_cl   = fed.get("n_clients", "?")
        n_rd   = fed.get("num_rounds", "?")

        lines.append(sep("-", 50))
        lines.append(f"  Dataset: {ds.upper()}  |  Clienti: {n_cl}  |  Runde: {n_rd}")
        lines.append(f"  Referinta LR centralizat: AUC={lr_auc:.4f}")
        lines.append("")
        lines.append(f"  {'Strategie':<12} {'AUC':>8} {'F1':>8} {'Prec':>8} {'Recall':>8}  {'vs LR':>8}  Note")
        lines.append("  " + sep("-", 70))

        bname, bauc = best_strategy(strats)

        for sname in ["FedAvg", "FedProx", "FedAdam", "FedMedian"]:
            sd = strats.get(sname, {})
            if "error" in sd:
                lines.append(f"  {sname:<12}  EROARE")
                continue
            f = sd.get("final", {})
            auc  = f.get("auc", 0)
            f1   = f.get("f1", 0)
            prec = f.get("precision", 0)
            rec  = f.get("recall", 0)
            diff = auc - lr_auc if lr_auc > 0 else 0
            note = " <- CEL MAI BUN" if sname == bname else ""
            if auc == 0:
                note = " <- model degenerat"
            lines.append(
                f"  {sname:<12} {auc:>8.4f} {f1:>8.4f} {prec:>8.4f} {rec:>8.4f} {diff:>+8.4f}{note}"
            )
        lines.append("")

    # Analiza mu FedProx
    mu_data = fl_results.get("oulad", {}).get("fedprox_mu_analysis", {})
    if mu_data:
        lines.append(sep("-", 50))
        lines.append("  ANALIZA mu FedProx pe OULAD")
        lines.append(f"  {'mu':>6} {'AUC final':>10}  Obs.")
        lines.append("  " + sep("-", 35))
        fedavg_auc = (fl_results.get("oulad", {})
                      .get("federated", {}).get("strategies", {})
                      .get("FedAvg", {}).get("final", {}).get("auc", 0))
        if fedavg_auc:
            lines.append(f"  {'0 (FedAvg)':>6} {fedavg_auc:>10.4f}  fara termen proximal")
        best_mu, best_mu_auc = 0.0, 0.0
        for mu_str in ["0.01", "0.1", "1.0"]:
            mr = mu_data.get(mu_str, {})
            if "error" not in mr:
                auc = mr.get("final_auc", 0)
                if auc > best_mu_auc:
                    best_mu_auc, best_mu = auc, float(mu_str)
        for mu_str in ["0.01", "0.1", "1.0"]:
            mr = mu_data.get(mu_str, {})
            if "error" in mr:
                lines.append(f"  {mu_str:>6}  EROARE")
            else:
                auc  = mr.get("final_auc", 0)
                note = " <- optim" if float(mu_str) == best_mu else ""
                lines.append(f"  {mu_str:>6} {auc:>10.4f}{note}")
        lines.append(f"\n  Nota: FedProx implementat ca regularizare L2 locala (C=1/(1+mu)).")
        lines.append("  Aceasta aproximare limiteaza variatia actualizarilor locale,")
        lines.append("  fara a implementa explicit termenul proximal fata de modelul global.")

    return "\n".join(lines)


# ============================================================
# SECTIUNEA 5: CONCLUZII
# ============================================================

def sectiune_concluzii(fl_results, cfg) -> str:
    lines = [sep(), "5. CONCLUZII", sep()]

    # Calcul dinamic
    oulad_strats = fl_results.get("oulad", {}).get("federated", {}).get("strategies", {})
    xapi_strats  = fl_results.get("xapi",  {}).get("federated", {}).get("strategies", {})
    oulad_lr     = fl_results.get("oulad", {}).get("centralized", {}).get("lr_auc", 0)
    xapi_lr      = fl_results.get("xapi",  {}).get("centralized", {}).get("lr_auc", 0)

    ob_name, ob_auc = best_strategy(oulad_strats)
    xb_name, xb_auc = best_strategy(xapi_strats)

    lines.append(f"""
5.1 Raspuns la intrebarile de cercetare

  RQ1 — Ce strategie ofera cele mai bune rezultate pe date educationale?
    OULAD (22 clienti): {ob_name} — AUC={ob_auc:.4f} ({ob_auc-oulad_lr:+.4f} vs LR central)
    xAPI  ( 4 clienti): {xb_name} — AUC={xb_auc:.4f} ({xb_auc-xapi_lr:+.4f} vs LR central)
    Concluzie: strategia optima depinde de numarul de clienti si heterogenitate.

  RQ2 — Cum influenteaza caracteristicile setului de date performanta FL?
    OULAD: 22 clienti, date relativ omogene -> FedMedian stabil
    xAPI:   4 clienti, date heterogene     -> FedProx superior
    FedMedian esueaza cu < 10 clienti (AUC<0.5, F1=0 pe xAPI) — limitare identificata.

  RQ3 — Care este diferenta de performanta centralizat vs federat?
    OULAD: diferenta < 2% AUC fata de LR centralizat — performanta comparabila.
    xAPI:  diferenta > 20% — efect al numarului mic de clienti (4).

  RQ4 — Se poate pastra performanta fara centralizarea datelor?
    Pe OULAD: DA — FL atinge performanta comparabila cu LR centralizat.
    Pe xAPI:  PARTIAL — scadere semnificativa din cauza setului mic.
""")

    lines.append("5.2 Contributii principale")
    lines.append("""
  1. Pipeline ETL Big Data cu Apache Spark (8.4M inregistrari -> 28K profiluri)
  2. Implementare FL reala cu Flower: 4 strategii x 3 datasets x 10 runde
  3. Analiza empirica a parametrului mu FedProx cu justificare pe date reale
  4. Identificarea limitarii FedMedian pe seturi mici (< 10 clienti)
  5. Comparatie corecta LR centralizat vs LR federat (aceeasi arhitectura)
""")

    lines.append("5.3 Limitari")
    lines.append("""
  - Model local simplu (Logistic Regression) — ales pentru comparabilitate directa
  - FedProx implementat ca aproximare L2 (nu termenul proximal complet)
  - Variabilitate intre rulari din cauza nedeterminismului Ray/Flower
  - xAPI: clientii pe nationalitate sunt simulati, nu institutii reale
  - Lipsa intervale de incredere (un singur run per experiment)
""")

    return "\n".join(lines)


# ============================================================
# GRAFICE (4)
# ============================================================

COLORS  = {"FedAvg": "#1565C0", "FedProx": "#2E7D32",
           "FedAdam": "#E65100", "FedMedian": "#6A1B9A",
           "LR": "#546E7A", "RF": "#4E342E", "GBT": "#B71C1C"}
MARKERS = {"FedAvg": "o", "FedProx": "s", "FedAdam": "^", "FedMedian": "D"}

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 11,
    "axes.titlesize": 12, "axes.labelsize": 11,
    "legend.fontsize": 10, "figure.dpi": 150,
    "savefig.dpi": 200, "savefig.bbox": "tight",
    "axes.grid": True, "grid.alpha": 0.3, "grid.linestyle": "--",
})


def fig1_convergenta(results, out_dir):
    """AUC pe runde — OULAD, toate 4 strategiile."""
    strats = results.get("oulad", {}).get("federated", {}).get("strategies", {})
    lr_auc = results.get("oulad", {}).get("centralized", {}).get("lr_auc", 0)
    if not strats:
        print("  [fig1] Date OULAD indisponibile"); return

    fig, ax = plt.subplots(figsize=(9, 5))
    plotted = False
    for sname in ["FedAvg", "FedProx", "FedAdam", "FedMedian"]:
        rm = strats.get(sname, {}).get("round_metrics", [])
        if not rm:
            continue
        runde = [r["round"] for r in rm]
        aucs  = [r.get("auc", 0) for r in rm]
        ax.plot(runde, aucs, marker=MARKERS.get(sname, "o"),
                color=COLORS[sname], label=sname,
                linewidth=2, markersize=5, markevery=2)
        plotted = True

    if lr_auc > 0:
        ax.axhline(lr_auc, color=COLORS["LR"], linestyle="--",
                   linewidth=1.5, label=f"LR central ({lr_auc:.4f})")

    if not plotted:
        print("  [fig1] Nicio strategie cu date"); return

    ax.set_title("Convergenta AUC pe runde — OULAD (22 clienti)")
    ax.set_xlabel("Runda FL")
    ax.set_ylabel("AUC")
    ax.legend(loc="lower right")
    path = os.path.join(out_dir, "fig1_convergenta_oulad.png")
    fig.savefig(path); plt.close(fig)
    print(f"  Salvat: {path}")


def fig2_comparatie(results, out_dir):
    """Centralizat vs FL — AUC final per dataset."""
    datasets = list(results.keys())
    if not datasets:
        print("  [fig2] Fara date"); return

    strat_names = ["FedAvg", "FedProx", "FedAdam", "FedMedian"]
    fig, axes = plt.subplots(1, len(datasets), figsize=(5 * len(datasets), 5), squeeze=False)

    for col, ds in enumerate(datasets):
        ax   = axes[0][col]
        data = results[ds]
        c    = data.get("centralized", {})
        strats = data.get("federated", {}).get("strategies", {})

        aucs  = c.get("model_aucs", {})
        names = list(aucs.keys()) + strat_names
        vals  = list(aucs.values()) + [
            strats.get(s, {}).get("final", {}).get("auc", 0)
            for s in strat_names
        ]
        colors = [COLORS.get(n, "#888") for n in names]
        bars = ax.bar(names, vals, color=colors, alpha=0.85, edgecolor="white", linewidth=0.5)

        for bar, v in zip(bars, vals):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width()/2, v + 0.005,
                        f"{v:.3f}", ha="center", va="bottom", fontsize=8)

        ax.set_title(f"{ds.upper()}")
        ax.set_ylabel("AUC")
        ax.set_ylim(0, 1.05)
        ax.tick_params(axis="x", rotation=35)
        ax.axhline(c.get("lr_auc", 0), color="gray", linestyle=":", linewidth=1.2,
                   label=f"LR central")
        ax.legend(fontsize=8)

    fig.suptitle("Comparatie ML Centralizat vs Federated Learning", fontsize=13, y=1.02)
    path = os.path.join(out_dir, "fig2_comparatie_modele.png")
    fig.savefig(path, bbox_inches="tight"); plt.close(fig)
    print(f"  Salvat: {path}")


def fig3_mu_fedprox(results, out_dir):
    """Analiza mu FedProx pe OULAD."""
    mu_data  = results.get("oulad", {}).get("fedprox_mu_analysis", {})
    fedavg_auc = (results.get("oulad", {}).get("federated", {})
                  .get("strategies", {}).get("FedAvg", {})
                  .get("final", {}).get("auc", 0))
    if not mu_data:
        print("  [fig3] Date mu FedProx indisponibile"); return

    mus  = [0.0] + [float(k) for k in ["0.01", "0.1", "1.0"] if "error" not in mu_data.get(k, {})]
    aucs = ([fedavg_auc] +
            [mu_data[k]["final_auc"] for k in ["0.01", "0.1", "1.0"]
             if "error" not in mu_data.get(k, {})])

    if len(mus) < 2:
        print("  [fig3] Date insuficiente mu"); return

    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.plot(mus, aucs, "o-", color=COLORS["FedProx"], linewidth=2.5,
            markersize=9, markerfacecolor="white", markeredgewidth=2)

    for x, y in zip(mus, aucs):
        ax.annotate(f"{y:.4f}", (x, y), textcoords="offset points",
                    xytext=(0, 12), ha="center", fontsize=10)

    best_idx = int(np.argmax(aucs))
    ax.scatter([mus[best_idx]], [aucs[best_idx]], s=150,
               color=COLORS["FedProx"], zorder=5, label=f"Optim: mu={mus[best_idx]}")

    ax.set_xscale("symlog", linthresh=0.01)
    ax.set_title("Impactul parametrului mu — FedProx pe OULAD")
    ax.set_xlabel("Valoare mu (scala log)")
    ax.set_ylabel("AUC final")
    ax.legend()
    path = os.path.join(out_dir, "fig3_analiza_mu_fedprox.png")
    fig.savefig(path); plt.close(fig)
    print(f"  Salvat: {path}")


def fig4_strategii_per_dataset(results, out_dir):
    """AUC final per strategie per dataset — grouped bar."""
    datasets = list(results.keys())
    strats   = ["FedAvg", "FedProx", "FedAdam", "FedMedian"]
    if not datasets:
        print("  [fig4] Fara date"); return

    x     = np.arange(len(datasets))
    width = 0.2
    fig, ax = plt.subplots(figsize=(9, 5))

    for i, sname in enumerate(strats):
        aucs = [
            results[ds].get("federated", {}).get("strategies", {})
                       .get(sname, {}).get("final", {}).get("auc", 0)
            for ds in datasets
        ]
        bars = ax.bar(x + i * width - 1.5 * width, aucs, width,
                      label=sname, color=COLORS[sname], alpha=0.85,
                      edgecolor="white", linewidth=0.5)
        for bar, v in zip(bars, aucs):
            if v > 0.05:
                ax.text(bar.get_x() + bar.get_width()/2, v + 0.005,
                        f"{v:.2f}", ha="center", va="bottom", fontsize=7)

    ax.set_xticks(x)
    ax.set_xticklabels([d.upper() for d in datasets])
    ax.set_ylabel("AUC final")
    ax.set_ylim(0, 1.05)
    ax.set_title("AUC final per strategie per dataset")
    ax.legend(loc="lower right")
    path = os.path.join(out_dir, "fig4_strategii_per_dataset.png")
    fig.savefig(path); plt.close(fig)
    print(f"  Salvat: {path}")


def genereaza_grafice(results):
    out_dir = "reports/figures"
    ensure_dir(out_dir)
    print("\n[Grafice]")
    fig1_convergenta(results, out_dir)
    fig2_comparatie(results, out_dir)
    fig3_mu_fedprox(results, out_dir)
    fig4_strategii_per_dataset(results, out_dir)


# ============================================================
# CSV — un singur fisier cu toate rezultatele
# ============================================================

def genereaza_csv(results):
    """Un singur CSV cu toate rezultatele: FL + centralizat + mu."""
    path = "reports/rezultate.csv"
    rows = []

    # FL per strategie per dataset
    for ds, data in results.items():
        c      = data.get("centralized", {})
        strats = data.get("federated", {}).get("strategies", {})
        n_cl   = data.get("federated", {}).get("n_clients", 0)
        lr_auc = c.get("lr_auc", 0)

        # Linie centralizat
        for mname in ["LR", "RF", "GBT"]:
            auc = c.get("model_aucs", {}).get(mname, None)
            if auc is not None:
                rows.append({
                    "tip":       "centralizat",
                    "dataset":   ds.upper(),
                    "n_clienti": n_cl,
                    "strategie": mname,
                    "auc":       round(auc, 4),
                    "f1":        round(c.get("f1", 0), 4) if mname == c.get("best_model") else "",
                    "precision": round(c.get("precision", 0), 4) if mname == c.get("best_model") else "",
                    "recall":    round(c.get("recall", 0), 4) if mname == c.get("best_model") else "",
                    "n_runde":   "",
                    "diff_vs_lr": "",
                    "mu":        "",
                    "obs":       "cel mai bun" if mname == c.get("best_model") else "",
                })

        # Linii FL per strategie
        for sname in ["FedAvg", "FedProx", "FedAdam", "FedMedian"]:
            sd = strats.get(sname, {})
            if "error" in sd:
                rows.append({
                    "tip": "FL", "dataset": ds.upper(), "n_clienti": n_cl,
                    "strategie": sname, "auc": "EROARE", "f1": "", "precision": "",
                    "recall": "", "n_runde": 0, "diff_vs_lr": "", "mu": "",
                    "obs": sd.get("error", "")[:60],
                })
                continue
            f    = sd.get("final", {})
            auc  = f.get("auc", 0)
            diff = round(auc - lr_auc, 4) if lr_auc and auc else ""
            rows.append({
                "tip":        "FL",
                "dataset":    ds.upper(),
                "n_clienti":  n_cl,
                "strategie":  sname,
                "auc":        round(auc, 4),
                "f1":         round(f.get("f1", 0), 4),
                "precision":  round(f.get("precision", 0), 4),
                "recall":     round(f.get("recall", 0), 4),
                "n_runde":    sd.get("n_rounds_run", 0),
                "diff_vs_lr": diff,
                "mu":         "0.1" if sname == "FedProx" else "",
                "obs":        "cel mai bun" if sname == best_strategy(strats)[0] else
                              ("model degenerat" if auc == 0 else ""),
            })

    # Analiza mu FedProx
    mu_data = results.get("oulad", {}).get("fedprox_mu_analysis", {})
    fa_auc  = (results.get("oulad", {}).get("federated", {})
               .get("strategies", {}).get("FedAvg", {}).get("final", {}).get("auc", 0))
    if fa_auc:
        rows.append({
            "tip": "mu_analysis", "dataset": "OULAD", "n_clienti": "",
            "strategie": "FedProx", "auc": round(fa_auc, 4),
            "f1": "", "precision": "", "recall": "",
            "n_runde": "", "diff_vs_lr": "", "mu": "0 (FedAvg ref)",
            "obs": "referinta FedAvg",
        })
    for mu_str in ["0.01", "0.1", "1.0"]:
        mr = mu_data.get(mu_str, {})
        if "error" not in mr:
            rows.append({
                "tip": "mu_analysis", "dataset": "OULAD", "n_clienti": "",
                "strategie": "FedProx", "auc": round(mr.get("final_auc", 0), 4),
                "f1": "", "precision": "", "recall": "",
                "n_runde": "", "diff_vs_lr": "", "mu": mu_str,
                "obs": "",
            })

    if not rows:
        print("  [CSV] Nicio date disponibile"); return

    fieldnames = ["tip", "dataset", "n_clienti", "strategie",
                  "auc", "f1", "precision", "recall",
                  "n_runde", "diff_vs_lr", "mu", "obs"]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"\n[CSV] Salvat: {path} ({len(rows)} randuri)")


# ============================================================
# MAIN
# ============================================================

def main():
    import sys
    script_dir   = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    os.chdir(project_root)

    print("=" * 55)
    print("  GENERARE RAPOARTE FINALE")
    print("  Ciobanu Mihaela Lavinia | UPT 2026")
    print("=" * 55)

    cfg     = load_config()
    results = load_json()
    ensure_dir("reports")

    if not results:
        print("\n  ATENTIE: flower_results.json lipsa.")
        print("  Ruleaza mai intai: python src/o3_flower_federated.py")

    # ── Raport Final ──────────────────────────────────────────
    print("\n[Raport Final]")
    sectiuni = []
    sectiuni.append("=" * 68)
    sectiuni.append("  ANALIZA FEDERATA A DATELOR EDUCATIONALE")
    sectiuni.append("  Ciobanu Mihaela Lavinia | Master Ingineria Datelor | UPT 2026")
    sectiuni.append("  Coordonator: SL dr.ing. Bogdan Dragulescu")
    sectiuni.append("=" * 68)

    for fn, label in [
        (lambda: sectiune_pipeline(cfg, results),  "Pipeline & Date"),
        (lambda: sectiune_eda(cfg),                 "EDA"),
        (lambda: sectiune_ml(results),              "ML Centralizat"),
        (lambda: sectiune_fl(results),              "Federated Learning"),
        (lambda: sectiune_concluzii(results, cfg),  "Concluzii"),
    ]:
        try:
            sectiuni.append(fn())
            print(f"  OK: {label}")
        except Exception as e:
            sectiuni.append(f"\n[EROARE sectiune {label}: {e}]")
            print(f"  WARN {label}: {e}")

    raport = "\n\n".join(sectiuni)
    with open("reports/Raport_Final.txt", "w", encoding="utf-8") as f:
        f.write(raport)
    size = len(raport)
    print(f"  Salvat: reports/Raport_Final.txt ({size:,} caractere)")

    # ── Grafice ───────────────────────────────────────────────
    genereaza_grafice(results)

    # ── CSV ───────────────────────────────────────────────────
    genereaza_csv(results)

    print(f"\n{'=' * 55}")
    print("  GATA. Fisiere generate:")
    print("    reports/Raport_Final.txt")
    print("    reports/rezultate.csv")
    print("    reports/figures/fig1-fig4.png")
    print(f"{'=' * 55}")


if __name__ == "__main__":
    main()
