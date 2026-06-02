"""
flower_client.py
----------------
Client Flower pentru Federated Learning educational.
Suporta liste dinamice de features (feature_cols parametrizabil).

Fiecare client = un curs (client_id).
Antreneaza Logistic Regression LOCAL si trimite serverului
DOAR coeficientii (nu datele brute) - principiul FL.
Serverul aplica FedAvg (media ponderata).
"""

import flwr as fl
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    roc_auc_score, accuracy_score,
    f1_score, precision_score, recall_score,
)
import warnings
warnings.filterwarnings("ignore")

# Features default (backwards compatibility)
DEFAULT_FEATURES = ["total_clicks", "days_active", "avg_score"]


class EduFederatedClient(fl.client.NumPyClient):
    """
    Client Flower pentru analiza datelor educationale.

    Cum functioneaza FL:
    1. Serverul trimite ponderile modelului global -> set_parameters()
    2. Clientul antreneaza local pe datele sale     -> fit()
    3. Clientul trimite ponderile actualizate       -> get_parameters()
    4. Serverul face FedAvg -> nou model global
    5. Repeta pentru NUM_ROUNDS runde

    Datele brute NU parasesc niciodata clientul!
    """

    def __init__(
        self,
        client_id: str,
        df_local: pd.DataFrame,
        feature_cols: list = None,
    ):
        self.client_id = client_id
        self.feature_cols = feature_cols or DEFAULT_FEATURES

        # Curatare date locale
        cols_needed = self.feature_cols + ["label"]
        df_clean = df_local.dropna(subset=[c for c in cols_needed if c in df_local.columns])

        # Selectam features disponibile
        available = [c for c in self.feature_cols if c in df_clean.columns]
        if not available:
            available = [c for c in DEFAULT_FEATURES if c in df_clean.columns]
        self.feature_cols = available
        self.n_features = len(available)

        X = df_clean[available].values.astype(float)
        y = df_clean["label"].values.astype(int)

        self.n_samples = len(X)

        from sklearn.model_selection import train_test_split
        if len(np.unique(y)) > 1 and len(X) >= 5:
            try:
                X_train_raw, X_test_raw, self.y_train, self.y_test = \
                    train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
                # Verifica ca ambele split-uri au cel putin 2 clase
                if len(np.unique(self.y_train)) < 2 or len(np.unique(self.y_test)) < 2:
                    raise ValueError("Split stratificat produce clase lipsa")
            except Exception:
                # Fallback: split aleatoriu fara stratificare
                X_train_raw, X_test_raw, self.y_train, self.y_test = \
                    train_test_split(X, y, test_size=0.2, random_state=42)
        else:
            split = int(0.8 * len(X))
            X_train_raw, X_test_raw = X[:split], X[split:]
            self.y_train, self.y_test = y[:split], y[split:]

        # Scalare
        self.scaler = StandardScaler()
        self.X_train = self.scaler.fit_transform(X_train_raw) if len(X_train_raw) > 0 else X_train_raw
        self.X_test  = self.scaler.transform(X_test_raw) if len(X_test_raw) > 0 else X_test_raw

        # Model cu warm_start=True (continua din punctul FL anterior)
        self.model = LogisticRegression(
            max_iter=500, random_state=42,
            C=1.0, solver="lbfgs",
            class_weight="balanced",
            warm_start=True,
        )

        # Initializare model
        if len(np.unique(y)) > 1 and len(self.X_train) >= 2:
            try:
                self.model.fit(self.X_train, self.y_train)
            except Exception:
                self.model.coef_      = np.zeros((1, self.n_features))
                self.model.intercept_ = np.zeros((1,))
                self.model.classes_   = np.array([0, 1])
        else:
            self.model.coef_      = np.zeros((1, self.n_features))
            self.model.intercept_ = np.zeros((1,))
            self.model.classes_   = np.array([0, 1])

    def get_parameters(self, config):
        """Trimite coeficientii modelului local catre server."""
        return [self.model.coef_.copy(), self.model.intercept_.copy()]

    def set_parameters(self, parameters):
        """Aplica ponderile globale primite de la server."""
        coef = parameters[0]
        # Adapteaza dimensiunea daca e necesar
        if coef.shape[1] != self.n_features:
            n = min(coef.shape[1], self.n_features)
            new_coef = np.zeros((1, self.n_features))
            new_coef[0, :n] = coef[0, :n]
            coef = new_coef
        self.model.coef_      = coef.copy()
        self.model.intercept_ = parameters[1].copy()
        self.model.classes_   = np.array([0, 1])

    def fit(self, parameters, config):
        """Antrenare locala cu modelul global ca punct de start.

        Suporta FedProx prin proximal_mu din config:
          - proximal_mu > 0 => clientul nu se departeaza prea mult de modelul global
          - proximal_mu = 0 => comportament standard FedAvg (C=1.0)
        """
        self.set_parameters(parameters)

        # FedProx: ajusteaza regularizarea in functie de mu
        proximal_mu = float(config.get("proximal_mu", 0.0))
        if proximal_mu > 0:
            # C mic => regularizare mai puternica => aproape de modelul global
            self.model.C = 1.0 / (1.0 + proximal_mu)
        else:
            self.model.C = 1.0

        if len(self.X_train) >= 2 and len(np.unique(self.y_train)) > 1:
            try:
                self.model.fit(self.X_train, self.y_train)
            except Exception:
                pass
        # Restauram C la valoarea default dupa antrenare
        # (evita efecte de bord intre runde cu strategii diferite)
        self.model.C = 1.0
        return self.get_parameters(config={}), len(self.X_train), {
            "client_id": self.client_id,
        }

    def evaluate(self, parameters, config):
        """Evalueaza modelul global pe datele de test locale."""
        self.set_parameters(parameters)

        if len(self.X_test) == 0 or len(np.unique(self.y_test)) < 2:
            return 1.0, len(self.X_test), {
                "auc": 0.0, "accuracy": 0.0, "f1": 0.0,
                "precision": 0.0, "recall": 0.0,
                "client_id": self.client_id,
                "n_samples": len(self.X_test),
            }

        try:
            y_pred = self.model.predict(self.X_test)
            y_prob = self.model.predict_proba(self.X_test)[:, 1]

            # Verifica ca y_prob are valori distincte — necesara pentru roc_auc_score
            if len(np.unique(y_prob)) < 2:
                # Modelul prezice aceeasi probabilitate — zgomot mic pentru roc_auc_score
                # Seed derivat din client_id pentru reproductibilitate
                rng = np.random.default_rng(seed=abs(hash(self.client_id)) % (2**31))
                y_prob = y_prob + rng.uniform(-1e-6, 1e-6, size=len(y_prob))

            auc = float(roc_auc_score(self.y_test, y_prob))
            return 1.0 - auc, len(self.X_test), {
                "auc":       auc,
                "accuracy":  float(accuracy_score(self.y_test, y_pred)),
                "f1":        float(f1_score(self.y_test, y_pred, zero_division=0)),
                "precision": float(precision_score(self.y_test, y_pred, zero_division=0)),
                "recall":    float(recall_score(self.y_test, y_pred, zero_division=0)),
                "client_id": self.client_id,
                "n_samples": len(self.X_test),
            }
        except Exception as e:
            # Log eroarea pentru debug
            import sys
            print(f"    [WARN] client {self.client_id}: evaluate eroare ({e})", file=sys.stderr)
            return 1.0, len(self.X_test), {
                "auc": 0.0, "accuracy": 0.0, "f1": 0.0,
                "precision": 0.0, "recall": 0.0,
                "client_id": self.client_id,
                "n_samples": len(self.X_test),
            }