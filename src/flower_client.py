"""
flower_client.py
----------------
Clientul Flower pentru Federated Learning educational.

Fiecare client = un curs (client_id, ex: "AAA_2013J").
Clientul antreneaza un model Logistic Regression LOCAL pe datele sale,
apoi trimite serverului DOAR coeficientii (nu datele brute).
Serverul aplica FedAvg (media ponderata a coeficientilor).
"""

import flwr as fl
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    roc_auc_score,
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
)
import warnings
warnings.filterwarnings("ignore")


class EduFederatedClient(fl.client.NumPyClient):
    """
    Client Flower pentru analiza datelor educationale.

    Cum functioneaza FL pas cu pas:
    1. Serverul trimite ponderile modelului global  -> set_parameters()
    2. Clientul antreneaza local pe datele sale     -> fit()
    3. Clientul trimite ponderile actualizate       -> get_parameters()
    4. Serverul face FedAvg -> nou model global
    5. Repeta pentru NUM_ROUNDS runde

    Datele brute NU parasesc niciodata clientul!
    """

    def __init__(self, client_id: str, df_local: pd.DataFrame):
        self.client_id = client_id

        # Curatare date locale
        df_clean = df_local.dropna(
            subset=["total_clicks", "days_active", "avg_score", "label"]
        )

        X = df_clean[["total_clicks", "days_active", "avg_score"]].values
        y = df_clean["label"].values.astype(int)

        self.n_samples = len(X)
        self.n_features = X.shape[1]

        # Split 80/20 local
        split = int(0.8 * len(X))
        X_train_raw, X_test_raw = X[:split], X[split:]
        self.y_train, self.y_test = y[:split], y[split:]

        # Scalare necesara pentru LR
        self.scaler = StandardScaler()
        self.X_train = self.scaler.fit_transform(X_train_raw)
        self.X_test = (
            self.scaler.transform(X_test_raw) if len(X_test_raw) > 0 else X_test_raw
        )

        # Model local cu warm_start=True (continua din punctul anterior in FL)
        self.model = LogisticRegression(
            max_iter=500,
            random_state=42,
            C=1.0,
            solver="lbfgs",
            warm_start=True,
        )

        # Antrenare initiala pentru a initializa coef_
        if len(np.unique(y)) > 1 and len(self.X_train) >= 2:
            self.model.fit(self.X_train, self.y_train)
        else:
            self.model.coef_      = np.zeros((1, self.n_features))
            self.model.intercept_ = np.zeros((1,))
            self.model.classes_   = np.array([0, 1])

    # ------------------------------------------------------------------
    # METODE FLOWER (apelate de server in fiecare runda)
    # ------------------------------------------------------------------

    def get_parameters(self, config):
        """Trimite coeficientii modelului local catre server."""
        return [self.model.coef_.copy(), self.model.intercept_.copy()]

    def set_parameters(self, parameters):
        """Aplica ponderile globale primite de la server."""
        self.model.coef_      = parameters[0].copy()
        self.model.intercept_ = parameters[1].copy()
        self.model.classes_   = np.array([0, 1])

    def fit(self, parameters, config):
        """
        Antrenare locala cu modelul global ca punct de start.
        Returneaza: parametri actualizati, nr sample-uri (pentru FedAvg ponderat).
        """
        self.set_parameters(parameters)

        if len(self.X_train) >= 2 and len(np.unique(self.y_train)) > 1:
            self.model.fit(self.X_train, self.y_train)

        return self.get_parameters(config={}), len(self.X_train), {
            "client_id": self.client_id,
        }

    def evaluate(self, parameters, config):
        """
        Evalueaza modelul global pe datele de test locale.
        Returneaza AUC, Accuracy, F1, Precision, Recall per client.
        """
        self.set_parameters(parameters)

        if len(self.X_test) == 0 or len(np.unique(self.y_test)) < 2:
            return 1.0, len(self.X_test), {
                "auc": 0.0, "accuracy": 0.0, "f1": 0.0,
                "precision": 0.0, "recall": 0.0,
                "client_id": self.client_id,
                "n_samples": len(self.X_test),
            }

        y_pred = self.model.predict(self.X_test)
        y_prob = self.model.predict_proba(self.X_test)[:, 1]

        return 1.0 - roc_auc_score(self.y_test, y_prob), len(self.X_test), {
            "auc":       float(roc_auc_score(self.y_test, y_prob)),
            "accuracy":  float(accuracy_score(self.y_test, y_pred)),
            "f1":        float(f1_score(self.y_test, y_pred, zero_division=0)),
            "precision": float(precision_score(self.y_test, y_pred, zero_division=0)),
            "recall":    float(recall_score(self.y_test, y_pred, zero_division=0)),
            "client_id": self.client_id,
            "n_samples": len(self.X_test),
        }
