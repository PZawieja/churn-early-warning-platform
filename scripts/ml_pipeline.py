#!/usr/bin/env python3
"""
ml_pipeline.py — Churn risk ML pipeline.

Replaces rule-based weighted scoring with a trained GradientBoosting model.
Outputs risk_score_ml (0-100) and risk_tier_ml per account, written to
ml_output.ml_predictions in DuckDB for downstream dbt consumption.

Commands
--------
  train    Train LogisticRegression + GradientBoosting on synthetic labelled data.
           Prints 5-fold cross-val AUC-ROC and feature importance. Saves best
           model to scripts/churn_model.pkl.

  score    Load churn_model.pkl, score current accounts from int_churn_features,
           write ml_output.ml_predictions to DuckDB.

  compare  Print rule-based vs ML scores side by side with tier agreement stats.
           Requires: dbt run (mart_churn_risk_scores) + score both completed.

Usage
-----
  python scripts/ml_pipeline.py train --db dev.duckdb
  python scripts/ml_pipeline.py score --db dev.duckdb
  python scripts/ml_pipeline.py compare --db dev.duckdb

Note on training data
---------------------
In production, training labels come from a churn_events table — accounts that
actually cancelled, with their feature snapshot at T-90d before churn. Here we
generate synthetic training data using the same signal relationships as the
rule-based model (with added noise), which demonstrates the integration pattern
without requiring historical labelled data.
"""

import argparse
import pickle
import sys
import warnings
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")

# ── Config ───────────────────────────────────────────────────────────────────

FEATURE_COLS = [
    # Usage — 45% weight in rule-based
    "wau_rolling_4w",
    "usage_trend_pct",
    "active_user_ratio",
    "distinct_features_used",
    "days_since_last_event",
    "is_zero_usage_week",
    "is_low_utilization",
    # Commercial — 30% weight
    "days_to_renewal",
    "seat_contracted_ratio",
    "had_payment_failure_30d",
    "had_downgrade_90d",
    "is_in_renewal_window",
    # Support — 25% weight
    "p1_p2_tickets_30d",
    "open_tickets_count",
    "avg_ticket_resolution_days",
]

# Sensible defaults for NULL values from left-joins
NULL_FILL = {
    "usage_trend_pct": 0.0,           # no trend = neutral
    "days_since_last_event": 30,      # assume 30 days dormant
    "avg_ticket_resolution_days": 0.0, # no tickets = no resolution time
    "active_user_ratio": 0.0,
    "wau_rolling_4w": 0.0,
}

MODEL_PATH = Path("scripts/churn_model.pkl")
N_SYNTHETIC = 500       # training examples — larger than seed set for meaningful CV
RISK_HIGH = 70
RISK_MEDIUM = 40

# ── Data helpers ─────────────────────────────────────────────────────────────

def generate_synthetic_data(n: int = N_SYNTHETIC, seed: int = 42) -> pd.DataFrame:
    """
    Generate labelled synthetic training data.

    Churn probability is a weighted function of usage + commercial + support signals,
    consistent with the rule-based weights (45/30/25), with added noise to simulate
    real-world non-linearity and feature interactions.

    In production: replace with historical account snapshots joined to churn_events
    (accounts cancelled within 90 days of the feature snapshot date).
    """
    rng = np.random.default_rng(seed)

    df = pd.DataFrame({
        # Usage signals
        "wau_rolling_4w":           rng.uniform(0, 120, n),
        "usage_trend_pct":          rng.uniform(-1.0, 0.5, n),
        "active_user_ratio":        rng.uniform(0.0, 1.2, n).clip(0, 1),
        "distinct_features_used":   rng.integers(0, 15, n).astype(float),
        "days_since_last_event":    rng.integers(0, 60, n).astype(float),
        "is_zero_usage_week":       rng.choice([0.0, 1.0], n, p=[0.85, 0.15]),
        "is_low_utilization":       rng.choice([0.0, 1.0], n, p=[0.70, 0.30]),
        # Commercial signals
        "days_to_renewal":          rng.integers(-10, 400, n).astype(float),
        "seat_contracted_ratio":    rng.uniform(0.05, 1.5, n).clip(0, 1.5),
        "had_payment_failure_30d":  rng.choice([0.0, 1.0], n, p=[0.85, 0.15]),
        "had_downgrade_90d":        rng.choice([0.0, 1.0], n, p=[0.90, 0.10]),
        "is_in_renewal_window":     rng.choice([0.0, 1.0], n, p=[0.75, 0.25]),
        # Support signals
        "p1_p2_tickets_30d":        rng.integers(0, 5, n).astype(float),
        "open_tickets_count":       rng.integers(0, 8, n).astype(float),
        "avg_ticket_resolution_days": rng.uniform(0, 21, n),
    })

    # Churn signal mirrors rule-based component structure
    usage = (
        0.30 * (df["days_since_last_event"] / 60).clip(0, 1)
        + 0.25 * (1 - df["active_user_ratio"])
        + 0.20 * (-df["usage_trend_pct"].clip(-1, 0))
        + 0.15 * df["is_zero_usage_week"]
        + 0.10 * (df["distinct_features_used"] < 2).astype(float)
    )
    commercial = (
        0.35 * (df["days_to_renewal"].clip(0, 90) / 90).clip(0, 1)
        + 0.30 * (1 - df["seat_contracted_ratio"].clip(0, 1))
        + 0.25 * df["had_payment_failure_30d"]
        + 0.10 * df["had_downgrade_90d"]
    )
    # Invert renewal urgency: closer = higher risk
    commercial = commercial * (1 - df["days_to_renewal"].clip(0, 365) / 365 * 0.3)

    support = (
        0.50 * (df["p1_p2_tickets_30d"] / 4).clip(0, 1)
        + 0.30 * (df["open_tickets_count"] / 6).clip(0, 1)
        + 0.20 * (df["avg_ticket_resolution_days"] / 14).clip(0, 1)
    )

    p_churn = 0.45 * usage + 0.30 * commercial + 0.25 * support
    # Noise: simulates non-linear interactions and measurement error
    noise = rng.normal(0, 0.10, n)
    p_churn = (p_churn + noise).clip(0, 1)
    df["churned"] = (p_churn > 0.45).astype(int)   # ~25-30% churn rate

    return df


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    """Fill NULLs, cast booleans, return clean feature matrix."""
    X = df[FEATURE_COLS].copy()
    X = X.fillna(NULL_FILL)
    X = X.fillna(0.0)  # catch-all for any remaining NULLs
    # Cast boolean columns to float
    bool_cols = [
        "had_payment_failure_30d", "had_downgrade_90d",
        "is_in_renewal_window", "is_zero_usage_week", "is_low_utilization",
    ]
    for col in bool_cols:
        if col in X.columns:
            X[col] = X[col].astype(float)
    return X.astype(float)


def load_current_features(db_path: str) -> pd.DataFrame:
    """Read int_churn_features from DuckDB. Tries multiple schema prefixes."""
    con = duckdb.connect(db_path, read_only=True)
    cols = ", ".join(["account_id", "feature_week"] + FEATURE_COLS)

    candidates = [
        f"SELECT {cols} FROM intermediate.int_churn_features",
        f"SELECT {cols} FROM main.intermediate.int_churn_features",
        f"SELECT {cols} FROM int_churn_features",
    ]
    for query in candidates:
        try:
            df = con.execute(query).df()
            con.close()
            return df
        except Exception:
            continue

    con.close()
    sys.exit(
        "❌  Could not find int_churn_features in DuckDB.\n"
        "    Run `dbt run` first, then retry."
    )


def _print_bar(value: float, width: int = 20) -> str:
    filled = int(value * width)
    return "█" * filled + "░" * (width - filled)


# ── Model helpers ─────────────────────────────────────────────────────────────

def build_pipelines() -> dict:
    return {
        "Logistic Regression": Pipeline([
            ("scaler", StandardScaler()),
            ("model", LogisticRegression(
                max_iter=1000, class_weight="balanced", C=0.5, random_state=42,
            )),
        ]),
        "Gradient Boosting": Pipeline([
            ("model", GradientBoostingClassifier(
                n_estimators=150, max_depth=3, learning_rate=0.08,
                subsample=0.8, min_samples_leaf=8, random_state=42,
            )),
        ]),
    }


def print_feature_importance(pipeline: Pipeline, model_name: str) -> None:
    if "Logistic" in model_name:
        coefs = pipeline.named_steps["model"].coef_[0]
        pairs = sorted(zip(FEATURE_COLS, coefs), key=lambda x: abs(x[1]), reverse=True)[:10]
        for feat, coef in pairs:
            bar = _print_bar(min(abs(coef) / 2, 1.0))
            sign = "▲" if coef > 0 else "▼"
            print(f"   {feat:<35} {sign} {abs(coef):.3f}  {bar}")
    else:
        imps = pipeline.named_steps["model"].feature_importances_
        pairs = sorted(zip(FEATURE_COLS, imps), key=lambda x: x[1], reverse=True)[:10]
        for feat, imp in pairs:
            bar = _print_bar(imp * 4)
            print(f"   {feat:<35}   {imp:.4f}  {bar}")


# ── Commands ─────────────────────────────────────────────────────────────────

def cmd_train(args) -> None:
    SEP = "─" * 62
    print(f"\n{SEP}")
    print("  CHURN MODEL TRAINING")
    print(SEP)

    # ── Synthetic training data ──
    print(f"\n📊  Generating {N_SYNTHETIC} synthetic labelled examples...")
    df_train = generate_synthetic_data(n=N_SYNTHETIC)
    y = df_train["churned"].values
    X = prepare_features(df_train)
    churn_rate = y.mean()
    print(f"    Churn rate: {churn_rate:.1%}  "
          f"({y.sum()} churned / {len(y) - y.sum()} retained / {len(y)} total)")

    # ── Cross-validation ──
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    pipelines = build_pipelines()

    print("\n📈  5-fold stratified cross-validation:")
    print(f"    {'Model':<28}  {'AUC-ROC':>8}  {'±':>6}  {'Bar'}")
    print(f"    {'─'*28}  {'─'*8}  {'─'*6}  {'─'*20}")

    best_auc = -1.0
    best_name = ""
    best_pipe = None

    for name, pipe in pipelines.items():
        scores = cross_val_score(pipe, X, y, cv=cv, scoring="roc_auc", n_jobs=-1)
        mu, se = scores.mean(), scores.std()
        bar = _print_bar(mu)
        marker = "  ◀ best" if mu > best_auc else ""
        print(f"    {name:<28}  {mu:>8.4f}  {se:>6.4f}  {bar}{marker}")
        if mu > best_auc:
            best_auc, best_name, best_pipe = mu, name, pipe

    # ── Train best on full data ──
    print(f"\n🏆  Training {best_name} on full dataset...")
    best_pipe.fit(X, y)

    # ── Feature importance ──
    print(f"\n📊  Feature importance (top 10) — {best_name}:")
    print(f"    {'Feature':<35}  {'Value':>6}  {'Bar'}")
    print(f"    {'─'*35}  {'─'*6}  {'─'*20}")
    print_feature_importance(best_pipe, best_name)

    # ── Full-data metrics ──
    y_pred = best_pipe.predict(X)
    y_prob = best_pipe.predict_proba(X)[:, 1]
    train_auc = roc_auc_score(y, y_prob)
    print(f"\n🎯  Training-set AUC-ROC: {train_auc:.4f}  "
          f"(expected ~5-10 pts above CV due to in-sample scoring)")
    print(classification_report(y, y_pred, target_names=["Retained", "Churned"], digits=3))

    # ── Save ──
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    artifact = {
        "model": best_pipe,
        "model_name": best_name,
        "features": FEATURE_COLS,
        "cv_auc": round(best_auc, 4),
        "n_training": len(y),
        "churn_rate": round(churn_rate, 4),
    }
    with open(MODEL_PATH, "wb") as fh:
        pickle.dump(artifact, fh)

    print(f"✅  Model saved → {MODEL_PATH}")
    print("    Next step: python scripts/ml_pipeline.py score --db dev.duckdb\n")
    print(SEP + "\n")


def cmd_score(args) -> None:
    SEP = "─" * 62
    print(f"\n{SEP}")
    print("  CHURN MODEL SCORING")
    print(SEP)

    if not MODEL_PATH.exists():
        sys.exit(f"❌  Model not found at {MODEL_PATH}. Run `train` first.")

    with open(MODEL_PATH, "rb") as fh:
        artifact = pickle.load(fh)

    model = artifact["model"]
    model_name = artifact["model_name"]
    cv_auc = artifact.get("cv_auc", "?")
    print(f"\n📦  Loaded: {model_name}  (CV AUC-ROC = {cv_auc})")

    # ── Load current features ──
    print(f"\n📋  Reading int_churn_features from {args.db}...")
    df = load_current_features(args.db)
    if df.empty:
        sys.exit("❌  No accounts found in int_churn_features.")

    feature_week = str(df["feature_week"].iloc[0])
    print(f"    Accounts: {len(df)}  |  Feature week: {feature_week}")

    # ── Score ──
    X = prepare_features(df)
    churn_prob = model.predict_proba(X)[:, 1]
    risk_score_ml = (churn_prob * 100).round(2)

    df = df.assign(
        risk_score_ml=risk_score_ml,
        risk_tier_ml=pd.cut(
            risk_score_ml,
            bins=[-1, RISK_MEDIUM, RISK_HIGH, 101],
            labels=["LOW", "MEDIUM", "HIGH"],
        ).astype(str),
        churn_probability=churn_prob.round(4),
        model_name=model_name,
        cv_auc=cv_auc,
        scored_at=pd.Timestamp.utcnow(),
    )

    # ── Write to DuckDB ──
    con = duckdb.connect(args.db)
    con.execute("CREATE SCHEMA IF NOT EXISTS ml_output")
    con.execute("DROP TABLE IF EXISTS ml_output.ml_predictions")
    con.execute("CREATE TABLE ml_output.ml_predictions AS SELECT * FROM df")
    count = con.execute("SELECT count(*) FROM ml_output.ml_predictions").fetchone()[0]
    con.close()

    print(f"\n✅  ml_output.ml_predictions written ({count} rows)")

    # ── Tier summary ──
    tier_counts = df["risk_tier_ml"].value_counts()
    print("\n    Risk distribution:")
    for tier in ["HIGH", "MEDIUM", "LOW"]:
        n = tier_counts.get(tier, 0)
        bar = "█" * n
        print(f"    {'HIGH' if tier=='HIGH' else ('MED' if tier=='MEDIUM' else 'LOW'):<5}  "
              f"{bar}  {n}")

    print("\n    Next step: dbt run --select mart_churn_risk_scores_ml")
    print(f"    Then:      python scripts/ml_pipeline.py compare --db {args.db}\n")
    print(SEP + "\n")


def cmd_compare(args) -> None:
    SEP = "─" * 96
    print(f"\n{SEP}")
    print("  RULE-BASED vs ML COMPARISON")
    print(SEP)

    try:
        con = duckdb.connect(args.db, read_only=True)
    except Exception as e:
        sys.exit(f"❌  Cannot open {args.db}: {e}")

    # Try to fetch from mart + ml_predictions
    try:
        df = con.execute("""
            SELECT
                r.account_id
                , r.plan_name
                , r.mrr
                , ROUND(r.risk_score, 1)      AS score_rule
                , r.risk_tier                 AS tier_rule
                , ROUND(m.risk_score_ml, 1)   AS score_ml
                , m.risk_tier_ml              AS tier_ml
                , ROUND(m.churn_probability, 3) AS churn_prob
                , ROUND(m.risk_score_ml - r.risk_score, 1) AS delta
                , r.usage_score
                , r.commercial_score
                , r.support_score
            FROM marts.mart_churn_risk_scores r
            JOIN ml_output.ml_predictions m ON m.account_id = r.account_id
            ORDER BY r.risk_score DESC
        """).df()
    except Exception:
        try:
            df = con.execute("""
                SELECT
                    r.account_id
                    , r.plan_name
                    , r.mrr
                    , ROUND(r.risk_score, 1)        AS score_rule
                    , r.risk_tier                   AS tier_rule
                    , ROUND(m.risk_score_ml, 1)     AS score_ml
                    , m.risk_tier_ml                AS tier_ml
                    , ROUND(m.churn_probability, 3) AS churn_prob
                    , ROUND(m.risk_score_ml - r.risk_score, 1) AS delta
                    , r.usage_score
                    , r.commercial_score
                    , r.support_score
                FROM mart_churn_risk_scores r
                JOIN ml_output.ml_predictions m ON m.account_id = r.account_id
                ORDER BY r.risk_score DESC
            """).df()
        except Exception as e:
            con.close()
            sys.exit(
                f"❌  {e}\n\n"
                "    Ensure both are complete before comparing:\n"
                "      dbt run (builds mart_churn_risk_scores)\n"
                "      python scripts/ml_pipeline.py score --db dev.duckdb\n"
                "      dbt run --select mart_churn_risk_scores_ml\n"
            )

    con.close()

    # ── Table ──
    print(f"\n  {'ACCOUNT':<20} {'PLAN':<12} {'MRR':>6}  "
          f"{'RULE':>5} {'ML':>5} {'PROB':>5}  "
          f"{'TIER(R)':<8} {'TIER(ML)':<8} {'Δ':>5}  AGR")
    print(f"  {'─'*20} {'─'*12} {'─'*6}  {'─'*5} {'─'*5} {'─'*5}  "
          f"{'─'*8} {'─'*8} {'─'*5}  {'─'*3}")

    tier_changes = []
    for _, row in df.iterrows():
        agree = "✓" if row["tier_rule"] == row["tier_ml"] else "≠"
        delta = f"{row['delta']:+.1f}" if pd.notna(row["delta"]) else "  —"
        if row["tier_rule"] != row["tier_ml"]:
            tier_changes.append(row)
        print(
            f"  {row['account_id']:<20} {row['plan_name']:<12} ${row['mrr']:>5,.0f}  "
            f"{row['score_rule']:>5.1f} {row['score_ml']:>5.1f} {row['churn_prob']:>5.3f}  "
            f"{row['tier_rule']:<8} {row['tier_ml']:<8} {delta:>5}  {agree}"
        )

    # ── Summary ──
    agree_pct = (df["tier_rule"] == df["tier_ml"]).mean()
    n = len(df)
    print(f"\n{SEP}")
    print(f"\n  Tier agreement:     {agree_pct:.0%}  ({int(agree_pct * n)}/{n} accounts)")
    print(f"  Avg Δ (ML − rule):  {df['delta'].mean():+.1f} points")
    print(f"  Score correlation:  {df[['score_rule','score_ml']].corr().iloc[0,1]:.3f}")

    mrr_rule_high  = df.loc[df["tier_rule"] == "HIGH", "mrr"].sum()
    mrr_ml_high    = df.loc[df["tier_ml"]   == "HIGH", "mrr"].sum()
    n_rule_high    = (df["tier_rule"] == "HIGH").sum()
    n_ml_high      = (df["tier_ml"]   == "HIGH").sum()

    print("\n  HIGH tier:")
    print(f"    Rule-based:  {n_rule_high} accounts  ${mrr_rule_high:,.0f} MRR at risk")
    print(f"    ML model:    {n_ml_high} accounts  ${mrr_ml_high:,.0f} MRR at risk")

    if tier_changes:
        print(f"\n  Tier disagreements ({len(tier_changes)}):")
        for row in tier_changes:
            direction = (
                "ML upgraded  ▲" if row["tier_ml"] == "HIGH" and row["tier_rule"] != "HIGH"
                else "ML downgraded ▼" if row["tier_rule"] == "HIGH" and row["tier_ml"] != "HIGH"
                else "Tiers differ  ≠"
            )
            print(f"    {row['account_id']:<20}  {row['tier_rule']:<8} → {row['tier_ml']:<8}  "
                  f"{direction}  (rule={row['score_rule']}, ml={row['score_ml']})")
    else:
        print("\n  ✓ Perfect tier agreement — both approaches classify identically")

    print(f"\n{SEP}\n")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Churn risk ML pipeline — train, score, compare",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    for cmd in ("train", "score", "compare"):
        p = sub.add_parser(cmd)
        if cmd != "train":
            p.add_argument("--db", default="dev.duckdb",
                           help="Path to DuckDB file (default: dev.duckdb)")

    args = parser.parse_args()

    dispatch = {
        "train":   cmd_train,
        "score":   cmd_score,
        "compare": cmd_compare,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
