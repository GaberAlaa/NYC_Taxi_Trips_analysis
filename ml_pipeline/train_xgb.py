"""
Train two XGBoost models on the cleaned NYC taxi data (Jan-Nov 2025) and save the
models + metrics to SeaweedFS.

  regression      -> predict trip duration in minutes        (target: trip_minutes)
  classification  -> predict whether a trip is slow/congested (target: speed_mph < SLOW_MPH)

Run inside the spark-master container:
  /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 3g \
     --packages org.apache.hadoop:hadoop-aws:3.4.2 --conf spark.jars.ivy=/tmp/.ivy2 \
     /opt/project/ml_pipeline/train_xgb.py
"""
import json
import os
import tempfile
import time
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd
import xgboost as xgb
from pyspark.sql import SparkSession

# ----------------------------- settings you may change -----------------------------
CLEAN        = "s3a://traffic/clean/yellow_2025/"   # cleaned data written by clean_taxi.py
BUCKET       = "traffic"
SAMPLE_ROWS  = 2_000_000        # rows pulled into pandas (raise only if you have spare RAM)
SLOW_MPH     = 10               # classification label: slow trip if average speed < this
TRAIN_MONTHS = list(range(1, 10))   # Jan-Sep = train/validation, Oct-Nov = test
N_JOBS       = 2

# Only information that exists before the trip ends. NOT used (they leak the answer):
# fare_amount, total_amount, tip_amount, tolls_amount, speed_mph, dropoff time.
FEATURES = ["PULocationID", "DOLocationID", "pickup_hour", "pickup_dow", "is_weekend",
            "passenger_count", "trip_distance", "RatecodeID", "VendorID"]

# ----------------------------- 1. load a sample from the clean data -----------------------------
spark = (SparkSession.builder.appName("train_xgb")
    .config("spark.hadoop.fs.s3a.endpoint", "http://seaweedfs:8333")
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.access.key", "any")
    .config("spark.hadoop.fs.s3a.secret.key", "any")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

sdf = spark.read.parquet(CLEAN).select(*FEATURES, "pickup_month", "trip_minutes", "speed_mph")
total = sdf.count()
frac = min(1.0, SAMPLE_ROWS / total)
pdf = sdf.sample(False, frac, 42).toPandas()
spark.stop()
print(f"clean rows={total:,}  sampled={len(pdf):,}", flush=True)

pdf[FEATURES] = pdf[FEATURES].astype("float32")
pdf["is_slow"] = (pdf["speed_mph"] < SLOW_MPH).astype("int8")

# time-based split: Jan-Sep train (10% of it held out for early stopping), Oct-Nov test
is_train = pdf["pickup_month"].isin(TRAIN_MONTHS).to_numpy()
is_val   = is_train & (np.random.RandomState(42).rand(len(pdf)) < 0.10)
is_fit   = is_train & ~is_val
is_test  = ~is_train
X = pdf[FEATURES]
print(f"fit={is_fit.sum():,}  val={is_val.sum():,}  test={is_test.sum():,}", flush=True)

# ----------------------------- helpers -----------------------------
def reg_metrics(y, p, baseline_value):
    err = y - p
    return {
        "rmse": float(np.sqrt(np.mean(err ** 2))),
        "mae": float(np.mean(np.abs(err))),
        "r2": float(1 - np.sum(err ** 2) / np.sum((y - y.mean()) ** 2)),
        "baseline_rmse_predict_train_mean": float(np.sqrt(np.mean((y - baseline_value) ** 2))),
        "baseline_mae_predict_train_mean": float(np.mean(np.abs(y - baseline_value))),
    }


def clf_metrics(y, proba, threshold=0.5):
    pred = (proba >= threshold).astype(int)
    tp = int(((pred == 1) & (y == 1)).sum())
    fp = int(((pred == 1) & (y == 0)).sum())
    fn = int(((pred == 0) & (y == 1)).sum())
    tn = int(((pred == 0) & (y == 0)).sum())
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    n_pos, n_neg = tp + fn, tn + fp
    ranks = pd.Series(proba).rank().to_numpy()                      # ROC-AUC via rank statistic
    auc = float((ranks[y == 1].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)) if n_pos and n_neg else None
    return {
        "threshold": threshold,
        "accuracy": (tp + tn) / len(y),
        "precision": precision, "recall": recall, "f1": f1, "roc_auc": auc,
        "baseline_accuracy_majority_class": float(max(y.mean(), 1 - y.mean())),
        "confusion_matrix": {"tn": tn, "fp": fp, "fn": fn, "tp": tp},
    }


def importances(model):
    gain = model.get_booster().get_score(importance_type="gain")
    return {k: round(float(v), 2) for k, v in sorted(gain.items(), key=lambda kv: -kv[1])}


s3 = boto3.client("s3", endpoint_url="http://seaweedfs:8333",
                  aws_access_key_id="any", aws_secret_access_key="any", region_name="us-east-1")


def save(name, model, metrics):
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "model.json")
        model.save_model(path)
        s3.upload_file(path, BUCKET, f"models/{name}/model.json")
    body = json.dumps(metrics, indent=2,
                      default=lambda o: o.item() if hasattr(o, "item") else str(o)).encode()
    s3.put_object(Bucket=BUCKET, Key=f"models/{name}/metrics.json", Body=body,
                  ContentType="application/json")
    print(f"saved s3://{BUCKET}/models/{name}/ (model.json + metrics.json)", flush=True)


def common_info(task, target):
    return {
        "task": task, "target": target, "model_type": "xgboost",
        "xgboost_version": xgb.__version__,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "features": FEATURES,
        "data": {"clean_rows_total": total, "sampled_rows": len(pdf),
                 "fit_rows": int(is_fit.sum()), "validation_rows": int(is_val.sum()),
                 "test_rows": int(is_test.sum()),
                 "train_months": "Jan-Sep 2025", "test_months": "Oct-Nov 2025"},
    }


# ----------------------------- 2. regression: trip duration -----------------------------
t0 = time.time()
y = pdf["trip_minutes"].to_numpy()
reg = xgb.XGBRegressor(
    n_estimators=600, learning_rate=0.1, max_depth=8, min_child_weight=5,
    subsample=0.8, colsample_bytree=0.8, tree_method="hist", n_jobs=N_JOBS,
    eval_metric="rmse", early_stopping_rounds=30, random_state=42)
reg.fit(X[is_fit], y[is_fit], eval_set=[(X[is_val], y[is_val])], verbose=False)

base = float(y[is_fit].mean())
reg_info = common_info("regression", "trip_minutes")
reg_info.update({
    "params": {"n_estimators_max": 600, "best_iteration": int(reg.best_iteration),
               "learning_rate": 0.1, "max_depth": 8, "min_child_weight": 5,
               "subsample": 0.8, "colsample_bytree": 0.8},
    "validation": reg_metrics(y[is_val], reg.predict(X[is_val]), base),
    "test": reg_metrics(y[is_test], reg.predict(X[is_test]), base),
    "feature_importance_gain": importances(reg),
    "train_seconds": round(time.time() - t0, 1),
})
print("REGRESSION test:", json.dumps(reg_info["test"]), flush=True)
save("regression_trip_duration", reg, reg_info)

# ----------------------------- 3. classification: slow trip -----------------------------
t0 = time.time()
yc = pdf["is_slow"].to_numpy()
clf = xgb.XGBClassifier(
    n_estimators=600, learning_rate=0.1, max_depth=8, min_child_weight=5,
    subsample=0.8, colsample_bytree=0.8, tree_method="hist", n_jobs=N_JOBS,
    objective="binary:logistic", eval_metric="auc", early_stopping_rounds=30, random_state=42)
clf.fit(X[is_fit], yc[is_fit], eval_set=[(X[is_val], yc[is_val])], verbose=False)

clf_info = common_info("classification", f"is_slow (speed_mph < {SLOW_MPH})")
clf_info.update({
    "positive_rate": {"fit": float(yc[is_fit].mean()), "test": float(yc[is_test].mean())},
    "params": {"n_estimators_max": 600, "best_iteration": int(clf.best_iteration),
               "learning_rate": 0.1, "max_depth": 8, "min_child_weight": 5,
               "subsample": 0.8, "colsample_bytree": 0.8, "slow_mph_threshold": SLOW_MPH},
    "validation": clf_metrics(yc[is_val], clf.predict_proba(X[is_val])[:, 1]),
    "test": clf_metrics(yc[is_test], clf.predict_proba(X[is_test])[:, 1]),
    "feature_importance_gain": importances(clf),
    "train_seconds": round(time.time() - t0, 1),
})
print("CLASSIFICATION test:", json.dumps(clf_info["test"]), flush=True)
save("classification_slow_trip", clf, clf_info)

print("DONE")