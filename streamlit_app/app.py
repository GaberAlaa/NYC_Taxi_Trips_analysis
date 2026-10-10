"""
NYC Taxi Traffic Dashboard (Jan-Nov 2025)
 
Reads everything from SeaweedFS (S3 API), nothing from your local disk:
  s3://traffic/clean/agg_hourly|agg_zone|sample/   <- written by clean_taxi.py
  s3://traffic/models/<model>/model.json|metrics.json <- written by train_xgb.py
"""
import io
import json
import os
import tempfile
 
import boto3
import pandas as pd
import streamlit as st
import xgboost as xgb
 
st.set_page_config(page_title="NYC Taxi Traffic Dashboard", page_icon="🚕", layout="wide")
 
ENDPOINT = os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333")
BUCKET = "traffic"
REG_DIR = "models/regression_trip_duration"
CLF_DIR = "models/classification_slow_trip"
DAYS = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
MONTHS = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
          7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov"}
RATE_CODES = {1: "Standard rate", 2: "JFK", 3: "Newark", 4: "Nassau / Westchester",
              5: "Negotiated fare", 6: "Group ride"}
DEFAULT_FEATURES = ["PULocationID", "DOLocationID", "pickup_hour", "pickup_dow", "is_weekend",
                    "passenger_count", "trip_distance", "RatecodeID", "VendorID"]
 
 
# ----------------------------- SeaweedFS access -----------------------------
@st.cache_resource
def s3():
    return boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id="any",
                        aws_secret_access_key="any", region_name="us-east-1")
 
 
@st.cache_data(ttl=600, show_spinner="Loading data from SeaweedFS...")
def load_table(name):
    """Read a parquet 'folder' (Spark output) from s3://traffic/clean/<name>/ into one DataFrame."""
    try:
        resp = s3().list_objects_v2(Bucket=BUCKET, Prefix=f"clean/{name}/")
        keys = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]
        if not keys:
            return None
        parts = [pd.read_parquet(io.BytesIO(s3().get_object(Bucket=BUCKET, Key=k)["Body"].read()))
                 for k in keys]
        return pd.concat(parts, ignore_index=True)
    except Exception:
        return None
 
 
@st.cache_data(ttl=600)
def load_metrics(folder):
    try:
        return json.loads(s3().get_object(Bucket=BUCKET, Key=f"{folder}/metrics.json")["Body"].read())
    except Exception:
        return None
 
 
@st.cache_resource(show_spinner="Loading model...")
def load_booster(folder):
    try:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "model.json")
            s3().download_file(BUCKET, f"{folder}/model.json", path)
            booster = xgb.Booster()
            booster.load_model(path)
        return booster
    except Exception:
        return None
 
 
# ----------------------------- Tab 1: traffic overview -----------------------------
def overview_tab():
    hourly, zones, sample = load_table("agg_hourly"), load_table("agg_zone"), load_table("sample")
    if hourly is None or zones is None:
        st.warning("Aggregated tables not found in s3://traffic/clean/. "
                   "Run the Spark cleaning job (clean_taxi.py) first.")
        return
 
    c1, c2, c3 = st.columns(3)
    sel_m = c1.multiselect("Month", list(MONTHS.values()), default=list(MONTHS.values()))
    sel_d = c2.multiselect("Day of week", list(DAYS.values()), default=list(DAYS.values()))
    h1, h2 = c3.slider("Pickup hour", 0, 23, (0, 23))
 
    months = [k for k, v in MONTHS.items() if v in sel_m]
    dows = [k for k, v in DAYS.items() if v in sel_d]
 
    def flt(d):
        return d[d.pickup_month.isin(months) & d.pickup_dow.isin(dows) & d.pickup_hour.between(h1, h2)]
 
    h, z = flt(hourly), flt(zones)
    trips = int(h.trips.sum())
    if trips == 0:
        st.info("No trips match these filters.")
        return
 
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Trips", f"{trips:,}")
    k2.metric("Avg distance (mi)", f"{h.sum_distance.sum() / trips:.2f}")
    k3.metric("Avg fare ($)", f"{h.sum_fare.sum() / trips:.2f}")
    k4.metric("Avg duration (min)", f"{h.sum_minutes.sum() / trips:.1f}")
 
    st.subheader("Daily trips")
    daily = h.groupby("pickup_date").trips.sum()
    daily.index = pd.to_datetime(daily.index)
    st.line_chart(daily)
 
    left, right = st.columns(2)
    with left:
        st.subheader("Trips by hour")
        st.bar_chart(h.groupby("pickup_hour").trips.sum())
        st.subheader("Top 15 pickup zones (zone ID)")
        top = z.groupby("PULocationID").trips.sum().nlargest(15)
        top.index = top.index.astype(str)
        st.bar_chart(top)
    with right:
        st.subheader("Trips by day of week")
        by_day = h.groupby("pickup_dow").trips.sum().rename(index=DAYS)
        st.bar_chart(by_day.reindex(list(DAYS.values())).dropna())
        st.subheader("Average speed by hour (mph)")
        g = h.groupby("pickup_hour")[["sum_distance", "sum_minutes"]].sum()
        st.line_chart(g.sum_distance / (g.sum_minutes / 60))
 
    if sample is not None and len(sample):
        st.subheader("Distance vs. fare (random sample of all trips)")
        st.scatter_chart(sample.sample(min(5000, len(sample)), random_state=1),
                         x="trip_distance", y="fare_amount")
 
 
# ----------------------------- Tab 2: model performance -----------------------------
def importance_chart(m):
    imp = pd.Series(m["feature_importance_gain"]).sort_values()
    st.caption("Feature importance (gain)")
    st.bar_chart(imp, horizontal=True)
 
 
def models_tab():
    reg, clf = load_metrics(REG_DIR), load_metrics(CLF_DIR)
    if reg is None and clf is None:
        st.warning("No model metrics found in s3://traffic/models/. Run train_xgb.py first.")
        return
 
    if reg:
        t = reg["test"]
        st.subheader("Regression: trip duration (minutes)")
        st.caption(f"XGBoost, trained on {reg['data']['train_months']}, tested on "
                   f"{reg['data']['test_months']} ({reg['data']['test_rows']:,} test trips)")
        a, b, c = st.columns(3)
        a.metric("RMSE (min)", f"{t['rmse']:.2f}",
                 delta=f"{t['rmse'] - t['baseline_rmse_predict_train_mean']:.2f} vs mean baseline",
                 delta_color="inverse")
        b.metric("MAE (min)", f"{t['mae']:.2f}",
                 delta=f"{t['mae'] - t['baseline_mae_predict_train_mean']:.2f} vs mean baseline",
                 delta_color="inverse")
        c.metric("R²", f"{t['r2']:.3f}")
        importance_chart(reg)
        st.divider()
 
    if clf:
        t = clf["test"]
        st.subheader(f"Classification: {clf['target']}")
        st.caption(f"XGBoost, trained on {clf['data']['train_months']}, tested on "
                   f"{clf['data']['test_months']} ({clf['data']['test_rows']:,} test trips). "
                   f"Share of slow trips in test data: {clf['positive_rate']['test']:.1%}")
        a, b, c, d, e = st.columns(5)
        a.metric("Accuracy", f"{t['accuracy']:.3f}",
                 delta=f"{t['accuracy'] - t['baseline_accuracy_majority_class']:+.3f} vs majority baseline")
        b.metric("Precision", f"{t['precision']:.3f}")
        c.metric("Recall", f"{t['recall']:.3f}")
        d.metric("F1", f"{t['f1']:.3f}")
        e.metric("ROC-AUC", "n/a" if t["roc_auc"] is None else f"{t['roc_auc']:.3f}")
        cm = t["confusion_matrix"]
        st.caption("Confusion matrix (test set)")
        st.dataframe(pd.DataFrame([[cm["tn"], cm["fp"]], [cm["fn"], cm["tp"]]],
                                  index=["Actual: not slow", "Actual: slow"],
                                  columns=["Predicted: not slow", "Predicted: slow"]))
        importance_chart(clf)
 
    with st.expander("Raw metrics.json"):
        st.json({"regression": reg, "classification": clf})
 
 
# ----------------------------- Tab 3: live predictions -----------------------------
def predict_tab():
    reg_m, clf_m = load_metrics(REG_DIR), load_metrics(CLF_DIR)
    reg_b, clf_b = load_booster(REG_DIR), load_booster(CLF_DIR)
    if reg_b is None or clf_b is None:
        st.warning("Models not found in s3://traffic/models/. Run train_xgb.py first.")
        return
    features = (reg_m or {}).get("features", DEFAULT_FEATURES)
 
    st.caption("Enter a trip and both XGBoost models (loaded from SeaweedFS) predict how long "
               "it will take and whether it will be slow. Zone IDs follow the NYC TLC taxi zone table.")
    left, right = st.columns([1, 1.4])
    with left:
        pu = st.number_input("Pickup zone ID", 1, 265, 161)
        do = st.number_input("Dropoff zone ID", 1, 265, 237)
        hour = st.slider("Pickup hour", 0, 23, 18)
        day = st.selectbox("Day of week", list(DAYS.values()), index=4)
        dist = st.number_input("Trip distance (miles)", 0.1, 100.0, 3.0, step=0.1)
        pax = st.slider("Passengers", 1, 6, 1)
        rate = st.selectbox("Rate code", list(RATE_CODES), format_func=RATE_CODES.get)
        vendor = st.selectbox("Vendor ID", [1, 2], index=1)
 
    dow = {v: k for k, v in DAYS.items()}[day]
    base = {"PULocationID": pu, "DOLocationID": do, "pickup_hour": hour, "pickup_dow": dow,
            "is_weekend": int(dow in (1, 7)), "passenger_count": pax, "trip_distance": dist,
            "RatecodeID": rate, "VendorID": vendor}
 
    def run(rows):
        df = pd.DataFrame(rows)[features].astype("float32")
        d = xgb.DMatrix(df)
        return reg_b.predict(d), clf_b.predict(d)
 
    minutes, p_slow = run([base])
    minutes, p_slow = float(minutes[0]), float(p_slow[0])
    speed = dist / (max(minutes, 0.1) / 60)
 
    with right:
        a, b, c = st.columns(3)
        a.metric("Predicted duration", f"{minutes:.1f} min")
        b.metric("Implied speed", f"{speed:.1f} mph")
        c.metric("Chance of a slow trip", f"{p_slow:.0%}")
        if p_slow >= 0.5:
            st.error("Likely slow / congested")
        else:
            st.success("Likely flowing normally")
 
        st.subheader("Same trip across the day")
        sweep = pd.DataFrame([{**base, "pickup_hour": h} for h in range(24)])
        m24, p24 = run(sweep.to_dict("records"))
        st.caption("Predicted duration (min)")
        st.line_chart(pd.Series(m24, index=range(24)))
        st.caption("Probability of a slow trip")
        st.line_chart(pd.Series(p24, index=range(24)))
 
 
# ----------------------------- page -----------------------------
st.title("🚕 NYC Taxi Traffic Dashboard")
st.caption("Jan–Nov 2025 · cleaned with Spark · models trained with XGBoost · everything read from SeaweedFS")
 
with st.sidebar:
    st.header("Data source")
    st.write(f"`{ENDPOINT}`  \nbucket: `{BUCKET}`")
    if st.button("Reload from SeaweedFS"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()
 
tab1, tab2, tab3 = st.tabs(["Traffic overview", "Model performance", "Try the models"])
with tab1:
    overview_tab()
with tab2:
    models_tab()
with tab3:
    predict_tab()
