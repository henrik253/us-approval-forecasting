import streamlit as st
import pandas as pd
import numpy as np
from snowflake.snowpark.context import get_active_session

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Approval & Disapproval Forecast", layout="wide")
st.title("🏛️ Approval & Disapproval Forecast")
st.divider()

# ── Data loading ───────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    session = get_active_session()

    # 1. Get Metadata (Cutoff Date)
    meta_df = session.sql("""
        SELECT CUTOFF_DATE 
        FROM APPROVAL_DB.GOLD.FORECAST_METADATA 
        LIMIT 1
    """).to_pandas()
    cutoff_str = meta_df.iloc[0]['CUTOFF_DATE']
    
    # 2. Get Historical Data (Actuals)
    obt_df = session.sql("""
        SELECT DATE_DAY, APPROVAL_7D_SMA AS ACTUAL_APPROVAL, DISAPPROVAL_7D_SMA AS ACTUAL_DISAPPROVAL
        FROM APPROVAL_DB.GOLD.APPROVAL_OBT
        ORDER BY DATE_DAY
    """).to_pandas()

    # 3. Get Predictions
    preds_df = session.sql("""
        SELECT DATE_DAY, PREDICTED_APPROVAL, PREDICTED_DISAPPROVAL
        FROM APPROVAL_DB.GOLD.FORECAST_RESULTS
        ORDER BY DATE_DAY
    """).to_pandas()

    # 4. Get Feature Importances
    importance_df = session.sql("""
        SELECT FEATURE, IMPORTANCE, MODEL
        FROM APPROVAL_DB.GOLD.FEATURE_IMPORTANCE
        ORDER BY IMPORTANCE DESC
    """).to_pandas()

    # Date formatting
    obt_df["DATE_DAY"] = pd.to_datetime(obt_df["DATE_DAY"])
    preds_df["DATE_DAY"] = pd.to_datetime(preds_df["DATE_DAY"])
    cutoff_date = pd.to_datetime(cutoff_str)

    # Merge Actuals and Predictions for the test set metrics
    forecast_df = pd.merge(preds_df, obt_df, on="DATE_DAY", how="left")
    
    # Calculate Residuals & Errors on the fly
    forecast_df["APP_ABS_ERROR"] = (forecast_df["ACTUAL_APPROVAL"] - forecast_df["PREDICTED_APPROVAL"]).abs()
    forecast_df["DIS_ABS_ERROR"] = (forecast_df["ACTUAL_DISAPPROVAL"] - forecast_df["PREDICTED_DISAPPROVAL"]).abs()

    # Create a unified split dataframe
    obt_df["SPLIT"] = np.where(obt_df["DATE_DAY"] < cutoff_date, "TRAIN", "TEST")

    return forecast_df, obt_df, importance_df, cutoff_date

forecast_df, full_history_df, importance_df, cutoff_date = load_data()

# ── Accuracy metrics ───────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

app_mae = forecast_df["APP_ABS_ERROR"].mean()
dis_mae = forecast_df["DIS_ABS_ERROR"].mean()

col1.metric("Approval MAE", f"{app_mae:.3f}")
col2.metric("Disapproval MAE", f"{dis_mae:.3f}")
col3.metric("Test Window Starts", cutoff_date.strftime('%Y-%m-%d'))
col4.metric("Test Rows", len(forecast_df))

st.divider()

# ── Main chart — full timeline: training + ground truth + predictions ──────────
st.subheader("Public Sentiment: History, Ground Truth & Forecast")
st.caption(f"Vertical dotted line = train/test cutoff: **{cutoff_date.date()}**")

# Set up the data series for the chart
train_df = full_history_df[full_history_df["SPLIT"] == "TRAIN"].set_index("DATE_DAY")
test_actuals = full_history_df[full_history_df["SPLIT"] == "TEST"].set_index("DATE_DAY")
preds_indexed = forecast_df.set_index("DATE_DAY")

# Build the charting dataframe
chart_data = pd.DataFrame({
    "Hist. Approval": train_df["ACTUAL_APPROVAL"],
    "Hist. Disapproval": train_df["ACTUAL_DISAPPROVAL"],
    "Actual Approval": test_actuals["ACTUAL_APPROVAL"],
    "Actual Disapproval": test_actuals["ACTUAL_DISAPPROVAL"],
    "Predicted Approval": preds_indexed["PREDICTED_APPROVAL"],
    "Predicted Disapproval": preds_indexed["PREDICTED_DISAPPROVAL"]
})

# Streamlit line chart
st.line_chart(chart_data, height=450)

st.caption(
    f"ℹ️ Training data ends at **{cutoff_date.date()}**. "
    "Actuals and Model Predictions cover the test window only."
)

st.divider()

# ── Bottom row ─────────────────────────────────────────────────────────────────
left, right = st.columns([1, 1], gap="large")

with left:
    st.subheader("Feature Importances")
    
    # Toggle between models
    model_choice = st.radio("Select Model:", ["Approval", "Disapproval"], horizontal=True)
    model_filter = model_choice.lower()
    
    # 1. Filter and explicitly SORT the data
    imp_filtered = importance_df[importance_df["MODEL"] == model_filter].copy()
    imp_filtered = imp_filtered.sort_values(by="IMPORTANCE", ascending=False)
    
    top_n = st.slider("Show top N features", min_value=5, max_value=len(imp_filtered), value=15, step=1)
    imp_top = imp_filtered.head(top_n)

    # 2. Use Altair for better label control
    import altair as alt

    chart = (
        alt.Chart(imp_top)
        .mark_bar()
        .encode(
            # sort=None tells Altair to respect our Pandas order
            x=alt.X("FEATURE:N", sort=None, title="Feature Name", 
                    axis=alt.Axis(labelAngle=-45, labelPadding=10)), 
            y=alt.Y("IMPORTANCE:Q", title="Importance Score"),
            tooltip=["FEATURE", "IMPORTANCE"]
        )
        .properties(height=420)
        .configure_axis(
            labelFontSize=11,
            titleFontSize=13
        )
    )

    st.altair_chart(chart, use_container_width=True)

with right:
    st.subheader("Train / Test Split Explorer")

    split_filter = st.radio("Show Window:", ["Both", "Train only", "Test only"], horizontal=True)

    filtered = {
        "Both": full_history_df,
        "Train only": full_history_df[full_history_df["SPLIT"] == "TRAIN"],
        "Test only": full_history_df[full_history_df["SPLIT"] == "TEST"],
    }[split_filter]

    split_chart = filtered.set_index("DATE_DAY")[["ACTUAL_APPROVAL", "ACTUAL_DISAPPROVAL"]]
    
    st.line_chart(split_chart, height=420)
    st.caption(f"Cutoff date: **{cutoff_date.date()}**")

st.divider()

# ── Raw forecast table ─────────────────────────────────────────────────────────
with st.expander("Raw Forecast Data"):
    st.dataframe(
        forecast_df[["DATE_DAY", "ACTUAL_APPROVAL", "PREDICTED_APPROVAL", "APP_ABS_ERROR", "ACTUAL_DISAPPROVAL", "PREDICTED_DISAPPROVAL", "DIS_ABS_ERROR"]].style.format({
            "ACTUAL_APPROVAL": "{:.3f}",
            "PREDICTED_APPROVAL": "{:.3f}",
            "APP_ABS_ERROR": "{:.3f}",
            "ACTUAL_DISAPPROVAL": "{:.3f}",
            "PREDICTED_DISAPPROVAL": "{:.3f}",
            "DIS_ABS_ERROR": "{:.3f}",
        }),
        use_container_width=True,
    )