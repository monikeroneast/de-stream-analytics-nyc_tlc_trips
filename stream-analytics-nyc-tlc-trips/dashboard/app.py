"""
🚕 NYC Yellow Taxi Analytics Dashboard

Interactive Streamlit dashboard that visualizes NYC Yellow Taxi trip data
from BigQuery. Includes multiple tiles covering temporal trends and
categorical distributions.

Usage:
    streamlit run dashboard/app.py
"""

import os
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery

load_dotenv()
# ──────────────────────────────────────────────
# Page Configuration
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="🚕 NYC Taxi Analytics",
    page_icon=" ",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
GCP_PROJECT = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = "taxi_analytics"


@st.cache_data(ttl=600)
def query_bigquery(query: str) -> pd.DataFrame:
    """Execute a BigQuery query and return results as a DataFrame."""
    client = bigquery.Client(project=GCP_PROJECT)
    return client.query(query).to_dataframe()


def load_hourly_distribution() -> pd.DataFrame:
    """Load trip count by hour of day from fct_trips."""
    query = f"""
    SELECT
        pickup_hour,
        COUNT(*) as trip_count,
        ROUND(AVG(total_amount), 2) as avg_fare,
        ROUND(AVG(tip_percentage), 2) as avg_tip_pct
    FROM `{GCP_PROJECT}.{BQ_DATASET}.fct_trips`
    GROUP BY pickup_hour
    ORDER BY pickup_hour
    """
    return query_bigquery(query)


def load_monthly_stats() -> pd.DataFrame:
    """Load monthly aggregated stats from dim_monthly_stats."""
    query = f"""
    SELECT *
    FROM `{GCP_PROJECT}.{BQ_DATASET}.dim_monthly_stats`
    ORDER BY pickup_year, pickup_month
    """
    return query_bigquery(query)


def load_payment_distribution() -> pd.DataFrame:
    """Load payment type distribution from fct_trips."""
    query = f"""
    SELECT
        payment_type,
        COUNT(*) as trip_count,
        ROUND(SUM(total_amount), 2) as total_revenue
    FROM `{GCP_PROJECT}.{BQ_DATASET}.fct_trips`
    GROUP BY payment_type
    ORDER BY trip_count DESC
    """
    return query_bigquery(query)


def load_day_of_week_distribution() -> pd.DataFrame:
    """Load trip count by day of week."""
    query = f"""
    SELECT
        pickup_day_of_week,
        pickup_day_name,
        COUNT(*) as trip_count,
        ROUND(AVG(total_amount), 2) as avg_fare
    FROM `{GCP_PROJECT}.{BQ_DATASET}.fct_trips`
    GROUP BY pickup_day_of_week, pickup_day_name
    ORDER BY pickup_day_of_week
    """
    return query_bigquery(query)


def load_summary_metrics() -> dict:
    """Load high-level summary metrics."""
    query = f"""
    SELECT
        COUNT(*) as total_trips,
        ROUND(SUM(total_amount), 2) as total_revenue,
        ROUND(AVG(total_amount), 2) as avg_fare,
        ROUND(AVG(trip_distance_miles), 2) as avg_distance,
        ROUND(AVG(trip_duration_minutes), 2) as avg_duration,
        ROUND(AVG(tip_percentage), 2) as avg_tip_pct
    FROM `{GCP_PROJECT}.{BQ_DATASET}.fct_trips`
    """
    df = query_bigquery(query)
    return df.iloc[0].to_dict()


# ──────────────────────────────────────────────
# Dashboard Layout
# ──────────────────────────────────────────────

# Title
st.title("🚕 NYC Yellow Taxi Trip Analytics")
st.markdown(
    "Interactive dashboard analyzing NYC Yellow Taxi trip patterns, fares, "
    "and trends. Data sourced from [NYC TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)."
)
st.divider()

# ── Sidebar ──
with st.sidebar:
    st.header("ℹ️ About")
    st.markdown(
        """
        **Data Pipeline:**
        - **Source**: NYC TLC Trip Records
        - **Stream**: Apache Kafka
        - **Data Lake**: Google Cloud Storage
        - **Warehouse**: BigQuery (partitioned & clustered)
        - **Transform**: dbt
        - **Dashboard**: Streamlit + Plotly

        Built as part of the
        [DE Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) course project.
        """
    )

    st.divider()
    st.header("📡 Pipeline Architecture")
    st.markdown(
        """
        | Component | Technology | Status |
        |---|---|---|
        | **Ingestion** | Apache Kafka | 🟢 Stream |
        | **Data Lake** | Google Cloud Storage | 🟢 Active |
        | **Data Warehouse** | BigQuery | 🟢 Active |
        | **Transformation** | dbt | 🟢 Built |
        | **Visualization** | Streamlit + Plotly | 🟢 Running |
        """
    )

# ── Load Data ──
try:
    metrics = load_summary_metrics()
    df_hourly = load_hourly_distribution()
    df_monthly = load_monthly_stats()
    df_payment = load_payment_distribution()
    df_dow = load_day_of_week_distribution()
except Exception as e:
    st.error(f"⚠️ Error connecting to BigQuery: {e}")
    st.info(
        "Make sure you have:\n"
        "1. Set the `GCP_PROJECT_ID` environment variable\n"
        "2. Authenticated with `gcloud auth application-default login`\n"
        "3. Run the full pipeline (Kafka → GCS → BigQuery → dbt)"
    )
    st.stop()

# ── KPI Metrics Row ──
st.subheader("📊 Key Metrics")
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Trips", f"{metrics['total_trips']:,.0f}")
col2.metric("Total Revenue", f"${metrics['total_revenue']:,.0f}")
col3.metric("Avg Fare", f"${metrics['avg_fare']:.2f}")
col4.metric("Avg Distance", f"{metrics['avg_distance']:.1f} mi")
col5.metric("Avg Tip %", f"{metrics['avg_tip_pct']:.1f}%")

st.divider()

# ──────────────────────────────────────────────
# TILE 1: Monthly Trip Trends (Temporal Distribution)
# ──────────────────────────────────────────────
st.subheader("📈 Tile 1: Monthly Trip Trends")

fig_monthly = go.Figure()
fig_monthly.add_trace(
    go.Scatter(
        x=df_monthly["year_month"],
        y=df_monthly["total_trips"],
        mode="lines+markers",
        name="Trip Count",
        line=dict(color="#FFD700", width=3),
        marker=dict(size=8),
    )
)
fig_monthly.update_layout(
    title="Number of Taxi Trips per Month",
    xaxis_title="Month",
    yaxis_title="Number of Trips",
    template="plotly_dark",
    height=400,
    hovermode="x unified",
)
st.plotly_chart(fig_monthly, width="stretch")

# Monthly revenue as secondary chart
fig_revenue = px.bar(
    df_monthly,
    x="year_month",
    y="total_revenue",
    title="Monthly Total Revenue ($)",
    labels={"year_month": "Month", "total_revenue": "Revenue ($)"},
    color="total_revenue",
    color_continuous_scale="YlOrRd",
)
fig_revenue.update_layout(template="plotly_dark", height=350)
st.plotly_chart(fig_revenue, width="stretch")

st.divider()

# ──────────────────────────────────────────────
# TILE 2: Trip Distribution by Hour (Categorical Distribution)
# ──────────────────────────────────────────────
st.subheader("🕐 Tile 2: Trip Distribution by Hour of Day")

fig_hourly = px.bar(
    df_hourly,
    x="pickup_hour",
    y="trip_count",
    title="Number of Trips by Hour of Day",
    labels={"pickup_hour": "Hour of Day (0-23)", "trip_count": "Number of Trips"},
    color="trip_count",
    color_continuous_scale="Viridis",
)
fig_hourly.update_layout(
    template="plotly_dark",
    height=400,
    xaxis=dict(tickmode="linear", dtick=1),
)
st.plotly_chart(fig_hourly, width="stretch")

st.divider()

# ──────────────────────────────────────────────
# Additional Insights
# ──────────────────────────────────────────────
st.subheader("🔍 Additional Insights")

col_left, col_right = st.columns(2)

# Payment Type Distribution (Pie Chart)
with col_left:
    fig_payment = px.pie(
        df_payment,
        values="trip_count",
        names="payment_type",
        title="Payment Type Distribution",
        color_discrete_sequence=px.colors.qualitative.Set2,
        hole=0.3,
    )
    fig_payment.update_layout(template="plotly_dark", height=400)
    st.plotly_chart(fig_payment, width="stretch")

# Day of Week Distribution
with col_right:
    fig_dow = px.bar(
        df_dow,
        x="pickup_day_name",
        y="trip_count",
        title="Trips by Day of Week",
        labels={"pickup_day_name": "Day", "trip_count": "Trips"},
        color="avg_fare",
        color_continuous_scale="Blues",
    )
    fig_dow.update_layout(template="plotly_dark", height=400)
    st.plotly_chart(fig_dow, width="stretch")

# ── Footer ──
st.divider()
st.caption(
    "Data source: NYC Taxi & Limousine Commission | "
    "Pipeline: Kafka → GCS → BigQuery → dbt → Streamlit | "
    "Built for DE Zoomcamp Course Project"
)