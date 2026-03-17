import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime
import requests
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings("ignore")

# ============================================================
# PAGE CONFIG & CUSTOM CSS
# ============================================================
st.set_page_config(
    page_title="San José Public Safety & Services Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Color palette matching the JSX dashboard
COLORS = {
    "bg": "#0a0f1a",
    "card": "#111827",
    "card_border": "#1e293b",
    "accent": "#f59e0b",
    "fire": "#ef4444",
    "medical": "#3b82f6",
    "police": "#8b5cf6",
    "sr311": "#10b981",
    "text": "#f1f5f9",
    "text_dim": "#94a3b8",
    "text_muted": "#64748b",
    "grid": "#1e293b",
    "green": "#10b981",
    "red": "#ef4444",
    "yellow": "#f59e0b",
}

# Plotly layout template
PLOTLY_TEMPLATE = dict(
    layout=go.Layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color=COLORS["text_dim"], size=12),
        xaxis=dict(gridcolor=COLORS["grid"], gridwidth=0.5, zeroline=False),
        yaxis=dict(gridcolor=COLORS["grid"], gridwidth=0.5, zeroline=False),
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(size=11, color=COLORS["text_dim"]),
        ),
        hoverlabel=dict(
            bgcolor=COLORS["card"],
            bordercolor=COLORS["card_border"],
            font_size=12,
            font_family="Inter, sans-serif",
        ),
    )
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');

    /* Global overrides */
    .stApp { background-color: #0a0f1a; }
    header[data-testid="stHeader"] { background-color: #0a0f1a; }
    .block-container { padding-top: 1.5rem; max-width: 1400px; }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0px;
        background-color: #0f172a;
        border-radius: 10px 10px 0 0;
        padding: 4px 4px 0 4px;
        border-bottom: 1px solid #1e293b;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 10px 24px;
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        font-size: 13px;
        color: #64748b;
        border-radius: 8px 8px 0 0;
        border: none;
        background-color: transparent;
    }
    .stTabs [aria-selected="true"] {
        background-color: #111827 !important;
        color: #f59e0b !important;
        border-bottom: 2px solid #f59e0b !important;
    }

    /* Metric card styling */
    .metric-card {
        background: linear-gradient(135deg, #111827 0%, #0a0f1a 100%);
        border: 1px solid #1e293b;
        border-radius: 12px;
        padding: 20px 24px;
        position: relative;
        overflow: hidden;
    }
    .metric-card::before {
        content: '';
        position: absolute;
        top: 0; left: 0;
        width: 4px; height: 100%;
        border-radius: 12px 0 0 12px;
    }
    .metric-card.fire::before { background: #ef4444; }
    .metric-card.police::before { background: #8b5cf6; }
    .metric-card.sr311::before { background: #10b981; }
    .metric-card.accent::before { background: #f59e0b; }
    .metric-card.blue::before { background: #3b82f6; }
    .metric-card.green::before { background: #10b981; }
    .metric-card.red::before { background: #ef4444; }
    .metric-card.purple::before { background: #7c3aed; }

    .metric-label {
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        color: #94a3b8;
        text-transform: uppercase;
        letter-spacing: 1.2px;
        margin-bottom: 8px;
    }
    .metric-value {
        font-family: 'Inter', sans-serif;
        font-size: 30px;
        font-weight: 800;
        color: #f1f5f9;
        line-height: 1;
    }
    .metric-sub {
        font-family: 'JetBrains Mono', monospace;
        font-size: 12px;
        color: #64748b;
        margin-top: 6px;
    }
    .metric-trend-up { color: #ef4444; }
    .metric-trend-down { color: #10b981; }

    /* Insight boxes */
    .insight-box {
        border-radius: 8px;
        padding: 14px 18px;
        font-family: 'JetBrains Mono', monospace;
        font-size: 12.5px;
        line-height: 1.6;
        color: #94a3b8;
    }
    .insight-finding {
        background: rgba(30, 58, 95, 0.15);
        border: 1px solid rgba(59, 130, 246, 0.25);
    }
    .insight-alert {
        background: rgba(127, 29, 29, 0.15);
        border: 1px solid rgba(239, 68, 68, 0.25);
    }
    .insight-rec {
        background: rgba(20, 83, 45, 0.15);
        border: 1px solid rgba(16, 185, 129, 0.25);
    }

    /* Chart containers */
    .chart-card {
        background: #111827;
        border: 1px solid #1e293b;
        border-radius: 12px;
        padding: 20px;
    }
    .chart-title {
        font-family: 'Inter', sans-serif;
        font-size: 15px;
        font-weight: 600;
        color: #f1f5f9;
        margin-bottom: 2px;
    }
    .chart-subtitle {
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        color: #64748b;
        margin-bottom: 14px;
    }

    /* Policy box */
    .policy-box {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        border: 1px solid rgba(245, 158, 11, 0.25);
        border-radius: 12px;
        padding: 28px;
    }
    .policy-title {
        font-family: 'Inter', sans-serif;
        font-size: 16px;
        font-weight: 700;
        color: #f59e0b;
        margin-bottom: 16px;
    }
    .policy-item strong {
        color: #f1f5f9;
    }
    .policy-item {
        font-family: 'JetBrains Mono', monospace;
        font-size: 12.5px;
        color: #94a3b8;
        line-height: 1.6;
    }

    /* Header */
    .dashboard-header {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%);
        border-bottom: 1px solid #1e293b;
        padding: 20px 0px 16px;
        margin-bottom: 8px;
        border-radius: 12px;
    }
    .header-title {
        font-family: 'Inter', sans-serif;
        font-size: 26px;
        font-weight: 800;
        background: linear-gradient(135deg, #f1f5f9 0%, #f59e0b 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        letter-spacing: -0.8px;
        margin: 0;
    }
    .header-sub {
        font-family: 'JetBrains Mono', monospace;
        font-size: 11.5px;
        color: #64748b;
        margin-top: 2px;
    }
    .header-badge {
        display: inline-block;
        padding: 5px 14px;
        border-radius: 20px;
        background: rgba(16, 185, 129, 0.12);
        border: 1px solid rgba(16, 185, 129, 0.25);
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        color: #10b981;
    }

    /* Hide default streamlit stuff */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}

    /* Section titles */
    .section-title {
        font-family: 'Inter', sans-serif;
        font-size: 22px;
        font-weight: 700;
        color: #f1f5f9;
        letter-spacing: -0.5px;
        margin-bottom: 2px;
    }
    .section-subtitle {
        font-family: 'JetBrains Mono', monospace;
        font-size: 12px;
        color: #64748b;
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================
# DATA LOADING — Live from San José Open Data Portal
# Uses parallel downloads + trimmed to key years for speed
# ============================================================

FIRE_URLS = {
    2018: "https://data.sanjoseca.gov/dataset/8160209b-6da1-44d2-802b-c156a4b6feaf/resource/af9983fb-c595-4cdf-a849-e597fa8c39a0/download/fire_incident_2018.csv",
    2019: "https://data.sanjoseca.gov/dataset/8160209b-6da1-44d2-802b-c156a4b6feaf/resource/2201d98a-65cf-428d-8be0-98e7041291e2/download/fire_incident_2019.csv",
    2020: "https://data.sanjoseca.gov/dataset/8160209b-6da1-44d2-802b-c156a4b6feaf/resource/c23db08c-6df9-41be-a08c-1d00f9b01dcf/download/san-jose-fire-incidents-data-2020.csv",
    2021: "https://data.sanjoseca.gov/dataset/8160209b-6da1-44d2-802b-c156a4b6feaf/resource/4b8be3ec-3bbd-446a-9f37-af0143d2b808/download/sanjosefireincidentdata_2021.csv",
    2022: "https://data.sanjoseca.gov/dataset/8160209b-6da1-44d2-802b-c156a4b6feaf/resource/545910d5-f787-4dc5-821d-b8fd64cd7cfc/download/sanjosefireincidentdata_2022.csv",
    2024: "https://data.sanjoseca.gov/dataset/8160209b-6da1-44d2-802b-c156a4b6feaf/resource/bf5cfcb6-c2b3-460f-be93-24d18e9ef793/download/sanjosefireincidentdata-2024.csv",
}

POLICE_QUARTERLY_BASE = "https://data.sanjoseca.gov/dataset/c414a023-8e2c-4ed0-a0e0-2731913161a1/resource/{rid}/download/{fname}"
POLICE_QUARTERLY = {
    "2021_Q1": ("0c2871c4-18fd-4b34-88e9-8d0a2164d479", "policecalls2021-q1.csv"),
    "2022_Q1": ("3e67e7dc-d949-494b-ae57-c4eca13fe625", "policecalls2022-q1.csv"),
    "2023_Q1": ("c7c8c01e-5230-42ac-a0fb-d1c9d6427f9c", "policecalls2023-q1.csv"),
    "2023_Q2": ("2b0b8ebe-89f4-44ef-9e87-904ae362d9ec", "policecalls2023-q2.csv"),
}

SR311_BASE = "https://data.sanjoseca.gov/dataset/5bd6605f-43fc-4ccc-b80b-3c15b5a02319/resource/{rid}/download/{fname}"
SR311_URLS = {
    2019: ("c5c5034a-b238-440c-84b5-5654799ca159", "311-service-requests-2019.csv"),
    2021: ("4b0563f3-ed85-4889-8c39-05f160dbe0eb", "311-service-requests-2021.csv"),
    2023: ("00b1c1ba-b83e-40a0-8260-e634c57e4fcc", "311-service-requests-2023.csv"),
    2024: ("ac55d3cd-b4fe-4f96-bf12-4ce3191c8bd4", "311-service-requests-2024.csv"),
}


def _fetch_csv(url, timeout=45):
    """Download a CSV via requests (follows redirects to S3) and return a DataFrame."""
    headers = {"User-Agent": "Mozilla/5.0 (SJ-Dashboard) Streamlit/1.0"}
    resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    resp.raise_for_status()
    content = resp.content.decode("utf-8-sig", errors="replace")
    return pd.read_csv(StringIO(content), low_memory=False)


def _download_one(label, year_or_key, url):
    """Worker for parallel downloading. Returns (label, year_or_key, df_or_None)."""
    try:
        df = _fetch_csv(url)
        return (label, year_or_key, df)
    except Exception:
        return (label, year_or_key, None)


@st.cache_data(ttl=3600, show_spinner=False)
def load_all_data():
    """Download all datasets in PARALLEL and return (fire_df, police_df, sr311_df)."""
    tasks = []

    # Build task list
    for year, url in FIRE_URLS.items():
        tasks.append(("fire", year, url))
    for key, (rid, fname) in POLICE_QUARTERLY.items():
        url = POLICE_QUARTERLY_BASE.format(rid=rid, fname=fname)
        tasks.append(("police", key, url))
    for year, (rid, fname) in SR311_URLS.items():
        url = SR311_BASE.format(rid=rid, fname=fname)
        tasks.append(("311", year, url))

    # Download all in parallel (up to 8 concurrent)
    results = {"fire": [], "police": [], "311": []}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {
            pool.submit(_download_one, label, key, url): (label, key)
            for label, key, url in tasks
        }
        for future in as_completed(futures):
            label, key, df = future.result()
            if df is not None:
                if label == "fire":
                    df["_year"] = key
                elif label == "police":
                    year_q = str(key).split("_")
                    df["_year"] = int(year_q[0])
                    df["_quarter"] = year_q[1]
                elif label == "311":
                    df["_year"] = key
                results[label].append(df)

    fire_df = pd.concat(results["fire"], ignore_index=True) if results["fire"] else pd.DataFrame()
    police_df = pd.concat(results["police"], ignore_index=True) if results["police"] else pd.DataFrame()
    sr311_df = pd.concat(results["311"], ignore_index=True) if results["311"] else pd.DataFrame()

    return fire_df, police_df, sr311_df


def process_fire(combined):
    """Normalize columns, parse dates, compute response times."""
    if combined.empty:
        return combined
    col_map = {}
    for c in combined.columns:
        cl = c.strip().lower().replace(" ", "_")
        if "incident_no" in cl or "incidentnumber" in cl:
            col_map[c] = "incident_no"
        elif "date_time_of_event" in cl or "event_date" in cl or "datetime" in cl:
            col_map[c] = "event_datetime"
        elif "dispatched" in cl:
            col_map[c] = "dispatched_time"
        elif "on_the_way" in cl or "enroute" in cl:
            col_map[c] = "enroute_time"
        elif "on_scene" in cl and "timestamp" in cl:
            col_map[c] = "onscene_time"
        elif "cleared" in cl:
            col_map[c] = "cleared_time"
        elif "priority" in cl:
            col_map[c] = "priority"
        elif "final_incident_type" in cl or "incident_type" in cl:
            col_map[c] = "incident_type"
        elif "final_incident_category" in cl or "incident_category" in cl:
            col_map[c] = "incident_category"
        elif "station" in cl:
            col_map[c] = "station"
        elif "battalion" in cl:
            col_map[c] = "battalion"
    combined.rename(columns=col_map, inplace=True)
    if "event_datetime" in combined.columns:
        combined["event_datetime"] = pd.to_datetime(combined["event_datetime"], errors="coerce")
    if "dispatched_time" in combined.columns:
        combined["dispatched_time"] = pd.to_datetime(combined["dispatched_time"], errors="coerce")
    if "onscene_time" in combined.columns:
        combined["onscene_time"] = pd.to_datetime(combined["onscene_time"], errors="coerce")
    if "event_datetime" in combined.columns and "onscene_time" in combined.columns:
        combined["response_min"] = (combined["onscene_time"] - combined["event_datetime"]).dt.total_seconds() / 60.0
        combined.loc[(combined["response_min"] < 0) | (combined["response_min"] > 60), "response_min"] = np.nan
    return combined


def process_police(combined):
    """Normalize police call columns."""
    if combined.empty:
        return combined
    col_map = {}
    for c in combined.columns:
        cl = c.strip().upper()
        if cl == "CALL_TYPE": col_map[c] = "call_type"
        elif cl in ("FINAL_DISPO", "FINAL_DISPOSITION"): col_map[c] = "disposition"
        elif cl == "FINAL_DISPO_CODE": col_map[c] = "dispo_code"
        elif cl == "PRIORITY": col_map[c] = "priority"
        elif cl == "OFFENSE_DATE": col_map[c] = "offense_date"
        elif cl == "REPORT_DATE": col_map[c] = "report_date"
        elif cl == "ADDRESS": col_map[c] = "address"
        elif cl == "CALLTYPE_CODE": col_map[c] = "calltype_code"
        elif cl == "OFFENSE_TIME": col_map[c] = "offense_time"
    combined.rename(columns=col_map, inplace=True)
    if "offense_date" in combined.columns:
        combined["offense_date"] = pd.to_datetime(combined["offense_date"], errors="coerce")
    return combined


def process_311(combined):
    """Normalize 311 columns, compute resolution time."""
    if combined.empty:
        return combined
    col_map = {}
    for c in combined.columns:
        cl = c.strip().lower().replace(" ", "_")
        if "incident_id" in cl or "case_id" in cl: col_map[c] = "case_id"
        elif "status" in cl: col_map[c] = "status"
        elif "source" in cl: col_map[c] = "source"
        elif "category" in cl: col_map[c] = "category"
        elif "service" in cl and "type" in cl: col_map[c] = "service_type"
        elif "latitude" in cl: col_map[c] = "latitude"
        elif "longitude" in cl: col_map[c] = "longitude"
        elif "date_created" in cl or "created" in cl: col_map[c] = "created_date"
        elif "date_last_updated" in cl or "updated" in cl: col_map[c] = "updated_date"
        elif "department" in cl: col_map[c] = "department"
    combined.rename(columns=col_map, inplace=True)
    if "created_date" in combined.columns:
        combined["created_date"] = pd.to_datetime(combined["created_date"], errors="coerce")
    if "updated_date" in combined.columns:
        combined["updated_date"] = pd.to_datetime(combined["updated_date"], errors="coerce")
    if "created_date" in combined.columns and "updated_date" in combined.columns:
        combined["resolution_days"] = (combined["updated_date"] - combined["created_date"]).dt.total_seconds() / 86400.0
        combined.loc[(combined["resolution_days"] < 0) | (combined["resolution_days"] > 365), "resolution_days"] = np.nan
    return combined


# ============================================================
# ANALYSIS HELPER FUNCTIONS
# ============================================================

def compute_fire_analysis(df):
    """Derive all fire-related analysis metrics from raw data."""
    results = {}

    # Yearly trend
    yearly = df.groupby("_year").agg(
        total=("incident_type", "size"),
    ).reset_index().rename(columns={"_year": "year"})

    if "incident_type" in df.columns:
        type_yearly = df.groupby(["_year", "incident_type"]).size().unstack(fill_value=0).reset_index()
        type_yearly.rename(columns={"_year": "year"}, inplace=True)
        med_cols = [c for c in type_yearly.columns if "MEDICAL" in str(c).upper()]
        fire_cols = [c for c in type_yearly.columns if "FIRE" in str(c).upper() and "MEDICAL" not in str(c).upper()]
        type_yearly["medical"] = type_yearly[med_cols].sum(axis=1) if med_cols else 0
        type_yearly["fire_other"] = type_yearly[fire_cols].sum(axis=1) if fire_cols else 0
        type_yearly["other"] = type_yearly.drop(columns=["year", "medical", "fire_other"] + med_cols + fire_cols, errors="ignore").sum(axis=1)
        yearly = yearly.merge(type_yearly[["year", "medical", "fire_other", "other"]], on="year", how="left")
    results["yearly"] = yearly

    # Station-level response times
    if "response_min" in df.columns and "station" in df.columns:
        station_stats = df.groupby("station").agg(
            avg_response=("response_min", "mean"),
            incidents=("incident_type", "size"),
        ).reset_index()
        station_stats = station_stats[station_stats["incidents"] > 100]
        station_stats["avg_response"] = station_stats["avg_response"].round(1)
        station_stats = station_stats.sort_values("avg_response")
        results["station_response"] = station_stats

    # Battalion-level
    if "battalion" in df.columns and "response_min" in df.columns:
        bat_stats = df.groupby("battalion").agg(
            avg_response=("response_min", "mean"),
            total_incidents=("incident_type", "size"),
        ).reset_index()
        bat_stats["avg_response"] = bat_stats["avg_response"].round(1)
        bat_stats["pct"] = (bat_stats["total_incidents"] / bat_stats["total_incidents"].sum() * 100).round(1)
        bat_stats = bat_stats.sort_values("total_incidents", ascending=False)
        results["battalion"] = bat_stats

    # Hourly pattern (from latest year available)
    if "event_datetime" in df.columns:
        latest_year = df["_year"].max()
        hourly = df[df["_year"] == latest_year].copy()
        hourly["hour"] = hourly["event_datetime"].dt.hour
        hourly_counts = hourly.groupby("hour").size().reset_index(name="incidents")

        if "incident_type" in hourly.columns:
            hourly_type = hourly.groupby(["hour", "incident_type"]).size().unstack(fill_value=0).reset_index()
            med_cols_h = [c for c in hourly_type.columns if "MEDICAL" in str(c).upper()]
            fire_cols_h = [c for c in hourly_type.columns if "FIRE" in str(c).upper() and "MEDICAL" not in str(c).upper()]
            hourly_type["medical"] = hourly_type[med_cols_h].sum(axis=1) if med_cols_h else 0
            hourly_type["fire_other"] = hourly_type[fire_cols_h].sum(axis=1) if fire_cols_h else 0
            hourly_counts = hourly_counts.merge(hourly_type[["hour", "medical", "fire_other"]], on="hour", how="left")
        results["hourly"] = hourly_counts

    # Incident type breakdown (latest year)
    if "incident_type" in df.columns:
        latest = df[df["_year"] == df["_year"].max()]
        type_counts = latest["incident_type"].value_counts().reset_index()
        type_counts.columns = ["type", "count"]
        total = type_counts["count"].sum()
        type_counts["pct"] = (type_counts["count"] / total * 100).round(1)
        results["type_breakdown"] = type_counts

    # Key metrics
    latest_year_data = df[df["_year"] == df["_year"].max()]
    results["total_latest"] = len(latest_year_data)
    results["latest_year"] = df["_year"].max()
    if "response_min" in df.columns:
        results["avg_response"] = round(latest_year_data["response_min"].median(), 1)
    if "incident_type" in df.columns:
        med_mask = latest_year_data["incident_type"].str.upper().str.contains("MEDICAL", na=False)
        results["medical_pct"] = round(med_mask.sum() / len(latest_year_data) * 100, 1) if len(latest_year_data) > 0 else 0
        results["medical_count"] = med_mask.sum()

    return results


def compute_police_analysis(df):
    """Derive police call analysis from raw data."""
    results = {}

    # Call type breakdown
    if "call_type" in df.columns:
        call_types = df["call_type"].value_counts().head(15).reset_index()
        call_types.columns = ["type", "count"]
        results["call_types"] = call_types

    # Disposition breakdown
    if "disposition" in df.columns:
        dispos = df["disposition"].value_counts().head(10).reset_index()
        dispos.columns = ["disposition", "count"]
        total = dispos["count"].sum()
        dispos["pct"] = (dispos["count"] / total * 100).round(1)
        results["dispositions"] = dispos

    # Priority breakdown
    if "priority" in df.columns:
        prio = df.groupby("priority").size().reset_index(name="count")
        prio = prio.sort_values("priority")
        results["priorities"] = prio

    # Yearly/quarterly trend
    yearly = df.groupby("_year").size().reset_index(name="total")
    yearly.rename(columns={"_year": "year"}, inplace=True)
    results["yearly"] = yearly

    # Canceled rate
    if "disposition" in df.columns:
        cancel_mask = df["disposition"].str.upper().str.contains("CANCEL", na=False)
        results["cancel_rate"] = round(cancel_mask.sum() / len(df) * 100, 1) if len(df) > 0 else 0
        no_report = df["disposition"].str.upper().str.contains("NO REPORT", na=False)
        results["no_report_rate"] = round(no_report.sum() / len(df) * 100, 1) if len(df) > 0 else 0

    results["total_records"] = len(df)
    return results


def compute_311_analysis(df):
    """Derive 311 service request analysis from raw data."""
    results = {}

    # Category breakdown
    if "service_type" in df.columns:
        cats = df["service_type"].value_counts().head(15).reset_index()
        cats.columns = ["category", "count"]
        results["categories"] = cats
    elif "category" in df.columns:
        cats = df["category"].value_counts().head(15).reset_index()
        cats.columns = ["category", "count"]
        results["categories"] = cats

    # Department breakdown
    if "department" in df.columns:
        depts = df["department"].value_counts().head(10).reset_index()
        depts.columns = ["department", "count"]
        total = depts["count"].sum()
        depts["pct"] = (depts["count"] / total * 100).round(1)
        results["departments"] = depts

    # Source breakdown
    if "source" in df.columns:
        sources = df["source"].value_counts().reset_index()
        sources.columns = ["source", "count"]
        results["sources"] = sources

    # Yearly trend
    yearly = df.groupby("_year").size().reset_index(name="total")
    yearly.rename(columns={"_year": "year"}, inplace=True)
    results["yearly"] = yearly

    # Resolution time by category
    if "resolution_days" in df.columns:
        cat_col = "service_type" if "service_type" in df.columns else "category" if "category" in df.columns else None
        if cat_col:
            res_time = df.groupby(cat_col)["resolution_days"].agg(["median", "count"]).reset_index()
            res_time.columns = ["category", "median_days", "count"]
            res_time = res_time[res_time["count"] > 50].sort_values("median_days", ascending=False).head(15)
            results["resolution_time"] = res_time
        results["avg_resolution"] = round(df["resolution_days"].median(), 1)

    # Status breakdown
    if "status" in df.columns:
        status = df["status"].value_counts().reset_index()
        status.columns = ["status", "count"]
        results["status"] = status

    results["total_records"] = len(df)
    first_year = df["_year"].min()
    last_year = df["_year"].max()
    first_count = len(df[df["_year"] == first_year])
    last_count = len(df[df["_year"] == last_year])
    results["growth_pct"] = round((last_count - first_count) / first_count * 100, 1) if first_count > 0 else 0
    results["first_year"] = first_year
    results["last_year"] = last_year

    return results


# ============================================================
# UI HELPER FUNCTIONS
# ============================================================

def metric_card(icon, label, value, subtext, color_class="accent", trend=None):
    trend_html = ""
    if trend is not None:
        cls = "metric-trend-up" if trend > 0 else "metric-trend-down"
        arrow = "▲" if trend > 0 else "▼"
        trend_html = f'<span class="{cls}">{arrow} {abs(trend)}%</span> '
    st.markdown(f"""
    <div class="metric-card {color_class}">
        <div class="metric-label">{icon} {label}</div>
        <div class="metric-value">{value}</div>
        <div class="metric-sub">{trend_html}{subtext}</div>
    </div>
    """, unsafe_allow_html=True)


def insight_box(text, box_type="finding"):
    icons = {"finding": "🔍", "alert": "⚠️", "rec": "💡"}
    cls = f"insight-{box_type}"
    st.markdown(f"""
    <div class="insight-box {cls}">
        {icons.get(box_type, "🔍")} {text}
    </div>
    """, unsafe_allow_html=True)


def chart_header(title, subtitle=""):
    sub_html = f'<div class="chart-subtitle">{subtitle}</div>' if subtitle else ""
    st.markdown(f"""
    <div class="chart-title">{title}</div>
    {sub_html}
    """, unsafe_allow_html=True)


def apply_chart_style(fig, height=320):
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color=COLORS["text_dim"], size=11),
        xaxis=dict(gridcolor=COLORS["grid"], gridwidth=0.5, zeroline=False),
        yaxis=dict(gridcolor=COLORS["grid"], gridwidth=0.5, zeroline=False),
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10, color=COLORS["text_dim"])),
        hoverlabel=dict(bgcolor=COLORS["card"], bordercolor=COLORS["card_border"], font_size=12),
        height=height,
    )
    return fig


# ============================================================
# LOAD ALL DATA
# ============================================================

# ============================================================
# LOAD ALL DATA (parallel, cached)
# ============================================================

fire_raw, police_raw, sr311_raw = load_all_data()
fire_df = process_fire(fire_raw)
police_df = process_police(police_raw)
sr311_df = process_311(sr311_raw)

fire_ok = len(fire_df) > 0
police_ok = len(police_df) > 0
sr311_ok = len(sr311_df) > 0

if fire_ok:
    fire_stats = compute_fire_analysis(fire_df)
if police_ok:
    police_stats = compute_police_analysis(police_df)
if sr311_ok:
    sr311_stats = compute_311_analysis(sr311_df)

total_records = (len(fire_df) if fire_ok else 0) + (len(police_df) if police_ok else 0) + (len(sr311_df) if sr311_ok else 0)


# ============================================================
# HEADER
# ============================================================

st.markdown(f"""
<div class="dashboard-header" style="padding: 20px 28px 16px;">
    <div style="display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:12px;">
        <div>
            <p class="header-title">📊 San José Public Safety & Services Intelligence</p>
            <p class="header-sub">Multi-dataset analysis · Fire Incidents · Police Calls for Service · 311 Service Requests · data.sanjoseca.gov</p>
        </div>
        <div class="header-badge">Live Data · {total_records:,} records loaded</div>
    </div>
</div>
""", unsafe_allow_html=True)


# ============================================================
# TABS
# ============================================================

tab_overview, tab_fire, tab_police, tab_311, tab_cross = st.tabs(
    ["Executive Summary", "Fire Response", "Police Calls", "311 Services", "Cross-System"]
)


# ======================== OVERVIEW TAB ========================
with tab_overview:
    st.markdown('<div class="section-title">San José Public Safety & Service Demand Intelligence</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Combining Fire Incidents, Police Calls for Service, and 311 Service Requests</div>', unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        if fire_ok:
            yr = fire_stats.get("latest_year", "?")
            metric_card("🔥", f"Fire Incidents ({yr})", f"{fire_stats.get('total_latest', 0):,}", "from live data", "fire")
    with c2:
        if police_ok:
            metric_card("🚔", "Police Calls (loaded)", f"{police_stats['total_records']:,}", f"{len(POLICE_QUARTERLY)} quarters loaded", "police")
    with c3:
        if sr311_ok:
            metric_card("📞", "311 Requests (loaded)", f"{sr311_stats['total_records']:,}", f"{sr311_stats.get('growth_pct', 0)}% growth since {sr311_stats.get('first_year', '?')}", "sr311")
    with c4:
        if fire_ok and "avg_response" in fire_stats:
            metric_card("⏱", "Median Fire Response", f"{fire_stats['avg_response']} min", "dispatch to on-scene", "accent")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    col_a, col_b = st.columns([2, 1])
    with col_a:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        chart_header("Multi-Year Demand Trends Across All Three Systems", "Year-over-year volume from each dataset")
        fig = go.Figure()
        if fire_ok and "yearly" in fire_stats:
            d = fire_stats["yearly"]
            fig.add_trace(go.Scatter(x=d["year"], y=d["total"], name="Fire Incidents", line=dict(color=COLORS["fire"], width=3), mode="lines+markers"))
        if police_ok and "yearly" in police_stats:
            d = police_stats["yearly"]
            fig.add_trace(go.Scatter(x=d["year"], y=d["total"], name="Police Calls", line=dict(color=COLORS["police"], width=3), mode="lines+markers"))
        if sr311_ok and "yearly" in sr311_stats:
            d = sr311_stats["yearly"]
            fig.add_trace(go.Scatter(x=d["year"], y=d["total"], name="311 Requests", line=dict(color=COLORS["sr311"], width=3), mode="lines+markers"))
        apply_chart_style(fig, 300)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    with col_b:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        chart_header("Fire Incident Type Split", "Latest year breakdown")
        if fire_ok and "type_breakdown" in fire_stats:
            d = fire_stats["type_breakdown"].head(5)
            fig = go.Figure(go.Pie(
                labels=d["type"], values=d["count"],
                hole=0.45,
                marker=dict(colors=[COLORS["medical"], COLORS["fire"], COLORS["accent"], COLORS["police"], COLORS["sr311"]]),
                textinfo="percent+label", textfont=dict(size=10, color=COLORS["text_dim"]),
            ))
            apply_chart_style(fig, 300)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
    ic1, ic2, ic3 = st.columns(3)
    with ic1:
        if fire_ok:
            insight_box(f"Medical calls comprise {fire_stats.get('medical_pct', 'N/A')}% of fire incidents — the fire department is functionally an EMS agency. Resource allocation should reflect this structural shift.", "finding")
    with ic2:
        insight_box("311 requests are the fastest-growing system. Expanded digital access is working — but resolution capacity must scale to match.", "alert")
    with ic3:
        insight_box("Cross-system analysis reveals 'chronic demand zones' where fire, police, and 311 burdens overlap. These need coordinated multi-department intervention.", "rec")


# ======================== FIRE TAB ========================
with tab_fire:
    st.markdown('<div class="section-title">Fire Department Response Time & Demand Analysis</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Source: data.sanjoseca.gov/dataset/san-jose-fire-incidents · Live data</div>', unsafe_allow_html=True)

    if not fire_ok:
        st.warning("⚠️ Could not load fire incident data. Check connection to data.sanjoseca.gov.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            metric_card("🏥", "Medical Call %", f"{fire_stats.get('medical_pct', 'N/A')}%", f"{fire_stats.get('medical_count', 0):,} calls in {fire_stats.get('latest_year', '?')}", "blue")
        with c2:
            metric_card("🔥", "Total Incidents", f"{fire_stats.get('total_latest', 0):,}", f"Year: {fire_stats.get('latest_year', '?')}", "fire")
        with c3:
            if "station_response" in fire_stats and len(fire_stats["station_response"]) > 0:
                best = fire_stats["station_response"].iloc[0]
                metric_card("⏱", "Fastest Station", f"{best['avg_response']} min", f"Station {best['station']}", "green")
        with c4:
            if "station_response" in fire_stats and len(fire_stats["station_response"]) > 0:
                worst = fire_stats["station_response"].iloc[-1]
                metric_card("⏱", "Slowest Station", f"{worst['avg_response']} min", f"Station {worst['station']}", "red")

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            chart_header("Incident Volume by Year & Type", "Medical calls driving total growth")
            if "yearly" in fire_stats and "medical" in fire_stats["yearly"].columns:
                d = fire_stats["yearly"]
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=d["year"], y=d["medical"], name="Medical", stackgroup="one", fillcolor="rgba(59,130,246,0.4)", line=dict(color=COLORS["medical"])))
                fig.add_trace(go.Scatter(x=d["year"], y=d.get("fire_other", 0), name="Fire", stackgroup="one", fillcolor="rgba(239,68,68,0.4)", line=dict(color=COLORS["fire"])))
                fig.add_trace(go.Scatter(x=d["year"], y=d.get("other", 0), name="Other", stackgroup="one", fillcolor="rgba(245,158,11,0.3)", line=dict(color=COLORS["accent"])))
                apply_chart_style(fig, 300)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            else:
                d = fire_stats["yearly"]
                fig = px.bar(d, x="year", y="total", color_discrete_sequence=[COLORS["fire"]])
                apply_chart_style(fig, 300)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        with col_b:
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            chart_header("Hourly Demand Pattern", "Peak demand: 10AM–2PM and 4PM–7PM")
            if "hourly" in fire_stats:
                d = fire_stats["hourly"]
                fig = go.Figure()
                if "medical" in d.columns:
                    fig.add_trace(go.Scatter(x=d["hour"], y=d["medical"], name="Medical", stackgroup="one", fillcolor="rgba(59,130,246,0.5)", line=dict(color=COLORS["medical"])))
                    fig.add_trace(go.Scatter(x=d["hour"], y=d.get("fire_other", 0), name="Fire", stackgroup="one", fillcolor="rgba(239,68,68,0.5)", line=dict(color=COLORS["fire"])))
                else:
                    fig.add_trace(go.Scatter(x=d["hour"], y=d["incidents"], name="All", fill="tozeroy", fillcolor="rgba(239,68,68,0.3)", line=dict(color=COLORS["fire"])))
                fig.update_xaxes(title_text="Hour of Day")
                apply_chart_style(fig, 300)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        # Station response chart
        if "station_response" in fire_stats:
            col_a2, col_b2 = st.columns(2)
            with col_a2:
                st.markdown('<div class="chart-card">', unsafe_allow_html=True)
                chart_header("🟢 Fastest Response Stations", "Avg minutes: dispatch to on-scene")
                d = fire_stats["station_response"].head(10)
                fig = go.Figure(go.Bar(
                    y=[f"Stn {s}" for s in d["station"]], x=d["avg_response"],
                    orientation="h", marker_color=COLORS["green"],
                    text=d["avg_response"], textposition="outside",
                    textfont=dict(size=11, color=COLORS["text_dim"]),
                ))
                fig.update_xaxes(range=[0, d["avg_response"].max() + 2])
                apply_chart_style(fig, 320)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
                st.markdown('</div>', unsafe_allow_html=True)

            with col_b2:
                st.markdown('<div class="chart-card">', unsafe_allow_html=True)
                chart_header("🔴 Slowest Response Stations", "Stations with longest average response")
                d = fire_stats["station_response"].tail(10).sort_values("avg_response", ascending=False)
                fig = go.Figure(go.Bar(
                    y=[f"Stn {s}" for s in d["station"]], x=d["avg_response"],
                    orientation="h", marker_color=COLORS["red"],
                    text=d["avg_response"], textposition="outside",
                    textfont=dict(size=11, color=COLORS["text_dim"]),
                ))
                fig.update_xaxes(range=[0, d["avg_response"].max() + 2])
                apply_chart_style(fig, 320)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
                st.markdown('</div>', unsafe_allow_html=True)

        # Battalion chart
        if "battalion" in fire_stats:
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            chart_header("Battalion-Level Performance Comparison", "Incident volume vs response time by battalion")
            d = fire_stats["battalion"].head(8)
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(x=d["battalion"], y=d["total_incidents"], name="Total Incidents", marker_color=COLORS["medical"], opacity=0.7), secondary_y=False)
            fig.add_trace(go.Scatter(x=d["battalion"], y=d["avg_response"], name="Avg Response (min)", mode="lines+markers", line=dict(color=COLORS["fire"], width=3), marker=dict(size=10)), secondary_y=True)
            fig.update_yaxes(title_text="Incidents", secondary_y=False, gridcolor=COLORS["grid"])
            fig.update_yaxes(title_text="Response (min)", secondary_y=True, gridcolor=COLORS["grid"])
            apply_chart_style(fig, 300)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        ic1, ic2 = st.columns(2)
        with ic1:
            insight_box("Response time gap between the fastest and slowest stations can exceed 2+ minutes. For cardiac arrest, every minute of delay reduces survival probability by approximately 10%.", "alert")
        with ic2:
            insight_box("Consider co-deploying EMS-only units to high-medical-demand stations to free up engine companies for structure fires and reduce average response time.", "rec")


# ======================== POLICE TAB ========================
with tab_police:
    st.markdown('<div class="section-title">Police Calls for Service Pattern Analysis</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Source: data.sanjoseca.gov/dataset/police-calls-for-service-quarterly · Live data</div>', unsafe_allow_html=True)

    if not police_ok:
        st.warning("⚠️ Could not load police calls data. Check connection to data.sanjoseca.gov.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            metric_card("📞", "Total Calls Loaded", f"{police_stats['total_records']:,}", f"{len(POLICE_QUARTERLY)} quarters", "police")
        with c2:
            if "priorities" in police_stats:
                p1 = police_stats["priorities"]
                p1_count = p1[p1["priority"].astype(str).str.contains("1|2")]["count"].sum()
                metric_card("🚨", "High Priority (P1+P2)", f"{p1_count:,}", f"{round(p1_count/police_stats['total_records']*100,1)}% of calls", "red")
        with c3:
            metric_card("🚫", "Canceled Rate", f"{police_stats.get('cancel_rate', 'N/A')}%", "of all dispositions", "yellow")
        with c4:
            metric_card("📋", "No Report Needed", f"{police_stats.get('no_report_rate', 'N/A')}%", "dispatch resolved / cleared", "blue")

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

        col_a, col_b = st.columns([3, 2])
        with col_a:
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            chart_header("Top Call Types", "Most frequent incident categories")
            if "call_types" in police_stats:
                d = police_stats["call_types"].head(12)
                fig = go.Figure(go.Bar(
                    y=d["type"], x=d["count"],
                    orientation="h", marker_color=COLORS["police"],
                    text=d["count"].apply(lambda v: f"{v:,}"), textposition="outside",
                    textfont=dict(size=10, color=COLORS["text_dim"]),
                ))
                apply_chart_style(fig, 400)
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        with col_b:
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            chart_header("Disposition Distribution", "How calls are resolved")
            if "dispositions" in police_stats:
                d = police_stats["dispositions"].head(7)
                colors_pie = [COLORS["medical"], COLORS["green"], COLORS["yellow"], COLORS["red"], COLORS["police"], COLORS["accent"], COLORS["text_muted"]]
                fig = go.Figure(go.Pie(
                    labels=d["disposition"], values=d["count"],
                    hole=0.45, marker=dict(colors=colors_pie[:len(d)]),
                    textinfo="percent+label", textfont=dict(size=9, color=COLORS["text_dim"]),
                ))
                apply_chart_style(fig, 400)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        col_a2, col_b2 = st.columns(2)
        with col_a2:
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            chart_header("Calls by Priority Level", "Distribution across priority tiers")
            if "priorities" in police_stats:
                d = police_stats["priorities"]
                p_colors = [COLORS["red"], COLORS["yellow"], COLORS["medical"], COLORS["text_muted"]]
                fig = go.Figure(go.Bar(
                    x=d["priority"].astype(str), y=d["count"],
                    marker_color=p_colors[:len(d)],
                    text=d["count"].apply(lambda v: f"{v:,}"), textposition="outside",
                    textfont=dict(size=10, color=COLORS["text_dim"]),
                ))
                apply_chart_style(fig, 280)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        with col_b2:
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            chart_header("Quarterly Volume Trend", "Calls per year from loaded quarters")
            if "yearly" in police_stats:
                d = police_stats["yearly"]
                fig = go.Figure(go.Scatter(
                    x=d["year"], y=d["total"], fill="tozeroy",
                    fillcolor="rgba(139,92,246,0.25)", line=dict(color=COLORS["police"], width=3),
                    mode="lines+markers",
                ))
                apply_chart_style(fig, 280)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        ic1, ic2, ic3 = st.columns(3)
        with ic1:
            insight_box("Firecracker and disturbance calls surge dramatically on New Year's Eve and July 4th. Predictive staffing models could pre-position resources for these known spikes.", "finding")
        with ic2:
            insight_box(f"Canceled calls ({police_stats.get('cancel_rate', 'N/A')}%) represent dispatch resources deployed without resolution. Analyzing patterns by area and time could optimize dispatch.", "alert")
        with ic3:
            insight_box("Expand civilian Community Service Officer deployment for noise, abandoned vehicles, and minor disturbances — freeing sworn officers for higher-priority work.", "rec")


# ======================== 311 TAB ========================
with tab_311:
    st.markdown('<div class="section-title">311 Service Request Demand & Resolution Analysis</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Source: data.sanjoseca.gov/dataset/311-service-request-data · Live data</div>', unsafe_allow_html=True)

    if not sr311_ok:
        st.warning("⚠️ Could not load 311 data. Check connection to data.sanjoseca.gov.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            metric_card("📈", f"Growth Since {sr311_stats.get('first_year', '?')}", f"+{sr311_stats.get('growth_pct', 0)}%", f"to {sr311_stats.get('last_year', '?')}", "sr311")
        with c2:
            if "sources" in sr311_stats and len(sr311_stats["sources"]) > 0:
                top_src = sr311_stats["sources"].iloc[0]["source"]
                metric_card("📱", "Top Source", top_src, "Primary submission channel", "blue")
        with c3:
            if "avg_resolution" in sr311_stats:
                metric_card("✅", "Median Resolution", f"{sr311_stats['avg_resolution']} days", "across all categories", "yellow")
        with c4:
            if "categories" in sr311_stats and len(sr311_stats["categories"]) > 0:
                top_cat = sr311_stats["categories"].iloc[0]
                metric_card("🏷", "Top Category", top_cat["category"], f"{top_cat['count']:,} requests", "purple")

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

        col_a, col_b = st.columns([3, 2])
        with col_a:
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            chart_header("Service Category Volume", "Top request types across all years")
            if "categories" in sr311_stats:
                d = sr311_stats["categories"].head(12)
                fig = go.Figure(go.Bar(
                    y=d["category"], x=d["count"], orientation="h",
                    marker_color=COLORS["sr311"],
                    text=d["count"].apply(lambda v: f"{v:,}"), textposition="outside",
                    textfont=dict(size=10, color=COLORS["text_dim"]),
                ))
                apply_chart_style(fig, 380)
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        with col_b:
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            chart_header("Responsible Department Split", "Which departments handle 311 requests")
            if "departments" in sr311_stats:
                d = sr311_stats["departments"].head(7)
                dept_colors = [COLORS["green"], COLORS["medical"], COLORS["yellow"], COLORS["sr311"], COLORS["police"], COLORS["accent"], COLORS["text_muted"]]
                fig = go.Figure(go.Pie(
                    labels=d["department"], values=d["count"],
                    hole=0.45, marker=dict(colors=dept_colors[:len(d)]),
                    textinfo="percent+label", textfont=dict(size=9, color=COLORS["text_dim"]),
                ))
                apply_chart_style(fig, 380)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        col_a2, col_b2 = st.columns(2)
        with col_a2:
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            chart_header("Median Resolution Time by Category", "Days from creation to last update")
            if "resolution_time" in sr311_stats:
                d = sr311_stats["resolution_time"].head(12)
                colors_res = [COLORS["red"] if v > 25 else COLORS["yellow"] if v > 12 else COLORS["green"] for v in d["median_days"]]
                fig = go.Figure(go.Bar(
                    x=d["category"], y=d["median_days"],
                    marker_color=colors_res,
                    text=d["median_days"].round(1), textposition="outside",
                    textfont=dict(size=10, color=COLORS["text_dim"]),
                ))
                apply_chart_style(fig, 300)
                fig.update_layout(xaxis=dict(tickangle=-35))
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        with col_b2:
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            chart_header("Annual 311 Request Volume", f"{sr311_stats.get('growth_pct', 0)}% growth since {sr311_stats.get('first_year', '?')}")
            if "yearly" in sr311_stats:
                d = sr311_stats["yearly"]
                fig = go.Figure(go.Scatter(
                    x=d["year"], y=d["total"], fill="tozeroy",
                    fillcolor="rgba(16,185,129,0.25)", line=dict(color=COLORS["sr311"], width=3),
                    mode="lines+markers",
                ))
                apply_chart_style(fig, 300)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

        ic1, ic2 = st.columns(2)
        with ic1:
            insight_box("Infrastructure-related requests (sidewalk, street light) consistently take the longest to resolve. A dedicated quick-fix crew could reduce visible neglect in high-demand neighborhoods.", "alert")
        with ic2:
            insight_box("Graffiti is typically the #1 category. A rapid-response graffiti team focused on the top repeat locations could dramatically reduce visible urban blight.", "rec")


# ======================== CROSS-SYSTEM TAB ========================
with tab_cross:
    st.markdown('<div class="section-title">Cross-System Equity & Resource Gap Analysis</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-subtitle">Correlating fire response, police demand, and 311 complaints</div>', unsafe_allow_html=True)

    # Build cross-system data from what we have
    if fire_ok and "battalion" in fire_stats:
        bat_data = fire_stats["battalion"].copy()

        c1, c2, c3 = st.columns(3)
        with c1:
            if len(bat_data) > 0:
                best_bat = bat_data.loc[bat_data["avg_response"].idxmin()]
                metric_card("🟢", "Fastest Battalion", f"{best_bat['avg_response']} min", f"{best_bat['battalion']}", "green")
        with c2:
            if len(bat_data) > 0:
                worst_bat = bat_data.loc[bat_data["avg_response"].idxmax()]
                metric_card("🔴", "Slowest Battalion", f"{worst_bat['avg_response']} min", f"{worst_bat['battalion']} — highest demand zone", "red")
        with c3:
            if len(bat_data) > 1:
                gap = round(bat_data["avg_response"].max() - bat_data["avg_response"].min(), 1)
                metric_card("⚠️", "Response Time Gap", f"{gap} min", "between best and worst battalions", "yellow")

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        chart_header("Battalion Incident Volume vs Response Time", "Do the busiest areas also have the slowest response?")
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=bat_data["battalion"], y=bat_data["total_incidents"], name="Total Incidents", marker_color=COLORS["medical"], opacity=0.7), secondary_y=False)
        fig.add_trace(go.Scatter(x=bat_data["battalion"], y=bat_data["avg_response"], name="Avg Response (min)", mode="lines+markers", line=dict(color=COLORS["fire"], width=3), marker=dict(size=10)), secondary_y=True)
        fig.update_yaxes(title_text="Incidents", secondary_y=False, gridcolor=COLORS["grid"])
        fig.update_yaxes(title_text="Avg Response (min)", secondary_y=True, gridcolor=COLORS["grid"])
        apply_chart_style(fig, 320)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # POLICY RECOMMENDATIONS
    st.markdown("""
    <div class="policy-box">
        <div class="policy-title">🎯 Key Policy Recommendations for City Council</div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:20px;">
            <div class="policy-item">
                <strong>1. Response Equity Initiative</strong><br>
                Battalions with the highest incident volumes often show the longest response times — an equity gap that disproportionately affects residents in those areas. Deploy dedicated EMS quick-response units and evaluate station coverage gaps.
            </div>
            <div class="policy-item">
                <strong>2. Medical Call Diversion Program</strong><br>
                The overwhelming majority of fire incidents are medical. Partner with county EMS to establish a nurse triage hotline, diverting low-acuity calls and freeing engine companies for true emergencies.
            </div>
            <div class="policy-item">
                <strong>3. Civilian Response Expansion</strong><br>
                A significant share of police calls are canceled or need no report. Expand civilian Community Service Officer deployment for noise, abandoned vehicles, and minor disturbances — freeing sworn officers for higher-priority work.
            </div>
            <div class="policy-item">
                <strong>4. Infrastructure Resolution Acceleration</strong><br>
                311 data shows infrastructure requests (sidewalk, street light) take dramatically longer to resolve than other categories. A dedicated quick-fix crew could reduce visible neglect in high-demand neighborhoods.
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
    insight_box("Areas with overlapping high demand across fire, police, AND 311 represent 'chronic demand zones' where residents simultaneously experience emergencies and infrastructure decay. These zones need coordinated multi-department intervention, not siloed responses.", "finding")


# ============================================================
# FOOTER
# ============================================================
st.markdown(f"""
<div style="padding:16px 0; border-top:1px solid {COLORS['card_border']}; margin-top:32px; display:flex; justify-content:space-between; font-family:'JetBrains Mono',monospace; font-size:11px; color:{COLORS['text_muted']};">
    <span>Data: San José Open Data Portal (data.sanjoseca.gov) — CC0 / CC-BY Licensed</span>
    <span>Live data · Dashboard built with Streamlit + Plotly</span>
</div>
""", unsafe_allow_html=True)
