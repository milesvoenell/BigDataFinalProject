import streamlit as st
import pandas as pd
from opensearchpy import OpenSearch
import plotly.graph_objects as go
from typing import Optional

# ----------------------------
# Connect to OpenSearch
# ----------------------------
client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200}],
    http_auth=("admin", "admin123")
)

INDEX_AGG = "nyc_marathon_aggregates"

res = client.search(
    index=INDEX_AGG,
    body={"size": 1000, "sort": [{"year": {"order": "asc"}}]}
)

data = [hit["_source"] for hit in res["hits"]["hits"]]
df = pd.DataFrame(data)

# ----------------------------
# Helper functions
# ----------------------------
def seconds_to_hms(seconds: Optional[float]) -> Optional[str]:
    if seconds is None or pd.isna(seconds):
        return None
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def seconds_to_mmss(seconds: Optional[float]) -> Optional[str]:
    if seconds is None or pd.isna(seconds):
        return None
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s"

# ----------------------------
# Derived columns
# ----------------------------
df["avg_100th_hover"] = df["avg_100th_place_time"].apply(seconds_to_hms)
df["winning_hover"] = df["winning_time"].apply(seconds_to_hms)

df["winning_pace_sec"] = df["winning_time"] / 26.2
df["avg_100th_pace_sec"] = df["avg_100th_place_time"] / 26.2

df["winning_pace_hover"] = df["winning_pace_sec"].apply(seconds_to_mmss)
df["avg_100th_pace_hover"] = df["avg_100th_pace_sec"].apply(seconds_to_mmss)

# ----------------------------
# Streamlit Dashboard
# ----------------------------
st.set_page_config(layout="wide", page_title="NYC Marathon Dashboard")
st.title("NYC Marathon Overview")

# ---- Aggregated Table ----
st.subheader("Aggregated Table")
st.dataframe(
    df[["year", "total_runners", "winning_hover", "avg_100th_hover"]]
    .rename(columns={
        "year": "Year",
        "total_runners": "Total Runners",
        "winning_hover": "Winning Time",
        "avg_100th_hover": "Avg Top 100 Time"
    })
)

# ----------------------------
# 1. Top 100 vs Winning Time
# ----------------------------
st.subheader("Top 100 Avg Time vs Winning Time")

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=df["year"],
    y=df["avg_100th_place_time"],
    mode="lines+markers",
    name="Avg Top 100 Time",
    hovertext=df["avg_100th_hover"],
    hovertemplate="%{hovertext}",
    line=dict(color="cyan")
))
fig.add_trace(go.Scatter(
    x=df["year"],
    y=df["winning_time"],
    mode="lines+markers",
    name="Winning Time",
    hovertext=df["winning_hover"],
    hovertemplate="%{hovertext}",
    line=dict(color="magenta")
))

yticks = list(range(7200, 14401, 900))
yticktext = [f"{h}h {m}m" for h, m in ((s // 3600, (s % 3600) // 60) for s in yticks)]

fig.update_layout(
    xaxis_title="Year",
    yaxis=dict(
        title="Time",
        range=[7200, 14400],
        tickvals=yticks,
        ticktext=yticktext
    ),
    plot_bgcolor="#1e1e1e",
    paper_bgcolor="#1e1e1e",
    font=dict(color="white"),
)

st.plotly_chart(fig, use_container_width=True)

# ----------------------------
# 2. Total Runners Over Time
# ----------------------------
st.subheader("Total Runners Over the Years")

fig1 = go.Figure()
fig1.add_trace(go.Scatter(
    x=df["year"],
    y=df["total_runners"],
    mode="lines+markers",
    name="Total Runners",
    fill="tozeroy",
    hovertemplate="%{y} runners in %{x}"
))

fig1.update_layout(
    xaxis_title="Year",
    yaxis_title="Runners",
    plot_bgcolor="#1e1e1e",
    paper_bgcolor="#1e1e1e",
    font=dict(color="white")
)

st.plotly_chart(fig1, use_container_width=True)

# ----------------------------
# 3. Pace Trend
# ----------------------------
st.subheader("Pace Trend: Winning vs Top 100")

fig_pace = go.Figure()
fig_pace.add_trace(go.Scatter(
    x=df["year"],
    y=df["winning_pace_sec"],
    mode="lines+markers",
    name="Winning Pace",
    hovertext=df["winning_pace_hover"],
    hovertemplate="%{hovertext} per mile"
))
fig_pace.add_trace(go.Scatter(
    x=df["year"],
    y=df["avg_100th_pace_sec"],
    mode="lines+markers",
    name="Avg Top 100 Pace",
    hovertext=df["avg_100th_pace_hover"],
    hovertemplate="%{hovertext} per mile"
))

yticks_pace = list(range(
    int(df[["winning_pace_sec", "avg_100th_pace_sec"]].min().min()) // 60 * 60,
    int(df[["winning_pace_sec", "avg_100th_pace_sec"]].max().max()) + 60,
    60
))

fig_pace.update_layout(
    xaxis_title="Year",
    yaxis=dict(
        title="Pace",
        tickvals=yticks_pace,
        ticktext=[seconds_to_mmss(s) for s in yticks_pace]
    ),
    plot_bgcolor="#1e1e1e",
    paper_bgcolor="#1e1e1e",
    font=dict(color="white")
)

st.plotly_chart(fig_pace, use_container_width=True)

# ----------------------------
# 4. Runner Count vs Performance
# ----------------------------
st.subheader("Runner Count vs Avg Top 100 Time")

fig_scatter = go.Figure()
fig_scatter.add_trace(go.Scatter(
    x=df["total_runners"],
    y=df["avg_100th_place_time"],
    mode="markers",
    marker=dict(size=10, color=df["year"], colorscale="Viridis", showscale=True),
    hovertext=df["avg_100th_hover"],
    hovertemplate="Year: %{customdata}<br>Runners: %{x}<br>Avg Time: %{hovertext}",
    customdata=df["year"]
))

fig_scatter.update_layout(
    xaxis_title="Total Runners",
    yaxis=dict(
        title="Avg Top 100 Time",
        tickvals=yticks,
        ticktext=yticktext
    ),
    plot_bgcolor="#1e1e1e",
    paper_bgcolor="#1e1e1e",
    font=dict(color="white")
)

st.plotly_chart(fig_scatter, use_container_width=True)
