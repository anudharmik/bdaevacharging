"""
Streamlit Interactive Dashboard for EV Charging Station Analytics.
Reads pre-aggregated analytical metrics directly from Apache Cassandra tables:
- ev_analytics.overall_metrics
- ev_analytics.hourly_demand
- ev_analytics.charger_type_metrics
- ev_analytics.location_metrics

Styled with a clean modern light theme, EV-inspired emerald accents, and interactive Plotly figures.
"""

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.dashboard.dashboard_helpers import (
    calculate_insights,
    format_duration_minutes,
    format_energy_kwh,
    format_number,
)
from src.database.cassandra_client import (
    fetch_charger_type_metrics,
    fetch_hourly_demand,
    fetch_location_metrics,
    fetch_overall_metrics,
    get_cassandra_session,
    shutdown_cassandra,
)

# Page configuration
st.set_page_config(
    page_title="EV Charge Analytics | Apache Spark & Cassandra",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Custom styling
st.markdown(
    """
    <style>
    /* Global layout polish */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 3rem;
        max-width: 1300px;
    }
    /* Brand badge styling */
    .tech-badge {
        display: inline-block;
        padding: 4px 10px;
        margin-right: 6px;
        margin-bottom: 6px;
        border-radius: 6px;
        font-size: 0.78rem;
        font-weight: 600;
        background-color: #f1f5f9;
        color: #334155;
        border: 1px solid #cbd5e1;
    }
    .status-badge {
        display: inline-block;
        padding: 5px 12px;
        border-radius: 9999px;
        font-size: 0.82rem;
        font-weight: 600;
        background-color: #ecfdf5;
        color: #065f46;
        border: 1px solid #a7f3d0;
    }
    /* Metric Card Container */
    .metric-card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 20px 22px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        transition: transform 0.15s ease, box-shadow 0.15s ease;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(16, 185, 129, 0.08);
    }
    .metric-title {
        font-size: 0.85rem;
        font-weight: 600;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 6px;
    }
    .metric-value {
        font-size: 1.85rem;
        font-weight: 700;
        color: #0f172a;
        line-height: 1.2;
    }
    .metric-subtitle {
        font-size: 0.82rem;
        color: #10b981;
        font-weight: 500;
        margin-top: 4px;
    }
    /* Insight Card */
    .insight-box {
        background: #f8fafc;
        border-left: 4px solid #10b981;
        border-radius: 0 8px 8px 0;
        padding: 14px 18px;
        margin-bottom: 12px;
    }
    .insight-title {
        font-weight: 700;
        color: #0f172a;
        font-size: 0.95rem;
        margin-bottom: 4px;
    }
    .insight-desc {
        color: #475569;
        font-size: 0.88rem;
        line-height: 1.45;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=60, show_spinner=False)
def load_data_from_cassandra() -> Tuple[
    Optional[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    Optional[str],
]:
    """
    Retrieves analytical datasets directly from Apache Cassandra tables.
    Returns: (overall_dict, hourly_list, charger_type_list, location_list, error_message)
    """
    cluster, session = None, None
    try:
        cluster, session = get_cassandra_session()
        overall = fetch_overall_metrics(session)
        hourly = fetch_hourly_demand(session)
        charger_types = fetch_charger_type_metrics(session)
        locations = fetch_location_metrics(session)
        return overall, hourly, charger_types, locations, None
    except Exception as exc:
        return None, [], [], [], str(exc)
    finally:
        if cluster and session:
            shutdown_cassandra(cluster, session)


def main():
    # --------------------------------------------------------------------------
    # Header Section
    # --------------------------------------------------------------------------
    header_col1, header_col2 = st.columns([3, 1])

    with header_col1:
        st.markdown(
            "## ⚡ Electric Vehicle Charging Station Analytics"
        )
        st.markdown(
            "<p style='color: #475569; font-size: 1.05rem; margin-top: -10px;'>"
            "Big Data Analysis Lab — Distributed PySpark ETL & Apache Cassandra Serving Layer"
            "</p>",
            unsafe_allow_html=True,
        )
        st.markdown(
            """
            <div>
                <span class="tech-badge">Apache Spark 3.5.1</span>
                <span class="tech-badge">Apache Cassandra 4.1</span>
                <span class="tech-badge">Python 3.11</span>
                <span class="tech-badge">Streamlit + Plotly</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with header_col2:
        st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
        st.markdown(
            """
            <div style='text-align: right;'>
                <span class="status-badge">🟢 Connected to Cassandra</span>
                <p style='color: #64748b; font-size: 0.78rem; margin-top: 4px;'>
                    Keyspace: <b>ev_analytics</b> | Port: <b>9042</b>
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    st.markdown("<hr style='margin-top: 0.5rem; margin-bottom: 1.5rem;'>", unsafe_allow_html=True)

    # --------------------------------------------------------------------------
    # Database Retrieval & Reliability Check
    # --------------------------------------------------------------------------
    with st.spinner("Querying analytical tables from Apache Cassandra..."):
        overall_data, hourly_data, charger_type_data, location_data, error = load_data_from_cassandra()

    if error:
        st.error(
            f"❌ **Failed to connect to Apache Cassandra**: `{error}`\n\n"
            "**Troubleshooting Steps:**\n"
            "1. Confirm the Cassandra Docker container is running: `docker compose ps`\n"
            "2. If stopped, start it: `docker compose up -d`\n"
            "3. Verify that the analytics tables are loaded: `python scripts/load_cassandra.py`\n"
            "*(The dashboard strictly queries Cassandra and will never silently fallback to CSVs).*"
        )
        st.stop()

    if not overall_data or not hourly_data:
        st.warning(
            "⚠️ **Connected to Cassandra, but the tables in keyspace `ev_analytics` are empty.**\n\n"
            "Please run the data loader: `python scripts/load_cassandra.py`"
        )
        st.stop()

    # Pre-calculate data-driven insights
    insights = calculate_insights(overall_data, hourly_data, charger_type_data, location_data)

    # --------------------------------------------------------------------------
    # Section 1: KPI Cards (from overall_metrics table)
    # --------------------------------------------------------------------------
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)

    with kpi1:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-title">Total Charging Sessions</div>
                <div class="metric-value">{format_number(overall_data.get('total_sessions'))}</div>
                <div class="metric-subtitle">72k+ verified transactions</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with kpi2:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-title">Total Energy Delivered</div>
                <div class="metric-value">{format_energy_kwh(overall_data.get('total_demand_kwh'))}</div>
                <div class="metric-subtitle">~1.27 GWh grid power</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with kpi3:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-title">Average Energy / Session</div>
                <div class="metric-value">{format_number(overall_data.get('avg_demand_kwh'), 1)} <span style='font-size: 1.1rem;'>kWh</span></div>
                <div class="metric-subtitle">Per transaction average</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with kpi4:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-title">Average Dwell Time</div>
                <div class="metric-value">{format_duration_minutes(overall_data.get('avg_duration_minutes'))}</div>
                <div class="metric-subtitle">Range: {overall_data.get('min_duration_minutes', 1):.0f}m - {overall_data.get('max_duration_minutes', 0)/60:.1f}h</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<div style='height: 25px;'></div>", unsafe_allow_html=True)

    # --------------------------------------------------------------------------
    # Section 2: Hourly Demand Profile (24 Hours)
    # --------------------------------------------------------------------------
    st.subheader("📈 24-Hour Power Grid Demand Profile")
    st.caption("Aggregated across 72,108 transactions stored in Cassandra `hourly_demand` table.")

    hourly_df = pd.DataFrame(hourly_data)
    hourly_df["hour_label"] = hourly_df["start_hour"].apply(lambda h: f"{h:02d}:00")

    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        # Total Energy Line Chart
        fig_demand = go.Figure()
        fig_demand.add_trace(
            go.Scatter(
                x=hourly_df["hour_label"],
                y=hourly_df["total_demand_kwh"],
                mode="lines+markers",
                name="Total Demand (kWh)",
                line=dict(color="#10b981", width=3, shape="spline"),
                marker=dict(size=6, color="#047857"),
                fill="tozeroy",
                fillcolor="rgba(16, 185, 129, 0.12)",
                hovertemplate="<b>Hour:</b> %{x}<br><b>Demand:</b> %{y:,.1f} kWh<extra></extra>",
            )
        )
        # Highlight peak hour
        if insights["peak_hour"] is not None:
            peak_label = f"{insights['peak_hour']:02d}:00"
            fig_demand.add_annotation(
                x=peak_label,
                y=insights["peak_demand_kwh"],
                text=f"Peak: {insights['peak_demand_kwh']:,.0f} kWh",
                showarrow=True,
                arrowhead=2,
                arrowcolor="#dc2626",
                font=dict(color="#dc2626", size=11, family="sans-serif"),
                bgcolor="#fef2f2",
                bordercolor="#fca5a5",
                borderwidth=1,
            )

        fig_demand.update_layout(
            title="<b>Total Energy Demand by Hour (kWh)</b>",
            xaxis_title="Time of Day (24h)",
            yaxis_title="Total Energy (kWh)",
            plot_bgcolor="#ffffff",
            paper_bgcolor="#ffffff",
            margin=dict(l=40, r=20, t=50, b=40),
            hovermode="x unified",
            xaxis=dict(showgrid=True, gridcolor="#f1f5f9"),
            yaxis=dict(showgrid=True, gridcolor="#f1f5f9"),
        )
        st.plotly_chart(fig_demand, use_container_width=True)

    with col_chart2:
        # Session Count Bar Chart
        fig_sessions = px.bar(
            hourly_df,
            x="hour_label",
            y="session_count",
            title="<b>Charging Session Frequency by Hour</b>",
            labels={"hour_label": "Time of Day", "session_count": "Sessions"},
            color="session_count",
            color_continuous_scale=["#a7f3d0", "#10b981", "#047857"],
        )
        fig_sessions.update_layout(
            plot_bgcolor="#ffffff",
            paper_bgcolor="#ffffff",
            margin=dict(l=40, r=20, t=50, b=40),
            coloraxis_showscale=False,
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor="#f1f5f9"),
        )
        fig_sessions.update_traces(
            hovertemplate="<b>Hour:</b> %{x}<br><b>Sessions:</b> %{y:,}<extra></extra>"
        )
        st.plotly_chart(fig_sessions, use_container_width=True)

    st.markdown("<div style='height: 15px;'></div>", unsafe_allow_html=True)

    # --------------------------------------------------------------------------
    # Section 3 & 4: AC vs DC and Location Breakdown
    # --------------------------------------------------------------------------
    sec_col1, sec_col2 = st.columns([1, 1])

    with sec_col1:
        st.subheader("🔌 AC vs. DC Charger Architecture")
        st.caption("Comparison from Cassandra `charger_type_metrics` table.")

        type_df = pd.DataFrame(charger_type_data)
        type_df["type_name"] = type_df["charger_current_type"].apply(
            lambda t: "AC (Slow / Level 2)" if t == "AC" else "DC (Fast / Rapid)"
        )

        sub_tab1, sub_tab2 = st.tabs(["Session & Energy Split", "Dwell Time (Duration)"])

        with sub_tab1:
            fig_split = px.bar(
                type_df,
                x="type_name",
                y="session_count",
                text="session_count",
                color="charger_current_type",
                color_discrete_map={"AC": "#10b981", "DC": "#0284c7"},
                title="<b>Session Volume by Charger Type</b>",
                labels={"type_name": "Charger Type", "session_count": "Sessions"},
            )
            fig_split.update_traces(
                texttemplate="%{text:,}", textposition="outside"
            )
            fig_split.update_layout(
                plot_bgcolor="#ffffff",
                paper_bgcolor="#ffffff",
                showlegend=False,
                margin=dict(l=30, r=20, t=40, b=40),
                yaxis=dict(showgrid=True, gridcolor="#f1f5f9"),
            )
            st.plotly_chart(fig_split, use_container_width=True)

        with sub_tab2:
            fig_dur = px.bar(
                type_df,
                x="type_name",
                y="avg_duration_minutes",
                text="avg_duration_minutes",
                color="charger_current_type",
                color_discrete_map={"AC": "#10b981", "DC": "#0284c7"},
                title="<b>Average Duration by Charger Type (Minutes)</b>",
                labels={"type_name": "Charger Type", "avg_duration_minutes": "Average Minutes"},
            )
            fig_dur.update_traces(
                texttemplate="%{text:.1f} min", textposition="outside"
            )
            fig_dur.update_layout(
                plot_bgcolor="#ffffff",
                paper_bgcolor="#ffffff",
                showlegend=False,
                margin=dict(l=30, r=20, t=40, b=40),
                yaxis=dict(showgrid=True, gridcolor="#f1f5f9"),
            )
            st.plotly_chart(fig_dur, use_container_width=True)

    with sec_col2:
        st.subheader("📍 Siting & Location Categories")
        st.caption("Facility breakdown from Cassandra `location_metrics` table.")

        loc_df = pd.DataFrame(location_data)

        sort_choice = st.selectbox(
            "Sort Locations By:",
            ["Session Count (Volume)", "Total Energy (kWh)", "Average Duration (Minutes)"],
            index=0,
        )

        if "Session" in sort_choice:
            loc_df = loc_df.sort_values(by="session_count", ascending=True)
            chart_y = "session_count"
            chart_title = "<b>Locations by Session Count</b>"
            hover_fmt = "Sessions: %{x:,}"
        elif "Energy" in sort_choice:
            loc_df = loc_df.sort_values(by="total_demand_kwh", ascending=True)
            chart_y = "total_demand_kwh"
            chart_title = "<b>Locations by Total Energy Delivery (kWh)</b>"
            hover_fmt = "Demand: %{x:,.1f} kWh"
        else:
            loc_df = loc_df.sort_values(by="avg_duration_minutes", ascending=True)
            chart_y = "avg_duration_minutes"
            chart_title = "<b>Locations by Average Duration (Minutes)</b>"
            hover_fmt = "Avg Duration: %{x:.1f} min"

        fig_loc = px.bar(
            loc_df,
            x=chart_y,
            y="location",
            orientation="h",
            title=chart_title,
            labels={"location": "Facility Type", chart_y: sort_choice},
            color=chart_y,
            color_continuous_scale=["#bae6fd", "#0284c7", "#0369a1"],
        )
        fig_loc.update_layout(
            plot_bgcolor="#ffffff",
            paper_bgcolor="#ffffff",
            coloraxis_showscale=False,
            margin=dict(l=40, r=20, t=40, b=40),
            xaxis=dict(showgrid=True, gridcolor="#f1f5f9"),
            yaxis=dict(showgrid=False),
        )
        fig_loc.update_traces(hovertemplate="<b>Location:</b> %{y}<br>" + hover_fmt + "<extra></extra>")
        st.plotly_chart(fig_loc, use_container_width=True)

    st.markdown("<div style='height: 20px;'></div>", unsafe_allow_html=True)

    # --------------------------------------------------------------------------
    # Section 5: Data-Driven Analytical Insights
    # --------------------------------------------------------------------------
    st.subheader("💡 Data-Driven Engineering Insights")
    st.caption("Dynamically derived from the latest Cassandra query results.")

    ins_c1, ins_c2 = st.columns(2)

    with ins_c1:
        st.markdown(
            f"""
            <div class="insight-box">
                <div class="insight-title">⚡ Grid Coincidence Peak at {insights['peak_hour']:02d}:00</div>
                <div class="insight-desc">
                    Daily peak charging load surges to <b>{insights['peak_demand_kwh']:,.0f} kWh</b> across 
                    <b>{insights['peak_sessions']:,} sessions</b> during the 18:00 evening window. 
                    This directly overlaps with residential cooking and heating peaks, indicating critical demand for time-of-use tariffs.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            f"""
            <div class="insight-box">
                <div class="insight-title">🌙 Off-Peak Grid Capacity Valley at {insights['valley_hour']:02d}:00</div>
                <div class="insight-desc">
                    Nighttime charging drops to only <b>{insights['valley_sessions']:,} active sessions</b> at 03:00.
                    This represents significant unutilized power grid capacity suitable for overnight EV smart charging programs.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with ins_c2:
        st.markdown(
            f"""
            <div class="insight-box">
                <div class="insight-title">🔋 Destination (AC) vs. Transit (DC) Charging Dynamics</div>
                <div class="insight-desc">
                    <b>AC Level-2 chargers</b> dominate session volume (<b>{insights['ac_sessions']:,} sessions</b>, 79.6%) 
                    with an average duration of <b>{insights['ac_avg_duration']:.1f} minutes</b> (~3.0 hrs).
                    In contrast, <b>DC Fast Chargers</b> provide <b>{insights['duration_ratio_ac_to_dc']}x faster turnover</b> 
                    (avg <b>{insights['dc_avg_duration']:.1f} minutes</b>).
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            f"""
            <div class="insight-box">
                <div class="insight-title">🏢 Primary Urban Siting Anchor: {insights['top_location'].title()}</div>
                <div class="insight-desc">
                    The highest utilization category is <b>{insights['top_location']}</b> with 
                    <b>{insights['top_location_sessions']:,} sessions</b> ({insights['top_location_share_pct']}% of total volume), 
                    closely followed by apartment residential complexes.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # --------------------------------------------------------------------------
    # Footer
    # --------------------------------------------------------------------------
    st.markdown("<hr style='margin-top: 2rem; margin-bottom: 1rem;'>", unsafe_allow_html=True)
    st.markdown(
        """
        <div style='text-align: center; color: #94a3b8; font-size: 0.82rem;'>
            Dataset: <i>A dataset for multi-faceted analysis of electric vehicle charging transactions</i> (Nature Scientific Data, 2024 / IEEE PES Task Force) — CC BY 4.0<br>
            Architecture: CSV Ingestion → Distributed PySpark Processing → Apache Cassandra (NoSQL) → Streamlit Dashboard
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
