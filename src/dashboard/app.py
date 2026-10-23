"""
Streamlit Interactive Operations Dashboard for Electric Vehicle (EV) Charging Station Analytics.
Connects directly to the Apache Cassandra serving layer (keyspace: ev_analytics) and integrates
an in-memory multi-dimensional filter engine backed by cleaned session records.

Architecture:
1. Serving Layer: Apache Cassandra 4.1 (overall_metrics, hourly_demand, charger_type_metrics, location_metrics)
2. Interactive Slice Engine: Cleaned Parquet Analytical Cache (72,108 validated sessions)
3. Visuals & UX: Dark charcoal/navy theme with electric emerald & cyan accents, Plotly interactive graphics.
"""

from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.dashboard.dashboard_helpers import (
    aggregate_chargers_from_df,
    aggregate_hourly_from_df,
    aggregate_locations_from_df,
    calculate_filtered_insights,
    compute_kpis_from_df,
    filter_records,
    format_duration_minutes,
    format_energy_kwh,
    format_number,
    get_metric_peak_valley,
)
from src.database.cassandra_client import (
    fetch_charger_type_metrics,
    fetch_hourly_demand,
    fetch_location_metrics,
    fetch_overall_metrics,
    get_cassandra_session,
    shutdown_cassandra,
)

# -----------------------------------------------------------------------------
# 1. Page Configuration & Theme Styling
# -----------------------------------------------------------------------------

st.set_page_config(
    page_title="EV Charge Analytics | Energy Intelligence Operations",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .main .block-container {
        padding-top: 1.2rem;
        padding-bottom: 2.5rem;
        max-width: 1400px;
    }
    .app-header-box {
        background: #111827;
        border: 1px solid #1f2937;
        border-radius: 12px;
        padding: 16px 22px;
        margin-bottom: 16px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        flex-wrap: wrap;
        gap: 12px;
    }
    .app-title {
        font-size: 1.55rem;
        font-weight: 700;
        color: #f8fafc;
        margin: 0;
        display: flex;
        align-items: center;
        gap: 10px;
    }
    .app-subtitle {
        font-size: 0.85rem;
        color: #94a3b8;
        margin: 4px 0 0 0;
    }
    .tech-pill {
        display: inline-block;
        padding: 2px 8px;
        margin-right: 6px;
        border-radius: 6px;
        font-size: 0.72rem;
        font-weight: 600;
        background-color: #1f2937;
        color: #cbd5e1;
        border: 1px solid #374151;
    }
    .status-pill-connected {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 4px 12px;
        border-radius: 9999px;
        font-size: 0.78rem;
        font-weight: 600;
        background-color: #064e3b;
        color: #6ee7b7;
        border: 1px solid #059669;
    }
    .status-pill-disconnected {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 4px 12px;
        border-radius: 9999px;
        font-size: 0.78rem;
        font-weight: 600;
        background-color: #450a0a;
        color: #fca5a5;
        border: 1px solid #dc2626;
    }
    .filter-banner {
        background: #0f172a;
        border: 1px solid #1e293b;
        border-left: 4px solid #06b6d4;
        border-radius: 8px;
        padding: 10px 16px;
        margin-bottom: 16px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        flex-wrap: wrap;
        gap: 10px;
    }
    .filter-banner-text {
        font-size: 0.85rem;
        color: #cbd5e1;
    }
    .filter-source-badge {
        font-size: 0.74rem;
        font-weight: 600;
        padding: 2px 8px;
        border-radius: 4px;
        background: #1e293b;
        color: #38bdf8;
        border: 1px solid #0284c7;
    }
    .kpi-card {
        background: #111827;
        border: 1px solid #1f2937;
        border-radius: 12px;
        padding: 16px 18px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.25);
        transition: transform 0.15s ease, border-color 0.15s ease;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        border-color: #10b981;
    }
    .kpi-label {
        font-size: 0.76rem;
        font-weight: 600;
        color: #94a3b8;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 4px;
    }
    .kpi-value {
        font-size: 1.75rem;
        font-weight: 700;
        color: #f8fafc;
        line-height: 1.2;
    }
    .kpi-subtext {
        font-size: 0.78rem;
        color: #10b981;
        font-weight: 500;
        margin-top: 5px;
    }
    .insight-card {
        background: #111827;
        border: 1px solid #1f2937;
        border-left: 4px solid #10b981;
        border-radius: 0 10px 10px 0;
        padding: 14px 18px;
        margin-bottom: 12px;
    }
    .insight-card-title {
        font-size: 0.90rem;
        font-weight: 700;
        color: #f8fafc;
        margin-bottom: 4px;
    }
    .insight-card-body {
        font-size: 0.83rem;
        color: #94a3b8;
        line-height: 1.45;
    }
    .section-desc {
        font-size: 0.86rem;
        color: #94a3b8;
        margin-bottom: 16px;
    }
    .empty-state-box {
        background: #111827;
        border: 1px dashed #374151;
        border-radius: 12px;
        padding: 32px 24px;
        text-align: center;
        color: #94a3b8;
        margin: 20px 0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Plotly unified dark template defaults
PLOTLY_DARK_THEME = dict(
    paper_bgcolor="#111827",
    plot_bgcolor="#111827",
    font=dict(color="#e2e8f0", family="system-ui, -apple-system, sans-serif"),
    xaxis=dict(showgrid=True, gridcolor="#1f2937", tickfont=dict(color="#94a3b8"), linecolor="#374151"),
    yaxis=dict(showgrid=True, gridcolor="#1f2937", tickfont=dict(color="#94a3b8"), linecolor="#374151"),
    margin=dict(l=40, r=20, t=45, b=40),
)


# -----------------------------------------------------------------------------
# 2. Reusable Presentation Helpers
# -----------------------------------------------------------------------------


def render_kpi_card(label: str, value: str, subtext: str, border_left: Optional[str] = None) -> None:
    """Renders a styled KPI metric container card."""
    border_style = f"border-left: 4px solid {border_left};" if border_left else ""
    st.markdown(
        f"""
        <div class="kpi-card" style="{border_style}">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-subtext">{subtext}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_insight_card(title: str, body: str) -> None:
    """Renders a styled data-driven insight callout box."""
    st.markdown(
        f"""
        <div class="insight-card">
            <div class="insight-card-title">{title}</div>
            <div class="insight-card-body">{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# -----------------------------------------------------------------------------
# 3. Data Retrieval & Caching Layer
# -----------------------------------------------------------------------------


@st.cache_data(ttl=300, show_spinner=False)
def load_data_from_cassandra() -> Tuple[
    Optional[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    Optional[str],
    str,
]:
    """Queries pre-aggregated benchmark metrics directly from Apache Cassandra."""
    cluster, session = None, None
    timestamp_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    try:
        cluster, session = get_cassandra_session()
        overall = fetch_overall_metrics(session)
        hourly = fetch_hourly_demand(session)
        charger_types = fetch_charger_type_metrics(session)
        locations = fetch_location_metrics(session)
        return overall, hourly, charger_types, locations, None, timestamp_str
    except Exception as exc:
        return None, [], [], [], str(exc), timestamp_str
    finally:
        if cluster and session:
            shutdown_cassandra(cluster, session)


@st.cache_data(ttl=300, show_spinner=False)
def load_cleaned_records() -> Optional[pd.DataFrame]:
    """Loads cleaned transaction records from Parquet into an in-memory cache for fast filtering."""
    parquet_path = PROJECT_ROOT / "data" / "processed" / "cleaned_charging_records.parquet"
    if not parquet_path.exists():
        return None
    try:
        cols = ["charger_current_type", "Location", "start_hour", "Demand", "Duration"]
        return pd.read_parquet(parquet_path, columns=cols)
    except Exception:
        return None


# -----------------------------------------------------------------------------
# 4. Main Application Controller
# -----------------------------------------------------------------------------


def main():
    # Retrieve cached data
    overall_data, hourly_data, charger_type_data, location_data, db_error, fetch_ts = load_data_from_cassandra()
    session_records_df = load_cleaned_records()

    portfolio_total_sessions = overall_data.get("total_sessions", 72108) if overall_data else 72108

    # Extract location list
    if location_data:
        all_locations = [r["location"] for r in location_data]
    elif session_records_df is not None and "Location" in session_records_df.columns:
        all_locations = sorted(session_records_df["Location"].dropna().unique().tolist())
    else:
        all_locations = [
            "public area", "apartment", "resort", "hotel", "company",
            "public institution", "public parking lot", "market", "restaurant", "camping"
        ]

    # Sidebar Filter Controls
    with st.sidebar:
        st.markdown("### 🎛️ Filters")
        st.markdown(
            "<p style='font-size: 0.8rem; color: #94a3b8; margin-top: -8px;'>"
            "Filter by charger, location, and time. Charts and numbers update automatically."
            "</p>",
            unsafe_allow_html=True,
        )

        charger_labels = {"All": "All Types", "AC": "AC (Slow / Level 2)", "DC": "DC (Fast / Rapid)"}
        sel_charger_type = st.selectbox(
            "⚡ Charger Type",
            options=["All", "AC", "DC"],
            format_func=lambda x: charger_labels.get(x, x),
            index=0,
        )

        sel_location = st.selectbox(
            "📍 Location",
            options=["All"] + all_locations,
            format_func=lambda x: "All Locations" if x == "All" else x.title(),
            index=0,
        )

        st.markdown("<label style='font-size: 0.84rem; font-weight: 500; color: #e2e8f0;'>🕒 Start Time (Hour)</label>", unsafe_allow_html=True)
        sel_hour_range = st.slider(
            "Start Hour",
            min_value=0,
            max_value=23,
            value=(0, 23),
            step=1,
            format="%02d:00",
            label_visibility="collapsed",
        )

        preset_cols = st.columns(3)
        if preset_cols[0].button("All 24h", use_container_width=True):
            st.session_state["sel_hour_range"] = (0, 23)
            st.rerun()
        if preset_cols[1].button("Peak (16-20h)", use_container_width=True):
            st.session_state["sel_hour_range"] = (16, 20)
            st.rerun()
        if preset_cols[2].button("Night (22-06h)", use_container_width=True):
            st.session_state["sel_hour_range"] = (22, 6)
            st.rerun()

        if st.session_state.get("sel_hour_range"):
            sel_hour_range = st.session_state.pop("sel_hour_range")

        is_default_filter = (sel_charger_type == "All" and sel_location == "All" and sel_hour_range == (0, 23))

        st.markdown("<hr style='margin: 14px 0; border-color: #1f2937;'>", unsafe_allow_html=True)
        if st.button("🔄 Clear Cache & Refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.markdown(
            f"""
            <div style="font-size: 0.76rem; color: #64748b; line-height: 1.5; margin-top: 8px;">
                <b>Database:</b> Apache Cassandra<br>
                <b>Keyspace:</b> <code>ev_analytics</code><br>
                <b>Status:</b> {"<span style='color: #6ee7b7;'>Connected</span>" if not db_error and overall_data else "<span style='color: #fca5a5;'>Disconnected</span>"}
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Top Application Header Bar
    st.markdown(
        f"""
        <div class="app-header-box">
            <div>
                <h1 class="app-title">⚡ EV Charge Analytics</h1>
                <p class="app-subtitle">Electric Vehicle Charging Analytics using Apache Spark & Apache Cassandra</p>
                <div style="margin-top: 6px;">
                    <span class="tech-pill">Apache Spark 3.5.1</span>
                    <span class="tech-pill">Apache Cassandra 4.1</span>
                    <span class="tech-pill">Python 3.11</span>
                    <span class="tech-pill">Streamlit & Plotly</span>
                </div>
            </div>
            <div style="text-align: right;">
                <div>
                    {"<span class='status-pill-connected'>● Connected to Cassandra</span>" if not db_error and overall_data else "<span class='status-pill-disconnected'>● Cassandra Disconnected</span>"}
                </div>
                <div style="color: #64748b; font-size: 0.76rem; margin-top: 5px;">
                    Keyspace: <b>ev_analytics</b>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Database connectivity error handling
    if db_error:
        st.error(
            f"❌ **Failed to connect to Apache Cassandra**: `{db_error}`\n\n"
            "**Troubleshooting Checklist:**\n"
            "1. Confirm Cassandra container is running: `docker compose ps`\n"
            "2. If container is stopped: `docker compose up -d`\n"
            "3. Verify analytics tables are loaded: `python scripts/load_cassandra.py`\n\n"
            "*(This dashboard strictly reads from Apache Cassandra and never silently falls back to local CSV files).*"
        )
        if st.button("🔄 Retry Connection"):
            st.cache_data.clear()
            st.rerun()
        st.stop()

    if not overall_data or not hourly_data:
        st.warning(
            "⚠️ **Connected to Cassandra, but analytics tables are empty.**\n\n"
            "Please run the data loader to populate tables: `python scripts/load_cassandra.py`"
        )
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
        st.stop()

    # -------------------------------------------------------------------------
    # 5. Filter Evaluation & Exact Metric Computation
    # -------------------------------------------------------------------------
    if is_default_filter or session_records_df is None:
        active_kpis = {
            "has_data": True,
            "total_sessions": overall_data.get("total_sessions", 72108),
            "total_demand_kwh": overall_data.get("total_demand_kwh", 1267066.08),
            "avg_demand_kwh": overall_data.get("avg_demand_kwh", 17.57),
            "avg_duration_minutes": overall_data.get("avg_duration_minutes", 152.73),
            "min_duration_minutes": overall_data.get("min_duration_minutes", 1.0),
            "max_duration_minutes": overall_data.get("max_duration_minutes", 1573.0),
            "ac_sessions": next((r["session_count"] for r in charger_type_data if r["charger_current_type"] == "AC"), 57398),
            "dc_sessions": next((r["session_count"] for r in charger_type_data if r["charger_current_type"] == "DC"), 14710),
            "ac_share_pct": 79.6,
            "dc_share_pct": 20.4,
            "portfolio_share_pct": 100.0,
        }
        active_hourly = hourly_data
        active_chargers = charger_type_data
        active_locations = location_data
        data_source_label = "Cassandra"
        filter_status_text = "All Charging Sessions (No filters applied)"
    else:
        filtered_df = filter_records(
            session_records_df,
            charger_type=sel_charger_type,
            location=sel_location,
            start_hour_range=sel_hour_range,
        )
        active_kpis = compute_kpis_from_df(filtered_df, portfolio_total_sessions=portfolio_total_sessions)
        active_hourly = aggregate_hourly_from_df(filtered_df)
        active_chargers = aggregate_chargers_from_df(filtered_df)
        active_locations = aggregate_locations_from_df(filtered_df)
        data_source_label = "Filtered Data"

        loc_desc = sel_location.title() if sel_location != "All" else "All Locations"
        chr_desc = sel_charger_type if sel_charger_type != "All" else "All Chargers"
        hr_desc = f"{sel_hour_range[0]:02d}:00–{sel_hour_range[1]:02d}:00"
        filter_status_text = (
            f"Showing: <b>{chr_desc}</b> • <b>{loc_desc}</b> • <b>{hr_desc}</b> "
            f"({format_number(active_kpis['total_sessions'])} sessions, {active_kpis['portfolio_share_pct']}% of total)"
        )

    # Dynamic Analytical Insights
    insights = calculate_filtered_insights(active_kpis, active_hourly, active_chargers, active_locations)

    # Filter State Banner
    st.markdown(
        f"""
        <div class="filter-banner">
            <div class="filter-banner-text">{filter_status_text}</div>
            <div><span class="filter-source-badge">Source: {data_source_label}</span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Empty filter result handling
    if not active_kpis.get("has_data", False) or active_kpis.get("total_sessions", 0) == 0:
        st.markdown(
            f"""
            <div class="empty-state-box">
                <h3 style="color: #f87171; margin-bottom: 8px;">🔍 No Sessions Found</h3>
                <p>No charging sessions found for <b>{sel_charger_type}</b> chargers at <b>{sel_location.title()}</b> between <b>{sel_hour_range[0]:02d}:00 and {sel_hour_range[1]:02d}:00</b>.</p>
                <p style="font-size: 0.85rem; color: #64748b; margin-top: 10px;">Try selecting a different time window or reset filters to 'All'.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    # -------------------------------------------------------------------------
    # 6. Operational Tabs
    # -------------------------------------------------------------------------
    tab_overview, tab_hourly, tab_chargers, tab_locations, tab_about = st.tabs(
        [
            "📊 Overview",
            "⚡ Hourly Usage",
            "🔌 AC vs DC",
            "🏢 Locations",
            "ℹ️ Project Info",
        ]
    )

    # =========================================================================
    # TAB 1: OVERVIEW & KPIS
    # =========================================================================
    with tab_overview:
        st.markdown(
            "<p class='section-desc'>Summary of EV charging sessions and 24-hour energy usage. Values change with selected filters.</p>",
            unsafe_allow_html=True,
        )

        kpi_c1, kpi_c2, kpi_c3, kpi_c4 = st.columns(4)
        with kpi_c1:
            share_sub = f"{active_kpis['portfolio_share_pct']}% of total ({portfolio_total_sessions:,})" if not is_default_filter else "100% of all sessions"
            render_kpi_card("Total Sessions", format_number(active_kpis.get("total_sessions")), share_sub)
        with kpi_c2:
            render_kpi_card("Total Energy", format_energy_kwh(active_kpis.get("total_demand_kwh")), f"{active_kpis.get('total_demand_kwh', 0.0):,.1f} kWh total")
        with kpi_c3:
            render_kpi_card("Avg Energy / Session", f"{format_number(active_kpis.get('avg_demand_kwh'), 2)} <span style='font-size: 1.05rem; color: #94a3b8;'>kWh</span>", "Average per session")
        with kpi_c4:
            min_dur = active_kpis.get("min_duration_minutes", 0.0)
            max_dur = active_kpis.get("max_duration_minutes", 0.0)
            render_kpi_card("Avg Charging Time", format_duration_minutes(active_kpis.get("avg_duration_minutes")), f"Range: {min_dur:.0f}m to {max_dur/60:.1f}h")

        st.markdown("<div style='height: 14px;'></div>", unsafe_allow_html=True)

        # 24-Hour Energy and Frequency Visualizations
        hourly_df = pd.DataFrame(active_hourly).sort_values("start_hour")
        hourly_df["hour_label"] = hourly_df["start_hour"].apply(lambda h: f"{h:02d}:00")

        chart_c1, chart_c2 = st.columns(2)
        with chart_c1:
            fig_demand = go.Figure()
            fig_demand.add_trace(
                go.Scatter(
                    x=hourly_df["hour_label"],
                    y=hourly_df["total_demand_kwh"],
                    mode="lines+markers",
                    name="Energy (kWh)",
                    line=dict(color="#10b981", width=3, shape="spline"),
                    marker=dict(size=6, color="#059669"),
                    fill="tozeroy",
                    fillcolor="rgba(16, 185, 129, 0.12)",
                    hovertemplate="<b>Start Time:</b> %{x}<br><b>Energy:</b> %{y:,.1f} kWh<extra></extra>",
                )
            )
            if insights["peak_hour"] is not None and insights["peak_demand_kwh"] > 0:
                fig_demand.add_annotation(
                    x=f"{insights['peak_hour']:02d}:00",
                    y=insights["peak_demand_kwh"],
                    text=f"Peak: {insights['peak_demand_kwh']:,.0f} kWh",
                    showarrow=True,
                    arrowhead=2,
                    arrowcolor="#ef4444",
                    font=dict(color="#fca5a5", size=11),
                    bgcolor="#7f1d1d",
                    bordercolor="#ef4444",
                )
            fig_demand.update_layout(
                title="<b>24-Hour Energy Usage (kWh)</b>",
                xaxis_title="Start Hour (00:00 to 23:00)",
                yaxis_title="Total Energy (kWh)",
                hovermode="x unified",
                **PLOTLY_DARK_THEME,
            )
            st.plotly_chart(fig_demand, use_container_width=True)

        with chart_c2:
            fig_sessions = px.bar(
                hourly_df,
                x="hour_label",
                y="session_count",
                title="<b>Sessions by Start Hour</b>",
                labels={"hour_label": "Start Hour", "session_count": "Sessions"},
                color="session_count",
                color_continuous_scale=["#0e7490", "#06b6d4", "#10b981"],
            )
            fig_sessions.update_layout(coloraxis_showscale=False, hovermode="x unified", **PLOTLY_DARK_THEME)
            fig_sessions.update_traces(hovertemplate="<b>Start Hour:</b> %{x}<br><b>Sessions:</b> %{y:,}<extra></extra>")
            st.plotly_chart(fig_sessions, use_container_width=True)

        # Key Findings
        st.markdown("#### 💡 Key Findings")
        ins_col1, ins_col2 = st.columns(2)
        peak_hr_str = f"{insights['peak_hour']:02d}:00" if insights["peak_hour"] is not None else "N/A"
        valley_hr_str = f"{insights['valley_hour']:02d}:00" if insights["valley_hour"] is not None else "N/A"
        top_loc_str = insights["top_location"].title() if insights["top_location"] else "N/A"

        with ins_col1:
            render_insight_card(
                f"⚡ Busiest Time: {peak_hr_str}",
                f"Most drivers start charging around <b>{peak_hr_str}</b>, using <b>{insights['peak_demand_kwh']:,.1f} kWh</b> across <b>{insights['peak_sessions']:,} sessions</b>. "
                "This evening rush happens when people plug in after work."
            )
            render_insight_card(
                f"🌙 Quietest Time: {valley_hr_str}",
                f"The lowest activity is at <b>{valley_hr_str}</b> with only <b>{insights['valley_sessions']:,} sessions</b>. "
                "Late night is the best time for charging large fleets without straining the power grid."
            )

        with ins_col2:
            ratio_text = f"<b>{insights['duration_ratio_ac_to_dc']}x longer</b>" if insights["duration_ratio_ac_to_dc"] > 0 else "N/A"
            render_insight_card(
                "🔋 AC vs DC Chargers",
                f"<b>AC (slow):</b> {insights['ac_sessions']:,} sessions (avg: {insights['ac_avg_duration']:.1f} min).<br>"
                f"<b>DC (fast):</b> {insights['dc_sessions']:,} sessions (avg: {insights['dc_avg_duration']:.1f} min).<br>"
                f"AC charging takes {ratio_text} compared to DC."
            )
            render_insight_card(
                f"🏢 Most Popular Location: {top_loc_str}",
                f"<b>{top_loc_str}</b> had the most sessions with <b>{insights['top_location_sessions']:,} sessions</b> "
                f"({insights['top_location_share_pct']}% of this selection)."
            )

    # =========================================================================
    # TAB 2: HOURLY USAGE
    # =========================================================================
    with tab_hourly:
        st.markdown(
            "<p class='section-desc'>See how charging activity changes hour by hour throughout the day (based on when sessions started).</p>",
            unsafe_allow_html=True,
        )

        metric_options = {
            "Total Energy (kWh)": "total_demand_kwh",
            "Session Count": "session_count",
            "Avg Energy per Session (kWh)": "avg_demand_kwh",
            "Avg Duration (Minutes)": "avg_duration_minutes",
        }
        sel_metric_label = st.selectbox("Choose Metric to View Across 24 Hours:", list(metric_options.keys()), index=0)
        selected_metric_col = metric_options[sel_metric_label]

        p_hr, p_val, v_hr, v_val = get_metric_peak_valley(active_hourly, selected_metric_col)

        pv_c1, pv_c2 = st.columns(2)
        with pv_c1:
            p_hr_str = f"{p_hr:02d}:00" if p_hr is not None else "N/A"
            render_kpi_card(f"Highest {sel_metric_label}", f"{p_val:,.2f}", f"Occurs at: <b>{p_hr_str}</b>", border_left="#ef4444")
        with pv_c2:
            v_hr_str = f"{v_hr:02d}:00" if v_hr is not None else "N/A"
            render_kpi_card(f"Lowest {sel_metric_label}", f"{v_val:,.2f}", f"Occurs at: <b>{v_hr_str}</b>", border_left="#06b6d4")

        st.markdown("<div style='height: 14px;'></div>", unsafe_allow_html=True)

        fig_custom_hourly = go.Figure()
        fig_custom_hourly.add_trace(
            go.Scatter(
                x=hourly_df["hour_label"],
                y=hourly_df[selected_metric_col],
                mode="lines+markers",
                name=sel_metric_label,
                line=dict(color="#06b6d4", width=3, shape="spline"),
                marker=dict(size=7, color="#0891b2"),
                fill="tozeroy",
                fillcolor="rgba(6, 182, 212, 0.12)",
                hovertemplate="<b>Time:</b> %{x}<br><b>" + sel_metric_label + ":</b> %{y:,.2f}<extra></extra>",
            )
        )
        if p_hr is not None and p_val > 0:
            fig_custom_hourly.add_annotation(
                x=f"{p_hr:02d}:00",
                y=p_val,
                text=f"Peak: {p_val:,.1f}",
                showarrow=True,
                arrowhead=2,
                arrowcolor="#ef4444",
                font=dict(color="#fca5a5", size=11),
                bgcolor="#7f1d1d",
                bordercolor="#ef4444",
            )
        fig_custom_hourly.update_layout(
            title=f"<b>24-Hour Profile: {sel_metric_label}</b>",
            xaxis_title="Start Hour (00:00 to 23:00)",
            yaxis_title=sel_metric_label,
            hovermode="x unified",
            **PLOTLY_DARK_THEME,
        )
        st.plotly_chart(fig_custom_hourly, use_container_width=True)

        with st.expander("📋 View 24-Hour Data Table"):
            display_hourly = hourly_df[
                ["start_hour", "hour_label", "session_count", "total_demand_kwh", "avg_demand_kwh", "avg_duration_minutes"]
            ].rename(
                columns={
                    "start_hour": "Hour",
                    "hour_label": "Time",
                    "session_count": "Sessions",
                    "total_demand_kwh": "Total Energy (kWh)",
                    "avg_demand_kwh": "Avg Energy (kWh)",
                    "avg_duration_minutes": "Avg Time (min)",
                }
            )
            st.dataframe(display_hourly, use_container_width=True, hide_index=True)

    # =========================================================================
    # TAB 3: AC vs DC CHARGERS
    # =========================================================================
    with tab_chargers:
        st.markdown(
            "<p class='section-desc'>Compare slow destination chargers (AC) with fast transit chargers (DC).</p>",
            unsafe_allow_html=True,
        )

        type_df = pd.DataFrame(active_chargers)
        type_df["type_name"] = type_df["charger_current_type"].apply(
            lambda t: "AC (Slow / Level 2)" if t == "AC" else "DC (Fast / Rapid)"
        )

        ac_row = next((r for r in active_chargers if r["charger_current_type"] == "AC"), None)
        dc_row = next((r for r in active_chargers if r["charger_current_type"] == "DC"), None)

        ac_cnt = ac_row.get("session_count", 0) if ac_row else 0
        dc_cnt = dc_row.get("session_count", 0) if dc_row else 0
        tot_cnt = ac_cnt + dc_cnt
        ac_pct = (ac_cnt / tot_cnt * 100.0) if tot_cnt > 0 else 0.0
        dc_pct = (dc_cnt / tot_cnt * 100.0) if tot_cnt > 0 else 0.0

        ac_energy = ac_row.get("total_demand_kwh", 0.0) if ac_row else 0.0
        dc_energy = dc_row.get("total_demand_kwh", 0.0) if dc_row else 0.0
        tot_energy = ac_energy + dc_energy
        ac_energy_pct = (ac_energy / tot_energy * 100.0) if tot_energy > 0 else 0.0
        dc_energy_pct = (dc_energy / tot_energy * 100.0) if tot_energy > 0 else 0.0

        ac_dwell = ac_row.get("avg_duration_minutes", 0.0) if ac_row else 0.0
        dc_dwell = dc_row.get("avg_duration_minutes", 0.0) if dc_row else 0.0
        ac_avg_dem = ac_row.get("avg_demand_kwh", 0.0) if ac_row else 0.0
        dc_avg_dem = dc_row.get("avg_demand_kwh", 0.0) if dc_row else 0.0

        charger_c1, charger_c2 = st.columns([1, 1])
        with charger_c1:
            st.subheader("🔌 Summary Comparison")
            st.markdown(
                f"""
                <div style="background: #111827; border: 1px solid #1f2937; border-radius: 10px; padding: 14px 18px; margin-bottom: 16px;">
                    <table style="width: 100%; border-collapse: collapse; font-size: 0.86rem; color: #cbd5e1;">
                        <tr style="border-bottom: 1px solid #374151; color: #94a3b8; font-weight: 600;">
                            <th style="padding: 6px 0; text-align: left;">Charger Type</th>
                            <th style="padding: 6px 0; text-align: right;">Sessions</th>
                            <th style="padding: 6px 0; text-align: right;">Total Energy</th>
                            <th style="padding: 6px 0; text-align: right;">Avg Time</th>
                            <th style="padding: 6px 0; text-align: right;">Avg Energy</th>
                        </tr>
                        <tr style="border-bottom: 1px solid #1f2937;">
                            <td style="padding: 8px 0; font-weight: 600; color: #10b981;">AC (Slow)</td>
                            <td style="text-align: right;">{ac_cnt:,} ({ac_pct:.1f}%)</td>
                            <td style="text-align: right;">{format_energy_kwh(ac_energy)} ({ac_energy_pct:.1f}%)</td>
                            <td style="text-align: right;">{ac_dwell:.1f} min</td>
                            <td style="text-align: right;">{ac_avg_dem:.2f} kWh</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0; font-weight: 600; color: #06b6d4;">DC (Fast)</td>
                            <td style="text-align: right;">{dc_cnt:,} ({dc_pct:.1f}%)</td>
                            <td style="text-align: right;">{format_energy_kwh(dc_energy)} ({dc_energy_pct:.1f}%)</td>
                            <td style="text-align: right;">{dc_dwell:.1f} min</td>
                            <td style="text-align: right;">{dc_avg_dem:.2f} kWh</td>
                        </tr>
                    </table>
                </div>
                """,
                unsafe_allow_html=True,
            )

            fig_type_bar = px.bar(
                type_df,
                x="type_name",
                y="session_count",
                text="session_count",
                color="charger_current_type",
                color_discrete_map={"AC": "#10b981", "DC": "#06b6d4"},
                title="<b>Sessions by Charger Type</b>",
                labels={"type_name": "Charger Type", "session_count": "Sessions"},
            )
            fig_type_bar.update_traces(texttemplate="%{text:,}", textposition="outside")
            fig_type_bar.update_layout(showlegend=False, **PLOTLY_DARK_THEME)
            st.plotly_chart(fig_type_bar, use_container_width=True)

        with charger_c2:
            st.subheader("⚡ Energy & Charging Speed")
            fig_type_energy = px.bar(
                type_df,
                x="type_name",
                y="total_demand_kwh",
                text="total_demand_kwh",
                color="charger_current_type",
                color_discrete_map={"AC": "#10b981", "DC": "#06b6d4"},
                title="<b>Total Energy Delivered (kWh)</b>",
                labels={"type_name": "Charger Type", "total_demand_kwh": "Total Energy (kWh)"},
            )
            fig_type_energy.update_traces(texttemplate="%{text:,.0f} kWh", textposition="outside")
            fig_type_energy.update_layout(showlegend=False, **PLOTLY_DARK_THEME)
            st.plotly_chart(fig_type_energy, use_container_width=True)

            speedup_str = f"{ac_dwell / dc_dwell:.1f}x" if dc_dwell > 0 and ac_dwell > 0 else "N/A"
            render_insight_card(
                "⚡ Key Takeaway",
                f"DC fast chargers deliver similar energy in <b>{speedup_str} less time</b> "
                f"({dc_dwell:.1f} min vs {ac_dwell:.1f} min). "
                "DC is ideal for highways and quick stops, while AC is best for homes, apartments, and offices where cars stay parked for hours."
            )

    # =========================================================================
    # TAB 4: LOCATIONS
    # =========================================================================
    with tab_locations:
        st.markdown(
            "<p class='section-desc'>See which locations have the highest charging demand and usage.</p>",
            unsafe_allow_html=True,
        )

        loc_df = pd.DataFrame(active_locations)
        sort_options = {
            "Total Sessions": ("session_count", "Sessions: %{x:,}"),
            "Total Energy (kWh)": ("total_demand_kwh", "Energy: %{x:,.1f} kWh"),
            "Avg Energy per Session (kWh)": ("avg_demand_kwh", "Avg Energy: %{x:.2f} kWh"),
            "Avg Charging Time (Minutes)": ("avg_duration_minutes", "Time: %{x:.1f} min"),
        }
        sel_loc_sort = st.selectbox("Rank Locations By:", list(sort_options.keys()), index=0)
        loc_metric_col, hover_template = sort_options[sel_loc_sort]
        loc_df_sorted = loc_df.sort_values(by=loc_metric_col, ascending=True)

        loc_c1, loc_c2 = st.columns([1.2, 1])
        with loc_c1:
            fig_loc = px.bar(
                loc_df_sorted,
                x=loc_metric_col,
                y="location",
                orientation="h",
                title=f"<b>Locations Ranked by {sel_loc_sort}</b>",
                labels={"location": "Location", loc_metric_col: sel_loc_sort},
                color=loc_metric_col,
                color_continuous_scale=["#0e7490", "#06b6d4", "#10b981"],
            )
            fig_loc.update_layout(coloraxis_showscale=False, **PLOTLY_DARK_THEME)
            fig_loc.update_yaxes(showgrid=False)
            fig_loc.update_traces(hovertemplate="<b>Location:</b> %{y}<br>" + hover_template + "<extra></extra>")
            st.plotly_chart(fig_loc, use_container_width=True)

        with loc_c2:
            st.markdown("#### 📋 Location Leaderboard")
            table_display = loc_df.sort_values(by="session_count", ascending=False)[
                ["location", "session_count", "total_demand_kwh", "avg_demand_kwh", "avg_duration_minutes"]
            ].copy()
            tot_sess = table_display["session_count"].sum()
            table_display["share_pct"] = table_display["session_count"].apply(
                lambda c: f"{(c / tot_sess * 100):.1f}%" if tot_sess > 0 else "0.0%"
            )
            table_display = table_display.rename(
                columns={
                    "location": "Location",
                    "session_count": "Sessions",
                    "total_demand_kwh": "Total Energy (kWh)",
                    "avg_demand_kwh": "Avg Energy (kWh)",
                    "avg_duration_minutes": "Avg Time (min)",
                    "share_pct": "Share",
                }
            )
            st.dataframe(table_display, use_container_width=True, hide_index=True)
            render_insight_card(
                "💡 Location Takeaway",
                "Public areas and apartment complexes account for the most charging sessions "
                "because drivers leave their cars parked there for long periods. These are the best places for AC chargers."
            )

    # =========================================================================
    # TAB 5: PROJECT INFO
    # =========================================================================
    with tab_about:
        st.subheader("ℹ️ Project Architecture & Data Info")
        st.markdown(
            "<p class='section-desc'>End-to-end data pipeline from raw CSV data to PySpark processing and Cassandra storage.</p>",
            unsafe_allow_html=True,
        )

        about_c1, about_c2 = st.columns([1, 1])
        with about_c1:
            st.markdown(
                """
                #### 🌊 Pipeline Architecture
                ```text
                [Raw CSV: 72,856 rows]
                         │
                         ▼
                [PySpark ETL Processing]
                  - Define explicit schema
                  - Remove duplicates & invalid rows
                  - Extract start hour & map AC/DC
                         │
                         ▼
                [Cleaned Parquet: 72,108 rows]
                         │
                    ┌────┴───────────────────────────┐
                    ▼                                ▼
                [PySpark Aggregations]         [Filtered Data]
                  - overall_metrics              - Filter by Charger
                  - hourly_demand                - Filter by Location
                  - charger_type_metrics         - Filter by Time
                  - location_metrics                 │
                    │                                │
                    ▼                                │
                [Apache Cassandra]                   │
                  - Keyspace: ev_analytics           │
                    │                                │
                    └────────────────┬───────────────┘
                                     ▼
                [Streamlit + Plotly Dashboard]
                ```
                """
            )
            st.markdown(
                """
                #### 🗄️ Cassandra Tables
                1. `overall_metrics`: Total sessions, total energy, and duration bounds.
                2. `hourly_demand`: 24 rows showing hourly charging patterns (0..23).
                3. `charger_type_metrics`: Comparison between AC and DC charging.
                4. `location_metrics`: Usage across 10 location types.
                """
            )

        with about_c2:
            st.markdown(
                """
                #### 📊 Dataset & Cleaning Summary
                - **Source:** *Nature Scientific Data* (2024) / IEEE PES Task Force.
                - **Raw Records:** `72,856`
                - **Clean Records:** `72,108`
                - **Removed Records:** `748` (invalid durations, zero demand, or duplicates).
                - **Energy:** Measured in kilowatt-hours (kWh).
                - **Duration:** Measured in minutes.

                #### ⚠️ Important Notes
                1. **No GPS Coordinates:** Station locations are categorized by type (`apartment`, `hotel`, etc.) without exact latitude/longitude to protect user privacy.
                2. **No Battery Details:** Battery capacity and state-of-charge are not part of billing records.
                3. **Start Hour Grouping:** Hourly charts group sessions by when they started.
                """
            )

    # Footer
    st.markdown("<hr style='margin-top: 2rem; margin-bottom: 1rem; border-color: #1f2937;'>", unsafe_allow_html=True)
    st.markdown(
        """
        <div style='text-align: center; color: #64748b; font-size: 0.8rem;'>
            EV Charge Analytics | Apache Spark • Apache Cassandra • Streamlit<br>
            Keyspace: <code>ev_analytics</code> | Verified Records: <code>72,108 sessions</code>
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
