"""
Helper functions for EV Charging Station Analytics Dashboard.
Provides numerical formatting, unit presentation, multi-dimensional filtering,
and data-driven insight calculations.
Kept strictly modular so they can be unit-tested without a running Streamlit or Cassandra runtime.
"""

from typing import Any, Dict, List, Optional, Tuple
import pandas as pd


def format_number(val: Optional[float], decimals: int = 0) -> str:
    """Formats a numeric value with comma separators and specified decimal places."""
    if val is None:
        return "N/A"
    try:
        if decimals == 0:
            return f"{int(round(val)):,}"
        return f"{val:,.{decimals}f}"
    except (ValueError, TypeError):
        return "N/A"


def format_energy_kwh(val: Optional[float]) -> str:
    """Formats energy delivery in kWh with appropriate scale notation (kWh / MWh / GWh)."""
    if val is None:
        return "N/A"
    try:
        val_f = float(val)
        if val_f >= 1_000_000:
            return f"{val_f / 1_000_000:.2f} GWh ({val_f:,.0f} kWh)"
        elif val_f >= 1_000:
            return f"{val_f / 1_000:.1f} MWh ({val_f:,.0f} kWh)"
        return f"{val_f:,.1f} kWh"
    except (ValueError, TypeError):
        return "N/A"


def format_duration_minutes(minutes: Optional[float]) -> str:
    """Formats session duration into human-readable hours and minutes."""
    if minutes is None:
        return "N/A"
    try:
        mins = float(minutes)
        hours = int(mins // 60)
        rem_mins = int(round(mins % 60))
        if hours > 0:
            return f"{hours}h {rem_mins}m ({mins:.1f} min)"
        return f"{mins:.1f} min"
    except (ValueError, TypeError):
        return "N/A"


def calculate_insights(
    overall_data: Optional[Dict[str, Any]],
    hourly_data: List[Dict[str, Any]],
    charger_type_data: List[Dict[str, Any]],
    location_data: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Computes data-driven analytical findings directly from Cassandra query results.
    Never hardcodes metrics.
    """
    insights: Dict[str, Any] = {
        "has_data": False,
        "peak_hour": None,
        "peak_demand_kwh": 0.0,
        "peak_sessions": 0,
        "valley_hour": None,
        "valley_sessions": 0,
        "top_location": None,
        "top_location_sessions": 0,
        "top_location_share_pct": 0.0,
        "ac_sessions": 0,
        "dc_sessions": 0,
        "ac_avg_duration": 0.0,
        "dc_avg_duration": 0.0,
        "duration_ratio_ac_to_dc": 0.0,
        "total_energy_gwh": 0.0,
    }

    if not overall_data or not hourly_data or not charger_type_data or not location_data:
        return insights

    insights["has_data"] = True
    total_sessions = overall_data.get("total_sessions", 0)
    total_demand = overall_data.get("total_demand_kwh", 0.0)
    insights["total_energy_gwh"] = round(total_demand / 1_000_000, 3)

    # 1. Hourly Peak and Valley
    sorted_by_demand = sorted(
        hourly_data, key=lambda x: x.get("total_demand_kwh", 0.0), reverse=True
    )
    if sorted_by_demand:
        peak = sorted_by_demand[0]
        insights["peak_hour"] = peak["start_hour"]
        insights["peak_demand_kwh"] = peak["total_demand_kwh"]
        insights["peak_sessions"] = peak["session_count"]

    sorted_by_sessions = sorted(
        hourly_data, key=lambda x: x.get("session_count", 0)
    )
    if sorted_by_sessions:
        valley = sorted_by_sessions[0]
        insights["valley_hour"] = valley["start_hour"]
        insights["valley_sessions"] = valley["session_count"]

    # 2. Location Insights
    sorted_locations = sorted(
        location_data, key=lambda x: x.get("session_count", 0), reverse=True
    )
    if sorted_locations and total_sessions > 0:
        top_loc = sorted_locations[0]
        insights["top_location"] = top_loc["location"]
        insights["top_location_sessions"] = top_loc["session_count"]
        insights["top_location_share_pct"] = round(
            (top_loc["session_count"] / total_sessions) * 100.0, 1
        )

    # 3. Charger Type Comparison
    type_map = {r["charger_current_type"]: r for r in charger_type_data}
    ac_row = type_map.get("AC")
    dc_row = type_map.get("DC")

    if ac_row:
        insights["ac_sessions"] = ac_row.get("session_count", 0)
        insights["ac_avg_duration"] = ac_row.get("avg_duration_minutes", 0.0)

    if dc_row:
        insights["dc_sessions"] = dc_row.get("session_count", 0)
        insights["dc_avg_duration"] = dc_row.get("avg_duration_minutes", 0.0)

    if insights["dc_avg_duration"] > 0:
        insights["duration_ratio_ac_to_dc"] = round(
            insights["ac_avg_duration"] / insights["dc_avg_duration"], 1
        )

    return insights


def get_metric_peak_valley(
    hourly_data: List[Dict[str, Any]],
    metric_col: str,
) -> Tuple[Optional[int], float, Optional[int], float]:
    """
    Finds the peak and valley (minimum) hour and value for a given metric across hourly records.
    Returns: (peak_hour, peak_val, valley_hour, valley_val)
    """
    if not hourly_data or not metric_col:
        return None, 0.0, None, 0.0

    valid_rows = [r for r in hourly_data if metric_col in r and r[metric_col] is not None]
    if not valid_rows:
        return None, 0.0, None, 0.0

    peak_row = max(valid_rows, key=lambda r: r.get(metric_col, 0.0))
    valley_row = min(valid_rows, key=lambda r: r.get(metric_col, 0.0))

    return (
        peak_row.get("start_hour"),
        float(peak_row.get(metric_col, 0.0)),
        valley_row.get("start_hour"),
        float(valley_row.get(metric_col, 0.0)),
    )


# =============================================================================
# Multi-Dimensional Filtering & Dynamic Aggregation Functions
# =============================================================================


def filter_records(
    df: pd.DataFrame,
    charger_type: str = "All",
    location: str = "All",
    start_hour_range: Tuple[int, int] = (0, 23),
) -> pd.DataFrame:
    """
    Applies multi-dimensional filters on session-level records.

    Parameters:
        df: Pandas DataFrame with columns ['charger_current_type', 'Location', 'start_hour', 'Demand', 'Duration']
        charger_type: 'All', 'AC', or 'DC'
        location: 'All' or specific location name
        start_hour_range: (min_hour, max_hour) inclusive, e.g. (0, 23)

    Returns:
        Filtered copy of the DataFrame.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    mask = pd.Series(True, index=df.index)

    if charger_type and charger_type != "All":
        # Handle labels like 'AC (Level 2)' or simple 'AC'
        ct_clean = charger_type.split()[0].upper()
        if "charger_current_type" in df.columns:
            mask &= (df["charger_current_type"].str.upper() == ct_clean)

    if location and location != "All":
        loc_clean = location.strip().lower()
        if "Location" in df.columns:
            mask &= (df["Location"].astype(str).str.strip().str.lower() == loc_clean)

    if start_hour_range and "start_hour" in df.columns:
        min_h, max_h = start_hour_range
        mask &= (df["start_hour"] >= min_h) & (df["start_hour"] <= max_h)

    return df.loc[mask].copy()


def compute_kpis_from_df(
    df: pd.DataFrame,
    portfolio_total_sessions: int = 72108,
) -> Dict[str, Any]:
    """
    Computes exact numerical KPIs and metrics from a filtered session DataFrame.
    Averages are strictly calculated as sum / count to avoid Simpson's paradox or averaging averages.
    """
    if df is None or df.empty:
        return {
            "has_data": False,
            "total_sessions": 0,
            "total_demand_kwh": 0.0,
            "avg_demand_kwh": 0.0,
            "avg_duration_minutes": 0.0,
            "min_duration_minutes": 0.0,
            "max_duration_minutes": 0.0,
            "ac_sessions": 0,
            "dc_sessions": 0,
            "ac_share_pct": 0.0,
            "dc_share_pct": 0.0,
            "portfolio_share_pct": 0.0,
        }

    total_sessions = int(len(df))
    total_demand = float(df["Demand"].sum()) if "Demand" in df.columns else 0.0
    total_duration = float(df["Duration"].sum()) if "Duration" in df.columns else 0.0

    avg_demand = round(total_demand / total_sessions, 2) if total_sessions > 0 else 0.0
    avg_duration = round(total_duration / total_sessions, 2) if total_sessions > 0 else 0.0
    min_duration = round(float(df["Duration"].min()), 2) if (total_sessions > 0 and "Duration" in df.columns) else 0.0
    max_duration = round(float(df["Duration"].max()), 2) if (total_sessions > 0 and "Duration" in df.columns) else 0.0

    ac_count = int((df["charger_current_type"] == "AC").sum()) if "charger_current_type" in df.columns else 0
    dc_count = int((df["charger_current_type"] == "DC").sum()) if "charger_current_type" in df.columns else 0

    ac_share = round((ac_count / total_sessions) * 100.0, 1) if total_sessions > 0 else 0.0
    dc_share = round((dc_count / total_sessions) * 100.0, 1) if total_sessions > 0 else 0.0

    portfolio_share = (
        round((total_sessions / portfolio_total_sessions) * 100.0, 1)
        if portfolio_total_sessions > 0
        else 0.0
    )

    return {
        "has_data": True,
        "total_sessions": total_sessions,
        "total_demand_kwh": round(total_demand, 2),
        "avg_demand_kwh": avg_demand,
        "avg_duration_minutes": avg_duration,
        "min_duration_minutes": min_duration,
        "max_duration_minutes": max_duration,
        "ac_sessions": ac_count,
        "dc_sessions": dc_count,
        "ac_share_pct": ac_share,
        "dc_share_pct": dc_share,
        "portfolio_share_pct": portfolio_share,
    }


def aggregate_hourly_from_df(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Computes hourly aggregation from DataFrame across hours 0..23.
    Guarantees all 24 hours are present in ascending order (0 to 23).
    Hours with 0 records are explicitly zero-filled to maintain uninterrupted 24h curve.
    """
    hourly_records: List[Dict[str, Any]] = []

    if df is None or df.empty or "start_hour" not in df.columns:
        for h in range(24):
            hourly_records.append({
                "start_hour": h,
                "session_count": 0,
                "total_demand_kwh": 0.0,
                "avg_demand_kwh": 0.0,
                "avg_duration_minutes": 0.0,
            })
        return hourly_records

    grouped = df.groupby("start_hour")
    summary = grouped.agg(
        session_count=("Demand", "count"),
        total_demand_kwh=("Demand", "sum"),
        avg_demand_kwh=("Demand", "mean"),
        avg_duration_minutes=("Duration", "mean"),
    ).to_dict("index")

    for h in range(24):
        if h in summary:
            row = summary[h]
            hourly_records.append({
                "start_hour": h,
                "session_count": int(row["session_count"]),
                "total_demand_kwh": round(float(row["total_demand_kwh"]), 2),
                "avg_demand_kwh": round(float(row["avg_demand_kwh"]), 2),
                "avg_duration_minutes": round(float(row["avg_duration_minutes"]), 2),
            })
        else:
            hourly_records.append({
                "start_hour": h,
                "session_count": 0,
                "total_demand_kwh": 0.0,
                "avg_demand_kwh": 0.0,
                "avg_duration_minutes": 0.0,
            })

    return hourly_records


def aggregate_chargers_from_df(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Aggregates charger types (AC, DC) from filtered records.
    Always returns structured records for AC and DC.
    """
    if df is None or df.empty or "charger_current_type" not in df.columns:
        return [
            {"charger_current_type": "AC", "session_count": 0, "total_demand_kwh": 0.0, "avg_demand_kwh": 0.0, "avg_duration_minutes": 0.0},
            {"charger_current_type": "DC", "session_count": 0, "total_demand_kwh": 0.0, "avg_demand_kwh": 0.0, "avg_duration_minutes": 0.0},
        ]

    results: List[Dict[str, Any]] = []
    for c_type in ["AC", "DC"]:
        sub = df[df["charger_current_type"] == c_type]
        count_c = len(sub)
        if count_c > 0:
            tot_dem = float(sub["Demand"].sum())
            tot_dur = float(sub["Duration"].sum())
            results.append({
                "charger_current_type": c_type,
                "session_count": count_c,
                "total_demand_kwh": round(tot_dem, 2),
                "avg_demand_kwh": round(tot_dem / count_c, 2),
                "avg_duration_minutes": round(tot_dur / count_c, 2),
            })
        else:
            results.append({
                "charger_current_type": c_type,
                "session_count": 0,
                "total_demand_kwh": 0.0,
                "avg_demand_kwh": 0.0,
                "avg_duration_minutes": 0.0,
            })
    return results


def aggregate_locations_from_df(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Aggregates location categories from filtered records, sorted descending by session_count.
    Includes share_pct relative to the current slice total.
    """
    if df is None or df.empty or "Location" not in df.columns:
        return []

    grouped = df.groupby("Location").agg(
        session_count=("Demand", "count"),
        total_demand_kwh=("Demand", "sum"),
        avg_demand_kwh=("Demand", "mean"),
        avg_duration_minutes=("Duration", "mean"),
    ).reset_index()

    total_sessions = float(grouped["session_count"].sum())
    results: List[Dict[str, Any]] = []

    for _, row in grouped.iterrows():
        cnt = int(row["session_count"])
        share = round((cnt / total_sessions * 100.0), 1) if total_sessions > 0 else 0.0
        results.append({
            "location": str(row["Location"]),
            "session_count": cnt,
            "total_demand_kwh": round(float(row["total_demand_kwh"]), 2),
            "avg_demand_kwh": round(float(row["avg_demand_kwh"]), 2),
            "avg_duration_minutes": round(float(row["avg_duration_minutes"]), 2),
            "share_pct": share,
        })

    results.sort(key=lambda x: x["session_count"], reverse=True)
    return results


def calculate_filtered_insights(
    kpis: Dict[str, Any],
    hourly_data: List[Dict[str, Any]],
    charger_data: List[Dict[str, Any]],
    location_data: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Computes data-driven analytical observations for the active filtered selection.
    Safely handles empty subsets.
    """
    insights: Dict[str, Any] = {
        "has_data": False,
        "peak_hour": None,
        "peak_demand_kwh": 0.0,
        "peak_sessions": 0,
        "valley_hour": None,
        "valley_sessions": 0,
        "top_location": None,
        "top_location_sessions": 0,
        "top_location_share_pct": 0.0,
        "ac_sessions": 0,
        "dc_sessions": 0,
        "ac_avg_duration": 0.0,
        "dc_avg_duration": 0.0,
        "duration_ratio_ac_to_dc": 0.0,
        "total_energy_gwh": 0.0,
    }

    if not kpis.get("has_data", False):
        return insights

    insights["has_data"] = True
    total_demand = kpis.get("total_demand_kwh", 0.0)
    total_sessions = kpis.get("total_sessions", 0)
    insights["total_energy_gwh"] = round(total_demand / 1_000_000, 3)

    # Hourly peak/valley (only considering hours with > 0 sessions)
    active_hours = [h for h in hourly_data if h.get("session_count", 0) > 0]
    if active_hours:
        peak = max(active_hours, key=lambda x: x.get("total_demand_kwh", 0.0))
        valley = min(active_hours, key=lambda x: x.get("session_count", 0))
        insights["peak_hour"] = peak["start_hour"]
        insights["peak_demand_kwh"] = peak["total_demand_kwh"]
        insights["peak_sessions"] = peak["session_count"]
        insights["valley_hour"] = valley["start_hour"]
        insights["valley_sessions"] = valley["session_count"]

    # Top Location
    if location_data and total_sessions > 0:
        top_loc = location_data[0]
        insights["top_location"] = top_loc["location"]
        insights["top_location_sessions"] = top_loc["session_count"]
        insights["top_location_share_pct"] = round(
            (top_loc["session_count"] / total_sessions) * 100.0, 1
        )

    # Charger AC/DC
    type_map = {r["charger_current_type"]: r for r in charger_data}
    ac_row = type_map.get("AC")
    dc_row = type_map.get("DC")

    if ac_row:
        insights["ac_sessions"] = ac_row.get("session_count", 0)
        insights["ac_avg_duration"] = ac_row.get("avg_duration_minutes", 0.0)

    if dc_row:
        insights["dc_sessions"] = dc_row.get("session_count", 0)
        insights["dc_avg_duration"] = dc_row.get("avg_duration_minutes", 0.0)

    if insights["dc_avg_duration"] > 0 and insights["ac_avg_duration"] > 0:
        insights["duration_ratio_ac_to_dc"] = round(
            insights["ac_avg_duration"] / insights["dc_avg_duration"], 1
        )

    return insights
