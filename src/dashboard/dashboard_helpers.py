"""
Helper functions for EV Charging Station Analytics Dashboard.
Provides numerical formatting, unit presentation, and data-driven insight calculations.
Kept strictly modular so they can be unit-tested without a running Streamlit or Cassandra runtime.
"""

from typing import Any, Dict, List, Optional


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
