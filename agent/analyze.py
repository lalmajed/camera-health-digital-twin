"""
analyze.py

Agentic reasoning layer for the Riyadh Camera Health Digital Twin.

Exposes a single function:

    analyze_result(question: str, result: Any) -> str

It inspects the shape of `result` coming from the Java backend and
returns a clear, domain-aware narrative answer.
"""

from typing import Any, Dict, List, Tuple, Optional


# ------------------------------------------------------------
# Helper: detect what type of tool result this is
# ------------------------------------------------------------

def detect_result_type(result: Any) -> str:
    """
    Best-effort classification of the backend JSON shape.

    Returns one of:
        - "error"
        - "city_totals"
        - "site_totals"
        - "site_day_status"
        - "vehicle_degrade"
        - "trips"
        - "unknown"
    """
    if isinstance(result, dict):
        if "error" in result:
            return "error"

        if "uniqueVehicles" in result and "site" in result:
            return "site_totals"

        if "uniqueVehicles" in result and "site" not in result:
            return "city_totals"

        if {"detectionsTotal", "detectionsGood", "detectionsBad"}.issubset(result.keys()):
            return "site_day_status"

        if "plate" in result and "cumNFrames" in result:
            return "vehicle_degrade"

        return "unknown"

    if isinstance(result, list):
        if not result:
            # empty list – probably trips query with no data
            return "trips"

        first = result[0]
        if isinstance(first, dict) and {"plate", "day", "window30"}.issubset(first.keys()):
            return "trips"

    return "unknown"


# ------------------------------------------------------------
# Format helpers
# ------------------------------------------------------------

def pct(part: float, whole: float) -> float:
    if not whole:
        return 0.0
    return round(100.0 * part / whole, 1)


def format_bool_flag(flag: bool, label: str) -> Optional[str]:
    return label if flag else None


# ------------------------------------------------------------
# Analyzers per result type
# ------------------------------------------------------------

def analyze_error(question: str, result: Dict[str, Any]) -> str:
    msg = result.get("error", "Unknown backend error.")
    return (
        f"I tried to answer your question:\n\n"
        f'  "{question}"\n\n'
        f"but the backend returned an error:\n\n"
        f"  {msg}\n\n"
        f"This usually means there is no data for that day/site/plate combination "
        f"or the identifiers don't exist in the twin."
    )


def analyze_city_totals(question: str, result: Dict[str, Any]) -> str:
    day = result.get("day", "?")
    unique_veh = result.get("uniqueVehicles", 0)
    always_deg = result.get("alwaysDegraded", 0)
    not_always = result.get("notAlwaysDegraded", 0)

    always_pct = pct(always_deg, unique_veh)
    not_always_pct = pct(not_always, unique_veh)

    # simple health heuristic at city level
    if always_pct < 2:
        health = "excellent"
    elif always_pct < 5:
        health = "good"
    elif always_pct < 10:
        health = "borderline"
    else:
        health = "concerning"

    return (
        f"**City-wide vehicle degradation on {day}**\n\n"
        f"- Unique vehicles: **{unique_veh:,}**\n"
        f"- Always degraded: **{always_deg:,}** ({always_pct}%)\n"
        f"- Not always degraded: **{not_always:,}** ({not_always_pct}%)\n\n"
        f"**Interpretation**\n"
        f"- City health on this day is **{health}** based on the share of always-degraded vehicles.\n"
        f"- Always-degraded means the vehicle looked bad everywhere it appeared that day, "
        f"which strongly suggests a **vehicle-side issue** rather than a site-only problem.\n\n"
        f"If you want, you can drill into:\n"
        f"- A specific site with: *“How many degraded vehicles were at RUHSMxxx on {day}?”*\n"
        f"- A specific plate with: *“Show the degradation history for plate ABC1234.”*"
    )


def analyze_site_totals(question: str, result: Dict[str, Any]) -> str:
    day = result.get("day", "?")
    site = result.get("site", "?")
    unique_veh = result.get("uniqueVehicles", 0)
    always_deg = result.get("alwaysDegraded", 0)
    not_always = result.get("notAlwaysDegraded", 0)

    always_pct = pct(always_deg, unique_veh)
    not_always_pct = pct(not_always, unique_veh)

    # heuristics for site behaviour
    if always_pct < 2:
        site_health = "very healthy"
    elif always_pct < 5:
        site_health = "healthy"
    elif always_pct < 10:
        site_health = "borderline"
    else:
        site_health = "suspicious"

    lines = [
        f"**Site-level degraded vehicles — {site} on {day}**",
        "",
        f"- Unique vehicles seen: **{unique_veh:,}**",
        f"- Always degraded: **{always_deg:,}** ({always_pct}%)",
        f"- Not always degraded: **{not_always:,}** ({not_always_pct}%)",
        "",
        f"**Interpretation**",
        f"- This site on that day looks **{site_health}** from a vehicle degradation perspective."
    ]

    if site_health in ("borderline", "suspicious"):
        lines.append(
            "- A high share of always-degraded vehicles suggests something systematic "
            "at this site: alignment, illumination, or configuration."
        )
        lines.append(
            "- Cross-check this with trip patterns: do many of these vehicles look fine "
            "at other sites the same day?"
        )
    else:
        lines.append(
            "- Most vehicles either look fine or only occasionally degraded here, "
            "which is consistent with normal traffic and environment."
        )

    return "\n".join(lines)


def analyze_site_day_status(question: str, result: Dict[str, Any]) -> str:
    day = result.get("day", "?")
    site = result.get("site", "?")
    total = result.get("detectionsTotal", 0)
    good = result.get("detectionsGood", 0)
    bad = result.get("detectionsBad", 0)
    good_rate_pct = result.get("goodRatePct", None)
    status = result.get("status", "") or "unknown"
    color = result.get("color", "") or "none"

    if good_rate_pct is None and total:
        good_rate_pct = pct(good, total)

    # categorize if status is missing
    if status == "unknown":
        if good_rate_pct is None:
            qualitative = "no data"
        elif good_rate_pct >= 90:
            qualitative = "healthy"
        elif good_rate_pct >= 80:
            qualitative = "slightly degraded"
        elif good_rate_pct >= 60:
            qualitative = "borderline"
        else:
            qualitative = "heavily degraded"
    else:
        qualitative = status

    lines = [
        f"**Daily site health — {site} on {day}**",
        "",
        f"- Total detections: **{total:,}**",
        f"- Good detections: **{good:,}**",
        f"- Bad detections: **{bad:,}**",
        f"- Good rate: **{good_rate_pct if good_rate_pct is not None else 0:.1f}%**",
        f"- Status flag: **{status}** (color: `{color}`)",
        "",
        f"**Interpretation**",
        f"- Overall this site is **{qualitative}** on that day based on the good-rate percentage."
    ]

    if good_rate_pct is not None:
        if good_rate_pct >= 90:
            lines.append(
                "- This looks like a strong, clean day. Any vehicle issues here are more likely "
                "to be vehicle-specific rather than a site fault."
            )
        elif 80 <= good_rate_pct < 90:
            lines.append(
                "- Slight degradation: some conditions (e.g., lighting at specific hours or certain lanes) "
                "may be reducing quality, but the site is broadly OK."
            )
        elif 60 <= good_rate_pct < 80:
            lines.append(
                "- Borderline performance: you should inspect **per-lane / per-hour** breakdowns "
                "to see if issues cluster at specific times or directions."
            )
        else:
            lines.append(
                "- Heavy degradation: this likely indicates a **site-level problem** (alignment, focus, "
                "camera cleanliness, or configuration). This site should be prioritized for maintenance."
            )

    return "\n".join(lines)


def analyze_vehicle_degrade(question: str, result: Dict[str, Any]) -> str:
    plate = result.get("plate", "?")
    day = result.get("day") or "latest day in index"
    vehicle_type = result.get("vehicleType", None)
    vehicle_label = result.get("vehicleLabel", "") or "unknown type"
    first_day = result.get("firstDay") or "unknown"
    cum_min_q = result.get("cumMinQ", 0.0)
    cum_max_q = result.get("cumMaxQ", 0.0)
    frames = result.get("cumNFrames", 0)
    always = result.get("alwaysDegraded", False)

    health_desc = "always degraded" if always else "not always degraded"

    lines = [
        f"**Vehicle degradation profile — plate {plate}**",
        "",
        f"- Vehicle type: **{vehicle_label}** (code: {vehicle_type})",
        f"- First seen in degraded stats: **{first_day}**",
        f"- Latest record day: **{day}**",
        f"- Frames considered: **{frames:,}**",
        f"- Cumulative quality range: **{cum_min_q:.3f} – {cum_max_q:.3f}**",
        f"- Classification: **{health_desc}**",
        "",
        f"**Interpretation**"
    ]

    if always:
        lines.append(
            "- This vehicle is consistently degraded wherever it appears. "
            "That strongly suggests a **vehicle-side issue** (dirty plate, mounting angle, damage) "
            "rather than a site-only problem."
        )
    else:
        lines.append(
            "- This vehicle is sometimes good and sometimes bad. "
            "That typically means it's useful for **site diagnostics**: "
            "compare which sites see it with good quality versus poor quality."
        )

    lines.append(
        "If you want, you can ask next: "
        "\"Show all trips for this plate\" or "
        "\"Which sites does this vehicle look worst at?\""
    )

    return "\n".join(lines)


def analyze_trips(question: str, result: List[Dict[str, Any]]) -> str:
    if not result:
        return (
            "There are no trips matching that query.\n\n"
            "That usually means the plate did not appear in the 30-minute trip index "
            "for that day, or it was filtered out during preprocessing."
        )

    n_trips = len(result)
    plate = result[0].get("plate", "unknown plate")

    # aggregate basic stats
    sites_seen = set()
    min_q_values = []
    max_q_values = []
    issues_flags = []

    for trip in result:
        for s in trip.get("siteList", []) or []:
            sites_seen.add(s)

        min_q_values.append(trip.get("minQuality", 0.0))
        max_q_values.append(trip.get("maxQuality", 0.0))

        label = trip.get("issueLabel")
        if label:
            issues_flags.append(label)

    global_min_q = min(min_q_values) if min_q_values else 0.0
    global_max_q = max(max_q_values) if max_q_values else 0.0

    # simple health judgement
    if global_min_q >= 0.7:
        qual_summary = "consistently good quality across trips"
    elif global_min_q >= 0.5:
        qual_summary = "mostly acceptable, with some weaker segments"
    elif global_min_q >= 0.3:
        qual_summary = "mixed and often weak quality"
    else:
        qual_summary = "frequently very poor quality"

    lines = [
        f"**Trip summary for plate {plate}**",
        "",
        f"- Number of trips: **{n_trips}**",
        f"- Distinct sites visited: **{len(sites_seen)}** ({', '.join(sorted(sites_seen))})",
        f"- Overall min quality across trips: **{global_min_q:.3f}**",
        f"- Overall max quality across trips: **{global_max_q:.3f}**",
        f"- Quality summary: **{qual_summary}**",
        "",
        "**Trip-by-trip view**"
    ]

    # brief per-trip breakdown
    for idx, trip in enumerate(result, start=1):
        day = trip.get("day", "?")
        window = trip.get("window30", "")
        hour = trip.get("hour", None)
        sites = trip.get("siteList", []) or []
        min_q = trip.get("minQuality", 0.0)
        max_q = trip.get("maxQuality", 0.0)
        route = trip.get("routeSig", "") or "unknown route"
        issue_label = trip.get("issueLabel") or "none"

        lines.append(
            f"- Trip {idx}: day {day}, window {window} (hour {hour}), "
            f"route **{route}**, minQ={min_q:.3f}, maxQ={max_q:.3f}, "
            f"sites={','.join(sites)}, issue={issue_label}"
        )

    # high-level interpretation of issue labels
    if issues_flags:
        lines.append("")
        lines.append("**Issue label interpretation**")
        if any("car_issue" in lbl for lbl in issues_flags):
            lines.append(
                "- Trips flagged with `car_issue_singleton` or similar usually point to **vehicle-specific problems** "
                "rather than a general site fault."
            )
        if any("site" in lbl for lbl in issues_flags):
            lines.append(
                "- Any trips with `siteIssueStrict` suggest that particular sites are dragging quality down "
                "for many vehicles, not just this one."
            )
        if any("mixed" in lbl for lbl in issues_flags):
            lines.append(
                "- `mixed_quality` means some frames in the trip are good and others are bad, "
                "often due to partial occlusion or transient lighting effects."
            )

    return "\n".join(lines)


# ------------------------------------------------------------
# Public entrypoint
# ------------------------------------------------------------

def analyze_result(question: str, result: Any) -> str:
    """
    Main entrypoint called from agent.py.

    It:
      1) detects result type
      2) routes to specialised analyzer
      3) returns a human-readable narrative answer
    """
    rtype = detect_result_type(result)

    if rtype == "error":
        return analyze_error(question, result)
    if rtype == "city_totals":
        return analyze_city_totals(question, result)
    if rtype == "site_totals":
        return analyze_site_totals(question, result)
    if rtype == "site_day_status":
        return analyze_site_day_status(question, result)
    if rtype == "vehicle_degrade":
        return analyze_vehicle_degrade(question, result)
    if rtype == "trips":
        return analyze_trips(question, result)

    # Fallback
    return (
        f"I received data from the backend, but I don't recognize its structure.\n\n"
        f"Question was:\n  \"{question}\"\n\n"
        f"Raw result:\n{result}"
    )
