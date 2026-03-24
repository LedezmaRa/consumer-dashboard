"""Render a static HTML dashboard from dashboard-ready data."""

from __future__ import annotations

from html import escape

from consumer_dashboard.dashboard.datasets import build_dashboard_data
from consumer_dashboard.storage.filesystem import ensure_project_directories, write_json


def _unit_label(unit: str) -> str:
    mapping = {
        "percent": "Percent",
        "claims": "Claims",
        "trillions_of_dollars": "Trillions of dollars",
        "millions_of_dollars": "Millions of dollars",
        "dollars_per_hour": "Dollars per hour",
        "thousands_of_jobs": "Thousands of jobs",
        "thousands_of_persons": "Thousands of persons",
        "annual_rate_thousands_units": "Annual rate, thousands",
        "current dollars; level": "Current dollars",
        "chained dollars; level": "Chained dollars",
        "index_1982_84_100": "Index",
        "fisher price index; level": "Index",
        "real_proxy_index": "Index",
        "ratio; level": "Ratio",
        "score": "Score",
        "billions_of_dollars": "Billions of dollars",
    }
    return mapping.get(unit.lower(), unit.replace("_", " ").title()) if unit else "Value"


def _point_key(mode: str) -> str:
    return "rebased_points" if mode == "rebased" else "raw_points"


# ---------------------------------------------------------------------------
# Smart Y-axis formatting
# ---------------------------------------------------------------------------

def _smart_y_format(value: float, unit: str, value_range: float) -> str:
    """Format axis labels intelligently based on unit type and data range."""
    unit_lower = unit.lower() if unit else ""
    if unit_lower == "percent" or "percent" in unit_lower:
        if value_range < 2:
            return f"{value:.2f}%"
        if value_range < 10:
            return f"{value:.1f}%"
        return f"{value:.0f}%"
    if "trillion" in unit_lower:
        return f"${value:.1f}T"
    if "million" in unit_lower:
        if abs(value) >= 1_000_000:
            return f"${value / 1_000_000:.1f}T"
        if abs(value) >= 1_000:
            return f"${value / 1_000:.0f}B"
        return f"${value:.0f}M"
    if "claim" in unit_lower:
        if abs(value) >= 1000:
            return f"{value / 1000:.0f}k"
        return f"{value:.0f}"
    if "thousand" in unit_lower:
        if abs(value) >= 1000:
            return f"{value / 1000:.1f}M"
        return f"{value:.0f}k"
    if "dollar" in unit_lower:
        return f"${value:.2f}"
    if "index" in unit_lower:
        if value_range < 5:
            return f"{value:.1f}"
        return f"{value:.0f}"
    # Rebased or generic
    if value_range < 2:
        return f"{value:.2f}"
    if value_range < 20:
        return f"{value:.1f}"
    return f"{value:.0f}"


# ---------------------------------------------------------------------------
# Nice grid ticks (round numbers)
# ---------------------------------------------------------------------------

def _nice_ticks(minimum: float, maximum: float, target_count: int = 5) -> list[float]:
    """Generate human-friendly tick values spanning the data range."""
    data_range = maximum - minimum
    if data_range == 0:
        return [minimum]
    rough_step = data_range / max(target_count - 1, 1)
    # Round step to a nice number
    import math
    magnitude = 10 ** math.floor(math.log10(abs(rough_step)) if rough_step != 0 else 0)
    residual = rough_step / magnitude
    if residual <= 1.5:
        nice_step = 1.0 * magnitude
    elif residual <= 3.0:
        nice_step = 2.0 * magnitude
    elif residual <= 7.0:
        nice_step = 5.0 * magnitude
    else:
        nice_step = 10.0 * magnitude
    tick_min = math.floor(minimum / nice_step) * nice_step
    tick_max = math.ceil(maximum / nice_step) * nice_step
    ticks = []
    current = tick_min
    while current <= tick_max + nice_step * 0.01:
        ticks.append(round(current, 10))
        current += nice_step
    return ticks


# ---------------------------------------------------------------------------
# Anti-collision label placement
# ---------------------------------------------------------------------------

def _resolve_label_positions(
    labels: list[tuple[float, str, int]],
    min_gap: float = 13.0,
    pad_top: float = 20.0,
    pad_bottom: float = 280.0,
) -> list[tuple[float, str, int]]:
    """Push overlapping end-of-line labels apart vertically.

    Input: list of (y_position, label_text, series_index).
    Returns the same tuples with adjusted y positions.
    """
    if not labels:
        return labels
    sorted_labels = sorted(labels, key=lambda item: item[0])
    positions = [item[0] for item in sorted_labels]
    # Push overlapping labels apart
    for _ in range(20):
        changed = False
        for i in range(1, len(positions)):
            gap = positions[i] - positions[i - 1]
            if gap < min_gap:
                shift = (min_gap - gap) / 2
                positions[i - 1] -= shift
                positions[i] += shift
                changed = True
        if not changed:
            break
    # Clamp within bounds
    for i in range(len(positions)):
        positions[i] = max(pad_top, min(pad_bottom, positions[i]))
    return [(positions[i], sorted_labels[i][1], sorted_labels[i][2]) for i in range(len(sorted_labels))]


# ---------------------------------------------------------------------------
# Chart dimensions
# ---------------------------------------------------------------------------

_CHART_WIDTH = 920
_CHART_HEIGHT = 310
_PAD_LEFT = 60
_PAD_RIGHT = 170
_PAD_TOP = 28
_PAD_BOTTOM = 38


# ---------------------------------------------------------------------------
# Unique IDs for SVG gradients
# ---------------------------------------------------------------------------

_svg_id_counter = 0


def _next_svg_id() -> str:
    global _svg_id_counter
    _svg_id_counter += 1
    return f"cg{_svg_id_counter}"


# ---------------------------------------------------------------------------
# Core chart builder — single-axis
# ---------------------------------------------------------------------------

def _single_axis_chart_svg(
    series: list[dict[str, object]],
    mode: str,
    *,
    axis_label: str = "",
    chart_uid: str = "",
    reference_lines: list[dict] | None = None,
) -> str:
    if not series:
        return '<div class="chart-empty">Not enough history yet.</div>'

    width = _CHART_WIDTH
    height = _CHART_HEIGHT
    pad_left = _PAD_LEFT
    pad_right = _PAD_RIGHT
    pad_top = _PAD_TOP
    pad_bottom = _PAD_BOTTOM
    point_key = _point_key(mode)

    all_values = [float(point["value"]) for item in series for point in item.get(point_key, [])]
    if not all_values:
        return '<div class="chart-empty">Not enough history yet.</div>'
    data_min = min(all_values)
    data_max = max(all_values)
    if data_max == data_min:
        data_max += 1.0
        data_min -= 1.0
    data_range = data_max - data_min

    # Compute nice ticks
    ticks = _nice_ticks(data_min, data_max, target_count=6)
    tick_min = ticks[0]
    tick_max = ticks[-1]
    tick_range = tick_max - tick_min if tick_max != tick_min else 1.0

    steps = max(len(series[0][point_key]) - 1, 1)
    usable_width = width - pad_left - pad_right
    usable_height = height - pad_top - pad_bottom

    # Determine the representative unit from first series
    representative_unit = str(series[0].get("unit", "")) if series else ""
    is_percent = representative_unit.lower() in ("percent", "score") or mode == "rebased"

    def scale_x(index: int) -> float:
        return pad_left + (usable_width * (index / steps))

    def scale_y(value: float) -> float:
        return pad_top + usable_height - ((value - tick_min) / tick_range * usable_height)

    if not chart_uid:
        chart_uid = _next_svg_id()

    # --- Defs: gradients for area fills ---
    defs = ['<defs>']
    for index in range(len(series)):
        grad_id = f"{chart_uid}-grad-{index}"
        defs.append(
            f'<linearGradient id="{grad_id}" x1="0" x2="0" y1="0" y2="1">'
            f'<stop offset="0%" class="area-stop-top series-color-{index % 10}" />'
            f'<stop offset="100%" class="area-stop-bottom" />'
            f'</linearGradient>'
        )
    defs.append('</defs>')

    # --- Grid lines + Y axis labels ---
    grid_markup = []
    for tick_val in ticks:
        y = scale_y(tick_val)
        formatted = _smart_y_format(tick_val, representative_unit if mode != "rebased" else "", tick_range)
        is_zero = abs(tick_val) < tick_range * 0.001
        line_class = "chart-grid chart-grid-zero" if is_zero else "chart-grid"
        grid_markup.append(
            f'<line x1="{pad_left}" y1="{y:.2f}" x2="{width - pad_right}" y2="{y:.2f}" class="{line_class}" />'
            f'<text x="{pad_left - 10}" y="{y + 4:.2f}" class="chart-axis">{escape(formatted)}</text>'
        )
    if axis_label:
        grid_markup.append(
            f'<text x="{pad_left}" y="{pad_top - 10}" class="chart-axis chart-axis-title">{escape(axis_label)}</text>'
        )

    # --- Phase 1: Reference lines ---
    ref_markup = []
    if reference_lines:
        for line in reference_lines:
            line_val = float(line.get("value", 0.0))
            if line_val < tick_min or line_val > tick_max:
                continue
            ry = scale_y(line_val)
            label = str(line.get("label", ""))
            style = str(line.get("style", "dashed"))
            color_class = str(line.get("color_class", "ref-neutral"))
            ref_markup.append(
                f'<line x1="{pad_left}" y1="{ry:.2f}" x2="{width - pad_right}" y2="{ry:.2f}" '
                f'class="chart-ref-line chart-ref-{style} chart-ref-{color_class}" />'
            )
            ref_markup.append(
                f'<text x="{width - pad_right - 4}" y="{ry - 4:.2f}" '
                f'class="chart-ref-label chart-ref-{color_class}-text" text-anchor="end">{escape(label)}</text>'
            )

    # --- X axis labels (5-7 evenly spaced) ---
    label_points = series[0].get(point_key, [])
    x_label_count = min(7, max(3, len(label_points)))
    x_indices = []
    if len(label_points) > 1:
        step_size = (len(label_points) - 1) / (x_label_count - 1)
        x_indices = [round(i * step_size) for i in range(x_label_count)]
        x_indices = sorted(set(x_indices))
    x_labels = []
    for idx in x_indices:
        label = str(label_points[idx]["label"])
        x_labels.append(
            f'<text x="{scale_x(idx):.2f}" y="{height - 8}" class="chart-axis chart-axis-bottom">{escape(label)}</text>'
        )
    # Tick marks
    x_ticks = []
    for idx in x_indices:
        x = scale_x(idx)
        x_ticks.append(
            f'<line x1="{x:.2f}" y1="{height - pad_bottom}" x2="{x:.2f}" y2="{height - pad_bottom + 5}" class="chart-grid" />'
        )

    # --- Series: area fills, lines, dots, end labels ---
    area_markup = []
    line_markup = []
    dot_markup = []
    end_label_raw: list[tuple[float, str, int]] = []

    for index, item in enumerate(series):
        points = item.get(point_key, [])
        coords = [
            (scale_x(pi), scale_y(float(p["value"])))
            for pi, p in enumerate(points)
        ]
        if not coords:
            continue

        is_ghost = bool(item.get("is_ghost", False))

        # Area fill (subtle gradient under line) — skip for ghost series
        if not is_ghost:
            grad_id = f"{chart_uid}-grad-{index}"
            area_points = " ".join(f"{x:.2f},{y:.2f}" for x, y in coords)
            baseline_y = scale_y(tick_min)
            first_x = coords[0][0]
            last_x_coord = coords[-1][0]
            area_markup.append(
                f'<polygon class="chart-area" fill="url(#{grad_id})" '
                f'points="{first_x:.2f},{baseline_y:.2f} {area_points} {last_x_coord:.2f},{baseline_y:.2f}" />'
            )

        # Line
        polyline = " ".join(f"{x:.2f},{y:.2f}" for x, y in coords)
        if is_ghost:
            line_markup.append(
                f'<polyline class="chart-line chart-line-ghost series-color-{index % 10}" points="{polyline}" />'
            )
        else:
            line_markup.append(
                f'<polyline class="chart-line series-color-{index % 10}" points="{polyline}" />'
            )

        # Data point dots
        latest_delta = item.get("latest_delta")
        for pi, (x, y) in enumerate(coords):
            raw_val = float(points[pi]["value"])
            formatted_val = _smart_y_format(raw_val, representative_unit if mode != "rebased" else "", data_range)
            label_text = str(points[pi].get("label", ""))
            is_last_point = (pi == len(points) - 1)
            if is_last_point and latest_delta and not is_ghost:
                dot_markup.append(
                    f'<circle class="chart-dot chart-dot-latest series-color-{index % 10}" '
                    f'cx="{x:.2f}" cy="{y:.2f}" r="5" '
                    f'data-series="{index}" data-label="{escape(label_text)}" '
                    f'data-value="{escape(formatted_val)}" '
                    f'data-title="{escape(str(item.get("title", "")))}" />'
                )
                delta_display = str(latest_delta.get("display", ""))
                is_notable = bool(latest_delta.get("is_notable", False))
                badge_class = "chart-delta-notable" if is_notable else "chart-delta-badge"
                badge_y = y - 20 if y > 40 else y + 8
                text_y = y - 9 if y > 40 else y + 19
                dot_markup.append(
                    f'<rect x="{x - 18:.2f}" y="{badge_y:.2f}" width="36" height="14" rx="3" '
                    f'class="{badge_class}" />'
                    f'<text x="{x:.2f}" y="{text_y:.2f}" class="chart-delta-text" text-anchor="middle">{escape(delta_display)}</text>'
                )
            else:
                dot_markup.append(
                    f'<circle class="chart-dot series-color-{index % 10}" '
                    f'cx="{x:.2f}" cy="{y:.2f}" r="3" '
                    f'data-series="{index}" data-label="{escape(label_text)}" '
                    f'data-value="{escape(formatted_val)}" '
                    f'data-title="{escape(str(item.get("title", "")))}" />'
                )

        # Latest value callout (boxed) — skip for ghost series
        if not is_ghost:
            last_x_val, last_y_val = coords[-1]
            latest_display = str(item.get("rebased_latest_display" if mode == "rebased" else "raw_latest_display", ""))
            end_label_raw.append((last_y_val, f"{item.get('title', '')}  {latest_display}", index))

    # Anti-collision on end labels
    resolved = _resolve_label_positions(end_label_raw, min_gap=15, pad_top=pad_top, pad_bottom=height - pad_bottom)
    label_markup = []
    for y_pos, label_text, s_index in resolved:
        lx = coords[-1][0] if coords else width - pad_right
        # Line connecting data point to label
        label_markup.append(
            f'<line x1="{lx:.2f}" y1="{y_pos:.2f}" x2="{lx + 8:.2f}" y2="{y_pos:.2f}" class="chart-grid" />'
        )
        label_markup.append(
            f'<text x="{lx + 12:.2f}" y="{y_pos + 4:.2f}" class="chart-end-label series-color-{s_index % 10}">{escape(label_text)}</text>'
        )

    # --- Crosshair overlay rect (invisible, catches mouse events) ---
    crosshair = (
        f'<rect class="chart-hover-zone" x="{pad_left}" y="{pad_top}" '
        f'width="{usable_width}" height="{usable_height}" />'
        f'<line class="chart-crosshair" x1="0" y1="{pad_top}" x2="0" y2="{height - pad_bottom}" />'
    )

    # --- Tooltip container ---
    tooltip = f'<g class="chart-tooltip-group" style="display:none"><rect class="chart-tooltip-bg" rx="6" ry="6" /><text class="chart-tooltip-text"></text></g>'

    return (
        f'<svg class="interactive-chart" viewBox="0 0 {width} {height}" role="img" '
        f'data-chart-uid="{chart_uid}" data-steps="{steps}" '
        f'data-pad-left="{pad_left}" data-pad-right="{pad_right}" '
        f'data-usable-width="{usable_width}" data-pad-top="{pad_top}" '
        f'data-pad-bottom="{pad_bottom}">'
        + "".join(defs)
        + "".join(grid_markup)
        + "".join(x_ticks)
        + "".join(ref_markup)
        + "".join(area_markup)
        + "".join(line_markup)
        + "".join(dot_markup)
        + "".join(label_markup)
        + "".join(x_labels)
        + crosshair
        + tooltip
        + "</svg>"
    )


# ---------------------------------------------------------------------------
# Dual-axis chart
# ---------------------------------------------------------------------------

def _dual_axis_chart_svg(
    left_series: list[dict[str, object]],
    right_series: list[dict[str, object]],
    mode: str,
    *,
    left_label: str = "",
    right_label: str = "",
) -> str:
    all_series = left_series + right_series
    if not all_series:
        return '<div class="chart-empty">Not enough history yet.</div>'

    width = _CHART_WIDTH
    height = _CHART_HEIGHT
    pad_left = _PAD_LEFT + 4
    pad_right = _PAD_RIGHT + 10
    pad_top = _PAD_TOP
    pad_bottom = _PAD_BOTTOM
    point_key = _point_key(mode)
    chart_uid = _next_svg_id()

    def bounds(items: list[dict[str, object]]) -> tuple[float, float]:
        vals = [float(p["value"]) for item in items for p in item.get(point_key, [])]
        mn, mx = min(vals), max(vals)
        if mx == mn:
            mx += 1.0
            mn -= 1.0
        return mn, mx

    left_min, left_max = bounds(left_series)
    right_min, right_max = bounds(right_series)
    left_ticks = _nice_ticks(left_min, left_max, 5)
    right_ticks = _nice_ticks(right_min, right_max, 5)
    lt_min, lt_max = left_ticks[0], left_ticks[-1]
    rt_min, rt_max = right_ticks[0], right_ticks[-1]
    lt_range = lt_max - lt_min if lt_max != lt_min else 1.0
    rt_range = rt_max - rt_min if rt_max != rt_min else 1.0

    steps = max(len(all_series[0][point_key]) - 1, 1)
    usable_width = width - pad_left - pad_right
    usable_height = height - pad_top - pad_bottom
    left_unit = str(left_series[0].get("unit", "")) if left_series else ""
    right_unit = str(right_series[0].get("unit", "")) if right_series else ""

    def scale_x(index: int) -> float:
        return pad_left + (usable_width * (index / steps))

    def scale_y_left(value: float) -> float:
        return pad_top + usable_height - ((value - lt_min) / lt_range * usable_height)

    def scale_y_right(value: float) -> float:
        return pad_top + usable_height - ((value - rt_min) / rt_range * usable_height)

    defs = ['<defs>']
    for i in range(len(all_series)):
        gid = f"{chart_uid}-grad-{i}"
        defs.append(
            f'<linearGradient id="{gid}" x1="0" x2="0" y1="0" y2="1">'
            f'<stop offset="0%" class="area-stop-top series-color-{i % 10}" />'
            f'<stop offset="100%" class="area-stop-bottom" />'
            f'</linearGradient>'
        )
    defs.append('</defs>')

    grid_markup = []
    for tv in left_ticks:
        y = scale_y_left(tv)
        grid_markup.append(
            f'<line x1="{pad_left}" y1="{y:.2f}" x2="{width - pad_right}" y2="{y:.2f}" class="chart-grid" />'
            f'<text x="{pad_left - 10}" y="{y + 4:.2f}" class="chart-axis">'
            f'{escape(_smart_y_format(tv, left_unit, lt_range))}</text>'
        )
    for tv in right_ticks:
        y = scale_y_right(tv)
        grid_markup.append(
            f'<text x="{width - pad_right + 10}" y="{y + 4:.2f}" class="chart-axis chart-axis-right">'
            f'{escape(_smart_y_format(tv, right_unit, rt_range))}</text>'
        )
    if left_label:
        grid_markup.append(f'<text x="{pad_left}" y="{pad_top - 10}" class="chart-axis chart-axis-title">{escape(left_label)}</text>')
    if right_label:
        grid_markup.append(f'<text x="{width - pad_right}" y="{pad_top - 10}" class="chart-axis chart-axis-title chart-axis-right-title">{escape(right_label)}</text>')

    label_points = all_series[0].get(point_key, [])
    x_label_count = min(7, max(3, len(label_points)))
    x_indices = []
    if len(label_points) > 1:
        step_size = (len(label_points) - 1) / (x_label_count - 1)
        x_indices = sorted(set(round(i * step_size) for i in range(x_label_count)))
    x_labels = [
        f'<text x="{scale_x(idx):.2f}" y="{height - 8}" class="chart-axis chart-axis-bottom">{escape(str(label_points[idx]["label"]))}</text>'
        for idx in x_indices
    ]

    line_markup = []
    area_markup = []
    dot_markup = []
    end_label_raw: list[tuple[float, str, int]] = []

    def _render_series(items: list[dict[str, object]], scale_fn, unit: str, offset: int = 0):
        for i, item in enumerate(items):
            si = offset + i
            pts = item.get(point_key, [])
            coords = [(scale_x(pi), scale_fn(float(p["value"]))) for pi, p in enumerate(pts)]
            if not coords:
                continue
            gid = f"{chart_uid}-grad-{si}"
            area_pts = " ".join(f"{x:.2f},{y:.2f}" for x, y in coords)
            bl_y = scale_fn(lt_min if offset == 0 else rt_min)
            area_markup.append(
                f'<polygon class="chart-area" fill="url(#{gid})" '
                f'points="{coords[0][0]:.2f},{bl_y:.2f} {area_pts} {coords[-1][0]:.2f},{bl_y:.2f}" />'
            )
            polyline = " ".join(f"{x:.2f},{y:.2f}" for x, y in coords)
            line_markup.append(f'<polyline class="chart-line series-color-{si % 10}" points="{polyline}" />')
            dr = (lt_max - lt_min) if offset == 0 else (rt_max - rt_min)
            for pi, (x, y) in enumerate(coords):
                fv = _smart_y_format(float(pts[pi]["value"]), unit, dr)
                dot_markup.append(
                    f'<circle class="chart-dot series-color-{si % 10}" cx="{x:.2f}" cy="{y:.2f}" r="3" '
                    f'data-series="{si}" data-label="{escape(str(pts[pi].get("label", "")))}" '
                    f'data-value="{escape(fv)}" data-title="{escape(str(item.get("title", "")))}" />'
                )
            lv = str(item.get("rebased_latest_display" if mode == "rebased" else "raw_latest_display", ""))
            end_label_raw.append((coords[-1][1], f"{item.get('title', '')}  {lv}", si))

    _render_series(left_series, scale_y_left, left_unit, 0)
    _render_series(right_series, scale_y_right, right_unit, len(left_series))

    resolved = _resolve_label_positions(end_label_raw, min_gap=15, pad_top=pad_top, pad_bottom=height - pad_bottom)
    label_markup = []
    for y_pos, text, si in resolved:
        lx = scale_x(steps)
        label_markup.append(
            f'<line x1="{lx:.2f}" y1="{y_pos:.2f}" x2="{lx + 8:.2f}" y2="{y_pos:.2f}" class="chart-grid" />'
            f'<text x="{lx + 12:.2f}" y="{y_pos + 4:.2f}" class="chart-end-label series-color-{si % 10}">{escape(text)}</text>'
        )

    crosshair = (
        f'<rect class="chart-hover-zone" x="{pad_left}" y="{pad_top}" '
        f'width="{usable_width}" height="{usable_height}" />'
        f'<line class="chart-crosshair" x1="0" y1="{pad_top}" x2="0" y2="{height - pad_bottom}" />'
    )
    tooltip = '<g class="chart-tooltip-group" style="display:none"><rect class="chart-tooltip-bg" rx="6" ry="6" /><text class="chart-tooltip-text"></text></g>'

    return (
        f'<svg class="interactive-chart" viewBox="0 0 {width} {height}" role="img" '
        f'data-chart-uid="{chart_uid}" data-steps="{steps}" '
        f'data-pad-left="{pad_left}" data-pad-right="{pad_right}" '
        f'data-usable-width="{usable_width}" data-pad-top="{pad_top}" '
        f'data-pad-bottom="{pad_bottom}">'
        + "".join(defs)
        + "".join(grid_markup)
        + "".join(area_markup)
        + "".join(line_markup)
        + "".join(dot_markup)
        + "".join(label_markup)
        + "".join(x_labels)
        + crosshair
        + tooltip
        + "</svg>"
    )


# ---------------------------------------------------------------------------
# Raw chart views (groups by unit)
# ---------------------------------------------------------------------------

def _render_raw_chart_views(chart: dict[str, object]) -> str:
    groups: dict[str, list[dict[str, object]]] = {}
    for item in chart.get("series", []):
        groups.setdefault(str(item.get("unit", "")).lower(), []).append(item)

    if not groups:
        return '<div class="chart-empty">Not enough history yet.</div>'

    if len(groups) == 1:
        unit, items = next(iter(groups.items()))
        return (
            '<div class="chart-wrap">'
            + _single_axis_chart_svg(items, "raw", axis_label=_unit_label(unit))
            + '</div><div class="chart-legend">'
            + _render_chart_legend({"series": items}, "raw")
            + "</div>"
        )

    if len(groups) == 2:
        group_items = list(groups.items())
        left_unit, left_series = group_items[0]
        right_unit, right_series = group_items[1]
        return (
            '<div class="chart-wrap">'
            + _dual_axis_chart_svg(
                left_series,
                right_series,
                "raw",
                left_label=_unit_label(left_unit),
                right_label=_unit_label(right_unit),
            )
            + '</div><div class="chart-legend">'
            + _render_chart_legend({"series": left_series + right_series}, "raw")
            + "</div>"
        )

    group_markup = []
    for unit, items in groups.items():
        group_markup.append(
            '<div class="grouped-raw-panel">'
            f'<p class="grouped-raw-label">{escape(_unit_label(unit))}</p>'
            '<div class="chart-wrap">'
            + _single_axis_chart_svg(items, "raw", axis_label=_unit_label(unit))
            + '</div><div class="chart-legend">'
            + _render_chart_legend({"series": items}, "raw")
            + "</div></div>"
        )
    return "".join(group_markup)


def _multi_line_chart_svg(chart: dict[str, object], mode: str) -> str:
    return _single_axis_chart_svg(
        chart.get("series", []),
        mode,
        reference_lines=chart.get("reference_lines"),
    )


def _sparkline_svg(history: list[dict[str, object]], tone: str = "neutral") -> str:
    """Render a tiny inline SVG sparkline from a metric's history."""
    if len(history) < 2:
        return ""
    values = [float(item.get("value", 0.0)) for item in history]
    minimum = min(values)
    maximum = max(values)
    if maximum == minimum:
        maximum += 1.0
        minimum -= 1.0
    width = 120
    height = 32
    pad = 2
    steps = len(values) - 1
    usable_w = width - (2 * pad)
    usable_h = height - (2 * pad)
    coords = []
    for i, v in enumerate(values):
        x = pad + (usable_w * (i / steps))
        y = pad + usable_h - ((v - minimum) / (maximum - minimum) * usable_h)
        coords.append(f"{x:.1f},{y:.1f}")
    polyline = " ".join(coords)
    tone_class = f"sparkline-{tone}"
    return (
        f'<svg class="sparkline {tone_class}" viewBox="0 0 {width} {height}" preserveAspectRatio="none">'
        f'<polyline points="{polyline}" />'
        f'</svg>'
    )


def _render_metric_card(card: dict[str, object], compact: bool = False) -> str:
    link = ""
    if card.get("drilldown_href"):
        link = f'<a class="metric-link" href="{escape(str(card["drilldown_href"]))}">Open deep dive</a>'
    why = f'<p class="metric-why">{escape(str(card.get("why_it_matters", "")))}</p>' if card.get("why_it_matters") else ""
    class_name = "metric-card metric-card-compact" if compact else "metric-card"

    # Sparkline
    sparkline = _sparkline_svg(card.get("history", []), str(card.get("tone", "neutral")))

    # Trend indicator
    trend_arrow = escape(str(card.get("trend_arrow", "")))
    trend_momentum = escape(str(card.get("trend_momentum", "")))
    trend_direction = str(card.get("trend_direction", "flat"))
    trend_html = f'<span class="trend-indicator trend-{trend_direction}" title="{trend_momentum}">{trend_arrow} {trend_momentum}</span>' if trend_arrow else ""

    # Percentile rank
    percentile = card.get("percentile_rank")
    percentile_html = f'<span class="percentile-badge" title="Percentile rank vs full history">{percentile:.0f}th pctile</span>' if percentile is not None else ""

    return f"""
    <article class="{class_name} tone-{escape(str(card.get("tone", "neutral")))}">
      <div class="metric-meta">
        <span>{escape(str(card.get("frequency", "")).title())}</span>
        <span>{escape(str(card.get("period_label", "")))}</span>
      </div>
      <h3>{escape(str(card.get("title", "")))}</h3>
      <div class="metric-value-row">
        <div class="metric-value">{escape(str(card.get("value_display", "")))}</div>
        {sparkline}
      </div>
      <div class="metric-delta-row">
        <span class="metric-delta">{escape(str(card.get("delta_display", "")))}</span>
        {trend_html}
      </div>
      {percentile_html}
      {why}
      <div class="metric-foot">
        <span>{escape(str(card.get("source", "")).upper())}</span>
        <span>Release {escape(str(card.get("release_date", "")))}</span>
      </div>
      {link}
    </article>
    """


def _render_chart_legend(chart: dict[str, object], mode: str) -> str:
    legend_items = []
    value_key = "rebased_latest_display" if mode == "rebased" else "raw_latest_display"
    for index, item in enumerate(chart.get("series", [])):
        color_class = f"series-color-{index % 10}"
        legend_items.append(
            f'<div class="legend-item legend-toggle" data-series-index="{index}" tabindex="0" role="button" aria-pressed="true">'
            f'<span class="legend-swatch {color_class}"></span>'
            f'<span class="legend-label">{escape(str(item.get("title", "")))}</span>'
            f'<strong>{escape(str(item.get(value_key, "")))}</strong>'
            f'</div>'
        )
    return "".join(legend_items)


def _render_chart_panel(chart: dict[str, object], chart_id: str) -> str:
    default_mode = str(chart.get("default_mode", "rebased"))
    rebased_active = " active" if default_mode == "rebased" else ""
    raw_active = " active" if default_mode == "raw" else ""

    # Phase 4: runway annotation
    runway_html = ""
    runway_annotation = chart.get("runway_annotation")
    if runway_annotation:
        tone = str(runway_annotation.get("tone", "neutral"))
        label = str(runway_annotation.get("label", ""))
        runway_html = f'<p class="chart-runway-callout chart-runway-{escape(tone)}">{escape(label)}</p>'

    # Phase 5: lead-lag annotation
    lead_lag_html = ""
    lead_lag_annotation = chart.get("lead_lag_annotation")
    if lead_lag_annotation:
        lead_lag_html = f'<p class="chart-lead-lag-note">&#x27F3; {escape(str(lead_lag_annotation))}</p>'

    # Phase 6: cohort note
    cohort_html = ""
    cohort_note = chart.get("cohort_note")
    if cohort_note:
        cohort_html = f'<p class="chart-cohort-note">{escape(str(cohort_note))}</p>'

    return f"""
    <div class="chart-panel">
      <div class="chart-head">
        <div>
          <p class="section-kicker">Comparison Chart</p>
          <h3>{escape(str(chart.get("title", "")))}</h3>
        </div>
        <p>{escape(str(chart.get("note", "")))}</p>
      </div>
      <div class="chart-toolbar">
        <div class="chart-toggle" data-chart-toggle="{escape(chart_id)}">
          <button class="chart-toggle-button{' active' if default_mode == 'rebased' else ''}" data-target="{escape(chart_id)}" data-mode="rebased" type="button">Rebased comparison</button>
          <button class="chart-toggle-button{' active' if default_mode == 'raw' else ''}" data-target="{escape(chart_id)}" data-mode="raw" type="button">Raw values</button>
        </div>
      </div>
      <div class="chart-view{rebased_active}" data-chart-view="{escape(chart_id)}" data-mode="rebased">
        <div class="chart-wrap">
          {_multi_line_chart_svg(chart, 'rebased')}
        </div>
        <div class="chart-legend">{_render_chart_legend(chart, 'rebased')}</div>
      </div>
      <div class="chart-view{raw_active}" data-chart-view="{escape(chart_id)}" data-mode="raw">
        {_render_raw_chart_views(chart)}
      </div>
      {runway_html}
      {lead_lag_html}
      {cohort_html}
    </div>
    """


def _render_section(section: dict[str, object]) -> str:
    cards = "".join(_render_metric_card(card) for card in section.get("cards", []))
    report_links = "".join(
        f'<a class="report-chip" href="{escape(str(link["href"]))}">{escape(str(link["title"]))}</a>'
        for link in section.get("report_links", [])
    )
    return f"""
    <section class="dashboard-section" id="{escape(str(section.get("id", "")))}">
      <div class="section-head">
        <p class="section-kicker">{escape(str(section.get("label", "")))}</p>
        <h2>{escape(str(section.get("title", "")))}</h2>
        <p>{escape(str(section.get("intro", "")))}</p>
      </div>
      {_render_chart_panel(section.get("chart", {}), str(section.get("id", "")) + "-chart")}
      <div class="report-chip-row">{report_links}</div>
      <div class="card-grid">{cards}</div>
    </section>
    """


def _render_report(report: dict[str, object], open_by_default: bool = False) -> str:
    metrics = "".join(_render_metric_card(metric, compact=True) for metric in report.get("metrics", []))
    compare = "".join(
        f'<a class="compare-chip" href="{escape(str(item["href"]))}">{escape(str(item["title"]))}</a>'
        for item in report.get("compare_with", [])
    )
    reasoning = "".join(f"<li>{escape(str(item))}</li>" for item in report.get("reasoning_tips", []))
    open_attr = " open" if open_by_default else ""
    return f"""
    <details class="report-panel" id="report-{escape(str(report.get("id", "")))}"{open_attr}>
      <summary>
        <div class="report-summary-head">
          <div>
            <p class="section-kicker">{escape(str(report.get("source", "")))} • {escape(str(report.get("cadence", "")))}</p>
            <h3>{escape(str(report.get("title", "")))}</h3>
          </div>
          <div class="report-summary-meta">
            <span>{escape(str(report.get("metric_count", 0)))} series</span>
            <span>Latest release {escape(str(report.get("latest_release", "")))}</span>
          </div>
        </div>
        <p class="report-summary-copy">{escape(str(report.get("summary", "")))}</p>
      </summary>
      <div class="report-body">
        {_render_chart_panel(report.get("chart", {}), "report-" + str(report.get("id", "")) + "-chart")}
        <div class="report-grid">
          <div class="report-column">
            <h4>How To Reason About It</h4>
            <ul class="reasoning-list">{reasoning}</ul>
            <h4>Compare Against</h4>
            <div class="compare-chip-row">{compare}</div>
          </div>
          <div class="report-column report-column-wide">
            <h4>Series In This Report</h4>
            <div class="metric-grid-compact">{metrics}</div>
          </div>
        </div>
      </div>
    </details>
    """


def _render_investor_guide(guide: dict[str, object]) -> str:
    playbook_markup = []
    for item in guide.get("playbooks", []):
        signals = "".join(f"<li>{escape(str(signal))}</li>" for signal in item.get("signals", []))
        tailwinds = "".join(
            f'<span class="sector-chip sector-chip-positive">{escape(str(sector))}</span>'
            for sector in item.get("sector_tailwinds", [])
        )
        headwinds = "".join(
            f'<span class="sector-chip sector-chip-caution">{escape(str(sector))}</span>'
            for sector in item.get("sector_headwinds", [])
        )
        watch_links = "".join(
            f'<a class="guide-link" href="{escape(str(link["href"]))}">{escape(str(link["title"]))}</a>'
            for link in item.get("watch_links", [])
        )
        playbook_markup.append(
            f"""
            <article class="playbook-card" id="{escape(str(item.get('id', '')))}">
              <div class="playbook-head">
                <p class="section-kicker">Inflection Pattern</p>
                <h3>{escape(str(item.get("title", "")))}</h3>
              </div>
              <p class="playbook-copy">{escape(str(item.get("economic_read", "")))}</p>
              <div class="playbook-grid">
                <div>
                  <h4>What To Look For</h4>
                  <ul class="reasoning-list">{signals}</ul>
                </div>
                <div>
                  <h4>Usually Helps</h4>
                  <div class="sector-chip-row">{tailwinds}</div>
                  <h4>Usually Pressures</h4>
                  <div class="sector-chip-row">{headwinds}</div>
                </div>
              </div>
              <div class="guide-link-row">{watch_links}</div>
            </article>
            """
        )

    return f"""
    <section class="guide-section" id="{escape(str(guide.get('id', 'investor-guide')))}">
      <div class="section-head">
        <div>
          <p class="section-kicker">{escape(str(guide.get("label", "Investor Guide")))}</p>
          <h2>{escape(str(guide.get("title", "")))}</h2>
        </div>
        <p>{escape(str(guide.get("intro", "")))}</p>
      </div>
      <div class="guide-current">{escape(str(guide.get("current_setup", "")))}</div>
      <div class="playbook-grid-wrap">{''.join(playbook_markup)}</div>
    </section>
    """


def _render_freshness_panel(freshness: list[dict[str, object]]) -> str:
    if not freshness:
        return ""
    rows = []
    for item in freshness:
        status = str(item.get("freshness_status", "stale"))
        status_class = f"freshness-{status}"
        dot = {"fresh": "\u2705", "aging": "\u26a0\ufe0f", "stale": "\ud83d\udd34"}.get(status, "")
        rows.append(
            f'<tr class="{status_class}">'
            f'<td>{escape(str(item.get("source", "")).upper())}</td>'
            f'<td>{escape(str(item.get("frequency", "")).title())}</td>'
            f'<td>{escape(str(item.get("latest_period", "")))}</td>'
            f'<td>{escape(str(item.get("latest_release", "")))}</td>'
            f'<td>{dot} {escape(status.title())}</td>'
            f'</tr>'
        )
    return f"""
    <section class="freshness-section" id="data-freshness">
      <div class="section-head">
        <div>
          <p class="section-kicker">Data Pipeline</p>
          <h2>Data Freshness</h2>
        </div>
        <p>Shows the latest available data point for each source. Freshness helps you judge whether the dashboard reflects current conditions.</p>
      </div>
      <div class="freshness-table-wrap">
        <table class="freshness-table">
          <thead><tr><th>Source</th><th>Cadence</th><th>Latest Period</th><th>Release Date</th><th>Status</th></tr></thead>
          <tbody>{''.join(rows)}</tbody>
        </table>
      </div>
    </section>
    """


def _render_narrative_block(memo_ready: dict[str, object]) -> str:
    """Render the educational narrative block from memo_ready data.

    Appears between the hero and the fast-read section as a collapsible panel
    so users can expand it for context or skip directly to the numbers.
    """
    if not memo_ready:
        return ""

    regime_info = memo_ready.get("regime", {})
    regime_label = str(regime_info.get("regime", "slowing"))
    composite_score = float(regime_info.get("composite_score", 0.0))
    pillars = memo_ready.get("pillars", [])
    positives_list = memo_ready.get("positives", [])
    watchlist_list = memo_ready.get("watchlist", [])
    extreme_cautions = memo_ready.get("extreme_cautions", [])

    # Regime descriptions
    regime_descriptions = {
        "expansion": "The consumer is in expansion mode — labor firm, inflation cooling, spending growing, stress contained.",
        "slowing": "The consumer is in a slowing phase — at least one pillar is softening while others hold up.",
        "stressed": "The consumer is stressed — multiple pillars are flashing caution simultaneously.",
        "recessionary": "The consumer backdrop is recessionary — broad deterioration across labor, spending, and stress.",
    }
    regime_summary = regime_descriptions.get(regime_label, "The consumer backdrop is mixed.")

    pillar_reads_html = ""
    tone_consequence = {
        "positive": "This is a supportive condition for household spending and credit quality.",
        "neutral": "This warrants monitoring for further deterioration or improvement.",
        "caution": "This is a headwind for household cash flow and spending sustainability.",
    }
    for pillar in pillars:
        tone = str(pillar.get("tone", "neutral"))
        consequence = tone_consequence.get(tone, "")
        pillar_reads_html += f"""
        <div class="narrative-pillar tone-{escape(tone)}">
          <div class="narrative-pillar-header">
            <strong>{escape(str(pillar.get("title", "")))}</strong>
            <span class="narrative-stance">{escape(str(pillar.get("stance", "")))}</span>
          </div>
          <p>{escape(str(pillar.get("detail", "")))} {escape(consequence)}</p>
        </div>"""

    extreme_html = ""
    if extreme_cautions:
        items = "".join(
            f'<li><strong>{escape(str(item["title"]))}</strong> at {escape(str(item["value_display"]))} '
            f'(at the {item["percentile_rank"]:.0f}th historical percentile)</li>'
            for item in extreme_cautions
        )
        extreme_html = f'<div class="narrative-extremes"><h4>Elevated historical readings</h4><ul>{items}</ul></div>'

    return f"""
    <section class="narrative-panel" id="narrative">
      <details>
        <summary>
          <span class="narrative-summary-label">Educational Context &amp; What This Means</span>
          <span class="narrative-regime-badge tone-{escape(regime_label)}">{escape(regime_label.title())} &bull; Score: {composite_score:+.0f}</span>
        </summary>
        <div class="narrative-body">
          <div class="narrative-executive">
            <h3>Current Read</h3>
            <p>{escape(regime_summary)}</p>
          </div>
          <div class="narrative-pillars">
            <h3>Pillar-by-Pillar</h3>
            {pillar_reads_html}
          </div>
          {extreme_html}
        </div>
      </details>
    </section>"""


def _render_html(payload: dict[str, object]) -> str:
    executive = payload.get("executive_snapshot", {})
    regime_info = executive.get("regime", {})
    nav = "".join(
        f'<a href="{escape(str(item["href"]))}">{escape(str(item["label"]))}</a>'
        for item in payload.get("navigation", [])
    )
    pillar_markup = "".join(
        f'<article class="pillar tone-{escape(str(item.get("tone", "neutral")))}"><p>{escape(str(item.get("title", "")))}</p><h3>{escape(str(item.get("stance", "")))}</h3><span>{escape(str(item.get("detail", "")))}</span></article>'
        for item in executive.get("pillars", [])
    )
    positives = "".join(f"<li>{escape(str(item))}</li>" for item in executive.get("positives", []))
    watchlist = "".join(f"<li>{escape(str(item))}</li>" for item in executive.get("watchlist", []))
    fast_cards = "".join(_render_metric_card(card) for card in executive.get("cards", []))
    narrative_block = _render_narrative_block(payload.get("memo_ready", {}))
    sections = "".join(_render_section(section) for section in payload.get("sections", [])[1:])
    investor_guide = _render_investor_guide(payload.get("investor_guide", {}))
    report_library = "".join(
        _render_report(report, open_by_default=index < 2)
        for index, report in enumerate(payload.get("report_library", []))
    )
    freshness_panel = _render_freshness_panel(payload.get("data_freshness", []))
    generated_at = escape(str(payload.get("generated_at", "")))
    title = escape(str(executive.get("title", "U.S. Consumer Dashboard")))
    headline = escape(str(executive.get("headline", "")))
    regime_display = escape(str(regime_info.get("regime_display", "")))
    regime_tone = escape(str(regime_info.get("tone", "neutral")))
    composite_score = regime_info.get("composite_score", 0)
    regime_badge = f'<div class="regime-badge tone-{regime_tone}"><span class="regime-label">Consumer Regime</span><span class="regime-value">{regime_display}</span><span class="regime-score">Composite: {composite_score}</span></div>' if regime_display else ""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
    :root {{
      --bg: #f4efe7;
      --panel: rgba(255, 251, 245, 0.92);
      --panel-strong: rgba(255, 248, 239, 0.97);
      --ink: #182426;
      --muted: #65706c;
      --line: rgba(24, 36, 38, 0.1);
      --positive: #1f7a61;
      --neutral: #b67b1a;
      --caution: #b24c32;
      --accent: #0d6378;
      --shadow: 0 24px 60px rgba(35, 44, 46, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 0% 0%, rgba(13, 99, 120, 0.16), transparent 28%),
        radial-gradient(circle at 100% 0%, rgba(182, 123, 26, 0.18), transparent 24%),
        linear-gradient(180deg, #efe7da 0%, var(--bg) 40%, #fbf7f0 100%);
    }}
    h1, h2, h3, h4 {{
      font-family: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif;
      letter-spacing: -0.02em;
      margin: 0;
    }}
    .page {{
      width: min(1280px, calc(100vw - 28px));
      margin: 12px auto 40px;
    }}
    .jumpbar {{
      position: sticky;
      top: 12px;
      z-index: 20;
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      padding: 12px;
      border: 1px solid rgba(255,255,255,0.4);
      background: rgba(255, 250, 245, 0.8);
      backdrop-filter: blur(14px);
      border-radius: 18px;
      box-shadow: var(--shadow);
      margin-bottom: 14px;
    }}
    .jumpbar a {{
      text-decoration: none;
      color: var(--ink);
      background: rgba(24, 36, 38, 0.04);
      border: 1px solid rgba(24, 36, 38, 0.08);
      padding: 8px 12px;
      border-radius: 999px;
      font-size: 0.92rem;
      transition: background 0.15s, border-color 0.15s;
    }}
    .jumpbar a:hover {{
      background: rgba(13, 99, 120, 0.1);
      border-color: rgba(13, 99, 120, 0.18);
    }}
    .hero {{
      position: relative;
      overflow: hidden;
      background: linear-gradient(138deg, rgba(13, 55, 67, 0.97), rgba(36, 57, 45, 0.94));
      color: #f7efe1;
      border-radius: 30px;
      padding: 34px;
      box-shadow: var(--shadow);
    }}
    .hero::before {{
      content: "";
      position: absolute;
      width: 360px;
      height: 360px;
      right: -120px;
      top: -110px;
      border-radius: 999px;
      background: rgba(255, 214, 134, 0.12);
    }}
    .eyebrow {{
      text-transform: uppercase;
      letter-spacing: 0.16em;
      font-size: 11px;
      opacity: 0.76;
      margin-bottom: 10px;
    }}
    .hero-grid {{
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 24px;
      align-items: start;
      position: relative;
    }}
    .hero h1 {{
      font-size: clamp(2.2rem, 4.4vw, 4.3rem);
      max-width: 12ch;
    }}
    .hero-lede {{
      margin-top: 14px;
      max-width: 62ch;
      color: rgba(247, 239, 225, 0.88);
      font-size: 1.08rem;
      line-height: 1.72;
    }}
    .hero-side {{
      background: rgba(255, 248, 239, 0.08);
      border: 1px solid rgba(255, 248, 239, 0.12);
      border-radius: 20px;
      padding: 18px;
      backdrop-filter: blur(12px);
    }}
    .hero-side h3 {{
      font-size: 1.15rem;
      margin-bottom: 10px;
    }}
    .hero-side ul {{
      margin: 0;
      padding-left: 18px;
      line-height: 1.6;
      color: rgba(247, 239, 225, 0.9);
    }}
    .pillar-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
      margin-top: 22px;
    }}
    .pillar {{
      background: rgba(255,255,255,0.08);
      border: 1px solid rgba(255,255,255,0.12);
      border-radius: 18px;
      padding: 16px;
    }}
    .pillar p {{
      margin: 0 0 8px;
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      opacity: 0.78;
    }}
    .pillar h3 {{
      font-size: 1.35rem;
      margin-bottom: 6px;
    }}
    .pillar span {{
      display: block;
      line-height: 1.5;
      font-size: 0.94rem;
      color: rgba(247,239,225,0.88);
    }}
    .dashboard-section {{
      margin-top: 22px;
      border-radius: 28px;
      padding: 26px;
      background: var(--panel);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
    }}
    .section-head {{
      display: grid;
      grid-template-columns: 1.1fr 0.9fr;
      gap: 18px;
      align-items: end;
      margin-bottom: 16px;
    }}
    .section-kicker {{
      text-transform: uppercase;
      letter-spacing: 0.16em;
      font-size: 11px;
      color: var(--accent);
      margin: 0 0 8px;
    }}
    .section-head p:last-child {{
      color: var(--muted);
      line-height: 1.65;
      margin: 0;
    }}
    .chart-panel {{
      border-radius: 22px;
      padding: 14px 14px 12px;
      background: rgba(255,255,255,0.72);
      border: 1px solid rgba(24,36,38,0.08);
    }}
    .chart-head {{
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 14px;
      align-items: end;
      margin-bottom: 8px;
    }}
    .chart-head p {{
      margin: 0;
      color: var(--muted);
      max-width: 42ch;
      line-height: 1.5;
      font-size: 0.9rem;
      text-align: right;
    }}
    .chart-wrap svg {{
      width: 100%;
      height: auto;
      display: block;
    }}
    .chart-toolbar {{
      display: flex;
      justify-content: flex-end;
      margin-bottom: 8px;
    }}
    .chart-toggle {{
      display: inline-flex;
      gap: 8px;
      padding: 6px;
      border-radius: 999px;
      background: rgba(24,36,38,0.05);
      border: 1px solid rgba(24,36,38,0.08);
    }}
    .chart-toggle-button {{
      border: 0;
      background: transparent;
      color: var(--muted);
      padding: 8px 12px;
      border-radius: 999px;
      cursor: pointer;
      font: inherit;
    }}
    .chart-toggle-button.active {{
      background: rgba(13, 99, 120, 0.12);
      color: var(--ink);
      font-weight: 600;
    }}
    .chart-view {{
      display: none;
    }}
    .chart-view.active {{
      display: block;
    }}
    .grouped-raw-panel + .grouped-raw-panel {{
      margin-top: 16px;
      padding-top: 14px;
      border-top: 1px solid rgba(24,36,38,0.08);
    }}
    .grouped-raw-label {{
      margin: 0 0 8px;
      color: var(--muted);
      font-size: 0.9rem;
      font-weight: 500;
    }}
    /* --- Chart grid and axes --- */
    .chart-grid {{
      stroke: rgba(24,36,38,0.08);
      stroke-width: 1;
    }}
    .chart-grid-zero {{
      stroke: rgba(24,36,38,0.22);
      stroke-width: 1.5;
      stroke-dasharray: 6 3;
    }}
    .chart-axis {{
      font-size: 11px;
      fill: rgba(101, 112, 108, 0.88);
      text-anchor: end;
      font-variant-numeric: tabular-nums;
    }}
    .chart-axis-right {{
      text-anchor: start;
    }}
    .chart-axis-bottom {{
      text-anchor: middle;
      font-size: 10.5px;
    }}
    .chart-axis-title {{
      text-anchor: start;
      font-weight: 600;
      font-size: 10.5px;
      fill: rgba(24, 36, 38, 0.55);
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }}
    .chart-axis-right-title {{
      text-anchor: end;
    }}
    /* --- Phase 1: Reference lines --- */
    .chart-ref-line {{ stroke-width: 1.5; fill: none; }}
    .chart-ref-dashed {{ stroke-dasharray: 5,3; }}
    .chart-ref-solid {{ stroke-dasharray: none; }}
    .chart-ref-ref-target {{ stroke: #22c55e; opacity: 0.8; }}
    .chart-ref-ref-warning {{ stroke: #f59e0b; opacity: 0.8; }}
    .chart-ref-ref-neutral {{ stroke: #94a3b8; opacity: 0.6; }}
    .chart-ref-label {{ font-size: 9px; fill: #94a3b8; }}
    .chart-ref-ref-target-text {{ fill: #22c55e; opacity: 0.9; }}
    .chart-ref-ref-warning-text {{ fill: #f59e0b; opacity: 0.9; }}
    .chart-ref-ref-neutral-text {{ fill: #94a3b8; }}
    /* --- Phase 2: Delta badges --- */
    .chart-dot-latest {{ stroke-width: 2; }}
    .chart-delta-badge {{ fill: #1e293b; opacity: 0.85; }}
    .chart-delta-notable {{ fill: #7c3aed; opacity: 0.9; }}
    .chart-delta-text {{ font-size: 9px; fill: #f1f5f9; font-weight: 600; }}
    /* --- Phase 4: Runway callout --- */
    .chart-runway-callout {{ font-size: 12px; padding: 6px 12px; border-radius: 6px; margin-top: 6px; }}
    .chart-runway-positive {{ background: rgba(34,197,94,0.1); color: #86efac; border-left: 3px solid #22c55e; }}
    .chart-runway-neutral {{ background: rgba(245,158,11,0.08); color: #fde68a; border-left: 3px solid #f59e0b; }}
    .chart-runway-caution {{ background: rgba(239,68,68,0.1); color: #fca5a5; border-left: 3px solid #ef4444; }}
    /* --- Phase 5: Lead-lag note --- */
    .chart-lead-lag-note {{ font-size: 11px; color: #94a3b8; margin-top: 4px; font-style: italic; }}
    .chart-line-ghost {{ stroke-dasharray: 6,4; opacity: 0.4; stroke-width: 1.5; fill: none; }}
    /* --- Phase 6: Cohort note --- */
    .chart-cohort-note {{ font-size: 12px; color: var(--muted); margin-top: 6px; padding: 6px 12px; border-left: 3px solid rgba(24,36,38,0.15); font-style: italic; }}
    /* --- Chart lines --- */
    .chart-line {{
      fill: none;
      stroke-width: 2.5;
      stroke-linecap: round;
      stroke-linejoin: round;
      transition: opacity 0.25s;
    }}
    .chart-line.dimmed {{ opacity: 0.12; }}
    /* --- Area fill under lines --- */
    .chart-area {{
      opacity: 0.10;
      transition: opacity 0.25s;
    }}
    .chart-area.dimmed {{ opacity: 0.02; }}
    .area-stop-bottom {{ stop-color: transparent; stop-opacity: 0; }}
    /* Gradient top stops use series fill color via class */
    .area-stop-top.series-color-0 {{ stop-color: #0077a8; stop-opacity: 0.6; }}
    .area-stop-top.series-color-1 {{ stop-color: #d85940; stop-opacity: 0.6; }}
    .area-stop-top.series-color-2 {{ stop-color: #2d8e3e; stop-opacity: 0.6; }}
    .area-stop-top.series-color-3 {{ stop-color: #8655c5; stop-opacity: 0.6; }}
    .area-stop-top.series-color-4 {{ stop-color: #c98a1e; stop-opacity: 0.6; }}
    .area-stop-top.series-color-5 {{ stop-color: #d9508a; stop-opacity: 0.6; }}
    .area-stop-top.series-color-6 {{ stop-color: #4b68d6; stop-opacity: 0.6; }}
    .area-stop-top.series-color-7 {{ stop-color: #2d9696; stop-opacity: 0.6; }}
    .area-stop-top.series-color-8 {{ stop-color: #a06a2a; stop-opacity: 0.6; }}
    .area-stop-top.series-color-9 {{ stop-color: #5478a8; stop-opacity: 0.6; }}
    /* --- Data dots --- */
    .chart-dot {{
      stroke: #fff;
      stroke-width: 1.5;
      opacity: 0;
      transition: opacity 0.15s, r 0.15s;
      cursor: crosshair;
    }}
    .interactive-chart:hover .chart-dot {{ opacity: 0.55; }}
    .chart-dot.active {{ opacity: 1; r: 5; }}
    .chart-dot.dimmed {{ opacity: 0; }}
    /* --- End-of-line labels --- */
    .chart-end-label {{
      font-size: 11px;
      font-weight: 500;
      letter-spacing: 0;
    }}
    /* --- Crosshair --- */
    .chart-hover-zone {{
      fill: transparent;
      cursor: crosshair;
    }}
    .chart-crosshair {{
      stroke: rgba(24, 36, 38, 0.25);
      stroke-width: 1;
      stroke-dasharray: 4 3;
      pointer-events: none;
      display: none;
    }}
    .interactive-chart:hover .chart-crosshair {{ display: block; }}
    /* --- Tooltip --- */
    .chart-tooltip-group {{
      pointer-events: none;
    }}
    .chart-tooltip-bg {{
      fill: rgba(24, 36, 38, 0.92);
      stroke: none;
    }}
    .chart-tooltip-text {{
      fill: #f4efe7;
      font-size: 11px;
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      font-variant-numeric: tabular-nums;
    }}
    /* --- Tone colors --- */
    .tone-positive {{ color: var(--positive); }}
    .tone-neutral {{ color: var(--neutral); }}
    .tone-caution {{ color: var(--caution); }}
    /* --- Series color palette (high contrast, colorblind-friendly) --- */
    .series-color-0 {{ stroke: #0077a8; fill: #0077a8; }}
    .series-color-1 {{ stroke: #d85940; fill: #d85940; }}
    .series-color-2 {{ stroke: #2d8e3e; fill: #2d8e3e; }}
    .series-color-3 {{ stroke: #8655c5; fill: #8655c5; }}
    .series-color-4 {{ stroke: #c98a1e; fill: #c98a1e; }}
    .series-color-5 {{ stroke: #d9508a; fill: #d9508a; }}
    .series-color-6 {{ stroke: #4b68d6; fill: #4b68d6; }}
    .series-color-7 {{ stroke: #2d9696; fill: #2d9696; }}
    .series-color-8 {{ stroke: #a06a2a; fill: #a06a2a; }}
    .series-color-9 {{ stroke: #5478a8; fill: #5478a8; }}
    /* --- Legend --- */
    .chart-legend {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 8px;
      margin-top: 12px;
    }}
    .legend-item {{
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 9px 12px;
      background: rgba(24,36,38,0.02);
      border: 1px solid rgba(24,36,38,0.06);
      border-radius: 12px;
      font-size: 0.88rem;
      cursor: pointer;
      transition: background 0.15s, opacity 0.2s;
      user-select: none;
    }}
    .legend-item:hover {{
      background: rgba(24,36,38,0.06);
    }}
    .legend-item.legend-hidden {{
      opacity: 0.35;
      text-decoration: line-through;
    }}
    .legend-label {{
      flex: 1 1 auto;
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }}
    .legend-item strong {{
      margin-left: auto;
      font-weight: 600;
      font-variant-numeric: tabular-nums;
      color: rgba(24, 36, 38, 0.86);
      flex: 0 0 auto;
    }}
    .legend-swatch {{
      width: 22px;
      height: 3px;
      border-radius: 2px;
      flex: 0 0 auto;
    }}
    .report-chip-row, .compare-chip-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin: 16px 0 0;
    }}
    .report-chip, .compare-chip {{
      text-decoration: none;
      color: var(--ink);
      background: rgba(13, 99, 120, 0.08);
      border: 1px solid rgba(13, 99, 120, 0.14);
      padding: 9px 12px;
      border-radius: 999px;
      font-size: 0.9rem;
    }}
    .card-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px;
      margin-top: 16px;
    }}
    .metric-card {{
      background: rgba(255,255,255,0.72);
      border: 1px solid rgba(24,36,38,0.08);
      border-radius: 20px;
      padding: 18px;
      min-height: 226px;
      display: flex;
      flex-direction: column;
    }}
    .metric-card-compact {{
      min-height: auto;
    }}
    .metric-meta, .metric-foot {{
      display: flex;
      justify-content: space-between;
      gap: 10px;
      color: var(--muted);
      font-size: 0.8rem;
    }}
    .metric-value {{
      font-size: 2rem;
      font-weight: 700;
      margin-top: 10px;
    }}
    .metric-delta {{
      margin-top: 6px;
      color: var(--muted);
      font-size: 0.92rem;
    }}
    .metric-why {{
      margin: auto 0 12px;
      color: var(--muted);
      line-height: 1.58;
    }}
    .metric-link {{
      margin-top: 12px;
      color: var(--accent);
      text-decoration: none;
      font-weight: 600;
    }}
    .report-library {{
      margin-top: 22px;
      border-radius: 28px;
      padding: 26px;
      background: var(--panel-strong);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
    }}
    .guide-section {{
      margin-top: 22px;
      border-radius: 28px;
      padding: 26px;
      background: rgba(255, 250, 244, 0.96);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
    }}
    .guide-section .section-head h2 {{
      color: var(--ink);
    }}
    .guide-section .section-head p:last-child {{
      color: var(--ink);
      opacity: 0.78;
    }}
    .guide-current {{
      margin-top: 8px;
      padding: 14px 16px;
      border-radius: 16px;
      background: rgba(13, 99, 120, 0.1);
      border: 1px solid rgba(13, 99, 120, 0.18);
      color: var(--ink);
      line-height: 1.62;
      font-size: 0.95rem;
      font-weight: 500;
    }}
    .playbook-grid-wrap {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 14px;
      margin-top: 16px;
    }}
    .playbook-card {{
      background: rgba(255,255,255,0.88);
      border: 1px solid rgba(24,36,38,0.1);
      border-radius: 22px;
      padding: 20px;
    }}
    .playbook-head {{
      margin-bottom: 10px;
    }}
    .playbook-head h3 {{
      color: var(--ink);
      font-size: 1.15rem;
    }}
    .playbook-copy {{
      margin: 0 0 14px;
      color: var(--ink);
      opacity: 0.82;
      line-height: 1.65;
      font-size: 0.94rem;
    }}
    .playbook-grid {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }}
    .playbook-grid h4 {{
      color: var(--ink);
      font-size: 0.92rem;
      margin-bottom: 8px;
      opacity: 0.9;
    }}
    .playbook-grid .reasoning-list {{
      color: var(--ink);
      opacity: 0.8;
      line-height: 1.68;
      font-size: 0.9rem;
    }}
    .playbook-grid .reasoning-list li + li {{
      margin-top: 6px;
    }}
    .sector-chip-row, .guide-link-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }}
    .guide-link-row {{
      margin-top: 14px;
    }}
    .sector-chip {{
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 7px 12px;
      font-size: 0.84rem;
      font-weight: 500;
      border: 1px solid transparent;
    }}
    .sector-chip-positive {{
      background: rgba(31, 122, 97, 0.14);
      border-color: rgba(31, 122, 97, 0.25);
      color: #0f4a38;
    }}
    .sector-chip-caution {{
      background: rgba(178, 76, 50, 0.14);
      border-color: rgba(178, 76, 50, 0.25);
      color: #7a3422;
    }}
    .guide-link {{
      text-decoration: none;
      color: #08536a;
      background: rgba(13, 99, 120, 0.12);
      border: 1px solid rgba(13, 99, 120, 0.2);
      border-radius: 999px;
      padding: 8px 13px;
      font-size: 0.86rem;
      font-weight: 500;
      transition: background 0.15s;
    }}
    .guide-link:hover {{
      background: rgba(13, 99, 120, 0.2);
    }}
    .report-panel {{
      margin-top: 14px;
      background: rgba(255,255,255,0.66);
      border: 1px solid rgba(24,36,38,0.09);
      border-radius: 22px;
      overflow: hidden;
    }}
    .report-panel summary {{
      list-style: none;
      cursor: pointer;
      padding: 20px;
    }}
    .report-panel summary::-webkit-details-marker {{ display: none; }}
    .report-summary-head {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: start;
    }}
    .report-summary-meta {{
      color: var(--muted);
      font-size: 0.88rem;
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }}
    .report-summary-copy {{
      margin: 10px 0 0;
      color: var(--muted);
      line-height: 1.65;
      max-width: 74ch;
    }}
    .report-body {{
      padding: 0 20px 20px;
    }}
    .report-grid {{
      display: grid;
      grid-template-columns: 0.8fr 1.2fr;
      gap: 18px;
      margin-top: 16px;
    }}
    .report-column h4 {{
      margin-bottom: 10px;
      font-size: 1.05rem;
    }}
    .reasoning-list {{
      margin: 0;
      padding-left: 18px;
      color: var(--muted);
      line-height: 1.65;
    }}
    .metric-grid-compact {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
    }}
    /* ---- Narrative / Educational Panel ---- */
    .narrative-panel {{
      background: var(--panel-strong);
      border: 1px solid rgba(13, 99, 120, 0.15);
      border-radius: 20px;
      margin-bottom: 14px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }}
    .narrative-panel summary {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 16px 22px;
      cursor: pointer;
      list-style: none;
      user-select: none;
      gap: 12px;
    }}
    .narrative-panel summary::-webkit-details-marker {{ display: none; }}
    .narrative-panel summary::after {{
      content: "\\25BC";
      font-size: 10px;
      color: var(--muted);
      transition: transform 0.2s;
    }}
    .narrative-panel[open] summary::after {{ transform: rotate(180deg); }}
    .narrative-summary-label {{
      font-weight: 600;
      font-size: 0.95rem;
    }}
    .narrative-regime-badge {{
      font-size: 0.82rem;
      padding: 4px 10px;
      border-radius: 999px;
      font-weight: 600;
    }}
    .narrative-regime-badge.tone-expansion {{ background: rgba(31, 122, 97, 0.12); color: var(--positive); }}
    .narrative-regime-badge.tone-slowing {{ background: rgba(182, 123, 26, 0.12); color: var(--neutral); }}
    .narrative-regime-badge.tone-stressed {{ background: rgba(178, 76, 50, 0.12); color: var(--caution); }}
    .narrative-regime-badge.tone-recessionary {{ background: rgba(178, 76, 50, 0.18); color: var(--caution); }}
    .narrative-body {{
      padding: 0 22px 22px;
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 20px;
      border-top: 1px solid var(--line);
    }}
    .narrative-executive {{
      grid-column: 1 / -1;
    }}
    .narrative-executive h3, .narrative-pillars h3 {{
      font-size: 1rem;
      margin-bottom: 8px;
      margin-top: 16px;
      color: var(--accent);
    }}
    .narrative-pillars {{
      grid-column: 1 / -1;
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
      gap: 12px;
    }}
    .narrative-pillars h3 {{
      grid-column: 1 / -1;
    }}
    .narrative-pillar {{
      background: rgba(24, 36, 38, 0.03);
      border: 1px solid var(--line);
      border-left: 3px solid var(--line);
      border-radius: 10px;
      padding: 12px 14px;
    }}
    .narrative-pillar.tone-positive {{ border-left-color: var(--positive); }}
    .narrative-pillar.tone-neutral {{ border-left-color: var(--neutral); }}
    .narrative-pillar.tone-caution {{ border-left-color: var(--caution); }}
    .narrative-pillar-header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 6px;
    }}
    .narrative-stance {{
      font-size: 0.8rem;
      opacity: 0.72;
    }}
    .narrative-pillar p {{
      margin: 0;
      font-size: 0.87rem;
      line-height: 1.65;
      color: var(--muted);
    }}
    .narrative-extremes {{
      grid-column: 1 / -1;
      background: rgba(178, 76, 50, 0.05);
      border: 1px solid rgba(178, 76, 50, 0.15);
      border-radius: 12px;
      padding: 14px 16px;
    }}
    .narrative-extremes h4 {{
      margin: 0 0 8px;
      font-size: 0.88rem;
      color: var(--caution);
    }}
    .narrative-extremes ul {{
      margin: 0;
      padding-left: 18px;
    }}
    .narrative-extremes li {{
      font-size: 0.86rem;
      line-height: 1.6;
      color: var(--muted);
    }}
    /* ---- End Narrative Panel ---- */
    .footer {{
      margin-top: 16px;
      color: var(--muted);
      font-size: 0.85rem;
      text-align: right;
    }}
    .chart-empty {{
      color: var(--muted);
      padding: 24px 0;
    }}
    /* --- Sparkline --- */
    .sparkline {{
      width: 120px;
      height: 32px;
      flex: 0 0 auto;
    }}
    .sparkline polyline {{
      fill: none;
      stroke-width: 1.8;
      stroke-linecap: round;
      stroke-linejoin: round;
    }}
    .sparkline-positive polyline {{ stroke: var(--positive); }}
    .sparkline-neutral polyline {{ stroke: var(--neutral); }}
    .sparkline-caution polyline {{ stroke: var(--caution); }}
    .metric-value-row {{
      display: flex;
      align-items: center;
      gap: 12px;
      margin-top: 10px;
    }}
    .metric-value-row .metric-value {{
      margin-top: 0;
    }}
    .metric-delta-row {{
      display: flex;
      align-items: center;
      gap: 10px;
      margin-top: 6px;
      flex-wrap: wrap;
    }}
    /* --- Trend indicator --- */
    .trend-indicator {{
      font-size: 0.82rem;
      padding: 3px 8px;
      border-radius: 999px;
      white-space: nowrap;
    }}
    .trend-up {{
      background: rgba(31, 122, 97, 0.08);
      color: var(--positive);
    }}
    .trend-down {{
      background: rgba(178, 76, 50, 0.08);
      color: var(--caution);
    }}
    .trend-flat {{
      background: rgba(24, 36, 38, 0.04);
      color: var(--muted);
    }}
    /* --- Percentile badge --- */
    .percentile-badge {{
      display: inline-block;
      margin-top: 6px;
      font-size: 0.78rem;
      color: var(--muted);
      background: rgba(24, 36, 38, 0.04);
      border: 1px solid rgba(24, 36, 38, 0.06);
      border-radius: 999px;
      padding: 2px 8px;
    }}
    /* --- Regime badge --- */
    .regime-badge {{
      display: inline-flex;
      flex-direction: column;
      gap: 2px;
      margin: 12px 0 6px;
      padding: 10px 16px;
      border-radius: 16px;
      background: rgba(255, 248, 239, 0.14);
      border: 1px solid rgba(255, 248, 239, 0.2);
    }}
    .regime-label {{
      text-transform: uppercase;
      letter-spacing: 0.14em;
      font-size: 10px;
      opacity: 0.7;
    }}
    .regime-value {{
      font-family: "Iowan Old Style", "Palatino Linotype", Georgia, serif;
      font-size: 1.4rem;
      font-weight: 700;
      letter-spacing: -0.01em;
    }}
    .regime-score {{
      font-size: 0.82rem;
      opacity: 0.72;
    }}
    .regime-badge.tone-positive {{ border-color: rgba(31, 122, 97, 0.35); background: rgba(31, 122, 97, 0.12); }}
    .regime-badge.tone-neutral {{ border-color: rgba(182, 123, 26, 0.3); background: rgba(182, 123, 26, 0.1); }}
    .regime-badge.tone-caution {{ border-color: rgba(178, 76, 50, 0.3); background: rgba(178, 76, 50, 0.1); }}
    /* --- Data Freshness --- */
    .freshness-section {{
      margin-top: 22px;
      border-radius: 28px;
      padding: 26px;
      background: var(--panel);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
    }}
    .freshness-table-wrap {{
      overflow-x: auto;
      margin-top: 14px;
    }}
    .freshness-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.92rem;
    }}
    .freshness-table th {{
      text-align: left;
      padding: 10px 14px;
      font-size: 0.82rem;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      color: var(--muted);
      border-bottom: 2px solid var(--line);
    }}
    .freshness-table td {{
      padding: 10px 14px;
      border-bottom: 1px solid var(--line);
    }}
    .freshness-fresh td:last-child {{ color: var(--positive); font-weight: 600; }}
    .freshness-aging td:last-child {{ color: var(--neutral); font-weight: 600; }}
    .freshness-stale td:last-child {{ color: var(--caution); font-weight: 600; }}
    /* --- Dark mode --- */
    .theme-toggle {{
      position: fixed;
      bottom: 18px;
      right: 18px;
      z-index: 30;
      width: 44px;
      height: 44px;
      border-radius: 999px;
      border: 1px solid rgba(24,36,38,0.12);
      background: var(--panel);
      box-shadow: var(--shadow);
      cursor: pointer;
      font-size: 1.2rem;
      display: flex;
      align-items: center;
      justify-content: center;
    }}
    html.dark {{
      --bg: #141a1c;
      --panel: rgba(28, 34, 38, 0.92);
      --panel-strong: rgba(32, 38, 42, 0.97);
      --ink: #e2ddd4;
      --muted: #8a9490;
      --line: rgba(226, 221, 212, 0.1);
      --positive: #3dbd8d;
      --neutral: #d4a83a;
      --caution: #e06b54;
      --accent: #49b8d3;
      --shadow: 0 24px 60px rgba(0, 0, 0, 0.3);
    }}
    html.dark body {{
      background: radial-gradient(circle at 0% 0%, rgba(13, 99, 120, 0.1), transparent 28%),
        radial-gradient(circle at 100% 0%, rgba(182, 123, 26, 0.08), transparent 24%),
        linear-gradient(180deg, #0f1416 0%, var(--bg) 40%, #1a2024 100%);
    }}
    html.dark .hero {{
      background: linear-gradient(138deg, rgba(8, 36, 44, 0.97), rgba(18, 30, 24, 0.94));
    }}
    html.dark .jumpbar {{
      background: rgba(28, 34, 38, 0.85);
      border-color: rgba(226, 221, 212, 0.08);
    }}
    html.dark .jumpbar a {{
      background: rgba(226, 221, 212, 0.05);
      border-color: rgba(226, 221, 212, 0.08);
      color: var(--ink);
    }}
    html.dark .metric-card, html.dark .chart-panel, html.dark .report-panel,
    html.dark .playbook-card, html.dark .legend-item {{
      background: rgba(40, 48, 52, 0.72);
      border-color: rgba(226, 221, 212, 0.06);
    }}
    html.dark .hero-side {{
      background: rgba(255, 248, 239, 0.04);
      border-color: rgba(255, 248, 239, 0.08);
    }}
    html.dark .guide-section {{
      background: rgba(28, 34, 38, 0.94);
    }}
    html.dark .guide-current {{
      background: rgba(73, 184, 211, 0.1);
      border-color: rgba(73, 184, 211, 0.2);
      color: var(--ink);
    }}
    html.dark .playbook-copy {{
      color: var(--ink);
      opacity: 0.78;
    }}
    html.dark .playbook-grid h4 {{
      color: var(--ink);
      opacity: 0.85;
    }}
    html.dark .playbook-grid .reasoning-list {{
      color: var(--ink);
      opacity: 0.72;
    }}
    html.dark .sector-chip-positive {{
      background: rgba(61, 189, 141, 0.15);
      border-color: rgba(61, 189, 141, 0.28);
      color: #6ddbb5;
    }}
    html.dark .sector-chip-caution {{
      background: rgba(224, 107, 84, 0.15);
      border-color: rgba(224, 107, 84, 0.28);
      color: #f0a08e;
    }}
    html.dark .guide-link {{
      color: var(--accent);
      background: rgba(73, 184, 211, 0.1);
      border-color: rgba(73, 184, 211, 0.2);
    }}
    html.dark .guide-link:hover {{
      background: rgba(73, 184, 211, 0.2);
    }}
    html.dark .chart-grid {{ stroke: rgba(226, 221, 212, 0.08); }}
    html.dark .chart-grid-zero {{ stroke: rgba(226, 221, 212, 0.22); }}
    html.dark .chart-axis {{ fill: rgba(226, 221, 212, 0.6); }}
    html.dark .chart-crosshair {{ stroke: rgba(226, 221, 212, 0.3); }}
    html.dark .chart-tooltip-bg {{ fill: rgba(226, 221, 212, 0.92); }}
    html.dark .chart-tooltip-text {{ fill: #141a1c; }}
    html.dark .chart-dot {{ stroke: #1c2226; }}
    html.dark .theme-toggle {{
      background: rgba(40, 48, 52, 0.9);
      border-color: rgba(226, 221, 212, 0.1);
      color: var(--ink);
    }}
    /* --- Responsive --- */
    @media (max-width: 960px) {{
      .hero-grid, .section-head, .report-grid, .chart-head {{
        grid-template-columns: 1fr;
      }}
      .pillar-grid {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}
      .chart-head p {{
        text-align: left;
      }}
      .report-summary-head {{
        flex-direction: column;
      }}
    }}
    @media (max-width: 640px) {{
      .page {{
        width: min(100vw - 16px, 1280px);
        margin: 8px auto 24px;
      }}
      .jumpbar, .hero, .dashboard-section, .guide-section, .report-library, .freshness-section {{
        border-radius: 20px;
      }}
      .hero, .dashboard-section, .guide-section, .report-library, .freshness-section {{
        padding: 20px;
      }}
      .pillar-grid {{
        grid-template-columns: 1fr;
      }}
      .card-grid {{
        grid-template-columns: 1fr;
      }}
      .sparkline {{
        width: 80px;
      }}
      .metric-value {{
        font-size: 1.6rem;
      }}
    }}
    /* --- Print --- */
    @media print {{
      body {{ background: #fff !important; }}
      .jumpbar, .theme-toggle, .chart-toolbar, .report-chip-row,
      .compare-chip-row, .guide-link-row, .metric-link {{ display: none !important; }}
      .hero, .dashboard-section, .guide-section, .report-library, .freshness-section {{
        break-inside: avoid;
        box-shadow: none;
        border: 1px solid #ddd;
      }}
      .chart-view {{ display: block !important; }}
      .chart-view[data-mode="raw"] {{ display: none !important; }}
      details {{ open: true; }}
      details .report-body {{ display: block !important; }}
      .page {{ width: 100%; margin: 0; }}
    }}
  </style>
</head>
<body>
  <main class="page">
    <nav class="jumpbar">{nav}</nav>

    <section class="hero">
      <div class="hero-grid">
        <div>
          <p class="eyebrow">Investor Workbench</p>
          <h1>{title}</h1>
          {regime_badge}
          <p class="hero-lede">{headline}</p>
          <div class="pillar-grid">{pillar_markup}</div>
        </div>
        <aside class="hero-side">
          <h3>What improved</h3>
          <ul>{positives}</ul>
          <h3 style="margin-top:14px;">What to watch</h3>
          <ul>{watchlist}</ul>
        </aside>
      </div>
    </section>

    {narrative_block}

    <section class="dashboard-section" id="fast-read">
      <div class="section-head">
        <div>
          <p class="section-kicker">Fast Read</p>
          <h2>What matters right now</h2>
        </div>
        <p>Use this strip when you want the shortest possible read on labor, inflation, spending power, and consumer stress before deciding where to dig deeper.</p>
      </div>
      {_render_chart_panel(payload.get("sections", [])[0].get("chart", {}), "fast-read-chart")}
      <div class="card-grid">{fast_cards}</div>
    </section>

    {sections}

    {investor_guide}

    <section class="report-library" id="report-library">
      <div class="section-head">
        <div>
          <p class="section-kicker">Deep Dives</p>
          <h2>Report Library</h2>
        </div>
        <p>Every report below is expandable. Use them when you want to reason through a topic carefully, compare series inside the same report, and connect it back to the rest of the dashboard.</p>
      </div>
      {report_library}
    </section>

    {freshness_panel}

    <p class="footer">Generated {generated_at} from normalized and derived pipeline outputs.</p>
  </main>
  <button class="theme-toggle" id="theme-toggle" title="Toggle dark mode" aria-label="Toggle dark mode">&#9790;</button>
  <script>
    /* ======= Chart view toggle (rebased / raw) ======= */
    document.addEventListener("click", function(event) {{
      var button = event.target.closest(".chart-toggle-button");
      if (!button) return;
      var target = button.getAttribute("data-target");
      var mode = button.getAttribute("data-mode");
      document.querySelectorAll('[data-chart-toggle="' + target + '"] .chart-toggle-button').forEach(function(node) {{
        node.classList.toggle("active", node === button);
      }});
      document.querySelectorAll('[data-chart-view="' + target + '"]').forEach(function(node) {{
        node.classList.toggle("active", node.getAttribute("data-mode") === mode);
      }});
    }});

    /* ======= Interactive crosshair + tooltip on charts ======= */
    (function() {{
      document.querySelectorAll("svg.interactive-chart").forEach(function(svg) {{
        var hoverZone = svg.querySelector(".chart-hover-zone");
        var crosshair = svg.querySelector(".chart-crosshair");
        var tooltipGroup = svg.querySelector(".chart-tooltip-group");
        var tooltipBg = tooltipGroup ? tooltipGroup.querySelector(".chart-tooltip-bg") : null;
        var tooltipText = tooltipGroup ? tooltipGroup.querySelector(".chart-tooltip-text") : null;
        var dots = svg.querySelectorAll(".chart-dot");
        var padLeft = parseFloat(svg.getAttribute("data-pad-left") || "60");
        var usableWidth = parseFloat(svg.getAttribute("data-usable-width") || "690");
        var steps = parseInt(svg.getAttribute("data-steps") || "1", 10);
        var padTop = parseFloat(svg.getAttribute("data-pad-top") || "28");

        if (!hoverZone || !crosshair || !tooltipGroup) return;

        function getStepIndex(evt) {{
          var pt = svg.createSVGPoint();
          pt.x = evt.clientX;
          pt.y = evt.clientY;
          var svgPt = pt.matrixTransform(svg.getScreenCTM().inverse());
          var ratio = (svgPt.x - padLeft) / usableWidth;
          return Math.max(0, Math.min(steps, Math.round(ratio * steps)));
        }}

        function getXForStep(stepIdx) {{
          return padLeft + (usableWidth * (stepIdx / steps));
        }}

        hoverZone.addEventListener("mousemove", function(evt) {{
          var stepIdx = getStepIndex(evt);
          var x = getXForStep(stepIdx);
          crosshair.setAttribute("x1", x);
          crosshair.setAttribute("x2", x);
          crosshair.style.display = "block";

          /* Highlight dots at this step, dim others */
          var tooltipLines = [];
          var lineY = padTop + 4;
          dots.forEach(function(dot) {{
            var dx = parseFloat(dot.getAttribute("cx"));
            if (Math.abs(dx - x) < (usableWidth / steps) * 0.6) {{
              dot.classList.add("active");
              dot.classList.remove("dimmed");
              var title = dot.getAttribute("data-title") || "";
              var value = dot.getAttribute("data-value") || "";
              var label = dot.getAttribute("data-label") || "";
              if (tooltipLines.length === 0 && label) {{
                tooltipLines.push(label);
              }}
              tooltipLines.push(title + ": " + value);
            }} else {{
              dot.classList.remove("active");
            }}
          }});

          /* Render tooltip */
          if (tooltipLines.length > 0 && tooltipText && tooltipBg) {{
            tooltipGroup.style.display = "block";
            /* Build tspan elements */
            var tspans = "";
            for (var i = 0; i < tooltipLines.length; i++) {{
              var weight = i === 0 ? "font-weight:600;" : "";
              tspans += '<tspan x="0" dy="' + (i === 0 ? 0 : 15) + '" style="' + weight + '">' + tooltipLines[i] + '</tspan>';
            }}
            tooltipText.innerHTML = tspans;

            /* Position tooltip to the right of crosshair, flip if near edge */
            var tooltipWidth = 200;
            var tooltipHeight = 10 + tooltipLines.length * 15;
            var tx = x + 14;
            if (tx + tooltipWidth > padLeft + usableWidth) {{
              tx = x - tooltipWidth - 14;
            }}
            var ty = padTop + 10;
            tooltipBg.setAttribute("x", tx - 6);
            tooltipBg.setAttribute("y", ty - 12);
            tooltipBg.setAttribute("width", tooltipWidth + 12);
            tooltipBg.setAttribute("height", tooltipHeight + 10);
            tooltipText.setAttribute("transform", "translate(" + tx + "," + ty + ")");
          }}
        }});

        hoverZone.addEventListener("mouseleave", function() {{
          crosshair.style.display = "none";
          tooltipGroup.style.display = "none";
          dots.forEach(function(dot) {{
            dot.classList.remove("active");
            dot.classList.remove("dimmed");
          }});
        }});
      }});
    }})();

    /* ======= Clickable legend to toggle series ======= */
    (function() {{
      document.querySelectorAll(".legend-toggle").forEach(function(legendItem) {{
        legendItem.addEventListener("click", function() {{
          var index = legendItem.getAttribute("data-series-index");
          var panel = legendItem.closest(".chart-panel");
          if (!panel) return;
          var isHidden = legendItem.classList.toggle("legend-hidden");

          /* Find all SVGs in this chart panel */
          panel.querySelectorAll("svg.interactive-chart").forEach(function(svg) {{
            /* Toggle lines */
            svg.querySelectorAll("polyline.chart-line.series-color-" + index).forEach(function(el) {{
              el.classList.toggle("dimmed", isHidden);
            }});
            /* Toggle area fills */
            svg.querySelectorAll("polygon.chart-area").forEach(function(el) {{
              var fill = el.getAttribute("fill") || "";
              if (fill.indexOf("-grad-" + index + ")") !== -1) {{
                el.classList.toggle("dimmed", isHidden);
              }}
            }});
            /* Toggle dots */
            svg.querySelectorAll('circle.chart-dot[data-series="' + index + '"]').forEach(function(el) {{
              el.classList.toggle("dimmed", isHidden);
            }});
            /* Toggle end labels */
            svg.querySelectorAll("text.chart-end-label.series-color-" + index).forEach(function(el) {{
              el.style.display = isHidden ? "none" : "";
            }});
          }});
        }});
      }});
    }})();

    /* ======= Dark mode toggle ======= */
    (function() {{
      var toggle = document.getElementById("theme-toggle");
      var html = document.documentElement;
      var stored = localStorage.getItem("consumer-dashboard-theme");
      if (stored === "dark" || (!stored && window.matchMedia("(prefers-color-scheme: dark)").matches)) {{
        html.classList.add("dark");
        toggle.textContent = "\\u2600";
      }}
      toggle.addEventListener("click", function() {{
        html.classList.toggle("dark");
        var isDark = html.classList.contains("dark");
        toggle.textContent = isDark ? "\\u2600" : "\\u263E";
        localStorage.setItem("consumer-dashboard-theme", isDark ? "dark" : "light");
      }});
    }})();
  </script>
</body>
</html>"""


def build_dashboard_html(settings) -> dict:
    ensure_project_directories(settings)
    payload = build_dashboard_data(settings)
    html = _render_html(payload)
    output_path = settings.processed_dir / "consumer_dashboard.html"
    output_path.write_text(html, encoding="utf-8")
    status = {
        "status": "built",
        "output_path": str(output_path),
        "message": f"Built static HTML dashboard at {output_path}.",
    }
    write_json(settings.processed_dir / "dashboard_html_status.json", status)
    return status
