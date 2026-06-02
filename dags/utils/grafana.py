"""Shared Grafana dashboard-as-code utilities.

Provides highly parameterized helper functions to construct panels (stat, time series,
bar chart, pie chart, table, row) and full dashboards programmatically.
Eliminates thousands of lines of duplicated JSON-like boilerplate.
"""

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

PLUGIN_VERSION = "11.4.0"
SCHEMA_VERSION = 40
DEFAULT_DATASOURCE = {"type": "grafana-postgresql-datasource", "uid": "postgres"}


def create_row_panel(panel_id: int, title: str, y: int) -> dict[str, Any]:
    """Create a collapsible dashboard row panel."""
    return {
        "collapsed": False,
        "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
        "id": panel_id,
        "title": title,
        "type": "row",
    }


def create_stat_panel(
    panel_id: int,
    title: str,
    sql: str,
    x: int,
    y: int,
    w: int,
    h: int,
    unit: str | None = None,
    color_mode: str = "value",
    decimals: int | None = None,
) -> dict[str, Any]:
    """Create a single stat panel (KPI card)."""
    defaults: dict[str, Any] = {
        "color": {"mode": "thresholds"},
        "mappings": [],
        "thresholds": {
            "mode": "absolute",
            "steps": [{"color": "green", "value": None}],
        },
    }
    if unit:
        defaults["unit"] = unit
    if decimals is not None:
        defaults["decimals"] = decimals

    return {
        "datasource": DEFAULT_DATASOURCE,
        "fieldConfig": {
            "defaults": defaults,
            "overrides": [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {
            "colorMode": color_mode,
            "graphMode": "none",
            "justifyMode": "auto",
            "orientation": "auto",
            "reduceOptions": {
                "calcs": ["lastNotNull"],
                "fields": "",
                "values": False,
            },
            "textMode": "auto",
        },
        "pluginVersion": PLUGIN_VERSION,
        "targets": [{"format": "table", "rawSql": sql, "refId": "A"}],
        "title": title,
        "type": "stat",
    }


def create_time_series_panel(
    panel_id: int,
    title: str,
    sql: str,
    x: int,
    y: int,
    w: int,
    h: int,
    unit: str | None = None,
    legend_mode: str = "list",
    draw_style: str = "line",
) -> dict[str, Any]:
    """Create a time series visualization panel."""
    defaults: dict[str, Any] = {
        "color": {"mode": "palette-classic"},
        "custom": {
            "axisBorderShow": False,
            "axisCenteredZero": False,
            "axisColorMode": "text",
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": draw_style,
            "fillOpacity": 10 if draw_style == "line" else 80,
            "gradientMode": "none",
            "hideFrom": {"legend": False, "tooltip": False, "viz": False},
            "lineInterpolation": "linear",
            "lineWidth": 2,
            "pointSize": 5,
            "scaleDistribution": {"type": "linear"},
            "showPoints": "auto",
            "spanNulls": False,
            "stacking": {"group": "A", "mode": "none"},
            "thresholdsStyle": {"mode": "off"},
        },
        "mappings": [],
        "thresholds": {
            "mode": "absolute",
            "steps": [{"color": "green", "value": None}],
        },
    }
    if unit:
        defaults["unit"] = unit

    return {
        "datasource": DEFAULT_DATASOURCE,
        "fieldConfig": {
            "defaults": defaults,
            "overrides": [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {
            "legend": {
                "calcs": [],
                "displayMode": legend_mode,
                "placement": "bottom",
                "showLegend": True,
            },
            "tooltip": {"mode": "multi", "sort": "none"},
        },
        "pluginVersion": PLUGIN_VERSION,
        "targets": [{"format": "time_series", "rawSql": sql, "refId": "A"}],
        "title": title,
        "type": "timeseries",
    }


def create_bar_panel(
    panel_id: int,
    title: str,
    sql: str,
    x: int,
    y: int,
    w: int,
    h: int,
    unit: str | None = None,
    orientation: str = "vertical",
    legend_display: str = "hidden",
) -> dict[str, Any]:
    """Create a bar chart panel."""
    defaults: dict[str, Any] = {
        "color": {"mode": "palette-classic"},
        "custom": {
            "axisBorderShow": False,
            "axisCenteredZero": False,
            "axisColorMode": "text",
            "axisLabel": "",
            "axisPlacement": "auto",
            "fillOpacity": 80,
            "gradientMode": "none",
            "hideFrom": {"legend": False, "tooltip": False, "viz": False},
            "lineWidth": 1,
            "scaleDistribution": {"type": "linear"},
            "thresholdsStyle": {"mode": "off"},
        },
        "mappings": [],
        "thresholds": {
            "mode": "absolute",
            "steps": [{"color": "green", "value": None}],
        },
    }
    if unit:
        defaults["unit"] = unit

    return {
        "datasource": DEFAULT_DATASOURCE,
        "fieldConfig": {
            "defaults": defaults,
            "overrides": [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {
            "barRadius": 0,
            "barWidth": 0.6,
            "fullHighlight": False,
            "groupWidth": 0.7,
            "legend": {
                "calcs": [],
                "displayMode": legend_display,
                "placement": "bottom",
                "showLegend": legend_display != "hidden",
            },
            "orientation": orientation,
            "showValue": "always",
            "stacking": "none",
            "tooltip": {"mode": "single", "sort": "none"},
            "xTickLabelRotation": 0,
            "xTickLabelSpacing": 0,
        },
        "pluginVersion": PLUGIN_VERSION,
        "targets": [{"format": "table", "rawSql": sql, "refId": "A"}],
        "title": title,
        "type": "barchart",
    }


def create_pie_panel(
    panel_id: int,
    title: str,
    sql: str,
    x: int,
    y: int,
    w: int,
    h: int,
    pie_type: str = "donut",
) -> dict[str, Any]:
    """Create a pie or donut chart panel."""
    return {
        "datasource": DEFAULT_DATASOURCE,
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "palette-classic"},
                "custom": {"hideFrom": {"legend": False, "tooltip": False, "viz": False}},
                "mappings": [],
                "thresholds": {
                    "mode": "absolute",
                    "steps": [{"color": "green", "value": None}],
                },
            },
            "overrides": [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {
            "legend": {
                "calcs": ["lastNotNull"],
                "displayMode": "list",
                "placement": "bottom",
                "showLegend": True,
            },
            "pieType": pie_type,
            "reduceOptions": {
                "calcs": ["lastNotNull"],
                "fields": "",
                "values": True,
            },
            "tooltip": {"mode": "single", "sort": "none"},
        },
        "pluginVersion": PLUGIN_VERSION,
        "targets": [{"format": "table", "rawSql": sql, "refId": "A"}],
        "title": title,
        "type": "piechart",
    }


def create_table_panel(
    panel_id: int,
    title: str,
    sql: str,
    x: int,
    y: int,
    w: int,
    h: int,
    overrides: list[dict[str, Any]] | None = None,
    sort_by: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Create a grid table panel with column overrides and sorting rules."""
    return {
        "datasource": DEFAULT_DATASOURCE,
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "thresholds"},
                "custom": {
                    "align": "auto",
                    "cellOptions": {"type": "auto"},
                    "filterable": True,
                    "inspect": True,
                },
                "mappings": [],
                "thresholds": {
                    "mode": "absolute",
                    "steps": [{"color": "green", "value": None}],
                },
            },
            "overrides": overrides or [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {
            "cellHeight": "sm",
            "footer": {
                "countRows": True,
                "enablePagination": True,
                "fields": "",
                "reducer": ["count"],
                "show": False,
            },
            "showHeader": True,
            "sortBy": sort_by or [],
        },
        "pluginVersion": PLUGIN_VERSION,
        "targets": [{"format": "table", "rawSql": sql, "refId": "A"}],
        "title": title,
        "type": "table",
    }


def create_dashboard_structure(
    uid: str,
    title: str,
    description: str,
    panels: list[dict[str, Any]],
    tags: list[str] | None = None,
    time_from: str = "now-30d",
) -> dict[str, Any]:
    """Build the final dashboard structure wrapping panel definitions."""
    return {
        "annotations": {
            "list": [
                {
                    "builtIn": 1,
                    "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                    "enable": True,
                    "hide": True,
                    "iconColor": "rgba(0, 211, 255, 1)",
                    "name": "Annotations & Alerts",
                    "type": "dashboard",
                }
            ]
        },
        "description": description,
        "editable": True,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 0,
        "id": None,
        "links": [],
        "panels": panels,
        "preload": False,
        "refresh": "5m",
        "schemaVersion": SCHEMA_VERSION,
        "tags": tags or [],
        "templating": {"list": []},
        "time": {"from": time_from, "to": "now"},
        "timepicker": {},
        "timezone": "browser",
        "title": title,
        "uid": uid,
        "version": 1,
        "weekStart": "monday",
    }


def save_dashboard(dashboard: dict[str, Any], output_path: Path) -> None:
    """Save the constructed dashboard to a JSON file."""
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(dashboard, f, indent=2, ensure_ascii=False)
        logger.info("Saved dashboard successfully to %s", output_path)
    except OSError as e:
        logger.error("Failed to write dashboard to %s: %s", output_path, e)
        raise
