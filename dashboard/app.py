import time
from datetime import datetime, timezone, timedelta
from collections import deque, Counter

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

from mqtt_client import MqttBuffer
from integrity import verify_hash


st.set_page_config(
    page_title="SOC | Industrial Boiler Monitoring",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ---------- Enhanced Theme Colors ----------
def get_theme_colors():
    return {
        # Backgrounds
        "bg": "#0a0e1a",
        "card_bg": "#111827",
        "card_bg_elevated": "#1a2332",
        "sidebar_bg": "#0f1419",
        
        # Borders
        "border": "#1e293b",
        "border_hover": "#334155",
        "border_accent": "#3b82f6",
        
        # Text - Enhanced visibility
        "text_primary": "#ffffff",
        "text_secondary": "#e2e8f0",
        "text_muted": "#94a3b8",
        "text_dim": "#64748b",
        
        # Accents
        "accent_blue": "#3b82f6",
        "accent_cyan": "#06b6d4",
        "accent_purple": "#8b5cf6",
        "accent_green": "#10b981",
        "accent_emerald": "#059669",
        "accent_red": "#ef4444",
        "accent_orange": "#f97316",
        "accent_yellow": "#f59e0b",
        "accent_amber": "#fbbf24",
        
        # Status colors
        "success": "#22c55e",
        "warning": "#f59e0b",
        "danger": "#ef4444",
        "info": "#3b82f6",
        
        # Grid and charts
        "grid": "#1e293b",
        "chart_bg": "rgba(0,0,0,0)",
        
        # Gradients
        "gradient_blue": "linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%)",
        "gradient_purple": "linear-gradient(135deg, #8b5cf6 0%, #6d28d9 100%)",
        "gradient_red": "linear-gradient(135deg, #ef4444 0%, #dc2626 100%)",
        "gradient_green": "linear-gradient(135deg, #10b981 0%, #059669 100%)",
    }

# ---------- Professional SOC Styling ----------
def get_custom_css():
    colors = get_theme_colors()
    
    return f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&display=swap');

/* ========== GLOBAL RESET ========== */
* {{
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
}}

.stApp {{
    background: {colors['bg']};
    background-image: 
        radial-gradient(at 0% 0%, rgba(59, 130, 246, 0.05) 0px, transparent 50%),
        radial-gradient(at 100% 0%, rgba(139, 92, 246, 0.05) 0px, transparent 50%),
        radial-gradient(at 100% 100%, rgba(59, 130, 246, 0.03) 0px, transparent 50%);
}}

.block-container {{
    padding-top: 2rem;
    padding-bottom: 2rem;
    max-width: 1600px;
}}

/* ========== TYPOGRAPHY ========== */
h1, h2, h3, h4, h5, h6 {{
    font-family: 'Inter', sans-serif !important;
    font-weight: 700 !important;
    color: {colors['text_primary']} !important;
    letter-spacing: -0.025em !important;
    line-height: 1.2 !important;
}}

h1 {{ 
    font-size: 2rem !important; 
    margin-bottom: 0.5rem !important;
}}

p, span, div {{
    font-family: 'Inter', sans-serif !important;
}}

/* SOC Header */
.soc-header {{
    background: {colors['card_bg']};
    border: 1px solid {colors['border']};
    border-radius: 12px;
    padding: 1.5rem 2rem;
    margin-bottom: 2rem;
    display: flex;
    align-items: center;
    justify-content: space-between;
    box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3), 0 2px 4px -1px rgba(0, 0, 0, 0.2);
    position: relative;
    overflow: hidden;
}}

.soc-header::before {{
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 3px;
    background: {colors['gradient_blue']};
}}

.soc-title {{
    font-family: 'Inter', sans-serif !important;
    font-size: 1.75rem !important;
    font-weight: 900 !important;
    color: {colors['text_primary']} !important;
    letter-spacing: 0.05em !important;
    margin: 0 !important;
    text-transform: uppercase !important;
}}

.soc-subtitle {{
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.7rem !important;
    color: {colors['text_secondary']} !important;
    font-weight: 500 !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
    margin-top: 0.35rem !important;
}}

.soc-status {{
    display: flex;
    align-items: center;
    gap: 1rem;
}}

.status-indicator {{
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 1rem;
    background: rgba(16, 185, 129, 0.1);
    border: 1px solid {colors['accent_green']};
    border-radius: 6px;
    font-size: 0.75rem;
    font-weight: 600;
    color: {colors['accent_green']};
    letter-spacing: 0.05em;
}}

.status-dot {{
    width: 8px;
    height: 8px;
    background: {colors['accent_green']};
    border-radius: 50%;
    animation: pulse 2s ease-in-out infinite;
}}

@keyframes pulse {{
    0%, 100% {{ 
        opacity: 1;
        box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7);
    }}
    50% {{ 
        opacity: 0.7;
        box-shadow: 0 0 0 8px rgba(16, 185, 129, 0);
    }}
}}

/* ========== METRIC CARDS ========== */
div[data-testid="stMetric"] {{
    background: {colors['card_bg']};
    border: 1px solid {colors['border']};
    border-radius: 10px;
    padding: 1.5rem 1.25rem !important;
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    min-height: 120px;
    display: flex;
    flex-direction: column;
    justify-content: center;
    position: relative;
    overflow: hidden;
    box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.3);
}}

div[data-testid="stMetric"]::before {{
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 3px;
    background: {colors['border']};
    transition: all 0.3s ease;
}}

div[data-testid="stMetric"]:hover {{
    border-color: {colors['border_accent']};
    transform: translateY(-4px);
    box-shadow: 0 10px 20px -5px rgba(59, 130, 246, 0.3), 0 4px 6px -2px rgba(0, 0, 0, 0.3);
}}

div[data-testid="stMetric"]:hover::before {{
    background: {colors['gradient_blue']};
}}

/* Metric Label */
div[data-testid="stMetricLabel"] {{
    font-family: 'Inter', sans-serif !important;
    font-size: 0.7rem !important;
    font-weight: 700 !important;
    color: {colors['text_secondary']} !important;
    text-transform: uppercase !important;
    letter-spacing: 0.12em !important;
    margin-bottom: 0.75rem !important;
    line-height: 1.3 !important;
}}

div[data-testid="stMetricLabel"] * {{
    font-family: 'Inter', sans-serif !important;
    color: {colors['text_secondary']} !important;
    font-size: 0.7rem !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.12em !important;
}}

/* Metric Value */
div[data-testid="stMetricValue"] {{
    font-family: 'Inter', sans-serif !important;
    font-size: 2.5rem !important;
    font-weight: 900 !important;
    color: {colors['text_primary']} !important;
    letter-spacing: -0.02em !important;
    line-height: 1 !important;
}}

div[data-testid="stMetricValue"] * {{
    font-family: 'Inter', sans-serif !important;
    font-size: 2.5rem !important;
    font-weight: 900 !important;
}}

/* Metric Delta */
div[data-testid="stMetricDelta"] {{
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.75rem !important;
    margin-top: 0.5rem !important;
}}

/* Risk-based Metric Colors */
.metric-normal div[data-testid="stMetric"]::before {{
    background: {colors['gradient_green']};
}}

.metric-normal div[data-testid="stMetricValue"],
.metric-normal div[data-testid="stMetricValue"] * {{
    color: {colors['text_primary']} !important;
}}

.metric-warning div[data-testid="stMetric"]::before {{
    background: {colors['gradient_purple']};
}}

.metric-warning div[data-testid="stMetricValue"],
.metric-warning div[data-testid="stMetricValue"] * {{
    color: {colors['accent_amber']} !important;
}}

.metric-critical div[data-testid="stMetric"]::before {{
    background: {colors['gradient_red']};
    animation: criticalPulse 2s ease-in-out infinite;
}}

.metric-critical div[data-testid="stMetricValue"],
.metric-critical div[data-testid="stMetricValue"] * {{
    color: {colors['accent_red']} !important;
}}

@keyframes criticalPulse {{
    0%, 100% {{ opacity: 1; }}
    50% {{ opacity: 0.6; }}
}}

/* ========== STATUS BADGES ========== */
.badge-container {{
    display: flex;
    align-items: center;
    justify-content: center;
    height: 100%;
    min-height: 120px;
}}

.badge {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 0.5rem;
    padding: 1.25rem 2rem;
    border-radius: 10px;
    font-family: 'Inter', sans-serif !important;
    font-size: 0.85rem !important;
    font-weight: 800 !important;
    letter-spacing: 0.15em !important;
    text-transform: uppercase !important;
    min-height: 70px;
    min-width: 180px;
    cursor: default;
    border: 2px solid transparent;
    transition: all 0.3s ease;
    position: relative;
    overflow: hidden;
}}

.badge::before {{
    content: '';
    position: absolute;
    top: 50%;
    left: 50%;
    width: 0;
    height: 0;
    border-radius: 50%;
    background: rgba(255, 255, 255, 0.1);
    transform: translate(-50%, -50%);
    transition: width 0.6s, height 0.6s;
}}

.badge:hover::before {{
    width: 300px;
    height: 300px;
}}

/* Badge Icons */
.badge-icon {{
    font-size: 1.1rem;
    font-weight: 400;
}}

/* OK Badge */
.badge-ok {{ 
    background: linear-gradient(135deg, rgba(16, 185, 129, 0.15) 0%, rgba(5, 150, 105, 0.15) 100%);
    color: {colors['accent_green']} !important;
    border-color: {colors['accent_green']};
    box-shadow: 0 0 20px rgba(16, 185, 129, 0.2);
}}

/* WARNING Badge */
.badge-warning {{ 
    background: linear-gradient(135deg, rgba(245, 158, 11, 0.15) 0%, rgba(217, 119, 6, 0.15) 100%);
    color: {colors['accent_amber']} !important;
    border-color: {colors['accent_yellow']};
    box-shadow: 0 0 20px rgba(245, 158, 11, 0.2);
}}

/* CRITICAL Badge */
.badge-critical {{ 
    background: linear-gradient(135deg, rgba(239, 68, 68, 0.2) 0%, rgba(220, 38, 38, 0.2) 100%);
    color: {colors['accent_red']} !important;
    border: 2px solid {colors['accent_red']};
    animation: criticalGlow 2s ease-in-out infinite;
    box-shadow: 0 0 30px rgba(239, 68, 68, 0.4);
}}

@keyframes criticalGlow {{
    0%, 100% {{ 
        box-shadow: 0 0 20px rgba(239, 68, 68, 0.4), 0 0 40px rgba(239, 68, 68, 0.2);
        border-color: {colors['accent_red']};
    }}
    50% {{ 
        box-shadow: 0 0 30px rgba(239, 68, 68, 0.6), 0 0 60px rgba(239, 68, 68, 0.3);
        border-color: #ff6b6b;
    }}
}}

/* VERIFIED Badge */
.badge-secure {{ 
    background: linear-gradient(135deg, rgba(16, 185, 129, 0.15) 0%, rgba(5, 150, 105, 0.15) 100%);
    color: {colors['accent_green']} !important;
    border: 2px solid {colors['accent_green']};
    box-shadow: 0 0 20px rgba(16, 185, 129, 0.3);
}}

/* TAMPERED Badge */
.badge-tampered {{ 
    background: linear-gradient(135deg, rgba(239, 68, 68, 0.2) 0%, rgba(220, 38, 38, 0.2) 100%);
    color: {colors['accent_red']} !important;
    border: 2px solid {colors['accent_red']};
    animation: alarmPulse 1s ease-in-out infinite;
    box-shadow: 0 0 30px rgba(239, 68, 68, 0.5);
}}

@keyframes alarmPulse {{
    0%, 100% {{ 
        box-shadow: 0 0 20px rgba(239, 68, 68, 0.5), 0 0 40px rgba(239, 68, 68, 0.3);
        border-color: {colors['accent_red']};
    }}
    50% {{ 
        box-shadow: 0 0 40px rgba(239, 68, 68, 0.8), 0 0 80px rgba(239, 68, 68, 0.4);
        border-color: #ff6b6b;
    }}
}}

/* ========== ALERT BOX ========== */
.alert-box {{
    background: linear-gradient(135deg, rgba(239, 68, 68, 0.1) 0%, rgba(220, 38, 38, 0.1) 100%);
    border: 1px solid {colors['accent_red']};
    border-left: 4px solid {colors['accent_red']};
    border-radius: 10px;
    padding: 1.25rem 1.5rem;
    margin: 1.5rem 0;
    color: {colors['text_primary']} !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 0.9rem !important;
    font-weight: 500 !important;
    display: flex;
    align-items: center;
    gap: 1rem;
    box-shadow: 0 0 30px rgba(239, 68, 68, 0.2);
    animation: alertPulse 2s ease-in-out infinite;
}}

@keyframes alertPulse {{
    0%, 100% {{ 
        box-shadow: 0 0 20px rgba(239, 68, 68, 0.2);
    }}
    50% {{ 
        box-shadow: 0 0 40px rgba(239, 68, 68, 0.4);
    }}
}}

.alert-icon {{
    font-size: 1.5rem;
    min-width: 24px;
}}

.alert-box strong {{
    color: {colors['accent_red']} !important;
    font-weight: 700 !important;
    font-family: 'Inter', sans-serif !important;
}}

/* ========== CHARTS ========== */
[data-testid="stPlotlyChart"] {{
    background: {colors['card_bg']};
    border: 1px solid {colors['border']};
    border-radius: 10px;
    padding: 0.75rem;
    margin-bottom: 1rem;
    box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.3);
    transition: all 0.3s ease;
}}

[data-testid="stPlotlyChart"]:hover {{
    border-color: {colors['border_hover']};
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
}}

/* ========== INFO CARDS ========== */
.info-card {{
    background: {colors['card_bg']};
    border: 1px solid {colors['border']};
    border-radius: 10px;
    overflow: hidden;
    margin-bottom: 1rem;
    box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.3);
    transition: all 0.3s ease;
}}

.info-card:hover {{
    border-color: {colors['border_hover']};
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
}}

.info-card-header {{
    font-family: 'Inter', sans-serif !important;
    font-size: 0.75rem !important;
    font-weight: 700 !important;
    color: {colors['text_secondary']} !important;
    text-transform: uppercase !important;
    letter-spacing: 0.15em !important;
    padding: 1rem 1.25rem;
    background: {colors['card_bg_elevated']};
    border-bottom: 1px solid {colors['border']};
}}

.info-card-body {{
    padding: 1.25rem;
}}

/* KPI Card */
.kpi-card {{
    background: {colors['card_bg']};
    border: 1px solid {colors['border']};
    border-radius: 10px;
    padding: 1.5rem;
    text-align: center;
    transition: all 0.3s ease;
    box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.3);
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
}}

.kpi-card:hover {{
    transform: translateY(-4px);
    box-shadow: 0 8px 16px rgba(0, 0, 0, 0.4);
    border-color: {colors['border_accent']};
}}

.kpi-label {{
    font-family: 'Inter', sans-serif !important;
    font-size: 0.7rem !important;
    font-weight: 700 !important;
    color: {colors['text_secondary']} !important;
    text-transform: uppercase !important;
    letter-spacing: 0.12em !important;
    margin-bottom: 0.75rem !important;
}}

.kpi-value {{
    font-family: 'Inter', sans-serif !important;
    font-size: 2rem !important;
    font-weight: 900 !important;
    color: {colors['text_primary']} !important;
    margin-bottom: 0.5rem !important;
    letter-spacing: -0.01em !important;
}}

.kpi-subtitle {{
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.75rem !important;
    color: {colors['text_muted']} !important;
    font-weight: 500 !important;
}}

.kpi-change {{
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.75rem !important;
    font-weight: 600 !important;
}}

.kpi-change.positive {{
    color: {colors['accent_green']} !important;
}}

.kpi-change.negative {{
    color: {colors['accent_red']} !important;
}}

/* ========== TABS ========== */
.stTabs [data-baseweb="tab-list"] {{
    gap: 0.5rem;
    background: transparent;
    border-bottom: 2px solid {colors['border']};
    padding: 0 0.5rem;
    margin-bottom: 2rem;
}}

.stTabs [data-baseweb="tab"] {{
    background: transparent !important;
    border: 1px solid transparent;
    border-bottom: 3px solid transparent;
    color: {colors['text_muted']} !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 700 !important;
    font-size: 0.8rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
    padding: 0.875rem 1.75rem;
    margin-bottom: -2px;
    transition: all 0.3s ease;
    border-radius: 8px 8px 0 0;
}}

.stTabs [data-baseweb="tab"]:hover {{
    color: {colors['text_primary']} !important;
    background: rgba(59, 130, 246, 0.05) !important;
    border-color: {colors['border_hover']};
    border-bottom-color: {colors['border_hover']};
}}

.stTabs [aria-selected="true"] {{
    color: {colors['accent_blue']} !important;
    border-bottom-color: {colors['accent_blue']} !important;
    background: rgba(59, 130, 246, 0.08) !important;
    border-color: {colors['border']};
}}

/* ========== DATAFRAME ========== */
[data-testid="stDataFrame"] {{
    border: 1px solid {colors['border']};
    border-radius: 10px;
    overflow: hidden;
    box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.3);
}}

[data-testid="stDataFrame"] table {{
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.75rem !important;
}}

[data-testid="stDataFrame"] th {{
    font-family: 'Inter', sans-serif !important;
    font-size: 0.75rem !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
    color: {colors['text_secondary']} !important;
    background: {colors['card_bg_elevated']} !important;
}}

[data-testid="stDataFrame"] td {{
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.8rem !important;
    color: {colors['text_primary']} !important;
}}

/* ========== JSON DISPLAY ========== */
[data-testid="stJson"] {{
    background: {colors['card_bg']} !important;
    border: 1px solid {colors['border']} !important;
    border-radius: 10px !important;
    padding: 1.25rem !important;
    box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.3) !important;
    overflow-x: auto !important;
    max-width: 100% !important;
}}

[data-testid="stJson"] * {{
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.75rem !important;
    letter-spacing: 0.02em !important;
    line-height: 1.7 !important;
    word-break: break-all !important;
    overflow-wrap: break-word !important;
    white-space: pre-wrap !important;
}}

/* ========== DIVIDER ========== */
hr {{
    border-color: {colors['border']} !important;
    margin: 2rem 0 !important;
    opacity: 0.5;
}}

/* ========== SCROLLBAR ========== */
::-webkit-scrollbar {{
    width: 12px;
    height: 12px;
}}

::-webkit-scrollbar-track {{
    background: {colors['bg']};
}}

::-webkit-scrollbar-thumb {{
    background: {colors['border']};
    border-radius: 6px;
    border: 2px solid {colors['bg']};
}}

::-webkit-scrollbar-thumb:hover {{
    background: {colors['border_hover']};
}}

/* ========== THREAT INDICATOR ========== */
.threat-level {{
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 1rem;
    border-radius: 6px;
    font-family: 'Inter', sans-serif !important;
    font-size: 0.7rem !important;
    font-weight: 700 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
}}

.threat-level-low {{
    background: rgba(16, 185, 129, 0.1);
    border: 1px solid {colors['accent_green']};
    color: {colors['accent_green']};
}}

.threat-level-medium {{
    background: rgba(245, 158, 11, 0.1);
    border: 1px solid {colors['accent_yellow']};
    color: {colors['accent_amber']};
}}

.threat-level-high {{
    background: rgba(239, 68, 68, 0.1);
    border: 1px solid {colors['accent_red']};
    color: {colors['accent_red']};
}}

/* ========== TIMELINE ========== */
.timeline-item {{
    display: flex;
    gap: 1rem;
    padding: 0.75rem 0;
    border-left: 2px solid {colors['border']};
    padding-left: 1.5rem;
    position: relative;
    margin-left: 0.5rem;
}}

.timeline-item::before {{
    content: '';
    position: absolute;
    left: -6px;
    top: 1rem;
    width: 10px;
    height: 10px;
    border-radius: 50%;
    background: {colors['accent_blue']};
    border: 2px solid {colors['bg']};
}}

.timeline-time {{
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.7rem !important;
    color: {colors['text_muted']} !important;
    min-width: 80px;
}}

.timeline-content {{
    font-family: 'Inter', sans-serif !important;
    font-size: 0.8rem !important;
    color: {colors['text_secondary']} !important;
}}

/* ========== STATS GRID ========== */
.stats-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
    margin: 1rem 0;
}}

/* ========== SPACING ========== */
.spacing-xs {{ margin-bottom: 0.5rem; }}
.spacing-sm {{ margin-bottom: 0.75rem; }}
.spacing-md {{ margin-bottom: 1.5rem; }}
.spacing-lg {{ margin-bottom: 2rem; }}
.spacing-xl {{ margin-bottom: 3rem; }}
</style>
"""

st.markdown(get_custom_css(), unsafe_allow_html=True)


# ---------- Configuration ----------
broker = "test.mosquitto.org"
topic = "cu/bca/boiler/secure_digital_twin"
refresh_rate = 1
history_window_min = 30


# ---------- Session State Initialization ----------
if "mqtt" not in st.session_state:
    st.session_state.mqtt = MqttBuffer(broker=broker, port=1883, topic=topic, qos=1, maxlen=10000)
    st.session_state.mqtt.start()

if "alert_history" not in st.session_state:
    st.session_state.alert_history = deque(maxlen=50)

if "integrity_violations" not in st.session_state:
    st.session_state.integrity_violations = 0

if "total_packets" not in st.session_state:
    st.session_state.total_packets = 0

mqtt = st.session_state.mqtt


# ---------- Helper Functions ----------
def to_df(buffer_list):
    if not buffer_list:
        return pd.DataFrame()

    df = pd.DataFrame(buffer_list)

    for col in ["device_id", "timestamp", "temperature", "pressure", "status", "hash"]:
        if col not in df.columns:
            df[col] = None

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])
    df = df.sort_values("timestamp")

    def check_integrity(row):
        row_dict = row.to_dict()
        if pd.notna(row_dict.get('timestamp')):
            row_dict['timestamp'] = row_dict['timestamp'].isoformat()
        return verify_hash(row_dict)
    
    df["integrity_ok"] = df.apply(check_integrity, axis=1)
    return df


def badge(label, badge_type, icon=""):
    return f'<div class="badge badge-{badge_type}"><span class="badge-icon">{icon}</span>{label}</div>'


def get_threat_level(temp, pressure, integrity_ok):
    """Calculate threat level based on parameters"""
    if not integrity_ok:
        return "HIGH", "high"
    
    score = 0
    if temp >= 100 or pressure >= 50:
        score = 3
    elif temp >= 85 or pressure >= 40:
        score = 2
    else:
        score = 1
    
    if score >= 3:
        return "HIGH", "high"
    elif score >= 2:
        return "MEDIUM", "medium"
    else:
        return "LOW", "low"


def create_gauge(title, value, unit, vmin, vmax, color, subtitle=None):
    colors = get_theme_colors()
    
    muted_color = f"rgba({int(color[1:3], 16)}, {int(color[3:5], 16)}, {int(color[5:7], 16)}, 0.8)"
    
    # Determine status zones
    warning_threshold = vmax * 0.7
    critical_threshold = vmax * 0.85
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        number={
            "suffix": f" {unit}", 
            "font": {"size": 44, "color": colors['text_primary'], "family": "Inter", "weight": 900}
        },
        title={
            "text": f"{title}<br><span style='font-size:12px;color:{colors['text_secondary']}'>{subtitle if subtitle else ''}</span>", 
            "font": {"size": 16, "color": colors['text_primary'], "family": "Inter", "weight": 700}
        },
        gauge={
            "axis": {
                "range": [vmin, vmax], 
                "tickwidth": 2, 
                "tickcolor": colors['grid'],
                "tickfont": {"color": colors['text_secondary'], "size": 12, "family": "JetBrains Mono"}
            },
            "bar": {"color": muted_color, "thickness": 0.7},
            "bgcolor": colors['card_bg'],
            "borderwidth": 0,
            "steps": [
                {"range": [vmin, warning_threshold], "color": f"rgba(16, 185, 129, 0.08)"},
                {"range": [warning_threshold, critical_threshold], "color": f"rgba(245, 158, 11, 0.12)"},
                {"range": [critical_threshold, vmax], "color": f"rgba(239, 68, 68, 0.15)"}
            ],
            "threshold": {
                "line": {"color": colors['accent_red'], "width": 4},
                "thickness": 0.8,
                "value": critical_threshold
            }
        }
    ))
    
    fig.update_layout(
        height=260,
        margin=dict(l=20, r=20, t=80, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": colors['text_secondary'], "family": "Inter", "size": 12}
    )
    return fig


def create_line_chart(df, y_col, title, unit, color, show_anomalies=False):
    colors = get_theme_colors()
    
    muted_line = f"rgba({int(color[1:3], 16)}, {int(color[3:5], 16)}, {int(color[5:7], 16)}, 0.9)"
    
    fig = go.Figure()
    
    # Main line
    fig.add_trace(go.Scatter(
        x=df["timestamp"],
        y=df[y_col],
        mode="lines",
        name=y_col.capitalize(),
        line=dict(color=muted_line, width=2.5, shape='spline'),
        fill='tozeroy',
        fillcolor=f'rgba({int(color[1:3], 16)}, {int(color[3:5], 16)}, {int(color[5:7], 16)}, 0.1)',
        hovertemplate=f'<b>%{{y:.2f}} {unit}</b><br>%{{x|%H:%M:%S}}<extra></extra>'
    ))
    
    # Add anomaly markers if requested
    if show_anomalies and y_col in df.columns and len(df) > 10:
        mean_val = df[y_col].mean()
        std_val = df[y_col].std()
        anomalies = df[(df[y_col] > mean_val + 2*std_val) | (df[y_col] < mean_val - 2*std_val)]
        
        if not anomalies.empty:
            fig.add_trace(go.Scatter(
                x=anomalies["timestamp"],
                y=anomalies[y_col],
                mode="markers",
                name="Anomalies",
                marker=dict(
                    size=12,
                    color=colors['accent_red'],
                    symbol='x',
                    line=dict(width=2, color=colors['accent_red'])
                ),
                hovertemplate=f'<b>ANOMALY</b><br>%{{y:.2f}} {unit}<br>%{{x|%H:%M:%S}}<extra></extra>'
            ))
    
    fig.update_layout(
        height=320,
        margin=dict(l=20, r=20, t=60, b=50),
        title={
            "text": f"{title.upper()}",
            "font": {"size": 14, "color": colors['text_primary'], "family": "Inter", "weight": 700},
            "x": 0.02,
            "xanchor": "left"
        },
        xaxis=dict(
            title=None,
            showgrid=True, 
            gridcolor=f"rgba({int(colors['grid'][1:3], 16)}, {int(colors['grid'][3:5], 16)}, {int(colors['grid'][5:7], 16)}, 0.3)",
            gridwidth=1,
            tickfont={"color": colors['text_primary'], "size": 11, "family": "JetBrains Mono"},
            linecolor=colors['border'],
            tickformat="%H:%M:%S"
        ),
        yaxis=dict(
            title=None,
            showgrid=True, 
            gridcolor=f"rgba({int(colors['grid'][1:3], 16)}, {int(colors['grid'][3:5], 16)}, {int(colors['grid'][5:7], 16)}, 0.3)",
            gridwidth=1,
            tickfont={"color": colors['text_primary'], "size": 11, "family": "JetBrains Mono"},
            linecolor=colors['border'],
            ticksuffix=f" {unit}"
        ),
        showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": colors['text_primary'], "family": "Inter", "size": 12},
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor=colors['card_bg_elevated'],
            font_size=12,
            font_family="JetBrains Mono",
            bordercolor=colors['border']
        )
    )
    
    fig.update_xaxes(
        rangeslider=dict(visible=False),
        rangeselector=dict(
            buttons=list([
                dict(count=5, label="5M", step="minute", stepmode="backward"),
                dict(count=15, label="15M", step="minute", stepmode="backward"),
                dict(count=30, label="30M", step="minute", stepmode="backward"),
                dict(step="all", label="ALL")
            ]),
            font=dict(color=colors['text_primary'], size=11, family="Inter", weight=600),
            bgcolor=colors['card_bg'],
            activecolor=colors['accent_blue'],
            bordercolor=colors['border'],
            borderwidth=1,
            x=1.0,
            xanchor="right",
            y=1.15
        )
    )
    
    return fig


def create_status_distribution(df):
    """Create a pie chart showing status distribution"""
    colors = get_theme_colors()
    
    if df.empty:
        return None
    
    status_counts = df['status'].value_counts()
    
    color_map = {
        'ok': colors['accent_green'],
        'OK': colors['accent_green'],
        'warning': colors['accent_yellow'],
        'WARNING': colors['accent_yellow'],
        'critical': colors['accent_red'],
        'CRITICAL': colors['accent_red']
    }
    
    pie_colors = [color_map.get(status, colors['text_muted']) for status in status_counts.index]
    
    fig = go.Figure(data=[go.Pie(
        labels=status_counts.index,
        values=status_counts.values,
        hole=0.6,
        marker=dict(colors=pie_colors, line=dict(color=colors['border'], width=2)),
        textfont=dict(size=12, family="Inter", weight=600),
        hovertemplate='<b>%{label}</b><br>Count: %{value}<br>%{percent}<extra></extra>'
    )])
    
    fig.update_layout(
        title={
            "text": "STATUS DISTRIBUTION",
            "font": {"size": 14, "color": colors['text_primary'], "family": "Inter", "weight": 700},
            "x": 0.02,
            "xanchor": "left"
        },
        annotations=[dict(text=f'{len(df)}<br><span style="font-size:12px">TOTAL</span>', x=0.5, y=0.5, font_size=18, font_family="Inter", 
                         font_color=colors['text_primary'], font_weight=900, showarrow=False)],
        height=280,
        margin=dict(l=20, r=20, t=60, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=True,
        legend=dict(
            font=dict(color=colors['text_primary'], family="Inter", size=12),
            bgcolor="rgba(0,0,0,0)",
            bordercolor=colors['border'],
            borderwidth=1
        )
    )
    
    return fig


def create_dual_axis_chart(df):
    """Create a chart with dual y-axes for temperature and pressure"""
    colors = get_theme_colors()
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["temperature"],
            name="Temperature",
            line=dict(color=colors['accent_blue'], width=2),
            hovertemplate='<b>Temp:</b> %{y:.1f} °C<extra></extra>'
        ),
        secondary_y=False,
    )
    
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["pressure"],
            name="Pressure",
            line=dict(color=colors['accent_purple'], width=2),
            hovertemplate='<b>Pressure:</b> %{y:.1f} PSI<extra></extra>'
        ),
        secondary_y=True,
    )
    
    fig.update_xaxes(
        title_text=None,
        showgrid=True,
        gridcolor=f"rgba({int(colors['grid'][1:3], 16)}, {int(colors['grid'][3:5], 16)}, {int(colors['grid'][5:7], 16)}, 0.3)",
        tickfont={"color": colors['text_primary'], "size": 11, "family": "JetBrains Mono"},
        tickformat="%H:%M:%S"
    )
    
    fig.update_yaxes(
        title_text="Temperature (°C)",
        title_font=dict(color=colors['accent_blue'], size=12, family="Inter"),
        tickfont={"color": colors['accent_blue'], "size": 11, "family": "JetBrains Mono"},
        showgrid=True,
        gridcolor=f"rgba({int(colors['grid'][1:3], 16)}, {int(colors['grid'][3:5], 16)}, {int(colors['grid'][5:7], 16)}, 0.3)",
        secondary_y=False
    )
    
    fig.update_yaxes(
        title_text="Pressure (PSI)",
        title_font=dict(color=colors['accent_purple'], size=12, family="Inter"),
        tickfont={"color": colors['accent_purple'], "size": 11, "family": "JetBrains Mono"},
        showgrid=False,
        secondary_y=True
    )
    
    fig.update_layout(
        title={
            "text": "CORRELATED PARAMETER ANALYSIS",
            "font": {"size": 14, "color": colors['text_primary'], "family": "Inter", "weight": 700},
            "x": 0.02,
            "xanchor": "left"
        },
        height=320,
        margin=dict(l=20, r=20, t=60, b=50),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(color=colors['text_primary'], family="Inter", size=12),
            bgcolor="rgba(0,0,0,0)"
        ),
        hoverlabel=dict(
            bgcolor=colors['card_bg_elevated'],
            font_size=11,
            font_family="JetBrains Mono",
            bordercolor=colors['border']
        )
    )
    
    return fig


# ---------- Header ----------
st.markdown(f'''
    <div class="soc-header">
        <div>
            <div class="soc-title"> INDUSTRIAL BOILER </div>
            <div class="soc-subtitle">SHA-256 INTEGRITY • REAL-TIME TELEMETRY • THREAT ANALYTICS</div>
        </div>
        <div class="soc-status">
            <div class="status-indicator">
                <div class="status-dot"></div>
                <span>SYSTEM OPERATIONAL</span>
            </div>
        </div>
    </div>
''', unsafe_allow_html=True)


# ---------- Main Loop ----------
placeholder = st.empty()

while True:
    with placeholder.container():
        raw = list(mqtt.buffer)
        df = to_df(raw)

        if df.empty:
            st.info("◉ Connecting to MQTT broker and waiting for telemetry data...")
            time.sleep(refresh_rate)
            st.rerun()

        now = datetime.now(timezone.utc)
        window_start = pd.Timestamp(now) - pd.Timedelta(minutes=history_window_min)
        df_recent = df[df["timestamp"] >= window_start].copy()
        
        if df_recent.empty:
            df_recent = df.tail(100)
            
        latest = df_recent.iloc[-1].to_dict()

        # Update session state
        st.session_state.total_packets = len(df)
        integrity_violations = len(df[df["integrity_ok"] == False])
        st.session_state.integrity_violations = integrity_violations

        integrity_ok = bool(latest.get("integrity_ok", False))
        status = str(latest.get("status", "Unknown"))
        temp = float(latest.get('temperature', 0))
        pressure = float(latest.get('pressure', 0))

        # Calculate simplified metrics
        temp_mean = df_recent['temperature'].mean()
        pressure_mean = df_recent['pressure'].mean()
        temp_max = df_recent['temperature'].max()
        pressure_max = df_recent['pressure'].max()

        # Determine risk levels
        def get_temp_risk(t):
            if t >= 100: return "critical"
            elif t >= 85: return "warning"
            return "normal"
        
        def get_pressure_risk(p):
            if p >= 50: return "critical"
            elif p >= 40: return "warning"
            return "normal"
        
        temp_risk = get_temp_risk(temp)
        pressure_risk = get_pressure_risk(pressure)
        
        threat_level_text, threat_level_class = get_threat_level(temp, pressure, integrity_ok)

        # Add to alert history if there's an issue
        if not integrity_ok or temp_risk != "normal" or pressure_risk != "normal":
            alert_entry = {
                "timestamp": datetime.now(timezone.utc),
                "type": "INTEGRITY VIOLATION" if not integrity_ok else f"{temp_risk.upper()} RISK",
                "message": f"Temp: {temp:.1f}°C, Pressure: {pressure:.1f} PSI"
            }
            if len(st.session_state.alert_history) == 0 or st.session_state.alert_history[-1]["message"] != alert_entry["message"]:
                st.session_state.alert_history.append(alert_entry)

        # ========== TOP METRICS ROW ==========
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown(f'<div class="metric-{temp_risk}">', unsafe_allow_html=True)
            st.metric("Temperature", f"{temp:.1f}°C")
            st.markdown('</div>', unsafe_allow_html=True)

        with col2:
            st.markdown(f'<div class="metric-{pressure_risk}">', unsafe_allow_html=True)
            st.metric("Pressure", f"{pressure:.1f} PSI")
            st.markdown('</div>', unsafe_allow_html=True)

        with col3:
            status_type = "ok"
            status_icon = "✓"
            if status.lower() == "warning":
                status_type = "warning"
                status_icon = "△"
            elif status.lower() == "critical":
                status_type = "critical"
                status_icon = "⚠"
            
            st.markdown(f'<div class="badge-container">{badge(status.upper(), status_type, status_icon)}</div>', unsafe_allow_html=True)

        with col4:
            if integrity_ok:
                st.markdown(f'<div class="badge-container">{badge("VERIFIED", "secure", "✓")}</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="badge-container">{badge("TAMPERED", "tampered", "⚠")}</div>', unsafe_allow_html=True)

        # ========== KPI ROW: Temperature & Pressure Stats ==========
        kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
        
        with kpi_col1:
            st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">Max Temperature</div>
                    <div class="kpi-value">{temp_max:.1f}°C</div>
                </div>
            ''', unsafe_allow_html=True)
        
        with kpi_col2:
            st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">Avg Temperature</div>
                    <div class="kpi-value">{temp_mean:.1f}°C</div>
                </div>
            ''', unsafe_allow_html=True)
        
        with kpi_col3:
            st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">Max Pressure</div>
                    <div class="kpi-value">{pressure_max:.1f} PSI</div>
                </div>
            ''', unsafe_allow_html=True)
        
        with kpi_col4:
            st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">Avg Pressure</div>
                    <div class="kpi-value">{pressure_mean:.1f} PSI</div>
                </div>
            ''', unsafe_allow_html=True)

        # Alert Banner
        if not integrity_ok:
            st.markdown(
                f"""
                <div class="alert-box">
                    <span class="alert-icon">⚠</span>
                    <div>
                        <strong>SECURITY ALERT:</strong> Integrity verification failed. Data signature invalid — potential false data injection detected.
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )
        elif temp_risk == "critical" or pressure_risk == "critical":
            st.markdown(
                f"""
                <div class="alert-box">
                    <span class="alert-icon">△</span>
                    <div>
                        <strong>CRITICAL ALERT:</strong> Parameters have exceeded critical thresholds. Immediate attention required.
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )

        st.markdown('<div class="spacing-md"></div>', unsafe_allow_html=True)

        # ========== THREAT & STATUS ROW ==========
        threat_col1, threat_col2, threat_col3 = st.columns(3)
        
        with threat_col1:
            threat_icon = "●" if threat_level_class == 'low' else "▲" if threat_level_class == 'medium' else "⬤"
            st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">Threat Level</div>
                    <div class="threat-level threat-level-{threat_level_class}">
                        {threat_icon} {threat_level_text}
                    </div>
                </div>
            ''', unsafe_allow_html=True)
        
        with threat_col2:
            uptime_hours = (datetime.now(timezone.utc) - df["timestamp"].min()).total_seconds() / 3600 if not df.empty else 0
            st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">System Uptime</div>
                    <div class="kpi-value">{uptime_hours:.1f}h</div>
                    <div class="kpi-subtitle">Continuous Monitoring</div>
                </div>
            ''', unsafe_allow_html=True)
        
        with threat_col3:
            violation_pct = (integrity_violations / st.session_state.total_packets * 100) if st.session_state.total_packets > 0 else 0
            integrity_score = 100 - violation_pct
            st.markdown(f'''
                <div class="kpi-card">
                    <div class="kpi-label">Integrity Score</div>
                    <div class="kpi-value">{integrity_score:.1f}%</div>
                    <div class="kpi-subtitle">{st.session_state.total_packets} Total Packets</div>
                </div>
            ''', unsafe_allow_html=True)

        st.markdown('<div class="spacing-lg"></div>', unsafe_allow_html=True)
        
        # ========== TABS ==========
        tab1, tab2, tab3 = st.tabs(["■ Live Monitoring", "■ Security Analysis", "■ System Health"])

        with tab1:
            # Gauges Row
            gauge_col1, gauge_col2 = st.columns(2)
            with gauge_col1:
                st.plotly_chart(
                    create_gauge("TEMPERATURE", temp, "°C", 0, 120, get_theme_colors()['accent_blue'], 
                                f"Average: {temp_mean:.1f}°C"),
                    use_container_width=True,
                    key="gauge_temp"
                )
            with gauge_col2:
                st.plotly_chart(
                    create_gauge("PRESSURE", pressure, "PSI", 0, 60, get_theme_colors()['accent_purple'],
                                f"Average: {pressure_mean:.1f} PSI"),
                    use_container_width=True,
                    key="gauge_pressure"
                )

            st.markdown('<div class="spacing-sm"></div>', unsafe_allow_html=True)
            
            # Line Charts Row
            chart_col1, chart_col2 = st.columns(2)
            with chart_col1:
                st.plotly_chart(
                    create_line_chart(df_recent, "temperature", "Temperature Trend", "°C", 
                                    get_theme_colors()['accent_blue'], show_anomalies=True),
                    use_container_width=True,
                    key="chart_temp"
                )
            with chart_col2:
                st.plotly_chart(
                    create_line_chart(df_recent, "pressure", "Pressure Trend", "PSI", 
                                    get_theme_colors()['accent_purple'], show_anomalies=True),
                    use_container_width=True,
                    key="chart_pressure"
                )

            st.markdown('<div class="spacing-sm"></div>', unsafe_allow_html=True)

            # Dual Axis Chart
            st.plotly_chart(
                create_dual_axis_chart(df_recent),
                use_container_width=True,
                key="dual_axis_chart"
            )

        with tab2:
            security_col1, security_col2 = st.columns([1, 1.5])

            with security_col1:
                st.markdown(f'''
                    <div class="info-card">
                        <div class="info-card-header">▸ Latest Packet</div>
                    </div>
                ''', unsafe_allow_html=True)
                
                view = {
                    "device_id": latest.get("device_id"),
                    "timestamp": str(latest.get("timestamp")),
                    "temperature": f"{temp:.2f} °C",
                    "pressure": f"{pressure:.2f} PSI",
                    "status": latest.get("status"),
                    "hash": latest.get("hash"),
                    "integrity": "✓ Verified" if integrity_ok else "✗ Tampered",
                    "threat_level": threat_level_text
                }
                st.json(view)

                st.markdown('<div class="spacing-md"></div>', unsafe_allow_html=True)

                # Status Distribution
                status_fig = create_status_distribution(df_recent)
                if status_fig:
                    st.plotly_chart(status_fig, use_container_width=True, key="status_dist")

            with security_col2:
                st.markdown(f'''
                    <div class="info-card">
                        <div class="info-card-header">▸ Event Log</div>
                    </div>
                ''', unsafe_allow_html=True)
                
                df_events = df_recent.tail(25)[
                    ["timestamp", "temperature", "pressure", "status", "integrity_ok"]
                ].copy()

                df_events["timestamp"] = df_events["timestamp"].dt.strftime("%H:%M:%S")
                df_events["temperature"] = df_events["temperature"].apply(lambda x: f"{x:.1f}°C")
                df_events["pressure"] = df_events["pressure"].apply(lambda x: f"{x:.1f} PSI")
                df_events["integrity_ok"] = df_events["integrity_ok"].apply(
                    lambda x: "✓ Secure" if x else "✗ Tampered"
                )
                df_events.columns = ["Time", "Temp", "Pressure", "Status", "Integrity"]

                st.dataframe(
                    df_events, 
                    use_container_width=True, 
                    height=420, 
                    hide_index=True,
                )

                # Recent Alerts
                if st.session_state.alert_history:
                    st.markdown('<div class="spacing-md"></div>', unsafe_allow_html=True)
                    st.markdown(f'''
                        <div class="info-card">
                            <div class="info-card-header">▸ Recent Alerts</div>
                        </div>
                    ''', unsafe_allow_html=True)
                    
                    for alert in list(st.session_state.alert_history)[-5:]:
                        st.markdown(f'''
                            <div class="timeline-item">
                                <div class="timeline-time">{alert['timestamp'].strftime('%H:%M:%S')}</div>
                                <div class="timeline-content"><strong>{alert['type']}</strong>: {alert['message']}</div>
                            </div>
                        ''', unsafe_allow_html=True)

        with tab3:
            health_col1, health_col2, health_col3 = st.columns(3)
            
            with health_col1:
                data_quality = (len(df_recent) / (history_window_min * 60) * 100) if history_window_min > 0 else 0
                st.markdown(f'''
                    <div class="kpi-card">
                        <div class="kpi-label">Data Quality</div>
                        <div class="kpi-value">{min(data_quality, 100):.0f}%</div>
                        <div class="kpi-subtitle">{len(df_recent)} packets received</div>
                    </div>
                ''', unsafe_allow_html=True)
            
            with health_col2:
                healthy_packets = len(df_recent[df_recent['integrity_ok'] == True])
                health_pct = (healthy_packets / len(df_recent) * 100) if len(df_recent) > 0 else 0
                st.markdown(f'''
                    <div class="kpi-card">
                        <div class="kpi-label">System Health</div>
                        <div class="kpi-value">{health_pct:.1f}%</div>
                        <div class="kpi-subtitle">{healthy_packets} verified packets</div>
                    </div>
                ''', unsafe_allow_html=True)
            
            with health_col3:
                critical_events = len(df_recent[df_recent['status'].str.lower() == 'critical']) if 'status' in df_recent.columns else 0
                st.markdown(f'''
                    <div class="kpi-card">
                        <div class="kpi-label">Critical Events</div>
                        <div class="kpi-value">{critical_events}</div>
                        <div class="kpi-subtitle">Last {history_window_min} minutes</div>
                    </div>
                ''', unsafe_allow_html=True)

            st.markdown('<div class="spacing-md"></div>', unsafe_allow_html=True)

            # System Metrics Grid
            sys_col1, sys_col2 = st.columns(2)
            
            with sys_col1:
                st.markdown(f'''
                    <div class="info-card">
                        <div class="info-card-header">▸ System Metrics</div>
                    </div>
                ''', unsafe_allow_html=True)
                
                metrics_data = {
                    "Metric": ["MQTT Connection", "Buffer Utilization", "Data Window", "Refresh Rate"],
                    "Value": [
                        "Connected",
                        f"{len(mqtt.buffer):,} packets",
                        f"{history_window_min} minutes",
                        f"{refresh_rate} second(s)"
                    ],
                    "Status": ["● ACTIVE", "● ACTIVE", "● ACTIVE", "● ACTIVE"]
                }
                st.dataframe(pd.DataFrame(metrics_data), use_container_width=True, hide_index=True, height=200)
            
            with sys_col2:
                st.markdown(f'''
                    <div class="info-card">
                        <div class="info-card-header">▸ Network Statistics</div>
                    </div>
                ''', unsafe_allow_html=True)
                
                network_data = {
                    "Parameter": ["Broker Address", "Topic", "QoS Level", "Protocol"],
                    "Value": [
                        broker,
                        topic.split('/')[-1],
                        "1 (At least once)",
                        "MQTT v3.1.1"
                    ]
                }
                st.dataframe(pd.DataFrame(network_data), use_container_width=True, hide_index=True, height=200)

    time.sleep(refresh_rate)
    st.rerun()