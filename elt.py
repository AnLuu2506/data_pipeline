import time
import inspect
import functools
import itertools
import duckdb
import requests
import os
import polars as pl
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
from pandas.tseries.offsets import DateOffset
import numpy as np
import subprocess
import re
import glob
import math
import builtins
import gcsfs

from typing import Union
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from multiprocessing import Pool  # added
from datetime import datetime, timedelta           # covers datetime, timedelta, etc.
from dateutil.relativedelta import *
from gspread.utils import *
from pandas.tseries.offsets import DateOffset  # added
from functools import reduce      # added
import random  # added for exponential backoff jitter

# Import for image generation
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter

# Import for OpenAI
import openai
import base64
from openai import OpenAI

# Import for Excel handling
#import xlsxwriter

# Import for zip & json handling
import zipfile
import json, ast
import glob
import pytz

# Import for temporary file handling
import tempfile

#Import for LLM class
from pathlib import Path

# Import for DockerOperator def
# from airflow.providers.docker.operators.docker import DockerOperator
# from docker.types import Mount

# Import for Airflow
def _sleep_n_minutes(n_min: int):
    time.sleep(n_min * 60)
    print(f"Slept for {n_min} minutes")
    return n_min

# --- Retry decorator for Google Sheets API ---
def gsheet_retry(max_retries: int = 5, initial_delay: float = 2.0):
    """Decorator for retrying Google Sheets API calls with exponential backoff."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(self, *args, **kwargs)
                except Exception as e:
                    last_exception = e
                    error_str = str(e).lower()
                    
                    # Check if it's a rate limit, quota, or service unavailable error
                    is_retryable = any([
                        "429" in error_str,
                        "503" in error_str,
                        "quota exceeded" in error_str,
                        "rate limit" in error_str,
                        "too many requests" in error_str,
                        "service is currently unavailable" in error_str,
                        "backend error" in error_str,
                    ])
                    
                    if not is_retryable or attempt == max_retries - 1:
                        raise
                    
                    # Exponential backoff with jitter
                    delay = initial_delay * (2 ** attempt) + random.uniform(0, 1)
                    print(f"⚠️ Google Sheets rate limit hit (attempt {attempt + 1}/{max_retries}). "
                          f"Retrying in {delay:.1f}s...")
                    time.sleep(delay)
            
            raise last_exception
        return wrapper
    return decorator

# --- ETL classes ---
class ETL:
    def __init__(self, client) -> None:
        self.google_client = client['google_client']
        self.bigquery_client = client['bigquery_client']
        self.df = None
        print(' ------ class ETL was created!')

class Charting:
    """
    A class for creating advanced visualizations with dual-axis charts.
    Designed for displaying metrics with both absolute values and percentage changes.
    """
    
    def __init__(self, style='darkgrid', palette='husl'):
        """
        Initialize the Charting class with seaborn styling.
        
        Args:
            style: seaborn style ('darkgrid', 'whitegrid', 'dark', 'white', 'ticks')
            palette: color palette for the charts
        """
        sns.set_style(style)
        sns.set_palette(palette)
        
    def dual_axis_line_bar(
        self, 
        df, 
        x_col,
        value_col='value', 
        volume_col='volume',
        title='Metric Trend with Volume',
        x_label='Time Period',
        value_label='Value',
        volume_label='Volume',
        figsize=(14, 6),
        line_color='#1E88E5',
        bar_color='#FFA726',
        save_path=None,
        category_col=None,
        bar_category=None
    ):
        """
        Create a dual-axis chart with line plot for values and bar chart for volume.
        
        Args:
            df: pandas or polars DataFrame
            x_col: column name for x-axis (time/period)
            value_col: column name for the metric value (line chart)
            volume_col: column name for the volume metric (bar chart)
            title: chart title
            x_label: x-axis label
            value_label: label for primary y-axis (values)
            volume_label: label for secondary y-axis (volume)
            figsize: figure size as tuple (width, height)
            line_color: color for the line chart (used when category_col is None)
            bar_color: color for volume bars
            save_path: path to save the figure (optional)
            category_col: column name for categories (for multiple line series)
            bar_category: specific category value to show bars for (required if category_col is used)
            
        Returns:
            matplotlib figure object
        """
        # Convert polars to pandas if needed
        if hasattr(df, 'to_pandas'):
            df = df.to_pandas()
        

        # Create figure and primary axis
        fig, ax1 = plt.subplots(figsize=figsize)
        
        if category_col:
            # Handle multiple categories
            categories = df[category_col].unique()
            for cat in categories:
                cat_df = df[df[category_col] == cat].reset_index(drop=True)
                # Use solid line for bar_category, dashed for others
                if cat == bar_category:
                    ax1.plot(cat_df[x_col], cat_df[value_col], 
                            marker='o', linewidth=1, markersize=1, 
                            label=f'{cat}', alpha=0.8, linestyle='-')
                else:
                    ax1.plot(cat_df[x_col], cat_df[value_col], 
                            marker='o', linewidth=0.5, markersize=1, 
                            label=f'{cat}', alpha=0.8, linestyle='--')
                
                # Add percentage labels on markers
                for idx, (x_val, y_val) in enumerate(zip(cat_df[x_col], cat_df[value_col])):
                    if not pd.isna(y_val):
                        percentage_label = f'{y_val*100:.1f}%'
                        # Position label very close to marker - use tiny offset relative to actual y value
                        y_offset = y_val * 0.005
                        ax1.text(x_val, y_val + y_offset, percentage_label, 
                                fontsize=4, ha='center', va='bottom', 
                                rotation=0, alpha=0.7)
        else:
            # Single series
            ax1.plot(df[x_col], df[value_col], 
                    color=line_color, marker='o', linewidth=1, 
                    markersize=1, label=value_label, alpha=0.9)
            
            # Add percentage labels on markers
            for x_val, y_val in zip(df[x_col], df[value_col]):
                if not pd.isna(y_val):
                    percentage_label = f'{y_val*100:.1f}%'
                    # Position label very close to marker - use tiny offset relative to actual y value
                    y_offset = y_val * 0.005
                    ax1.text(x_val, y_val + y_offset, percentage_label, 
                            fontsize=4, ha='center', va='bottom', 
                            rotation=0, alpha=0.7)
        
        # Style primary axis
        ax1.set_xlabel(x_label, fontsize=12, fontweight='bold')
        ax1.set_ylabel(value_label, fontsize=12, fontweight='bold', color=line_color)
        ax1.tick_params(axis='y', labelcolor=line_color)
        ax1.grid(False)
        
        # Format left axis as percentage
        from matplotlib.ticker import PercentFormatter
        ax1.yaxis.set_major_formatter(PercentFormatter(1.0))
        
        # Set y-axis to start from 0 and double the vertical space
        # Get the max value and set upper limit to keep labels visible
        max_val = df[value_col].max() if not category_col else df[value_col].max()
        ax1.set_ylim(bottom=0, top=max_val*1.15 if max_val > 0 else 1)
        
        # Create secondary axis for volume bars
        ax2 = ax1.twinx()
        
        if category_col:
            # Show bars only for the specified category
            if bar_category is None:
                raise ValueError("bar_category must be specified when using category_col")
            
            # Filter data for the selected category
            bar_df = df[df[category_col] == bar_category].copy()
            
            # Plot volume bars
            bars = ax2.bar(bar_df[x_col], bar_df[volume_col], 
                   color=bar_color, alpha=0.6, width=0.5, 
                   label=f'{bar_category} {volume_label}')
            
            # Add value labels on bars (in thousands with 'k' or millions with 'M')
            for bar, val in zip(bars, bar_df[volume_col]):
                height = bar.get_height()
                if height > 0:
                    if height >= 1_000_000:
                        label = f'{height/1_000_000:.1f}M'
                    elif height >= 1_000:
                        label = f'{height/1_000:.0f}k'
                    else:
                        label = f'{height:.0f}'
                    ax2.text(bar.get_x() + bar.get_width()/2., height/2,
                            label, ha='center', va='center', fontsize=6, 
                            fontweight='bold', color='black')
        else:
            # Single series bars for volume
            bars = ax2.bar(df[x_col], df[volume_col], 
                   color=bar_color, alpha=0.6, width=0.5, 
                   label=volume_label)
            
            # Add value labels on bars (in thousands with 'k' or millions with 'M')
            for bar, val in zip(bars, df[volume_col]):
                height = bar.get_height()
                if height > 0:
                    if height >= 1_000_000:
                        label = f'{height/1_000_000:.1f}M'
                    elif height >= 1_000:
                        label = f'{height/1_000:.0f}k'
                    else:
                        label = f'{height:.0f}'
                    ax2.text(bar.get_x() + bar.get_width()/2., height/2,
                            label, ha='center', va='center', fontsize=6, 
                            fontweight='bold', color='black')
        
        # Style secondary axis
        ax2.set_ylabel(volume_label, fontsize=12, fontweight='bold')
        ax2.tick_params(axis='y')
        
        # Format right axis with k for thousands and M for millions
        def format_volume(x, pos):
            if x >= 1_000_000:
                return f'{x/1_000_000:.1f}M'
            elif x >= 1_000:
                return f'{x/1_000:.0f}k'
            else:
                return f'{x:.0f}'
        
        from matplotlib.ticker import FuncFormatter
        ax2.yaxis.set_major_formatter(FuncFormatter(format_volume))
        
        # Title and legend
        plt.title(title, fontsize=14, fontweight='bold', pad=20)
        
        # Combine legends from both axes
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, 
                  loc='upper left', framealpha=0.9, fontsize=5)
        
        # Rotate x-axis labels for better readability
        ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45, ha='right', fontsize=7)
        ax1.tick_params(axis='both', labelsize=5)
        ax2.tick_params(axis='both', labelsize=5)
        
        # Adjust layout
        plt.tight_layout()
        
        # Save if path provided
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"Chart saved to: {save_path}")
        
        return fig
    
    def four_line_week_month(
        self,
        df,
        x_col='x_label',
        sort_col='sort_key',
        section_col='section',
        week_cy_col='week_cy',
        week_py_col='week_py',
        month_cy_col='month_cy',
        month_py_col='month_py',
        title=None,
        subtitle='Bưu Cục cảnh báo',
        figsize=(18, 6),
        colors=None,
        value_format='percent',
        save_path=None,
    ):
        """
        Draw a 4-line chart split into two sections on the same x-axis:
          Left  section (section=1): weekly CY & PY lines
          Right section (section=2): monthly CY & PY lines

        The DataFrame must have columns produced by the dbt macro
        `four_line_week_month_chart` (or equivalent):
          x_label, sort_key, section, week_cy, week_py, month_cy, month_py

        Args:
            df          : pandas or polars DataFrame
            x_col       : x-axis label column
            sort_col    : sort order column (date or numeric)
            section_col : 1 = weeks, 2 = months
            week_cy_col : current-year weekly metric column
            week_py_col : prior-year weekly metric column
            month_cy_col: current-year monthly metric column
            month_py_col: prior-year monthly metric column
            title       : main chart title
            subtitle    : subtitle shown below title
            figsize     : matplotlib figure size
            colors      : dict with keys 'week_cy','week_py','month_cy','month_py'
            value_format: 'percent' multiplies by 100 and adds %, 'number' raw value
            save_path   : optional path to save the figure

        Returns:
            matplotlib figure object
        """
        # Convert polars → pandas
        if hasattr(df, 'to_pandas'):
            df = df.to_pandas()

        # Sort within each section
        df = df.sort_values([section_col, sort_col]).reset_index(drop=True)

        # Split sections
        weeks  = df[df[section_col] == 1].reset_index(drop=True)
        months = df[df[section_col] == 2].reset_index(drop=True)

        # Default colors — match the reference chart
        if colors is None:
            colors = {
                'week_cy' : '#1A237E',   # dark blue  (solid)
                'week_py' : '#90CAF9',   # light blue (solid)
                'month_cy': '#B71C1C',   # dark red   (solid)
                'month_py': '#FFCDD2',   # light pink (solid)
            }

        def _fmt(v):
            if pd.isna(v):
                return None
            if value_format == 'percent':
                return f'{v * 100:.1f}%'
            if value_format == 'volume':
                return f'{v / 1_000_000:.1f}M' if abs(v) >= 1_000_000 else f'{v:,.0f}'
            if value_format == 'currency':
                if abs(v) >= 1_000_000_000:
                    return f'{v / 1_000_000_000:.1f}B'
                if abs(v) >= 1_000_000:
                    return f'{v / 1_000_000:.1f}M'
                return f'{v:,.0f}'
            return f'{v:,.1f}'  # 'number' / default

        def _plot_series(ax, x_vals, y_vals, color, label, linewidth=1.5, markersize=4):
            """Plot one line, skipping NaN segments."""
            xs = list(range(len(x_vals)))
            ys = [v if not pd.isna(v) else None for v in y_vals]

            # Draw line through non-null points
            valid_x = [x for x, y in zip(xs, ys) if y is not None]
            valid_y = [y for y in ys if y is not None]
            ax.plot(valid_x, valid_y,
                    color=color, marker='o',
                    linewidth=linewidth, markersize=markersize,
                    label=label)

            # Add data labels
            for x, y in zip(xs, ys):
                if y is not None:
                    lbl = _fmt(y)
                    ax.annotate(
                        lbl,
                        xy=(x, y),
                        xytext=(0, 5),
                        textcoords='offset points',
                        ha='center', va='bottom',
                        fontsize=7, color=color, fontweight='bold'
                    )

        # ── Build figure with two sub-axes joined visually ──────────────────
        n_weeks  = len(weeks)
        n_months = len(months)
        total    = n_weeks + n_months

        # Width ratio: weeks take proportional space
        width_ratios = [n_weeks, n_months] if n_weeks and n_months else [1, 1]
        fig, (ax_w, ax_m) = plt.subplots(
            1, 2,
            figsize=figsize,
            gridspec_kw={'width_ratios': width_ratios},
            sharey=True
        )

        # ── WEEK axes ────────────────────────────────────────────────────────
        if n_weeks:
            _plot_series(ax_w, weeks[x_col], weeks[week_cy_col],
                         colors['week_cy'], 'Week 2026', linewidth=2)
            _plot_series(ax_w, weeks[x_col], weeks[week_py_col],
                         colors['week_py'], 'Week 2025', linewidth=1.5)

            ax_w.set_xticks(range(n_weeks))
            ax_w.set_xticklabels(weeks[x_col], rotation=45, ha='right', fontsize=8)
            ax_w.tick_params(axis='y', labelsize=8)
            ax_w.grid(axis='y', linestyle='--', alpha=0.4)
            ax_w.spines['right'].set_visible(False)

            ax_w.legend(loc='upper left', fontsize=7, framealpha=0.8)

        # ── MONTH axes ───────────────────────────────────────────────────────
        if n_months:
            _plot_series(ax_m, months[x_col], months[month_cy_col],
                         colors['month_cy'], 'Month 2025', linewidth=2)
            _plot_series(ax_m, months[x_col], months[month_py_col],
                         colors['month_py'], 'Month 2024', linewidth=1.5)

            ax_m.set_xticks(range(n_months))
            ax_m.set_xticklabels(months[x_col], rotation=45, ha='right', fontsize=8)
            ax_m.tick_params(axis='y', labelsize=8)
            ax_m.grid(axis='y', linestyle='--', alpha=0.4)
            ax_m.spines['left'].set_visible(False)
            ax_m.yaxis.set_ticks_position('right')

            ax_m.legend(loc='upper left', fontsize=7, framealpha=0.8)

        # ── Single shared Y-axis scale (all 4 series) ────────────────────────
        all_vals = pd.concat([
            weeks[week_cy_col] if n_weeks else pd.Series(dtype=float),
            weeks[week_py_col] if n_weeks else pd.Series(dtype=float),
            months[month_cy_col] if n_months else pd.Series(dtype=float),
            months[month_py_col] if n_months else pd.Series(dtype=float),
        ]).dropna()
        if not all_vals.empty:
            ax_w.set_ylim(bottom=0, top=all_vals.max() * 1.25)

        from matplotlib.ticker import FuncFormatter
        if value_format == 'percent':
            _axis_fmt = FuncFormatter(lambda v, _: f'{v*100:.0f}%')
        elif value_format == 'volume':
            def _axis_fmt(v, _):
                return f'{v/1_000_000:.1f}M' if abs(v) >= 1_000_000 else f'{v:,.0f}'
            _axis_fmt = FuncFormatter(_axis_fmt)
        elif value_format == 'currency':
            def _axis_fmt(v, _):
                if abs(v) >= 1_000_000_000:
                    return f'{v/1_000_000_000:.1f}B'
                if abs(v) >= 1_000_000:
                    return f'{v/1_000_000:.1f}M'
                return f'{v:,.0f}'
            _axis_fmt = FuncFormatter(_axis_fmt)
        else:
            _axis_fmt = FuncFormatter(lambda v, _: f'{v:,.1f}')
        ax_w.yaxis.set_major_formatter(_axis_fmt)
        ax_m.yaxis.set_major_formatter(_axis_fmt)

        # ── Shared title ─────────────────────────────────────────────────────
        fig.suptitle(title, fontsize=13, fontweight='bold', color='darkorange', y=1.01)
        if subtitle:
            fig.text(0.5, 0.98, subtitle, ha='center', fontsize=9, color='steelblue')

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"Chart saved to: {save_path}")

        return fig

    @staticmethod
    def close_all():
        """Close all matplotlib figures to free memory."""
        plt.close('all')

class LLM:
    """
    Mixin: TOON formatter + OpenAI call helpers.
    Assumes:
      - Polars DataFrame as input
      - OPENAI_API_KEY is available in env
    """

    # -------------------------
    # TOON helpers
    # -------------------------
    def _sanitize_toon_value(self, v) -> str:
        if v is None:
            return ""
        s = str(v)
        s = s.replace("\r", "\\r").replace("\n", "\\n")
        if ("," in s) or (s != s.strip()) or ('"' in s):
            s = s.replace('"', '""')
            return f'"{s}"'
        return s

    def polars_to_toon(self, df_pl, *, root_name: str = "rows", max_rows: int | None = None) -> str:
        cols = df_pl.columns
        n = df_pl.height
        take_n = min(n, max_rows) if max_rows else n

        header = f"{root_name}[{take_n}]{{{','.join(cols)}}}:"
        if take_n == 0:
            return header

        df_small = df_pl.head(take_n)
        rows = df_small.to_dicts()

        lines = [header]
        for r in rows:
            line = ",".join(self._sanitize_toon_value(r.get(c)) for c in cols)
            lines.append(f"  {line}")
        return "\n".join(lines)

    def write_toon_file(self, toon_text: str, *, prefix: str = "bq_extract", out_dir: str = "/tmp") -> str:
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = Path(out_dir) / f"{prefix}_{ts}.toon"
        out_path.write_text(toon_text, encoding="utf-8")
        return str(out_path)

    # -------------------------
    # Analytics quick summary
    # -------------------------
    def build_quick_analytics_text(self, df_pl, *, title: str = "BQ -> GSheet Load", topk: int = 8) -> str:
        n_rows = df_pl.height
        n_cols = df_pl.width
        cols = df_pl.columns

        null_ratios = []
        for c in cols:
            try:
                null_cnt = df_pl.select(df_pl[c].is_null().sum()).item()
                null_ratios.append((c, null_cnt, (null_cnt / n_rows if n_rows else 0.0)))
            except Exception:
                continue

        null_ratios.sort(key=lambda x: x[2], reverse=True)
        top_null = "\n".join(
            [f"- {c}: {cnt}/{n_rows} ({ratio:.1%})" for c, cnt, ratio in null_ratios[:topk]]
        ) or "- (no null stats)"

        return (
            f"📊 {title}\n"
            f"- rows: {n_rows:,}\n"
            f"- cols: {n_cols:,}\n"
            f"- top null columns:\n{top_null}\n"
        )

    # -------------------------
    # OpenAI call
    # -------------------------
    def call_openai_with_toon(
        self,
        toon_text: str,
        *,
        system_instructions: str,
        instructions: str,
        model: str = "gpt-4",
        api_key_env: str = "OPENAI_API_KEY",
    ) -> str:
        """
        Requires: pip install openai
        Env: OPENAI_API_KEY
        """
        api_key = api_key_env or os.getenv(api_key_env)
        if not api_key:
            raise RuntimeError(f"Missing env var {api_key_env}. Cannot call OpenAI API.")
        
        client = OpenAI(api_key=api_key)

        # Combine instructions and toon_text into a user message
        user_message = f"{instructions}\n\nData:\n{toon_text}"
        
        print("[INFO] Calling OpenAI API...")
        print("----- System Instructions -----")
        print(system_instructions)
        print("----- User Message -----")
        print(user_message[:1000]) # Print only the first 100 characters for brevity
        
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_instructions},
                {"role": "user", "content": user_message}
            ],
            temperature=0.7,
        )
        return resp.choices[0].message.content or ""

class Extract(ETL):
    def __init__(self, client) -> None:
        super().__init__(client)
        print(' ------ class Extract was created!')

    def sleep_n_minutes(self, n_min: int):
        """Sleep for n_min minutes."""
        import time
        time.sleep(n_min * 60)
        print(f"Slept for {n_min} minutes")
        return n_min

    def get_data_from_bigquery(self, sql):  # pl is the best choice
        if self.bigquery_client:
            query = self.bigquery_client.query(sql)
            self.df = query.result().to_arrow()
            self.df = pl.from_arrow(self.df)
            print("------ got data from BigQuery successfully!")
            return self.df
        else:
            print("------ could not find client BigQuery")

    def get_file_by_format(self, link_file, format, **param):
        match format:
            case 'csv':
                self.df = pl.read_csv(link_file, infer_schema=False, **param)
                print("------ read data from csv: ", link_file)
                return self.df
            case 'parquet':
                self.df = pl.read_parquet(link_file, **param)
                print("------ read data from parquet: ", link_file)
                return self.df
            case 'xls':
                self.df = pl.from_pandas(pd.read_excel(link_file, engine="xlrd", **param)[0])
                print("------ read data from excel xsl: ", link_file)
                return self.df
            case 'html':
                self.df = pl.from_pandas(pd.read_html(link_file, **param)[0])
                print("------ read data from excel html: ", link_file)
                return self.df
            case 'xlsx':
                self.df = pl.from_pandas(pd.read_excel(link_file, **param))
                print("------ read data from excel: ", link_file)
                return self.df

    def get_file_by_format_limit_column(self, link_file, format, column_name, **param):
        match format:
            case 'csv':
                self.df = pl.read_csv(link_file, infer_schema=False, **param)[:, :71]
                self.df.columns = column_name
                print("------ read data from csv: ", link_file)
                return self.df

    def get_data_from_drive(self, link_file, format, **param):
        files = glob.glob(link_file + f"/*.{format}")
        self.df = pl.concat([self.get_file_by_format(
            f, format, **param) for f in files], how="vertical")
        print("------ read data from csv: ", link_file)
        return self.df

    def get_data_from_drive_limit_column(self, link_file, format, column_name, regex_string, **param):
        files = glob.glob(link_file + f"/*.{format}")
        files = [f for f in files if re.search(regex_string, f)]
        self.df = pl.concat([self.get_file_by_format_limit_column(
            f, format, column_name, **param) for f in files], how="vertical")
        print("------ read data from csv: ", link_file)
        return self.df

    def get_data_from_drive_by_condition(self, link_file, format, limit_file: int, *param):
        files = glob.glob(link_file + f"*.{format}")
        files.sort(reverse=True)
        files = files[:limit_file]
        self.df = pl.concat([self.get_file_by_format(
            f, format, *param) for f in files], how="vertical")
        print("------ read data from csv: ", link_file)
        return self.df

    @gsheet_retry()
    def get_data_from_sheet(self,spreadsheet,sheet='',range_data ='', column=''):
        header = column if column else sheet.get_values(range_data)[0]
        data=sheet.get_values(range_data)[1:]
        print(sheet.title)
        self.df = pl.DataFrame(data=data, schema=header, orient="row").with_columns(pl.lit(sheet.title).alias("sheet_name"))
        if len(self.df) < 2:
            raise ValueError(f"Sheet '{sheet.title}' has fewer than 2 data rows (got {len(self.df)}). Aborting to prevent overwriting with empty data.")
        return self.df

    def get_data_from_gsheet(self, link_gsheet='', sheet_name_regex='', range_data='', column=None):
        spreadsheet = self.google_client.open_by_url(link_gsheet)
        all_sheets = spreadsheet.worksheets()

        # IMPORTANT: pass the Worksheet object (not .title)
        listofFrame = [
            self.get_data_from_sheet(spreadsheet, sheet, range_data, column)
            for sheet in all_sheets
            if re.search(sheet_name_regex, sheet.title) is not None
        ]

        self.df = pl.concat(listofFrame, how="vertical") if listofFrame else pl.DataFrame()
        print('------ got data from google sheet successfully!')
        return self.df

    def get_data_from_gsheet_dynamic_column(self, sheet_config: dict = None):
        listofFrame_all_version = []
        a = 0
        for version, config in sheet_config.items():
            print(f"Version: {version}")
            print(f"  url: {config.get('url')}")
            print(f"  sheet_name_regex: {config.get('sheet_name_regex')}")
            print(f"  data_range: {config.get('range_data')}")
            print(f"  column: {config.get('column', None)}")
            print(f"  column_output: {config.get('column_output', None)}")
            a = config.get('sheet_name_regex')
            spreadsheet = self.google_client.open_by_url(config.get('url'))
            sheet_name_regex = config.get('sheet_name_regex')
            range_data = config.get('range_data')
            column = config.get('column', None)
            column_output = config.get('column_output', None)
            all_sheets = spreadsheet.worksheets()
            listofFrame = [self.get_data_from_sheet(spreadsheet, sheet, range_data, column)
                           for sheet in all_sheets if re.search(sheet_name_regex, sheet.title) is not None]
            listofFrame_aligned = pl.concat(listofFrame, how="vertical").select(column_output)
            listofFrame_all_version.append(listofFrame_aligned)

        self.df = pl.concat(listofFrame_all_version, how="vertical")
        print(a)
        self.df.write_csv(f"data{a}.csv")
        print('------ got data from google sheet successfully!')
        return self.df

    def get_file_size(self, file_path):
        return os.path.getsize(file_path)/1024

    def is_empty_file(self, file_path):
        return os.path.getsize(file_path)/1024 < 1

    def get_average_file_size(self, folder_path):
        total_size = __builtins__.sum([os.path.getsize(
            folder_path + filename) / 1024 for filename in os.listdir(folder_path)])
        file_count = len(os.listdir(folder_path))
        return total_size / file_count if file_count > 0 else 0

    def check_file_capacity(self, folder_path, file_path):
        """Calculates the average file size in KB for all files in a given folder."""
        average_size = self.get_average_file_size(folder_path)
        file_size = self.get_file_size(file_path)
        return 1 if file_size > average_size/10 else 0

    def check_file_nearests(self, link_path):
        updated_date = re.search(r'(\d{4}-\d{2}-\d{2})', link_path).group(1)
        current_date = datetime.now().strftime('%Y-%m-%d')
        return 1 if (datetime.strptime(current_date, '%Y-%m-%d') - datetime.strptime(updated_date, '%Y-%m-%d')).days <= 2 else 0

class Load(ETL):
    # ✅ Inner notifier class with a distinct name
    class TelegramNotifier:
        def __init__(self, telegram_token: str, chat_id: int, topic_id: int = None) -> None:
            self.telegram_token = telegram_token
            self.chat_id = chat_id
            self.topic_id = topic_id
            print(" ------ class TelegramNotifier (inside Load) was created!")

        def send_message(self, message: str):
            try:
                url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                payload = {
                    "chat_id": self.chat_id, 
                    "text": message,
                    "parse_mode": "HTML"
                }
                if self.topic_id:
                    payload["message_thread_id"] = self.topic_id
                response = requests.post(url, json=payload, timeout=5)
                return response.json()
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                print(f"⚠️ Telegram notification failed (network unreachable): {e}")
                return {"ok": False, "error": str(e)}

    def __init__(self, client, telegram_token=None, chat_id=None, topic_id=None) -> None:
        super().__init__(client)
        # ✅ Create a TelegramNotifier instance automatically if creds are given
        self.notifier = None
        if telegram_token and chat_id:
            self.notifier = self.TelegramNotifier(telegram_token, chat_id, topic_id)
        print(" ------ class Load was created!")

    def _repair_delta_log_created_time_null(self, log_file_path: str) -> bool:
        """
        Repair invalid Delta metadata entries where metaData.createdTime is null.
        delta-rs expects this field to be a long, but some schema-evolution writes
        can leave a null in the transaction log and wedge later reads/merges.
        """
        if not log_file_path:
            return False

        normalized_path = log_file_path.replace("bigstore/", "gs://", 1)
        if not normalized_path.startswith("gs://"):
            return False

        fs = gcsfs.GCSFileSystem()
        replacement_created_time = int(time.time() * 1000)
        changed = False
        repaired_lines = []

        with fs.open(normalized_path, "r") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue

                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    repaired_lines.append(line)
                    continue

                meta = payload.get("metaData")
                if isinstance(meta, dict) and meta.get("createdTime") is None:
                    meta["createdTime"] = replacement_created_time
                    payload["metaData"] = meta
                    changed = True

                repaired_lines.append(json.dumps(payload, separators=(",", ":")))

        if changed:
            with fs.open(normalized_path, "w") as f:
                f.write("\n".join(repaired_lines) + "\n")
            print(f"[INFO] Repaired Delta log createdTime=null in {normalized_path}")

        return changed

    def _sanitize_delta_log_created_time(self, destination: str) -> int:
        """
        Scan a Delta table's _delta_log directory and repair any JSON log files
        where metaData.createdTime is null.
        """
        if not destination or not str(destination).startswith("gs://"):
            return 0

        table_root = str(destination).rstrip("/")
        delta_log_glob = f"{table_root}/_delta_log/*.json"
        fs = gcsfs.GCSFileSystem()

        try:
            log_files = sorted(fs.glob(delta_log_glob))
        except Exception as e:
            print(f"[WARN] Unable to list Delta log files at {delta_log_glob}: {e}")
            return 0

        repaired_count = 0
        for log_file in log_files:
            log_path = log_file if str(log_file).startswith("gs://") else f"gs://{log_file}"
            if self._repair_delta_log_created_time_null(log_path):
                repaired_count += 1

        if repaired_count:
            print(f"[INFO] Repaired {repaired_count} Delta log file(s) under {table_root}/_delta_log")

        return repaired_count

    def _repair_delta_log_from_error(self, error: Exception) -> bool:
        """
        Parse a delta-rs error and repair the referenced log file if the failure
        was caused by metaData.createdTime being null.
        """
        error_text = str(error)
        if "createdTime" not in error_text or "Expected type: long, but found: null" not in error_text:
            return False

        match = re.search(r"(gs://[^\s]+_delta_log/\d+\.json|bigstore/[^\s]+_delta_log/\d+\.json)", error_text)
        if not match:
            return False

        try:
            return self._repair_delta_log_created_time_null(match.group(1))
        except Exception as repair_error:
            print(f"[WARN] Delta log repair failed for {match.group(1)}: {repair_error}")
            return False

    def _load_delta_table_with_repair(self, destination: str, storage_options=None, *, sanitize_full_log: bool = False):
        from deltalake import DeltaTable

        try:
            if sanitize_full_log:
                self._sanitize_delta_log_created_time(destination)
            return DeltaTable(destination, storage_options=storage_options)
        except Exception as e:
            if self._repair_delta_log_from_error(e):
                print("[INFO] Retrying DeltaTable load after log repair")
                if sanitize_full_log:
                    self._sanitize_delta_log_created_time(destination)
                return DeltaTable(destination, storage_options=storage_options)
            raise

    def _write_delta_with_repair(self, df, destination: str, mode: str, *, delta_write_options=None, delta_merge_options=None, sanitize_full_log: bool = False):
        try:
            result = df.write_delta(
                destination,
                mode=mode,
                delta_write_options=delta_write_options or {},
                delta_merge_options=delta_merge_options,
            )
            if sanitize_full_log:
                self._sanitize_delta_log_created_time(destination)
            return result
        except Exception as e:
            if self._repair_delta_log_from_error(e):
                print(f"[INFO] Retrying Delta write in mode='{mode}' after log repair")
                result = df.write_delta(
                    destination,
                    mode=mode,
                    delta_write_options=delta_write_options or {},
                    delta_merge_options=delta_merge_options,
                )
                if sanitize_full_log:
                    self._sanitize_delta_log_created_time(destination)
                return result
            raise

    def update_to_bucket_by_format(self, df, destination, format, delta_merge_options=None,  delta_write_options=None, partition_column=None, merge_keys=None):
        """
        Write a Polars DataFrame to GCS bucket in the specified format.
        
        Args:
            df: Polars DataFrame to write
            destination: GCS path (e.g., gs://bucket/path/file.csv)
            format: Output format ('csv', 'parquet', 'delta_overwrite', 'delta_merge', 'delta_append')
            delta_write_options: Optional dict with Delta Lake write options
            delta_merge_options: Optional dict with Delta Lake merge options (for delta_merge)
            partition_column: Column name used for partition-level scope (for delta_merge)
                             This column is kept in the data (not moved to folder paths) for BigQuery visibility
            merge_keys: List of column names used to match source and target rows (for delta_merge)
                       If not provided, uses partition_column as the only key
        """
        match format:
            case 'csv':
                df.write_csv(destination)
                print(f"------ wrote CSV to: {destination}")
            case 'parquet':
                df.write_parquet(destination)
                print(f"------ wrote Parquet to: {destination}")
            case 'delta_overwrite':
                # Simple overwrite mode - replaces entire table
                self._write_delta_with_repair(
                    df,
                    destination, 
                    mode="overwrite",
                    delta_write_options=delta_write_options or {}
                )
                print(f"------ wrote Delta (overwrite) to: {destination}")
            case 'delta_append':
                # Append mode - adds new data without replacing existing
                self._write_delta_with_repair(
                    df,
                    destination,
                    mode="append",
                    delta_write_options=delta_write_options or {}
                )
                print(f"------ wrote Delta (append) to: {destination}")
            case 'delta_merge':
                # Use merge mode for partition-level overwrite
                # This preserves other partitions while replacing the current partition
                try:
                    partition_col = partition_column
                    write_opts = delta_write_options or {}

                    # Pre-evolve schema before MERGE only when new columns are detected.
                    # Delta MERGE doesn't apply schema evolution for newly added columns,
                    # so we append 1 row first to register new columns in the Delta log.
                    # Skipped when schema is unchanged to avoid writing metadata with createdTime:null.
                    try:
                        try:
                            _existing = self._load_delta_table_with_repair(
                                destination,
                                storage_options=write_opts.get("storage_options"),
                                sanitize_full_log=False
                            )
                            _existing_cols = set(f.name for f in _existing.schema().fields)
                            _new_cols = set(df.columns) - _existing_cols
                        except Exception:
                            _new_cols = set(df.columns)  # Table doesn't exist yet

                        if _new_cols:
                            print(f"🧩 New columns detected: {_new_cols}. Running schema evolution pre-step...")
                            self._write_delta_with_repair(
                                df.head(1),
                                destination,
                                mode="append",
                                delta_write_options=write_opts,
                                sanitize_full_log=True,
                            )
                            print("🧩 Delta schema evolution pre-step completed (1-row append with schema_mode=merge).")
                        else:
                            print("🧩 Schema unchanged — skipping pre-evolution step.")
                    except Exception as schema_evolve_error:
                        print(f"[WARN] Pre-schema evolution step skipped/failed: {schema_evolve_error}")
                    
                    if partition_col and partition_col in df.columns:
                        # Get unique partition values in the new data
                        partition_values = df[partition_col].unique().to_list()
                        
                        # Build merge predicate with proper row-level matching
                        # merge_keys: columns used to match source rows to target rows
                        # partition_column: used to scope deletion to affected partitions only
                        if merge_keys:
                            # Resolve merge_keys case-insensitively against actual DataFrame columns.
                            # This handles config entries like "order_code" when the column is "OrderCode".
                            col_map = {c.lower(): c for c in df.columns}
                            resolved_keys = [col_map.get(k.lower(), k) for k in merge_keys]
                            # Quote column names to handle mixed-case (PascalCase) identifiers.
                            key_conditions = " AND ".join([f'source."{k}" = target."{k}"' for k in resolved_keys])
                            predicate = key_conditions
                        else:
                            # Default: match on partition column only (partition-level replace)
                            predicate = f'source."{partition_col}" = target."{partition_col}"'
                        
                        # Build delete predicate to scope deletion to affected partitions only
                        delete_predicate = " OR ".join([f"target.\"{partition_col}\" = '{val}'" for val in partition_values])
                        
                        print(f"🔄 Delta merge: partition_col={partition_col}, values={partition_values}")
                        print(f"   Merge predicate: {predicate}")
                        print(f"   Delete predicate: {delete_predicate}")
                        
                        # Merge strategy for partition-level overwrite:
                        # 1. Match source to target rows using predicate (row-level or partition-level)
                        # 2. Update matched rows, insert unmatched source rows
                        # 3. Delete target rows not in source BUT only within affected partitions
                        merger = self._write_delta_with_repair(
                            df,
                            destination,
                            mode="merge",
                            delta_merge_options={
                                "predicate": predicate,
                                "source_alias": "source",
                                "target_alias": "target"
                            },
                            delta_write_options=write_opts
                        )
                        (merger
                            .when_matched_update_all()                          # Update existing rows
                            .when_not_matched_insert_all()                      # Insert new rows  
                            .when_not_matched_by_source_delete(delete_predicate)# Delete old rows ONLY in affected partitions
                            .execute())
                        print(f"------ wrote Delta (merge) to: {destination}")
                    else:
                        # Fallback: use custom merge options if provided, or simple overwrite
                        if delta_merge_options:
                            merger = self._write_delta_with_repair(
                                df,
                                destination, 
                                mode="merge",
                                delta_merge_options=delta_merge_options,
                                delta_write_options=write_opts
                            )
                            merger.when_matched_update_all().when_not_matched_insert_all().execute()
                        else:
                            # No partition info, just overwrite
                            self._write_delta_with_repair(
                                df,
                                destination,
                                mode="overwrite",
                                delta_write_options=write_opts
                            )
                        print(f"------ wrote Delta (merge/overwrite fallback) to: {destination}")
                except Exception as e:
                    # If merge fails (e.g., table doesn't exist), use append to create
                    print(f"⚠️ Merge failed, using append mode to create table: {e}")
                    self._write_delta_with_repair(
                        df,
                        destination,
                        mode="append",
                        delta_write_options=write_opts
                    )
                    print(f"------ wrote Delta (append fallback) to: {destination}")
            case _:
                raise ValueError(f"Unsupported format: {format}")
        return 1

    # must simultaneously update both bucket and drive/bucket
    def update_to_bucket(self, df, link_bucket, index_to_break, name_partition, file_name, format, delta_write_options=None, delta_merge_options=None, merge_keys=None):
        """
        Args:
            merge_keys: List of columns for delta_merge row matching (e.g., ['load_date', 'id_buu_cuc'])
                       If None, defaults to [index_to_break] for partition-level replace
        """
        # Ensure descending sort
        values_01 = sorted(df[index_to_break].unique().to_list(), reverse=True)
        if format in ('csv','parquet'):
            for index in values_01:
                fullname = f"{link_bucket}{name_partition}={index}/{file_name}.{format}"
                data = df.filter(pl.col(index_to_break) == index).drop(index_to_break)
                data = data.with_columns(pl.all().cast(pl.Utf8))
                self.update_to_bucket_by_format(data, fullname, format)
        elif format in ('delta_overwrite', 'delta_merge', 'delta_append'):
            # For Delta Lake: write entire dataset once
            # delta_overwrite: full table overwrite (replaces ALL data)
            # delta_merge: partition-level overwrite (replaces only matching partitions, preserves others)
            # delta_append: append new data without replacing existing
            print(f"⚙️ Writing Delta Lake table (mode: {format})...")
            print(f"📋 DataFrame columns before write: {df.columns}")
            print(f"📋 index_to_break (partition column): {index_to_break}")
            
            # Add updated_time column in UTC+7
            tz = pytz.timezone("Asia/Ho_Chi_Minh")
            updated_time = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
            
            # Keep partition column (index_to_break) in the data for BigQuery visibility
            # Note: partition_by is NOT used in delta_write_options because BigQuery Delta Lake reader
            # doesn't extract partition columns from folder paths - they must be in the parquet files
            df_casted = df.with_columns([
                pl.all().cast(pl.Utf8),
                pl.lit(updated_time).alias("updated_time")
            ])
            print(f"📋 DataFrame columns after cast: {df_casted.columns}")
            
            # Delta write options
            # schema_mode="merge" allows schema evolution (adding new columns without error)
            if delta_write_options is None:
                delta_write_options = {
                    "engine": "rust",  # Use Rust engine for better performance
                    "schema_mode": "merge",  # Auto-merge schema if new columns are added
                }

            # Create Hive-style partition folders on GCS (e.g., call_date=2026-02-24)
            # while keeping the partition column in parquet data for downstream compatibility.
            if index_to_break in df_casted.columns:
                delta_write_options["partition_by"] = [index_to_break]
                print(f"📂 Delta partition_by enabled: {delta_write_options['partition_by']}")
            else:
                print(f"⚠️ Partition column '{index_to_break}' not found; writing Delta without partition_by.")
            
            # For merge mode, pass partition column and merge keys
            partition_column = index_to_break if format == 'delta_merge' else None
            # Default merge_keys to [index_to_break] if not specified (partition-level replace)
            effective_merge_keys = merge_keys if format == 'delta_merge' else None

            # Delta table must be written to a table root folder (contains _delta_log),
            # not to partition/file paths like .../call_date=.../data-00.delta_merge
            destination = str(link_bucket).rstrip("/") + "/"
            if file_name:
                print(f"ℹ️ Delta mode ignores file_name='{file_name}' and name_partition='{name_partition}'.")

            self.update_to_bucket_by_format(
                df_casted,
                destination=destination,
                format=format,
                delta_write_options=delta_write_options,
                partition_column=partition_column,
                merge_keys=effective_merge_keys
            )
            print(f"✅ Delta Lake write complete. Rows: {df_casted.height}, updated_time: {updated_time}")
        return 1

    def update_to_bucket_2_level(self, df, link_bucket, index_to_break_1, index_to_break_2,
                                 name_partition_1, name_partition_2, file_name, format):
        # Ensure descending sort
        values_01 = sorted(df[index_to_break_1].unique().to_list(), reverse=True)
        values_02 = sorted(df[index_to_break_2].unique().to_list(), reverse=True)
        for index_1 in values_01:
            for index_2 in values_02:
                fullname = f"{link_bucket}{name_partition_1}={index_1}/{name_partition_2}={index_2}/{file_name}.{format}"
                data = df.filter(
                    (pl.col(index_to_break_1) == index_1) & (pl.col(index_to_break_2) == index_2)
                ).drop([index_to_break_1, index_to_break_2])
                data = data.with_columns(pl.all().cast(pl.Utf8))
                self.update_to_bucket_by_format(data, fullname, format)
        return 1

    def update_to_bucket_2_level_const_partition(self, df, link_bucket,
                                                 value_partition_1, value_partition_2,
                                                 name_partition_1, name_partition_2,
                                                 file_name, format):
        fullname = f"{link_bucket}{name_partition_1}={value_partition_1}/{name_partition_2}={value_partition_2}/{file_name}.{format}"
        self.update_to_bucket_by_format(df, fullname, format)
        return 1

    def update_drive_to_bucket(self, link_drive, link_bucket):
        command = f"!gsutil -m cp -r {link_drive} {link_bucket}"
        subprocess.run(command, shell=True, check=True)
        print(f"------ copied from drive: {link_drive} to bucket: {link_bucket}")
        return 1

    def split_df(self, df, link_bucket, index_to_break, name_partition, file_name, format):
        list_date = df[index_to_break].unique()
        lis_df = {
            f"{link_bucket}{name_partition}={value}/{file_name}.{format}": df.filter(
                pl.col(index_to_break) == value
            ).drop(index_to_break)
            for value in list_date
        }
        return lis_df

    # must simultaneously update both bucket and drive/bucket
    def update_to_bucket_multithread(self, df, link_bucket, index_to_break,
                                     name_partition, file_name, format):
        num_processes = cpu_count()
        df = df.with_columns(pl.all().cast(pl.Utf8))
        dict_df = self.split_df(df, link_bucket, index_to_break, name_partition, file_name, format)
        with ThreadPool(num_processes) as pool:
            pool.starmap(
                self.update_to_bucket_by_format,
                [(df_i, path_i, format) for path_i, df_i in dict_df.items()]
            )

    @gsheet_retry(max_retries=5, initial_delay=3.0)
    def update_data_to_sheet(self, link, sheet_name, data_range, df, row, col, **params):
        # Normalize to pandas
        if isinstance(df, pl.DataFrame):
            df = df.to_pandas()
        elif not isinstance(df, pd.DataFrame):
            try:
                df = pd.DataFrame(df)
            except Exception as e:
                raise ValueError(f"Cannot convert to pandas DataFrame: {e}")

        # --- Open target sheet ---
        wb = self.google_client.open_by_url(link)
        ws = wb.worksheet(sheet_name)
        wb.values_clear(sheet_name + '!' + data_range)
        set_with_dataframe(ws, df, row, col, **params)
        # Success notify
        if getattr(self, "notifier", None):
            n_rows = len(df)
            self.notifier.send_message(
                f"✅ Updated sheet '{sheet_name}' successfully with {n_rows:,} rows."
            )
        return n_rows

    @gsheet_retry(max_retries=5, initial_delay=3.0)
    def update_data_gsheet(self, df, link, sheet_name, cell_range, **params):
        # --- Normalize df to Pandas ---# Normalize to pandas
        if isinstance(df, pl.DataFrame):
            df = df.to_pandas()
        elif not isinstance(df, pd.DataFrame):
            try:
                df = pd.DataFrame(df)
            except Exception as e:
                raise ValueError(f"Cannot convert to pandas DataFrame: {e}")

        # --- Open target sheet ---
        wb_target = self.google_client.open_by_url(link)
        target_sheet = wb_target.worksheet(sheet_name)

        wb_target.values_clear(f'{sheet_name}!{cell_range}')

        start_cell = cell_range.split(':')[0]
        start_row, start_col = a1_to_rowcol(start_cell)
        time.sleep(10)

        set_with_dataframe(
            target_sheet, df,
            row=start_row, col=start_col,
            include_index=False, **params
        )
        # Success notify
        if getattr(self, "notifier", None):
            n_rows = len(df)
            self.notifier.send_message(
                f'✅ Updated <a href="{link}">this gsheet</a> with sheet name \'{sheet_name}\' successfully with {n_rows:,} rows.'
            )
        time.sleep(10)
        return n_rows

    @gsheet_retry(max_retries=5, initial_delay=3.0)
    def update_data_gsheet_no_sleep(self, df, link, sheet_name, cell_range, **params):
        # --- Normalize df to Pandas ---# Normalize to pandas
        if isinstance(df, pl.DataFrame):
            df = df.to_pandas()
        elif not isinstance(df, pd.DataFrame):
            try:
                df = pd.DataFrame(df)
            except Exception as e:
                raise ValueError(f"Cannot convert to pandas DataFrame: {e}")

        # --- Open target sheet ---
        wb_target = self.google_client.open_by_url(link)
        target_sheet = wb_target.worksheet(sheet_name)

        wb_target.values_clear(f'{sheet_name}!{cell_range}')

        start_cell = cell_range.split(':')[0]
        start_row, start_col = a1_to_rowcol(start_cell)

        set_with_dataframe(
            target_sheet, df,
            row=start_row, col=start_col,
            include_index=False, **params
        )
        # Success notify
        if getattr(self, "notifier", None):
            n_rows = len(df)
            self.notifier.send_message(
                f'✅ Updated <a href="{link}">this gsheet</a> with sheet name \'{sheet_name}\' successfully with {n_rows:,} rows.'
            )
        
        return n_rows

    @gsheet_retry(max_retries=5, initial_delay=3.0)
    def update_multi_data_gsheet(self, dfs, link, sheet_names, cell_ranges, **params):
        """
        Update multiple DataFrames into multiple sheets in the same Google Sheet file.
        """
        if not (len(dfs) == len(sheet_names) == len(cell_ranges)):
            raise ValueError("dfs, sheet_names, and cell_ranges must have the same length")

        wb_target = self.google_client.open_by_url(link)

        for df, sheet_name, rng in zip(dfs, sheet_names, cell_ranges):
            target_sheet = wb_target.worksheet(sheet_name)

            wb_target.values_clear(f'{sheet_name}!{rng}')
            time.sleep(10)

            start_cell = rng.split(':')[0]
            start_row, start_col = a1_to_rowcol(start_cell)

            # Normalize to pandas
            if isinstance(df, pl.DataFrame):
                df = df.to_pandas()
            elif not isinstance(df, pd.DataFrame):
                try:
                    df = pd.DataFrame(df)
                except Exception as e:
                    raise ValueError(f"Cannot convert to pandas DataFrame: {e}")
                
            set_with_dataframe(
                target_sheet, df,
                row=start_row, col=start_col,
                include_index=False, **params
            )
            # Success notify
            if getattr(self, "notifier", None):
                n_rows = len(df)
                self.notifier.send_message(
                f'✅ Updated <a href="{link}">this gsheet</a> with sheet name \'{sheet_name}\' successfully with {n_rows:,} rows.'
            )
            time.sleep(10)

        return 1

    @gsheet_retry(max_retries=5, initial_delay=3.0)
    def update_data_gsheet_chunked(
        self, df: Union[pd.DataFrame, "pl.DataFrame"], link: str, sheet_name: str, cell_range: str, chunk_size: int = 10_000, include_index: bool = False, **params,) -> int:
        """
        Write a large DataFrame to a Google Sheet in chunks.
        - Always clears the provided cell_range before writing.
        - Sends a Telegram message via self.notifier (if available) after completion.

        Returns:
            int: Number of data rows written (excludes header row).
        """
        # Normalize to pandas
        if isinstance(df, pl.DataFrame):
            df = df.to_pandas()
        elif not isinstance(df, pd.DataFrame):
            try:
                df = pd.DataFrame(df)
            except Exception as e:
                raise ValueError(f"Cannot convert to pandas DataFrame: {e}")

        # Open target sheet
        wb = self.google_client.open_by_url(link)
        ws = wb.worksheet(sheet_name)

        # Resolve starting cell
        start_cell = cell_range.split(":")[0]
        start_row, start_col = a1_to_rowcol(start_cell)

        # Always clear before writing
        wb.values_clear(f"{sheet_name}!{cell_range}")

        n_rows = len(df)
        # Respect explicit include_column_header param when passed in params.
        # If not provided, default behavior is to include header on first chunk only.
        include_column_header_param = params.pop("include_column_header", None)
        if n_rows == 0:
            # Notify and exit early
            if getattr(self, "notifier", None):
                self.notifier.send_message(f"✅ Updated sheet '{sheet_name}' cleared (no rows written).")
            return 0

        # Compute number of chunks
        rows_per_chunk = builtins.max(1, chunk_size)
        n_chunks = math.ceil(n_rows / rows_per_chunk)

        total_written = 0
        write_row = start_row

        for i in range(n_chunks):
            lo = i * rows_per_chunk
            hi = builtins.min((i + 1) * rows_per_chunk, n_rows)
            df_chunk = df.iloc[lo:hi]

            # Header only for the first chunk
            if include_column_header_param is None:
                include_header = (i == 0)
            else:
                # If user explicitly set include_column_header=False, never include header.
                # If True, include header only on first chunk.
                include_header = bool(include_column_header_param) and (i == 0)

            set_with_dataframe(
                ws,
                df_chunk,
                row=write_row,
                col=start_col,
                include_index=include_index,
                include_column_header=include_header,
                **params,
            )

            # Advance write position and counters
            write_row += len(df_chunk) + (1 if include_header else 0)
            total_written += len(df_chunk)
            # Notify success
            if getattr(self, "notifier", None):
                self.notifier.send_message(
                f'✅ Updating <a href="{link}">this gsheet</a> with sheet name \'{sheet_name}\' successfully with {total_written:,} rows.'
            )

        # Notify success
        if getattr(self, "notifier", None):
            self.notifier.send_message(
                f'✅ Updated <a href="{link}">this gsheet</a> with sheet name \'{sheet_name}\' successfully with {total_written:,} rows.'
            )

        return total_written

class Transform(ETL):
    def __init__(self, client) -> None:
        super().__init__(client)
        print(' ------ class Transform was created!')

    def _remove_vn_marks_text(self, s: str) -> str:
        """Remove Vietnamese diacritics from a single string (keep ASCII letters/numbers/punctuation)."""
        if s is None:
            return s
        s = str(s)

        # Normalize Vietnamese marks (manual mapping)
        s = re.sub(r'[àáạảãâầấậẩẫăằắặẳẵ]', 'a', s)
        s = re.sub(r'[ÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴ]', 'A', s)

        s = re.sub(r'[èéẹẻẽêềếệểễ]', 'e', s)
        s = re.sub(r'[ÈÉẸẺẼÊỀẾỆỂỄ]', 'E', s)

        s = re.sub(r'[ìíịỉĩ]', 'i', s)
        s = re.sub(r'[ÌÍỊỈĨ]', 'I', s)

        s = re.sub(r'[òóọỏõôồốộổỗơờớợởỡ]', 'o', s)
        s = re.sub(r'[ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ]', 'O', s)

        s = re.sub(r'[ùúụủũưừứựửữ]', 'u', s)
        s = re.sub(r'[ÙÚỤỦŨƯỪỨỰỬỮ]', 'U', s)

        s = re.sub(r'[ỳýỵỷỹ]', 'y', s)
        s = re.sub(r'[ỲÝỴỶỸ]', 'Y', s)

        s = re.sub(r'[đ]', 'd', s)
        s = re.sub(r'[Đ]', 'D', s)

        return s
        
    def convert_ngay_human_input(self, df_input="", column_transform='', column_benchmark=''):
        column_name_after_transform = re.sub('_raw$', '', column_transform)
        df_converted = duckdb.query(f"""
            Select bt.*,
              CASE
                WHEN {column_transform} != '' AND try_strptime({column_transform}, '%Y-%m-%d') IS NOT NULL
                     AND strptime({column_transform}, '%Y-%m-%d') BETWEEN strptime(right({column_benchmark}, 10), '%Y-%m-%d')
                     AND date_add(strptime(concat(left(right({column_benchmark}, 10), 7), '-01'), '%Y-%m-01'), interval 1 month)
                  THEN strptime({column_transform}, '%d/%m/%Y')
                WHEN {column_transform} != '' AND try_strptime({column_transform}, '%d/%m/%Y') IS NOT NULL
                     AND strptime({column_transform}, '%d/%m/%Y') BETWEEN strptime(right({column_benchmark}, 10), '%Y-%m-%d')
                     AND date_add(strptime(concat(left(right({column_benchmark}, 10), 7), '-01'), '%Y-%m-01'), interval 1 month)
                  THEN strptime({column_transform}, '%d/%m/%Y')
                WHEN {column_transform} != '' AND try_strptime({column_transform}, '%m/%d/%Y') IS NOT NULL
                     AND strptime({column_transform}, '%m/%d/%Y') BETWEEN strptime(right({column_benchmark}, 10), '%Y-%m-%d')
                     AND date_add(strptime(concat(left(right({column_benchmark}, 10), 7), '-01'), '%Y-%m-01'), interval 1 month)
                  THEN strptime({column_transform}, '%m/%d/%Y')
                WHEN {column_transform} != '' THEN NULL
                ELSE NULL
              END AS {column_name_after_transform},
              concat(strftime(strptime(right({column_benchmark}, 10), '%Y-%m-%d'), '%Y-%m'), '-01') as month,
              date_add(strptime(concat(left(right({column_benchmark}, 10), 7), '-01'), '%Y-%m-01'), interval 1 month) as month_gioi_han_tren
            FROM df_input bt
        """).df()
        self.df = pl.from_pandas(df_converted)
        print('convert date column to yyyy-mm-dd successfully')
        return self.df

    def convert_character_wh(self, val: str) -> str:
        if "Grand Total" in val:
            return "ToanQuoc"
        val = re.sub(r"^([^ ]*)", "Bưu Cục", val)
        return val.strip()

    def max_time_from_string(self, df: pl.DataFrame, col: str, fmt: str = "%Y-%m-%d %H:%M:%S") -> str | None:
        if col not in df.columns:
            raise KeyError(f"Column '{col}' not in DataFrame.")
        dt = df.schema[col]
        if dt == pl.Datetime:
            max_dt = df.select(pl.col(col).max()).item()
        elif dt == pl.Date:
            max_dt = df.select(pl.col(col).cast(pl.Datetime).max()).item()
        else:
            s = pl.col(col).cast(pl.Utf8, strict=False)
            parsed_dt = pl.coalesce([
                s.str.strptime(pl.Datetime, fmt, strict=False),
                s.str.strptime(pl.Datetime, strict=False),
            ])
            max_dt = df.select(parsed_dt.max()).item()
        if max_dt is None:
            return None
        return max_dt.strftime(fmt)

    def remove_vietnamese_marks_specific_columns_add_en(
        self,
        df: pl.DataFrame,
        columns: list[str],
        lowercase: bool = False,
        strip: bool = True,
        collapse_spaces: bool = True,
    ) -> pl.DataFrame:
        """
        Remove Vietnamese marks for specific columns and
        ADD new columns with suffix `_en` (do NOT overwrite original columns).
    
        Example:
            name        -> name_en
            address     -> address_en
        """
    
        if isinstance(df, pd.DataFrame):
            df = pl.from_pandas(df)
    
        def _clean_expr(col: str) -> pl.Expr:
            e = pl.col(col)
    
            if strip:
                e = e.str.strip_chars()
    
            e = e.map_elements(
                self._remove_vn_marks_text,
                return_dtype=pl.Utf8
            )
    
            if collapse_spaces:
                e = e.str.replace_all(r"\s+", " ").str.strip_chars()
    
            if lowercase:
                e = e.str.to_lowercase()
    
            return e.alias(f"{col}_en")
    
        valid_cols = [
            c for c in columns
            if c in df.columns and df.schema[c] == pl.Utf8
        ]
    
        if not valid_cols:
            print("⚠️ No valid Utf8 columns found to transform.")
            return df
    
        return df.with_columns([_clean_expr(c) for c in valid_cols])

    def safe_parse_timestamp_to_yyyy_mm_dd(self, df: pl.DataFrame,col_name: str,index_to_break: str,date_format: str, output_format: str = "%Y-%m-%d") -> pl.DataFrame:
        """
        Safely parse a date/datetime column with a given format, 
        automatically filling nulls using fallback formats.

        Parameters
        ----------
        df : pl.DataFrame
            Input Polars DataFrame.
        col_name : str
            Name of the source column containing date strings.
        index_to_break : str
            Name of the target column for transformed dates.
        date_format : str
            The primary date format to use (must be provided).
        output_format : str, optional
            The output date format (default is "%Y-%m-%d").

        Returns
        -------
        pl.DataFrame
            DataFrame with target_col parsed to yyyy-mm-dd (string type).
        """

        if not date_format:
            raise ValueError("date_format must be provided.")

        # Sample a few values to check for existing yyyy-mm-dd format
        # sample_vals = (
        #     df[col_name]
        #     .drop_nulls()
        #     .head(5)
        #     .to_list()
        # )

        # already_iso = all(
        #     re.match(r"^\d{4}-\d{2}-\d{2}$", str(v)) for v in sample_vals
        # )

        # if already_iso:
        #     print(f"'{col_name}' already in yyyy-mm-dd format — copying to '{index_to_break}'.")
        #     df = df.with_columns(pl.col(col_name).alias(index_to_break))
        #     return df

        # Otherwise, parse and transform to yyyy-mm-dd
        print(f"🕒 Parsing '{col_name}' using provided format '{date_format}'.")
        df = df.with_columns(
            pl.col(col_name)
            .str.strip_chars()
            .str.strptime(pl.Datetime, date_format, strict=True)
            .dt.strftime(output_format)
            .alias(index_to_break)
        )

        print(f"✅ Finished parsing '{col_name}' → '{index_to_break}' ({output_format}).")
        return df
    
    def remove_vietnamese_mark_from_col(self, df: pl.DataFrame):
        if isinstance(df, pd.DataFrame):
            df = pl.DataFrame(df) # Convert Pandas to Polars
        old_cols = df.columns
        new_cols = []
        for col in old_cols:
            col = col.lower()
            col = re.sub(r'[àáạảãâầấậẩẫăằắặẳẵ]', 'a', col)
            col = re.sub(r'[èéẹẻẽêềếệểễ]', 'e', col)
            col = re.sub(r'[ìíịỉĩ]', 'i', col)
            col = re.sub(r'[òóọỏõôồốộổỗơờớợởỡ]', 'o', col)
            col = re.sub(r'[ùúụủũưừứựửữ]', 'u', col)
            col = re.sub(r'[ỳýỵỷỹ]', 'y', col)
            col = re.sub(r'[đ]', 'd', col)
            col = re.sub(r'\s+|[&\(\)/]+', '_', col)  # Replace spaces and &()\/ with underscores
            col = re.sub(r'[^\w_]+', '', col) # Remove any remaining non-alphanumeric characters except underscore
            if col == "": # Prevent empty column names
                col = "empty_col" # Or handle it differently, e.g., skip it
            new_cols.append(col)
        return df.rename(dict(zip(old_cols, new_cols)))

# -------------------------
# Standalone helper functions for ExtLoad
# -------------------------
def build_project_dict_from_airflow_dictionary(extract_instance) -> dict:
    """
    Pulls `ghn-cxcscc.ad_hoc.airflow_dictionary` and composes:
    {
      "<project_name>": {
        "task": {
          "<task_name>": [
            {
              "link_gsheet_gcs": ...,
              "detail_lv_01": ...,
              "detail_lv_02": ...,
              "detail_lv_03": ...
            }, ...
          ]
        }
      }
    }
    
    Args:
        extract_instance: An instance with get_data_from_bigquery method
    """
    sql = "SELECT * FROM `ghn-cxcscc.ad_hoc.airflow_dictionary`"
    df = extract_instance.get_data_from_bigquery(sql)  # Polars DataFrame from Extract
    project_dict: dict[str, dict] = {}

    for row in df.iter_rows(named=True):
        proj = row["project_name"]
        task_value = row["task"]
        item = {
            "link_gsheet_gcs": row["link_gsheet_gcs"],
            "detail_lv_01": row["detail_lv_01"],
            "detail_lv_02": row["detail_lv_02"],
            "detail_lv_03": row.get("detail_lv_03"),
            "pic": row.get("pic") or row.get("PIC"),
            "note_01": row.get("note_01"),
            "note_02": row.get("note_02"),
            "note_03": row.get("note_03")
        }
        project_dict.setdefault(proj, {"task": {}})
        project_dict[proj]["task"].setdefault(task_value, []).append(item)

    return project_dict


def format_pic_mention(pic: str | None, bot_params: dict | None = None) -> str:
    """
    Try to turn a PIC label into a Telegram mention.
    Optional: in your bot_params, provide a mapping like:
      bot_params["telegram"]["pic_map"] = {"Alice": 123456789, "bob": 222333444}
    
    Args:
        pic: The PIC identifier
        bot_params: Optional bot parameters dictionary containing telegram pic_map
    """
    if not pic:
        return ""
    try:
        mapping = (
            (bot_params or {})
            .get("telegram", {})
            .get("pic_map", {})
        )
        # normalize key lookup
        chat_id = mapping.get(pic) or mapping.get(str(pic)) or mapping.get(str(pic).lower())
        if chat_id:
            # Use HTML mention: requires parse_mode='HTML' in your notifier
            return f'<a href="tg://user?id={chat_id}">{pic}</a>'
    except Exception:
        pass
    # Fallback: plain text (or @username if you store usernames as PIC)
    return str(pic)


def notify_on_failure_for_pic(func):
    """
    Decorator that wraps instance methods to send failure notifications with PIC mentions.
    Should be used with ExtLoad instance methods.
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            import traceback, inspect
            pn = tk = None
            pi = 0
            try:
                bound = inspect.signature(func).bind(self, *args, **kwargs)
                bound.apply_defaults()
                pn = bound.arguments.get("project_name")
                tk = bound.arguments.get("task")
                pi = bound.arguments.get("pick_index", 0)
            except Exception:
                pass

            # Always use the main dictionary
            pic = None
            if pn and tk:
                try:
                    proj = build_project_dict_from_airflow_dictionary(self)
                    tasks = proj.get(pn, {}).get("task", {})
                    entries = tasks.get(tk, [])
                    if 0 <= pi < len(entries):
                        pic = entries[pi].get("pic") or entries[pi].get("PIC")
                except Exception:
                    pass

            tb = traceback.format_exc(limit=6)
            header = f"❌ [{func.__name__}] failed for project={pn}, task={tk}, pick_index={pi}"
            msg = f"{pic or ''} {header}\nError: {type(e).__name__}: {e}\nTraceback:\n{tb}"

            try:
                if getattr(self, "notifier", None):
                    self.notifier.send_message(msg)
                else:
                    print("[WARN] No notifier; printing error:\n" + msg)
            finally:
                raise
    return wrapper

def extract_audit_table_from_target_source():
    """
    Scan target/source/*.sql
    Return first (project, dataset, table) found
    """
    source_dir = "/opt/airflow/dbt/target/source"

    if not os.path.exists(source_dir):
        print("[DBT][WARN] target/source not found")
        return None

    for sql_path in glob(os.path.join(source_dir, "*.sql")):
        with open(sql_path, "r", encoding="utf-8") as f:
            sql = f.read()

        match = re.search(
            r"select\s+\*\s+from\s+`([^`]+)`\.`([^`]+)`\.`([^`]+)`",
            sql,
            flags=re.IGNORECASE,
        )

        if match:
            project, dataset, table = match.groups()
            print(f"[DBT] Found audit table in {sql_path}")
            return project, dataset, table

    print("[DBT][WARN] No audit table found in target/source")
    return None

# @title Class Bot
class Bot:
    """Send message to Bot Telegram or Gmail"""

    def __init__(self, bot_params: dict | None = None) -> None:
        bot_params = bot_params or {}
        self.bot_telegram = bot_params.get("telegram", {})  # {} if missing
        self.bot_gmail = bot_params.get("gmail", {})        # {} if missing
        print(' ------ class Bot was created!')

    @staticmethod
    def _base_url(token: str) -> str:
        return f"https://api.telegram.org/bot{token}"

    def _get_channel_conf(self, channel_key: str):
        if channel_key not in self.bot_telegram:
            raise KeyError(
                f"Telegram channel_key '{channel_key}' not found in bot_telegram. "
                f"Available: {list(self.bot_telegram.keys())}"
            )
        return self.bot_telegram[channel_key]

    @staticmethod
    def telegram_retry(max_retries: int = 3, retry_delay: float = 2.0):
        """Decorator for retrying Telegram API calls with exponential backoff."""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # Extract retry params from kwargs if provided, otherwise use defaults
                retries = kwargs.pop('max_retries', max_retries)
                delay = kwargs.pop('retry_delay', retry_delay)
                
                for attempt in range(retries):
                    try:
                        return func(*args, **kwargs)
                    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                        print(f"⚠️ Telegram notification failed (network unreachable): {e}")
                        return {"ok": False, "error": str(e)}
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code == 429:  # Rate limit
                            if attempt < retries - 1:
                                wait_time = delay * (2 ** attempt)
                                print(f"[WARN] Rate limited. Retrying in {wait_time}s... (attempt {attempt + 1}/{retries})")
                                time.sleep(wait_time)
                                continue
                        raise
                    except Exception as e:
                        if attempt < retries - 1:
                            wait_time = delay * (2 ** attempt)
                            print(f"[WARN] Error: {e}. Retrying in {wait_time}s... (attempt {attempt + 1}/{retries})")
                            time.sleep(wait_time)
                            continue
                        raise
                
                raise RuntimeError(f"Failed after {retries} attempts")
            return wrapper
        return decorator

    @telegram_retry()
    def send_message_to_telegram(self, telegram_token, chat_id, topic_id, message, max_retries: int = 3, retry_delay: float = 2.0):
        try:
            conf = self._get_channel_conf("error_colab")
            url = f"{self._base_url(conf['telegram_token'])}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": message,
                "message_thread_id": topic_id
            }
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"⚠️ Telegram notification failed (network unreachable): {e}")
            return {"ok": False, "error": str(e)}

    def send_message_to_gmail(self, *params):
        return 1
        
    @telegram_retry()
    def send_message_to_telegram_token(self, message: str, max_retries: int = 3, retry_delay: float = 2.0):
        conf = self._get_channel_conf("error_colab")
        url = f"{self._base_url(conf['telegram_token'])}/sendMessage"
        payload = {
            "chat_id": conf["chat_id"],
            "text": message,
            "message_thread_id": conf["topic"]["daily"],
        }
        resp = requests.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @telegram_retry()
    def send_message_to_topic(
        self,
        message: str,
        *,
        channel_key: str = "error_colab",
        topic: str = "general",
        parse_mode: str | None = None,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ):
        conf = self._get_channel_conf(channel_key)
        url = f"{self._base_url(conf['telegram_token'])}/sendMessage"
        payload = {
            "chat_id": conf["chat_id"],
            "text": message,
            "message_thread_id": conf["topic"][topic],
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        resp = requests.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @telegram_retry()
    def send_photo_to_topic(
        self,
        image_path: str,
        caption: str | None = None,
        *,
        channel_key: str = "error_colab",
        topic: str = "general",
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ):
        conf = self._get_channel_conf(channel_key)
        url = f"{self._base_url(conf['telegram_token'])}/sendPhoto"
        data = {
            "chat_id": conf["chat_id"],
            "message_thread_id": conf["topic"][topic],
        }
        if caption:
            data["caption"] = caption
        with open(image_path, "rb") as f:
            files = {"photo": f}
            resp = requests.post(url, data=data, files=files, timeout=30)
        resp.raise_for_status()
        return resp.json()

    # ---------- DataFrame -> image ----------
    @staticmethod
    def _save_table_as_image(df: pd.DataFrame, title: str, filename: str) -> None:
        # Normalize to pandas
        if isinstance(df, pl.DataFrame):
            df = df.to_pandas()
        elif not isinstance(df, pd.DataFrame):
            try:
                df = pd.DataFrame(df)
            except Exception as e:
                raise ValueError(f"Cannot convert to pandas DataFrame: {e}")
        
        df = df.copy()
        # add row labels if none exist
        if df.index.name is None:
            df.index = [str(i) for i in range(1, len(df) + 1)]

        def _fmt_col(c):
            try:
                if hasattr(c, "strftime"):
                    return c.strftime("%Y-%m-%d")
            except Exception:
                pass
            return str(c)

        col_labels = [_fmt_col(c) for c in df.columns]

        # ignore "total" rows for highlighting
        df_no_total = df[~df.applymap(lambda x: "total" in str(x).strip().lower()).any(axis=1)]

        def format_value(val):
            try:
                v = float(val)
                if abs(v) < 1:
                    return f"{v * 100:.0f}%"
                return f"{v:,.0f}"
            except Exception:
                return str(val)

        formatted_values = df.applymap(format_value).values

        n_rows, n_cols = df.shape
        fig_w = max(8, min(20, 1.2 * n_cols))
        fig_h = max(2.5, min(30, 0.6 * (n_rows + 2)))

        fig, ax = plt.subplots(figsize=(fig_w, fig_h))
        ax.axis("off")

        table = ax.table(
            cellText=formatted_values,
            rowLabels=df.index.astype(str),
            colLabels=col_labels,
            loc="center",
            cellLoc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(10 if n_cols <= 8 else 8)
        table.scale(1.1, 1.1)

        # highlight min/max values per column
        for j, col in enumerate(df.columns):
            col_series = pd.to_numeric(df_no_total[col], errors="coerce")
            if col_series.notna().any():
                max_row = col_series.idxmax()
                min_row = col_series.idxmin()
                for i, row_label in enumerate(df.index):
                    cell = table[i + 1, j]
                    if row_label == max_row:
                        cell.set_facecolor("#d0f0c0")
                        cell.set_text_props(color="black", fontweight="bold")
                    elif row_label == min_row:
                        cell.set_facecolor("#f8d7da")
                        cell.set_text_props(color="black", fontweight="bold")

        plt.title(title, fontsize=14, fontweight="bold", pad=16)
        plt.tight_layout()
        plt.savefig(filename, dpi=300, bbox_inches="tight")
        plt.close(fig)

    def send_dataframe_as_image(
        self,
        df: pd.DataFrame,
        title: str,
        *,
        message_prefix: str | None = None,
        channel_key: str = "error_colab",
        topic: str = "general",
    ):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        with tempfile.TemporaryDirectory() as tmpdir:
            img_path = os.path.join(tmpdir, f"df_{ts}.png")
            self._save_table_as_image(df, title=title, filename=img_path)

            if message_prefix:
                self.send_message_to_topic(
                    message_prefix, channel_key=channel_key, topic=topic
                )

            return self.send_photo_to_topic(
                img_path, caption=None, channel_key=channel_key, topic=topic
            )

    def send_message(self, required_bot, message):
        for key, value in required_bot.items():
            if key == 'telegram':
                for k, v in value.items():
                    print(f"send message to telegram:topic: {(k, v)}")
                    telegram_token = self.bot_telegram[k]['telegram_token']
                    chat_id = self.bot_telegram[k]['chat_id']
                    topic_id = self.bot_telegram[k]['topic'].get(v)
                    self.send_message_to_telegram(telegram_token, chat_id, topic_id, message)
            elif key == 'gmail':
                self.send_message_to_gmail(required_bot)

    def send_file(self, required_bot, file_path, caption, rm_after_send):
        for key, value in required_bot.items():
            if key == 'telegram':
                for k, v in value.items():
                    print(f"send message to telegram:topic: {(k, v)}")
                    telegram_token = self.bot_telegram[k]['telegram_token']
                    chat_id = self.bot_telegram[k]['chat_id']
                    topic_id = self.bot_telegram[k]['topic'].get(v)
                    self.send_file_to_telegram(
                        telegram_token, chat_id, topic_id, file_path, caption, rm_after_send
                    )
            elif key == 'gmail':
                self.send_message_to_gmail(required_bot)

    @telegram_retry()
    def send_file_to_telegram(self, telegram_token, chat_id, topic_id, file_path, caption, rm_after_send=1, max_retries: int = 3, retry_delay: float = 2.0):
        url = f"https://api.telegram.org/bot{telegram_token}/sendDocument"
        with open(file_path, 'rb') as f:
            files = {'document': f}
            data = {
                'chat_id': chat_id,
                "message_thread_id": topic_id,
                "caption": caption
            }
            response = requests.post(url, files=files, data=data, timeout=60)
        response.raise_for_status()

        if rm_after_send & os.path.exists(file_path):
            os.remove(file_path)
            print(f"{file_path} deleted.")

    def notify_telegram_when_fail(self, context):
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        execution_date = context['ts']
        log_url = context['task_instance'].log_url

        # Extract airflow exception (only exists reliably on failure callbacks)
        exception = context.get('exception')
        error_type = type(exception).__name__ if exception else None
        error_message = str(exception)[:500] if exception else None

        # ---- PIC ----
        pic = '@AnDaGHN @epsilongizdabest @nnthuann'
        try:
            project_dict = build_project_dict_from_airflow_dictionary(self)
            tasks = project_dict.get(dag_id, {}).get("task", {})
            # Primary: match exact task_id
            entries = tasks.get(task_id, [])
            if entries:
                pic = entries[0].get("pic") or entries[0].get("PIC")
            else:
                # Fallback: use PIC from the first task of the dag in airflow_dictionary
                first_task_entries = next(iter(tasks.values()), [])
                if first_task_entries:
                    pic = first_task_entries[0].get("pic") or first_task_entries[0].get("PIC")
        except Exception as e:
            print(f"[WARN] Could not retrieve PIC from airflow_dictionary: {e}")

        pic_mention = ""
        if pic:
            pic_mention = format_pic_mention(pic, getattr(self, "bot_params", None)) + " "

        bot_i4 = {'telegram': {'error_colab': 'daily'}}

        # ---- Read dbt run_results.json and collect warn/fail/error ----
        dbt_path = "/opt/airflow/dbt"
        run_results_path = os.path.join(dbt_path, "target/run_results.json")

        warn_items = []
        fail_items = []
        dbt_parse_error = None

        try:
            if os.path.exists(run_results_path):
                with open(run_results_path, "r", encoding="utf-8") as f:
                    run_results = json.load(f)

                for r in run_results.get("results", []):
                    status = (r.get("status") or "").lower()
                    unique_id = r.get("unique_id", "N/A")
                    msg = (r.get("message") or "No message").strip()
                    compiled_path = r.get("compiled_path", "")

                    if status == "warn":
                        warn_items.append({
                            "unique_id": unique_id,
                            "message": msg[:350],
                            "compiled_path": compiled_path
                        })
                    elif status in ("fail", "error"):
                        fail_items.append({
                            "unique_id": unique_id,
                            "message": msg[:350],
                            "compiled_path": compiled_path
                        })
        except Exception as e:
            dbt_parse_error = f"{type(e).__name__}: {str(e)[:200]}"

        # ---- If nothing to alert, return ----
        # Alert if: airflow exception OR dbt warn/fail/error OR parsing error
        if not exception and not warn_items and not fail_items and not dbt_parse_error:
            return

        # ---- Traceback (airflow) ----
        traceback_text = ""
        if exception:
            try:
                tb_lines = tb_module.format_exception(type(exception), exception, exception.__traceback__)
                # keep last ~10 lines for readability
                traceback_text = "".join(tb_lines[-10:])[:1500]
            except Exception:
                traceback_text = "Could not extract traceback"

        # ---- Build Telegram message ----
        is_error = bool(exception or fail_items)
        title = "🚨 *Airflow/DBT Failed!*" if is_error else "⚠️ *DBT Warning!*"

        message_parts = [
            f"{pic_mention}{title}\n",
            f"*DAG:* {dag_id}",
            f"*Task:* {task_id}",
            f"*Execution Time:* {execution_date}",
        ]

        # Airflow exception info (if any)
        if exception:
            message_parts += [
                f"*Error Type:* `{error_type}`",
                f"*Error Message:* `{error_message}`",
            ]
            if traceback_text:
                message_parts.append(f"\n*Traceback:*\n```\n{traceback_text}\n```")

        # dbt parsing error (rare but helpful)
        if dbt_parse_error:
            message_parts.append(f"\n*DBT Parse Error:* `{dbt_parse_error}`")

        # dbt failures
        if fail_items:
            lines = []
            for it in fail_items[:3]:
                lines.append(
                    f"• `{it['unique_id']}`\n"
                    f"  Error: `{it['message']}`\n"
                    f"  Path: `{it['compiled_path']}`"
                )
            more = len(fail_items) - 3
            if more > 0:
                lines.append(f"… và còn {more} lỗi khác")
            message_parts.append("\n*DBT Errors/Failures:*\n" + "\n\n".join(lines))

        # dbt warnings
        lookback_months = 3
        lookback_date = 7
        if warn_items:
            has_duplicate = False
            has_max_update = False

            for it in warn_items:
                uid = it.get("unique_id", "")
                if "MAX_UPDATE" in uid:
                    has_max_update = True
                else:
                    has_duplicate = True

            if has_duplicate or has_max_update:
                message_parts.append(":warning: *DBT Data Quality Warning*")

            if has_duplicate:
                message_parts.append(
                    f"• *Duplicate Order Code*: "
                    f"Phát hiện order_code bị trùng "
                    f"(Trong {lookback_months} tháng gần)"
                )

            if has_max_update:
                message_parts.append(
                    f"• *Data Stale*: "
                    f"Bảng không có dữ liệu mới trong {lookback_date} ngày gần nhất"
                )

        # link
        message_parts.append(f"\n:link: <{log_url}|View full log>")

        message = "\n".join(message_parts)
        self.send_message(bot_i4, message)

    
    @telegram_retry()
    def send_message_dbt_to_telegram(self, token, chat_id, topic_id, message, max_retries: int = 3, retry_delay: float = 2.0):
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        if topic_id:
            payload["message_thread_id"] = topic_id
        response = requests.post(url, json=payload, verify=False, timeout=30)
        response.raise_for_status()
        return response.json()

    def send_message_dbt(self, required_bot, message):
        """
        Dùng dict required_bot để gửi message tới các bot Telegram hoặc Gmail (nếu cần)
        """
        for key, value in required_bot.items():
            if key == 'telegram':
                for k, v in value.items():
                    telegram_token = self.bot_telegram[k]['telegram_token']
                    chat_id = self.bot_telegram[k]['chat_id']
                    topic_id = self.bot_telegram[k]['topic'].get(v)
                    self.send_message_dbt_to_telegram(telegram_token, chat_id, topic_id, message)
            elif key == 'gmail':
                # placeholder, nếu team có gửi Gmail
                pass

    def notify_dbt_completed(self, context, dbt_path="/opt/airflow/dbt", mode="run"):
        """
        Gửi thông báo khi dbt task hoàn thành.
        mode: "run"       → reads run_results.json, sends per-model success + aggregated warns
              "test"      → reads run_results.json, sends aggregated warns only (no per-test spam)
              "freshness" → reads sources.json only, sends aggregated warns + one pass summary
        """
        required_bot = {'telegram': {'error_colab': 'daily'}}
        dag_id = context.get('dag').dag_id if context and context.get('dag') else "N/A"
        task_id = context.get('task_instance').task_id if context and context.get('task_instance') else "N/A"

        def _md(text: str) -> str:
            """Escape characters that break Telegram Markdown v1."""
            return str(text).replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace('`', '\\`')

        warn_lines = []
        fail_lines = []

        # ── 1. run_results.json  (dbt run / dbt test) ──────────────────────
        if mode in ("run", "test"):
            run_results_path = os.path.join(dbt_path, "target/run_results.json")
            if not os.path.exists(run_results_path):
                execution_date = context.get('ts', "N/A") if context else "N/A"
                log_url = context.get('task_instance').log_url if context and context.get('task_instance') else ""
                self.send_message_dbt(required_bot, (
                    f"⚠️ Không tìm thấy run_results.json sau khi dbt chạy!\n"
                    f"*DAG:* {_md(dag_id)}\n*Task:* {_md(task_id)}\n"
                    f"*Execution Time:* {execution_date}\n[View Log]({log_url})"
                ))
                return

            with open(run_results_path, "r") as f:
                data = json.load(f)

            for result in data.get("results", []):
                status = result.get("status", "")
                model_name = result.get("unique_id", "N/A")
                exec_time = round(result.get("execution_time", 0), 2)
                msg_text = result.get("message", "")

                if status == "warn":
                    warn_lines.append(f"  • `{model_name}` — {_md(msg_text)} ({exec_time}s)")
                    continue

                if mode == "test":
                    if status in ("fail", "error"):
                        fail_lines.append(f"  • `{model_name}` — {_md(msg_text)} ({exec_time}s)")
                    continue  # skip pass/skip for test mode (too noisy)

                if status != "success":
                    # dbt run: skip non-success, non-warn statuses
                    continue

                # dbt run: send per-model success message
                relation = result.get("relation_name", "")
                adapter = result.get("adapter_response", {})
                rows = adapter.get("rows_affected", "N/A")
                bytes_processed = adapter.get("bytes_processed", 0)
                gb_processed = round(bytes_processed / 1024 / 1024 / 1024, 2)

                self.send_message_dbt(required_bot, (
                    f"*DBT Model Completed*\n\n"
                    f"Model: `{model_name}`\n"
                    f"Table: {_md(relation)}\n"
                    f"Rows: `{rows}`\n"
                    f"Processed: `{gb_processed} GB`\n"
                    f"Time: `{exec_time}s`\n"
                    f"Message: {_md(msg_text)}"
                ))

        # ── 2. sources.json  (dbt source freshness only) ───────────────────
        elif mode == "freshness":
            sources_path = os.path.join(dbt_path, "target/sources.json")
            if not os.path.exists(sources_path):
                execution_date = context.get('ts', "N/A") if context else "N/A"
                log_url = context.get('task_instance').log_url if context and context.get('task_instance') else ""
                self.send_message_dbt(required_bot, (
                    f"⚠️ Không tìm thấy sources.json sau khi dbt source freshness chạy!\n"
                    f"*DAG:* {_md(dag_id)}\n*Task:* {_md(task_id)}\n"
                    f"*Execution Time:* {execution_date}\n[View Log]({log_url})"
                ))
                return

            with open(sources_path, "r") as f:
                sources_data = json.load(f)

            pass_count = 0
            for result in sources_data.get("results", []):
                status = result.get("status", "")
                source_name = result.get("unique_id", "N/A")
                exec_time = round(result.get("execution_time", 0), 2)
                criteria = result.get("criteria", {})
                max_loaded_at = result.get("max_loaded_at", "N/A")

                if status == "warn":
                    warn_after = criteria.get("warn_after", {})
                    warn_lines.append(
                        f"  • `{source_name}` — freshness WARN "
                        f"(warn_after: {warn_after.get('count')} {warn_after.get('period')}, "
                        f"last loaded: {_md(str(max_loaded_at))}, {exec_time}s)"
                    )
                elif status == "pass":
                    pass_count += 1

            # Send one summary for passes (not per-source spam)
            if pass_count > 0 and not warn_lines:
                self.send_message_dbt(required_bot,
                    f"✅ *DBT Freshness OK*\n*DAG:* {_md(dag_id)} | *Task:* {_md(task_id)}\n{pass_count} source(s) fresh."
                )

        # ── 3. Send test failures with PIC mention ────────────────────────
        if mode == "test" and fail_lines:
            default_pic = '@AnDaGHN'
            pic = default_pic
            try:
                if hasattr(self, 'get_data_from_bigquery'):
                    project_dict = build_project_dict_from_airflow_dictionary(self)
                    dag_tasks = project_dict.get(dag_id, {}).get("task", {})
                    first_entries = next(iter(dag_tasks.values()), [])
                    if first_entries:
                        found_pic = first_entries[0].get("pic") or first_entries[0].get("PIC")
                        if found_pic:
                            pic = found_pic
            except Exception as e:
                print(f"[WARN] Could not retrieve PIC for dbt test failure: {e}")
            pic_mention = format_pic_mention(pic, getattr(self, "bot_params", None))
            fail_body = "\n".join(fail_lines[:5])
            if len(fail_lines) > 5:
                fail_body += f"\n  … and {len(fail_lines) - 5} more"
            self.send_message_dbt(required_bot,
                f"🚨 {pic_mention} *DBT Test Failed!*\n"
                f"*DAG:* {_md(dag_id)} | *Task:* {_md(task_id)}\n\n"
                + fail_body
            )

        # ── 4. Send aggregated warnings with PIC mention ─────────────────────
        if warn_lines:
            default_pic = '@AnDaGHN'
            warn_pic = default_pic
            try:
                if hasattr(self, 'get_data_from_bigquery'):
                    project_dict = build_project_dict_from_airflow_dictionary(self)
                    dag_tasks = project_dict.get(dag_id, {}).get("task", {})
                    first_entries = next(iter(dag_tasks.values()), [])
                    if first_entries:
                        found_pic = first_entries[0].get("pic") or first_entries[0].get("PIC")
                        if found_pic:
                            warn_pic = found_pic
            except Exception as e:
                print(f"[WARN] Could not retrieve PIC for dbt warning: {e}")
            warn_pic_mention = format_pic_mention(warn_pic, getattr(self, "bot_params", None))
            warn_text = (
                f"⚠️ {warn_pic_mention} *DBT Warning(s)*\n"
                f"*DAG:* {_md(dag_id)} | *Task:* {_md(task_id)}\n\n"
                + "\n".join(warn_lines)
            )
            self.send_message_dbt(required_bot, warn_text)

# Combined ExtLoad class
# =========================
class ExtLoad(Load, Extract, Transform, Bot, LLM, Charting):
    """
    ExtLoad combines Extract + Load + Transform + Bot + LLM + Charting.
    Ensures Bot.__init__ is called so self.bot_telegram exists,
    enabling send_dataframe_as_image(...) to work.
    """

    def __init__(
        self,
        client,
        *,
        telegram_token: str | None = None,
        chat_id: int | None = None,
        topic_id: int | None = None,
        bot_params: dict | None = None,
    ) -> None:
        # Initialize Load (this also wires self.notifier if token/chat_id are provided)
        Load.__init__(self, client, telegram_token=telegram_token, chat_id=chat_id, topic_id=topic_id)
        
        # Initialize Transform
        Transform.__init__(self, client)

        # If caller didn't pass a full bot_params, fabricate a minimal one from token/chat/topic
        if bot_params is None and telegram_token and chat_id:
            bot_params = {
                "telegram": {
                    "error_colab": {
                        "telegram_token": telegram_token,
                        "chat_id": chat_id,
                        "topic": {
                            # You can extend more named topics if you use them elsewhere
                            "general": topic_id or 0
                        },
                    }
                },
                "gmail": {},
            }

        # Initialize Bot with either provided or fabricated config (or harmless empty dict)
        Bot.__init__(self, bot_params)
        print(" ------ class ExtLoad was created!")

    # -------------------------
    # Internal helper (wrapper for standalone function)
    # -------------------------
    def _build_project_dict_from_airflow_dictionary(self) -> dict:
        """Wrapper method that calls the standalone function."""
        return build_project_dict_from_airflow_dictionary(self)
    
    def _format_pic_mention(self, pic: str | None) -> str:
        """Wrapper method that calls the standalone function."""
        return format_pic_mention(pic, getattr(self, "bot_params", None))
    
    @staticmethod
    def notify_on_failure_for_pic(func):
        """Static method wrapper for the standalone decorator."""
        return notify_on_failure_for_pic(func)
    
    # -------------------------
    # Public pipeline method
    # -------------------------
    @notify_on_failure_for_pic
    def update_all_rows_bigquery_to_gsheet(
        self,
        project_name: str,
        task: str,
        *,
        pick_index: int = 0,
        use_chunked: bool = False,
        chunk_size: int = 10_000,
        include_column_header: bool | None = True,
    ) -> int:
        """
        1) Read airflow_dictionary to locate (link, sheet, range, view/table).
        2) SELECT * FROM detail_lv_03 (view/table) in BigQuery.
        3) Write ALL rows to the Google Sheet range.
        """
        # 1) Build dictionary
        project_dict = self._build_project_dict_from_airflow_dictionary()
        print(project_dict)

        if project_name not in project_dict:
            raise KeyError(f"[airflow_dictionary] project_name '{project_name}' not found.")

        tasks = project_dict[project_name]["task"]
        if task not in tasks or not tasks[task]:
            raise KeyError(
                f"[airflow_dictionary] task '{task}' not found or has no entries under project '{project_name}'."
            )

        entries = tasks[task]
        if not (0 <= pick_index < len(entries)):
            raise IndexError(f"[airflow_dictionary] pick_index {pick_index} out of range (0..{len(entries)-1}).")

        cfg = entries[pick_index]

        # 2) Extract from BigQuery view/table
        vw_or_table = cfg.get("detail_lv_03")
        if not vw_or_table:
            raise ValueError(
                f"[airflow_dictionary] detail_lv_03 is empty for project='{project_name}', task='{task}'."
            )

        sql_view = f"{vw_or_table}"
        df_view = self.get_data_from_bigquery(sql_view)  # Polars DataFrame

        try:
            print(df_view.head(5))
        except Exception:
            print(f"Preview: {df_view.shape[0]} rows x {df_view.shape[1]} cols")

        # 3) Resolve targets
        link_gsheet = cfg["link_gsheet_gcs"]
        sheet_name = cfg["detail_lv_01"]
        data_range = cfg["detail_lv_02"]
        pic = cfg.get("pic")

        # 4) Write to Google Sheet
        n_rows = df_view.height
        if use_chunked:
            self.update_data_gsheet_chunked(
                df_view,
                link=link_gsheet,
                sheet_name=sheet_name,
                cell_range=data_range,
                chunk_size=chunk_size,
                include_index=False,
                include_column_header=include_column_header,
            )
        else:
            self.update_data_gsheet(
                df_view,
                link=link_gsheet,
                sheet_name=sheet_name,
                cell_range=data_range,
                include_column_header=include_column_header,
            )

        # (chart/LLM removed — use update_all_rows_bigquery_chart_n_llm instead)

        return n_rows
    
    @notify_on_failure_for_pic
    def update_all_rows_bigquery_to_gsheet_by_project(
        self,
        project_name: str,
        *,
        task_filter: list[str] | None = None,
        pick_index: int = 0,
        use_chunked: bool = False,
        chunk_size: int = 10_000,
        include_column_header: bool | None = True,
    ) -> dict[str, int]:
        """
        Run update_all_rows_bigquery_to_gsheet for multiple tasks within a project.
        
        Parameters
        ----------
        project_name : str
            Key from airflow_dictionary.project_name
        task_filter : list[str] | None
            If provided, only process tasks in this list. Otherwise process all tasks.
        pick_index : int
            Use the N-th entry if multiple rows exist for the same project/task (default 0).
        use_chunked : bool
            Use Load.update_data_gsheet_chunked(...) for very large frames.
        chunk_size : int
            Rows per chunk when use_chunked=True.
            
        Returns
        -------
        dict[str, int]
            Dictionary mapping task names to number of rows written.
        """
        # 1) Build dictionary
        project_dict = self._build_project_dict_from_airflow_dictionary()
        
        if project_name not in project_dict:
            raise KeyError(f"[airflow_dictionary] project_name '{project_name}' not found.")
        
        tasks = project_dict[project_name]["task"]
        if not tasks:
            raise ValueError(f"[airflow_dictionary] No tasks found for project '{project_name}'.")
        
        # Filter out None/NULL task names first
        tasks_clean = {k: v for k, v in tasks.items() if k is not None and str(k).strip()}
        
        if not tasks_clean:
            raise ValueError(
                f"[airflow_dictionary] No valid (non-NULL) tasks found for project '{project_name}'. "
                f"Please check the 'task' column in airflow_dictionary table."
            )
        
        # Filter tasks if requested
        if task_filter:
            tasks_to_process = {k: v for k, v in tasks_clean.items() if k in task_filter}
            if not tasks_to_process:
                raise ValueError(
                    f"[airflow_dictionary] None of the filtered tasks {task_filter} found in project '{project_name}'."
                )
        else:
            tasks_to_process = tasks_clean
        
        results = {}
        total_tasks = len(tasks_to_process)
        
        # Send initial notification
        if getattr(self, "notifier", None):
            task_list_str = ', '.join(str(k) for k in tasks_to_process.keys())
            self.notifier.send_message(
                f"🚀 Starting batch processing for project '{project_name}'\n"
                f"📋 Tasks to process: {total_tasks}\n"
                f"📝 Task list: {task_list_str}"
            )
        
        # 2) Iterate through each task
        for idx, (task_name, entries) in enumerate(tasks_to_process.items(), 1):
            try:
                print(f"\n{'='*60}")
                print(f"Processing task {idx}/{total_tasks}: {task_name}")
                print(f"{'='*60}\n")
                
                # Call the existing method for this task
                n_rows = self.update_all_rows_bigquery_to_gsheet(
                    project_name=project_name,
                    task=task_name,
                    pick_index=pick_index,
                    use_chunked=use_chunked,
                    chunk_size=chunk_size,
                    include_column_header=include_column_header,
                )
                
                results[task_name] = n_rows
                
                print(f"✅ Task '{task_name}' completed: {n_rows} rows written")
                
            except Exception as e:
                error_msg = f"❌ Task '{task_name}' failed: {type(e).__name__}: {e}"
                print(error_msg)
                results[task_name] = -1  # Indicate failure
                
                # Send error notification but continue with other tasks
                if getattr(self, "notifier", None):
                    self.notifier.send_message(
                        f"⚠️ Error in project '{project_name}', task '{task_name}'\n"
                        f"Error: {str(e)[:500]}\n"
                        f"Continuing with remaining tasks..."
                    )
        
        # 3) Send summary notification
        successful = sum(1 for v in results.values() if v >= 0)
        failed = sum(1 for v in results.values() if v < 0)
        total_rows = sum(v for v in results.values() if v >= 0)
        
        summary = (
            f"📊 Batch processing completed for project '{project_name}'\n\n"
            f"✅ Successful: {successful}/{total_tasks}\n"
            f"❌ Failed: {failed}/{total_tasks}\n"
            f"📝 Total rows written: {total_rows:,}\n\n"
            f"Details:\n"
        )
        
        for task_name, row_count in results.items():
            status = "✅" if row_count >= 0 else "❌"
            count_str = f"{row_count:,} rows" if row_count >= 0 else "FAILED"
            summary += f"{status} {task_name}: {count_str}\n"
        
        if getattr(self, "notifier", None):
            self.notifier.send_message(summary)
        
        print(f"\n{'='*60}")
        print("BATCH PROCESSING SUMMARY")
        print(f"{'='*60}")
        print(summary)
        
        # Raise exception if any tasks failed (for Airflow task failure)
        if failed > 0:
            raise RuntimeError(
                f"Batch processing completed with {failed}/{total_tasks} failed tasks. "
                f"See summary above for details."
            )
        
        return results

    @notify_on_failure_for_pic
    def update_all_rows_bigquery_chart_n_llm(
        self,
        project_name: str,
        task: str,
        *,
        pick_index: int = 0,
    ) -> None:
        """
        1) Read airflow_dictionary for (project_name, task).
        2) Parse detail_lv_03 as JSON: {"query": "<SQL>", "param": {...}}.
        3) Run SQL against BigQuery.
        4) Optionally send table image, chart, and/or LLM analysis to Telegram.

        detail_lv_03 JSON shape
        -----------------------
        {
            "query": "SELECT ... FROM `project.dataset.table`",
            "param": {
                "is_send_table_image": bool,
                "is_charting": bool,
                "chart_type": str | None,     # 'dual_axis_line_bar' | 'four_line_week_month'
                "chart_title": str | None,
                "chart_x_col": str | None,
                "chart_value_col": str,       # default 'value'
                "chart_volume_col": str,      # default 'volume'
                "chart_category_col": str | None,
                "chart_bar_category": str | None,
                "chart_sort_col": str,        # default 'sort_key'
                "chart_section_col": str,     # default 'section'
                "chart_week_cy_col": str,     # default 'week_cy'
                "chart_week_py_col": str,     # default 'week_py'
                "chart_month_cy_col": str,    # default 'month_cy'
                "chart_month_py_col": str,    # default 'month_py'
                "chart_subtitle": str,        # default ''
                "chart_value_format": str,    # default 'percent'
                "send_to_chatgpt_api": bool,
                "toon_max_rows": int | None,
                "toon_out_dir": str | None,
                "chatgpt_model": str | None,
                "api_key_env": str | None,
                "system_instructions": str | None,
                "chatgpt_instructions": str | None,
            }
        }
        """
        import json as _json

        # 1) Build dictionary and look up (project_name, task)
        project_dict = self._build_project_dict_from_airflow_dictionary()
        if project_name not in project_dict:
            raise KeyError(f"[airflow_dictionary] project_name '{project_name}' not found.")
        tasks = project_dict[project_name]["task"]
        if task not in tasks or not tasks[task]:
            raise KeyError(
                f"[airflow_dictionary] task '{task}' not found or has no entries "
                f"under project '{project_name}'."
            )
        entries = tasks[task]
        if not (0 <= pick_index < len(entries)):
            raise IndexError(
                f"[airflow_dictionary] pick_index {pick_index} out of range (0..{len(entries) - 1})."
            )
        cfg = entries[pick_index]

        # 2) Parse detail_lv_03 as JSON → query + param
        raw_lv03 = cfg.get("detail_lv_03")
        if not raw_lv03:
            raise ValueError(
                f"[airflow_dictionary] detail_lv_03 is empty for "
                f"project='{project_name}', task='{task}'."
            )
        if isinstance(raw_lv03, str):
            import ast as _ast
            try:
                lv03 = _json.loads(raw_lv03)
            except _json.JSONDecodeError:
                # Fallback: handle Python-style booleans (True/False/None) stored in the cell
                lv03 = _ast.literal_eval(raw_lv03)
        else:
            lv03 = raw_lv03
        query = lv03.get("query", "")
        if not query:
            raise ValueError(
                f"[airflow_dictionary] 'query' key missing in detail_lv_03 for "
                f"project='{project_name}', task='{task}'."
            )
        param = lv03.get("param", {})
        pic = param.get("pic") or cfg.get("pic")

        # 3) Run query
        print(f"[CHART_LLM] Running query for {project_name}/{task}")
        df = self.get_data_from_bigquery(query)
        try:
            print(df.head(5))
        except Exception:
            print(f"Preview: {df.shape[0]} rows x {df.shape[1]} cols")

        # 4) Optional table image
        if param.get("is_send_table_image"):
            self.send_dataframe_as_image(df.head(20), title=f"project_name={project_name}, task={task}")

        # 5) Optional charting
        if param.get("is_charting"):
            try:
                print(f"[CHARTING] Starting for {project_name}/{task}")
                chart = Charting(style="white")
                cols = list(df.columns)
                print(f"[CHARTING] Available columns: {cols}")

                chart_type = param.get("chart_type")
                chart_x_col = param.get("chart_x_col")
                chart_sort_col = param.get("chart_sort_col", "sort_key")
                chart_section_col = param.get("chart_section_col", "section")
                chart_week_cy_col = param.get("chart_week_cy_col", "week_cy")
                chart_week_py_col = param.get("chart_week_py_col", "week_py")
                chart_month_cy_col = param.get("chart_month_cy_col", "month_cy")
                chart_month_py_col = param.get("chart_month_py_col", "month_py")
                chart_subtitle = param.get("chart_subtitle", "")
                chart_value_format = param.get("chart_value_format", "percent")
                chart_title = param.get("chart_title") or f"{project_name} / {task}"
                chart_value_col = param.get("chart_value_col", "value")
                chart_volume_col = param.get("chart_volume_col", "volume")
                chart_category_col = param.get("chart_category_col")
                chart_bar_category = param.get("chart_bar_category")

                # Auto-detect chart type from column signature
                if not chart_type:
                    if {"x_label", "section", "week_cy", "month_cy"}.issubset(set(cols)):
                        chart_type = "four_line_week_month"
                    else:
                        chart_type = "dual_axis_line_bar"

                # Auto-detect x_col
                if not chart_x_col:
                    if chart_type == "four_line_week_month":
                        chart_x_col = "x_label" if "x_label" in cols else None
                    else:
                        chart_x_col = next(
                            (c for c in ["time", "time_level", "date", "dt", "created_date"] if c in cols),
                            None,
                        )

                if not chart_x_col:
                    print(f"[CHARTING] ❌ No x-axis column found. Available: {cols}")
                else:
                    print(f"[CHARTING] ✓ Using x_col={chart_x_col}, chart_type={chart_type}")
                    tmp_dir = tempfile.gettempdir()
                    chart_path = os.path.join(tmp_dir, f"{project_name}_{task}_chart.png")

                    if chart_type == "four_line_week_month":
                        required = [chart_sort_col, chart_section_col, chart_week_cy_col,
                                    chart_week_py_col, chart_month_cy_col, chart_month_py_col]
                        missing = [c for c in required if c not in cols]
                        if missing:
                            print(f"[CHARTING] ❌ Missing columns: {missing}")
                        else:
                            chart.four_line_week_month(
                                df=df,
                                x_col=chart_x_col,
                                sort_col=chart_sort_col,
                                section_col=chart_section_col,
                                week_cy_col=chart_week_cy_col,
                                week_py_col=chart_week_py_col,
                                month_cy_col=chart_month_cy_col,
                                month_py_col=chart_month_py_col,
                                title=chart_title,
                                subtitle=chart_subtitle,
                                value_format=chart_value_format,
                                save_path=chart_path,
                            )
                            print(f"[CHARTING] ✓ Chart created at: {chart_path}")
                    else:
                        if chart_value_col not in cols:
                            print(f"[CHARTING] ❌ value column '{chart_value_col}' not in {cols}")
                        elif chart_volume_col not in cols:
                            print(f"[CHARTING] ❌ volume column '{chart_volume_col}' not in {cols}")
                        else:
                            chart.dual_axis_line_bar(
                                df=df,
                                x_col=chart_x_col,
                                value_col=chart_value_col,
                                volume_col=chart_volume_col,
                                category_col=chart_category_col,
                                bar_category=chart_bar_category,
                                title=chart_title,
                                save_path=chart_path,
                            )
                            print(f"[CHARTING] ✓ Chart created at: {chart_path}")

                    if os.path.exists(chart_path):
                        pic_mention = self._format_pic_mention(pic)
                        caption = f'\U0001f4ca {pic_mention} Chart: {project_name}/{task}'
                        print("[CHARTING] Sending chart to Telegram...")
                        try:
                            self.send_photo_to_topic(
                                image_path=chart_path,
                                caption=caption,
                                channel_key="error_colab",
                                topic="general",
                            )
                            print("[CHARTING] Chart sent to Telegram successfully")
                        except Exception as send_err:
                            print(f"[CHARTING] Failed to send chart: {type(send_err).__name__}: {send_err}")
                    else:
                        print(f"[CHARTING] Chart file not found, skipping send: {chart_path}")

                    chart.close_all()
            except Exception as e:
                print(f"[ERROR] Charting failed: {type(e).__name__}: {e}")

        # 6) Optional LLM analysis
        if param.get("send_to_chatgpt_api"):
            toon_out_dir = param.get("toon_out_dir", "/tmp/toon")
            chatgpt_model = param.get("chatgpt_model", "gpt-4o-mini")
            api_key_env = param.get("api_key_env")
            system_instructions = param.get("system_instructions", "")
            chatgpt_instructions = param.get("chatgpt_instructions", "")
            toon_max_rows = param.get("toon_max_rows")

            pic_mention = self._format_pic_mention(pic)
            if getattr(self, "notifier", None):
                self.notifier.send_message(
                    f"\U0001f916 {pic_mention} Data for project='{project_name}', task='{task}' sent to ChatGPT for analysis."
                )

            toon_text = self.polars_to_toon(df, root_name="rows", max_rows=toon_max_rows)
            self.write_toon_file(toon_text, prefix=f"{project_name}_{task}", out_dir=toon_out_dir)

            llm_text = self.call_openai_with_toon(
                toon_text,
                system_instructions=system_instructions,
                instructions=chatgpt_instructions,
                model=chatgpt_model,
                api_key_env=api_key_env,
            )

            if llm_text and getattr(self, "notifier", None):
                self.notifier.send_message(f"\U0001f916 {chatgpt_model} insights:\n{llm_text}")

    @notify_on_failure_for_pic
    def update_all_rows_bigquery_chart_n_llm_by_project(
        self,
        project_name: str,
        *,
        task_filter: list[str] | None = None,
        pick_index: int = 0,
    ) -> dict[str, str]:
        """
        Run update_all_rows_bigquery_chart_n_llm for every task in a project.

        Parameters
        ----------
        project_name : str
            Key from airflow_dictionary.project_name
        task_filter : list[str] | None
            If provided, only process tasks in this list. Otherwise process all tasks.
        pick_index : int
            Use the N-th entry if multiple rows exist per project/task (default 0).

        Returns
        -------
        dict[str, str]
            Mapping of task_name → 'ok' | 'failed: <reason>'
        """
        project_dict = self._build_project_dict_from_airflow_dictionary()

        if project_name not in project_dict:
            raise KeyError(f"[airflow_dictionary] project_name '{project_name}' not found.")

        tasks = project_dict[project_name]["task"]
        tasks_clean = {k: v for k, v in tasks.items() if k is not None and str(k).strip()}

        if not tasks_clean:
            raise ValueError(
                f"[airflow_dictionary] No valid tasks found for project '{project_name}'."
            )

        if task_filter:
            tasks_to_run = {k: v for k, v in tasks_clean.items() if k in task_filter}
            if not tasks_to_run:
                raise ValueError(
                    f"[airflow_dictionary] None of {task_filter} found in project '{project_name}'."
                )
        else:
            tasks_to_run = tasks_clean

        total = len(tasks_to_run)
        results: dict[str, str] = {}

        for idx, task_name in enumerate(tasks_to_run, 1):
            print(f"\n{'='*60}")
            print(f"[CHART_LLM_PROJECT] Task {idx}/{total}: {task_name}")
            print(f"{'='*60}")
            try:
                self.update_all_rows_bigquery_chart_n_llm(
                    project_name=project_name,
                    task=task_name,
                    pick_index=pick_index,
                )
                results[task_name] = "ok"
                print(f"✅ Task '{task_name}' done")
            except Exception as e:
                msg = f"{type(e).__name__}: {e}"
                results[task_name] = f"failed: {msg}"
                print(f"❌ Task '{task_name}' failed: {msg}")

        ok_count = sum(1 for v in results.values() if v == "ok")
        fail_count = total - ok_count
        print(f"\n[CHART_LLM_PROJECT] Done — {ok_count}/{total} ok, {fail_count} failed.")

        if fail_count:
            failed_names = [k for k, v in results.items() if v != "ok"]
            raise RuntimeError(
                f"{fail_count}/{total} tasks failed for project '{project_name}': {failed_names}"
            )

        return results

    @notify_on_failure_for_pic
    def update_all_rows_local_to_gsheet(
        self, project_name: str, task: str, *,
        format: str = 'csv', pick_index: int = 0,
        drop_dup: bool | None = None, unique_list_cols: list | None = None,
        use_chunked: bool | None = None, chunk_size: int | None = None) -> int:
        """
        1) Read airflow_dictionary to locate (link, sheet, range, view/table).
        2) SELECT * FROM detail_lv_03 (view/table) in BigQuery.
        3) Write ALL rows to the Google Sheet range.

        Parameters
        ----------
        project_name : str
            Key from airflow_dictionary.project_name
        task : str
            Key from airflow_dictionary.task
        pick_index : int
            Use the N-th entry if multiple rows exist for the same project/task (default 0).
        use_chunked : bool
            Use Load.update_data_gsheet_chunked(...) for very large frames.
        chunk_size : int
            Rows per chunk when use_chunked=True.
        **set_with_dataframe_params :
            Extra kwargs passed to gspread_dataframe.set_with_dataframe
            (e.g., resize, include_column_header, etc.)

        Returns
        -------
        int
            Number of data rows written.
        """
        # 1) Build dictionary
        project_dict = self._build_project_dict_from_airflow_dictionary()
        print(project_dict)

        if project_name not in project_dict:
            raise KeyError(f"[airflow_dictionary] project_name '{project_name}' not found.")

        tasks = project_dict[project_name]["task"]
        if task not in tasks or not tasks[task]:
            raise KeyError(
                f"[airflow_dictionary] task '{task}' not found or has no entries under project '{project_name}'."
            )

        entries = tasks[task]
        if not (0 <= pick_index < len(entries)):
            raise IndexError(
                f"[airflow_dictionary] pick_index {pick_index} out of range (0..{len(entries)-1})."
            )
        cfg = entries[pick_index]

        # 3) Resolve targets
        link_gsheet = cfg["link_gsheet_gcs"]
        sheet_name = cfg["detail_lv_01"]
        data_range = cfg["detail_lv_02"]
        params_raw = cfg.get("detail_lv_03")
        print(f"Config: {link_gsheet=}, {sheet_name=}, {data_range=}, {params_raw=}")

        # Parse detail_lv_03
        if isinstance(params_raw, str):
            try:
                params = json.loads(params_raw)
            except json.JSONDecodeError:
                params = ast.literal_eval(params_raw)
        elif isinstance(params_raw, dict):
            params = params_raw
        else:
            raise ValueError("detail_lv_03 must be a JSON string or dict with GCS parameters.")

        # --- 2) Extract common parameters
        try:
            local_directory = params["local_directory"]
            drop_dup = params["drop_dup"]
            unique_list_cols = params["unique_list_cols"]
        except KeyError as e:
            raise KeyError(f"Missing key in detail_lv_03 params: {e!s}")

        # drop_dup data_frame
        if drop_dup:
            df_view = (self.get_data_from_drive(local_directory, format=format)\
                       .sort(unique_list_cols, descending=True).unique(subset=unique_list_cols, keep='first')
) # Polars DataFrame

        try:
            print(df_view.head(5))
        except Exception:
            print(f"Preview: {df_view.shape[0]} rows x {df_view.shape[1]} cols")

        # 4) Write to Google Sheet using existing Load writers (handles Telegram notify)
        n_rows = len(df_view)
        if use_chunked:
            self.update_data_gsheet_chunked(
                df_view,
                link=link_gsheet,
                sheet_name=sheet_name,
                cell_range=data_range,
                chunk_size=chunk_size,
                include_index=False
            )
        else:
            self.update_data_gsheet(
                df_view,
                link=link_gsheet,
                sheet_name=sheet_name,
                cell_range=data_range
            )
        self.send_dataframe_as_image(df_view.head(20), title=f"project_name={project_name}, task={task}")
        
        return n_rows

    @notify_on_failure_for_pic
    def update_all_rows_gsheet_to_bucket(
        self,
        project_name: str,
        task: str,
        *,
        pick_index: int = 0,
        is_multiple_sheet: bool | None = False,
        drop_dup: bool | None = None, unique_list_cols: list | None = None,
        drop_sheet_name: bool | None = True,
        drop_col: bool | None = None, drop_list_cols: list | None = None,
        is_transform: bool | None = None,
        is_n_day_limit: int | None = None,
        bucket_level: int | None = None,
        is_updated_date_added: bool | None = None,
    ) -> int:
        """
        1) Look up config from airflow_dictionary via self._build_project_dict_from_airflow_dictionary().
        2) Read rows from Google Sheet (link_gsheet_gcs/detail_lv_01/detail_lv_02).
        3) Write partitioned files to GCS (1-level or 2-level based on bucket_level).
        """

        # --- 1) Resolve config from airflow_dictionary
        project_dict = self._build_project_dict_from_airflow_dictionary()
        if project_name not in project_dict:
            raise KeyError(f"[airflow_dictionary] project_name '{project_name}' not found.")

        tasks = project_dict[project_name]["task"]
        if task not in tasks or not tasks[task]:
            raise KeyError(
                f"[airflow_dictionary] task '{task}' not found or has no entries under project '{project_name}'."
            )

        entries = tasks[task]
        if not (0 <= pick_index < len(entries)):
            raise IndexError(f"pick_index {pick_index} out of range (0..{len(entries)-1}).")

        cfg = entries[pick_index]

        link_gsheet = cfg["link_gsheet_gcs"]
        sheet_name = cfg["detail_lv_01"]
        range_name = cfg["detail_lv_02"]
        params_raw = cfg.get("detail_lv_03")
        print(f"Config: {link_gsheet=}, {sheet_name=}, {range_name=}, {params_raw=}")

        # Parse detail_lv_03
        if isinstance(params_raw, str):
            try:
                params = json.loads(params_raw)
            except json.JSONDecodeError:
                params = ast.literal_eval(params_raw)
        elif isinstance(params_raw, dict):
            params = params_raw
        else:
            raise ValueError("detail_lv_03 must be a JSON string or dict with GCS parameters.")

        # --- 2) Extract common parameters
        print(f'detail_lv_03: {params}')
        try:
            link_bucket = params["link_bucket"]
            column_list = params["column_list"]
            file_name = params["file_name"]
            fmt = params["format"]

            if bucket_level == 2:
                index_to_break_1 = params["index_to_break_1"]
                name_partition_1 = params["name_partition_1"]
                index_to_break_2 = params["index_to_break_2"]
                name_partition_2 = params["name_partition_2"]
            else:
                index_to_break = params["index_to_break"]
                name_partition = params["name_partition"]

            fmt_date = params.get("date_format", None)
            output_fmt_date = params.get("output_format", None)
            old_col_name = params.get("old_col_name", None)
            # For delta_merge: columns used to match source rows to target rows
            merge_keys = params.get("merge_keys", None)

        except KeyError as e:
            raise KeyError(f"Missing key in detail_lv_03 params: {e!s}")

        try:
            drop_dup = params["drop_dup"]
            unique_list_cols = params["unique_list_cols"]
        except KeyError as e:
            print(f"[WARN] Missing drop_dup in detail_lv_03 drop_dup params: {e!s}")
            # Provide safe defaults so code won't break
            drop_dup = False
            unique_list_cols = []
        
        try:
            drop_col = params["drop_col"]
            drop_list_cols = params["drop_list_cols"]
        except KeyError as e:
            print(f"[WARN] Missing drop_list_cols in detail_lv_03 drop_col params: {e!s}")
            # Provide safe defaults so code won't break
            drop_col = False
            drop_list_cols = []
        
        try:
            is_multiple_sheet = params["is_multiple_sheet"]
        except KeyError as e:
            print(f"[WARN] Missing is_multiple_sheet in detail_lv_03 drop_col params: {e!s}")
            is_multiple_sheet = False

        try:
            is_updated_date_added = params["is_updated_date_added"]
        except KeyError as e:
            print(f"[WARN] Missing is_updated_date_added in detail_lv_03 drop_col params: {e!s}")
            is_updated_date_added = False

        try:
            drop_sheet_name = params["drop_sheet_name"]
        except KeyError as e:
            print(f"[WARN] Missing drop_sheet_name in detail_lv_03 drop_col params: {e!s}")
            drop_sheet_name = True

        # --- 3) Read data from Google Sheet
        if is_multiple_sheet==False:
            print("Reading multiple sheets is not yet implemented.")
            spreadsheet = self.google_client.open_by_url(link_gsheet)
            ws = spreadsheet.worksheet(sheet_name)

            df = self.get_data_from_sheet(
                spreadsheet,
                sheet=ws,
                range_data=range_name,
                column=None,
            )
        else:
            df = self.get_data_from_gsheet(
                link_gsheet=link_gsheet,
                sheet_name_regex=sheet_name,
                range_data=range_name,
                column=None,
            )
        
        if drop_dup:
            df = df.sort(unique_list_cols, descending=True).unique(subset=unique_list_cols, keep='first')

        if drop_col:
            df = df.drop(drop_list_cols)  

        if drop_sheet_name == True and "sheet_name" in df.columns:
            df = df.drop("sheet_name")

        # Optional explicit schema
        if column_list is not None:
            if len(column_list) != len(df.columns):
                print(column_list)
                print(df.columns)
                raise ValueError(
                    f"`columns` length ({len(column_list)}) != DataFrame columns ({len(df.columns)})."
                )
            df = df.rename({old: new for old, new in zip(df.columns, column_list)})

        # Optional date format transformation
        if is_transform and fmt_date is not None:
            if bucket_level == 2:
                df = self.safe_parse_timestamp_to_yyyy_mm_dd(
                    df,
                    col_name=old_col_name,
                    index_to_break=index_to_break_1,
                    date_format=fmt_date,
                    output_format=output_fmt_date
                )
            else:
                df = self.safe_parse_timestamp_to_yyyy_mm_dd(
                    df,
                    col_name=old_col_name,
                    index_to_break=index_to_break,
                    date_format=fmt_date,
                    output_format=output_fmt_date
                )

        # Optional add an `updated_date` column with today's date
        if is_updated_date_added:
            try:
                tz = pytz.timezone("Asia/Ho_Chi_Minh")
                today_str = datetime.now(tz).strftime("%Y-%m-%d")
                df = df.with_columns(pl.lit(today_str).alias("updated_date"))
                print('add columns successfully updated_date')
            except Exception as e:
                print(f"[WARN] Failed to add updated_date column: {e}")
        print(df.columns)

        # --- 4) Validate partition column(s)
        if bucket_level == 2:
            for idx in [index_to_break_1, index_to_break_2]:
                if idx not in df.columns:
                    raise KeyError(f"Partition column '{idx}' not found in DataFrame columns: {df.columns}")
        else:
            if index_to_break not in df.columns:
                raise KeyError(f"Partition column '{index_to_break}' not found in DataFrame columns: {df.columns}")

        n_rows = len(df)
        if n_rows == 0:
            if getattr(self, "notifier", None):
                self.notifier.send_message(
                    f"✅ Sheet '{sheet_name}' read OK (0 rows). Nothing written to GCS."
                )
            return 0

        # Optional: filter to last N days
        if is_n_day_limit is not None and is_n_day_limit > 0:
            today = datetime.today()
            cutoff_date = (today - timedelta(days=is_n_day_limit)).strftime("%Y-%m-%d")
            target_col = index_to_break_1 if bucket_level == 2 else index_to_break
            df = df.filter(pl.col(target_col) >= cutoff_date)
            print(f"Filtered to last {is_n_day_limit} days since {cutoff_date}, now {len(df)} rows.")

        # --- 5) Write partitioned files to GCS
        if bucket_level == 2:
            print("⚙️ Writing 2-level partitioned data to GCS...")
            self.update_to_bucket_2_level(
                df,
                link_bucket=link_bucket,
                index_to_break_1=index_to_break_1,
                name_partition_1=name_partition_1,
                index_to_break_2=index_to_break_2,
                name_partition_2=name_partition_2,
                file_name=file_name,
                format=fmt,
            )
        else:
            print("⚙️ Writing 1-level partitioned data to GCS...")
            self.update_to_bucket(
                df,
                link_bucket=link_bucket,
                index_to_break=index_to_break,
                name_partition=name_partition,
                file_name=file_name,
                format=fmt,
                merge_keys=merge_keys,
            )

        return n_rows
    

    
# =========================
# calculate execution time of a function
def time_execution(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        return result, execution_time
    return wrapper
