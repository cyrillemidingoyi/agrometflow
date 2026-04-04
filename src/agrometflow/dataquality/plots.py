"""
Plotting functions for climate data quality control.
Inspired by the dataresqc R package.

Author: Adapted from dataresqc (Yuri Brugnara, Stefan Hunziker)
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from typing import Optional, Union, List, Tuple
from datetime import datetime
import calendar


def _missing_marker_y(ax, values: pd.Series) -> float:
    """
    Return a y-position for missing-data markers just below the plotted range.

    Uses an axis-relative offset instead of a fixed one-unit subtraction so
    variables with small magnitudes, such as light precipitation, do not show
    misleading markers at an arbitrary value like ``-1``.
    """
    finite = pd.to_numeric(values, errors="coerce").dropna()
    if finite.empty:
        return 0.0

    y_min, y_max = ax.get_ylim()
    y_range = y_max - y_min
    if not np.isfinite(y_range) or y_range <= 0:
        scale = max(abs(float(finite.min())), abs(float(finite.max())), 1.0)
        y_range = 0.05 * scale

    offset = 0.05 * y_range
    y_marker = y_min - offset
    ax.set_ylim(bottom=y_marker - offset, top=y_max)
    return y_marker


def plot_decimals(
    data: pd.DataFrame,
    date_col: str = "Date",
    value_col: str = "value",
    outfile: Optional[Union[str, Path]] = None,
    startyear: Optional[int] = None,
    endyear: Optional[int] = None,
    max_years_per_page: int = 30,
    figsize: Tuple[int, int] = (12, 7),
    show: bool = True,
    station_col: Optional[str] = None,
):
    """
    Plot year-by-year distribution of decimals to investigate reporting resolution.
    
    Parameters
    ----------
    data : pd.DataFrame
        DataFrame with date and value columns
    date_col : str
        Name of date column
    value_col : str
        Name of value column
    outfile : str or Path, optional
        Output PDF filename. If None, no PDF is saved.
    startyear : int, optional
        First year to plot
    endyear : int, optional
        Last year to plot
    max_years_per_page : int
        Maximum years per plot segment (default: 30)
    figsize : tuple
        Figure size (width, height) in inches
    show : bool
        If True (default), display figures inline (Jupyter). If False, only save PDF.
    station_col : str, optional
        Column name for station identifier. If provided and multiple stations exist,
        creates separate plots per-station
        
    References
    ----------
    Hunziker et al., 2017: Int. J. Climatol, 37: 4131-4145.
    https://doi.org/10.1002/joc.5037
    Hunziker et al., 2018: Clim. Past, 14: 1-20.
    """
    from matplotlib.backends.backend_pdf import PdfPages
    
    if outfile is not None:
        outfile = Path(outfile)
        if outfile.suffix != ".pdf":
            outfile = outfile.with_suffix(".pdf")
    
    df = data.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.dropna(subset=[value_col])
    df["year"] = df[date_col].dt.year
    
    if startyear is not None:
        df = df[df["year"] >= startyear]
    if endyear is not None:
        df = df[df["year"] <= endyear]
    
    # Check for multi-station data
    if station_col is not None and station_col in df.columns:
        stations = sorted(df[station_col].unique())
        if len(stations) > 1:
            # Multi-station: create separate plot per station
            for station in stations:
                station_data = df[df[station_col] == station]
                _plot_decimals_single(
                    station_data, date_col, value_col,
                    outfile, startyear, endyear, max_years_per_page, figsize, show,
                    suffix=f"_{station}"
                )
            return
    
    # Single station or station_col not provided
    _plot_decimals_single(df, date_col, value_col, outfile, startyear, endyear,
                         max_years_per_page, figsize, show)


def _plot_decimals_single(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    outfile: Optional[Union[str, Path]],
    startyear: Optional[int],
    endyear: Optional[int],
    max_years_per_page: int,
    figsize: Tuple[int, int],
    show: bool,
    suffix: str = ""
):
    """Helper function to plot decimals for single station."""
    from matplotlib.backends.backend_pdf import PdfPages
    
    if len(df) == 0:
        print("No valid data found in the selected time interval")
        return
    
    # Get first decimal digit
    rounded_values = np.round(df[value_col], 1)
    decimal_digit = (10 * np.abs(rounded_values - np.trunc(rounded_values))).astype(int)
    df = df.copy()
    df["decimal"] = decimal_digit
    
    # Count decimals per year
    years = sorted(df["year"].unique())
    decimal_labels = [f"x.{i}" for i in range(10)]
    
    counts = []
    for year in years:
        year_data = df[df["year"] == year]
        year_counts = [np.sum(year_data["decimal"] == i) for i in range(10)]
        if sum(year_counts) > 0:
            counts.append(year_counts)
        else:
            counts.append([np.nan] * 10)
    
    counts_df = pd.DataFrame(counts, index=years, columns=decimal_labels)
    
    # Determine number of segments
    n_years = len(years)
    n_segments = int(np.ceil(n_years / max_years_per_page))
    
    # Colors
    colors = ["black", "yellow", "orange", "red", "darkslateblue", 
              "darkgray", "magenta", "blue", "cyan", "darkgreen"]
    
    # Build segments and figures
    figs = []
    for seg_idx in range(n_segments):
        start_idx = seg_idx * max_years_per_page
        end_idx = min((seg_idx + 1) * max_years_per_page, n_years)

        seg_years = years[start_idx:end_idx]
        seg_data = counts_df.loc[seg_years]

        # Pad to max_years_per_page
        if len(seg_years) < max_years_per_page:
            padding_years = range(seg_years[-1] + 1,
                                  seg_years[0] + max_years_per_page)
            for py in padding_years:
                if py not in seg_data.index:
                    seg_data.loc[py] = [np.nan] * 10
            seg_data = seg_data.sort_index()

        fig, ax = plt.subplots(figsize=figsize)

        # Stacked bar chart
        bottom = np.zeros(len(seg_data))
        for i, (label, color) in enumerate(zip(decimal_labels, colors)):
            values = seg_data[label].values
            ax.bar(seg_data.index, values, bottom=bottom,
                   label=label, color=color, width=0.7)
            bottom += np.nan_to_num(values)

        ax.set_xlabel("")
        ax.set_ylabel("Count", fontsize=13)
        title = outfile.stem if outfile is not None else "Decimal distribution"
        ax.set_title(title, fontsize=18, fontweight="bold")
        ax.legend(loc="upper left", bbox_to_anchor=(1, 1))
        ax.set_xlim(seg_data.index[0] - 0.5, seg_data.index[-1] + 0.5)

        plt.tight_layout()
        figs.append(fig)

    # Save and/or show
    if outfile is not None:
        from matplotlib.backends.backend_pdf import PdfPages
        with PdfPages(outfile) as pdf:
            for fig in figs:
                pdf.savefig(fig)
        print(f"Plot saved in {outfile}")

    if show:
        for fig in figs:
            plt.figure(fig.number)
            plt.show()
    else:
        for fig in figs:
            plt.close(fig)


def plot_daily(
    data: pd.DataFrame,
    date_col: str = "Date",
    value_col: str = "value",
    var_name: str = "",
    units: str = "",
    outfile: Optional[Union[str, Path]] = None,
    startyear: Optional[int] = None,
    endyear: Optional[int] = None,
    years_per_panel: int = 1,
    show_missing: bool = True,
    figsize: Tuple[int, int] = (10, 3),
    show: bool = True,
    station_col: Optional[str] = None,
    **plot_kwargs
):
    """
    Plot daily data points in time series panels.
    
    Parameters
    ----------
    data : pd.DataFrame
        DataFrame with date and value columns
    date_col : str
        Name of date column
    value_col : str
        Name of value column
    var_name : str
        Variable name for y-axis label
    units : str
        Units for y-axis label
    outfile : str or Path, optional
        Output PDF filename. If None, no PDF is saved.
    startyear : int, optional
        First year to plot
    endyear : int, optional
        Last year to plot
    years_per_panel : int
        Number of years per panel (ignored for multi-station data)
    show_missing : bool
        Mark missing dates with red crosses
    figsize : tuple
        Figure size per panel (width, height)
    show : bool
        If True (default), display figures inline (Jupyter). If False, only save PDF.
    station_col : str, optional
        Column name for station identifier. If provided and multiple stations exist,
        creates matrix layout (rows=years, columns=stations)
    **plot_kwargs
        Additional arguments passed to plt.plot()
    """
    if outfile is not None:
        outfile = Path(outfile)
        if outfile.suffix != ".pdf":
            outfile = outfile.with_suffix(".pdf")
    
    df = data.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df["year"] = df[date_col].dt.year
    
    if startyear is not None:
        df = df[df["year"] >= startyear]
    if endyear is not None:
        df = df[df["year"] <= endyear]
    
    ylabel = f"{var_name} [{units}]" if units else var_name
    
    # Check for multi-station data
    if station_col is not None and station_col in df.columns:
        stations = sorted(df[station_col].unique())
        if len(stations) > 1:
            # Multi-station matrix layout
            years = sorted(df["year"].unique())
            n_years = len(years)
            n_stations = len(stations)
            
            fig, axes = plt.subplots(n_years, n_stations,
                                     figsize=(figsize[0] * n_stations, figsize[1] * n_years),
                                     squeeze=False)
            
            for year_idx, year in enumerate(years):
                for station_idx, station in enumerate(stations):
                    ax = axes[year_idx, station_idx]
                    
                    # Filter data for this year and station
                    subset = df[(df["year"] == year) & (df[station_col] == station)]
                    
                    if len(subset) > 0:
                        ax.plot(subset[date_col], subset[value_col], **plot_kwargs)
                        
                        # Mark missing dates
                        if show_missing:
                            all_dates = pd.date_range(
                                start=f"{year}-01-01",
                                end=f"{year}-12-31",
                                freq="D"
                            )
                            missing_dates = set(all_dates) - set(subset[date_col])
                            if missing_dates:
                                y_marker = _missing_marker_y(ax, subset[value_col])
                                ax.scatter(list(missing_dates), [y_marker] * len(missing_dates),
                                          marker="x", color="red", s=10, alpha=0.5)
                    
                    ax.grid(True, alpha=0.3)
                    
                    # Top row shows station names
                    if year_idx == 0:
                        ax.set_title(str(station), fontsize=10, fontweight="bold")
                    
                    # Left column shows years
                    if station_idx == 0:
                        ax.set_ylabel(str(year), fontsize=10)
                    else:
                        ax.set_ylabel("")
                    
                    # Format x-axis
                    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
                    ax.xaxis.set_major_locator(mdates.MonthLocator())
                    ax.tick_params(axis='x', labelsize=8)
                    ax.tick_params(axis='y', labelsize=8)
            
            fig.suptitle(f"{var_name} [{units}] - Multiple Stations" if units else f"{var_name} - Multiple Stations",
                        fontsize=14, fontweight="bold")
            plt.tight_layout()
            
            if outfile is not None:
                from matplotlib.backends.backend_pdf import PdfPages
                with PdfPages(outfile) as pdf:
                    pdf.savefig(fig)
                print(f"Plot saved in {outfile}")
            
            if show:
                plt.show()
            else:
                plt.close(fig)
            return
    
    # Single station or station_col not provided - use original logic
    years = sorted(df["year"].unique())
    n_segments = int(np.ceil(len(years) / years_per_panel))

    fig, axes = plt.subplots(n_segments, 1,
                             figsize=(figsize[0], figsize[1] * n_segments),
                             squeeze=False)
    axes = axes.flatten()

    for seg_idx in range(n_segments):
        start_year = years[0] + seg_idx * years_per_panel
        end_year = min(start_year + years_per_panel - 1, years[-1])

        seg_data = df[(df["year"] >= start_year) & (df["year"] <= end_year)]

        if len(seg_data) == 0:
            continue

        ax = axes[seg_idx]

        # Plot data
        ax.plot(seg_data[date_col], seg_data[value_col], **plot_kwargs)

        # Grid
        ax.grid(True, alpha=0.3)

        # Mark missing dates
        if show_missing and len(seg_data) > 0:
            all_dates = pd.date_range(
                start=f"{start_year}-01-01",
                end=f"{end_year}-12-31",
                freq="D"
            )
            missing_dates = set(all_dates) - set(seg_data[date_col])
            if missing_dates:
                y_marker = _missing_marker_y(ax, seg_data[value_col])
                ax.scatter(list(missing_dates), [y_marker] * len(missing_dates),
                           marker="x", color="red", s=10, alpha=0.5)

        # Labels
        ax.set_ylabel(ylabel)
        if start_year == end_year:
            ax.set_title(str(start_year))
        else:
            ax.set_title(f"{start_year} - {end_year}")

        # X-axis formatting
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        ax.xaxis.set_major_locator(mdates.MonthLocator())

    plt.tight_layout()

    if outfile is not None:
        from matplotlib.backends.backend_pdf import PdfPages
        with PdfPages(outfile) as pdf:
            pdf.savefig(fig)
        print(f"Plot saved in {outfile}")

    if show:
        plt.show()
    else:
        plt.close(fig)


def plot_subdaily(
    data: pd.DataFrame,
    datetime_col: str = "datetime",
    value_col: str = "value",
    var_name: str = "",
    units: str = "",
    year: Optional[Union[int, List[int]]] = None,
    outfile: Optional[Union[str, Path]] = None,
    fixed_ylim: bool = True,
    figsize: Tuple[int, int] = (9, 12),
    show: bool = True,
    **plot_kwargs
):
    """
    Plot sub-daily data points divided by month.
    
    Creates one panel per month showing all observations.
    
    Parameters
    ----------
    data : pd.DataFrame
        DataFrame with datetime and value columns
    datetime_col : str
        Name of datetime column
    value_col : str
        Name of value column
    var_name : str
        Variable name for y-axis label
    units : str
        Units for y-axis label
    year : int or list of int, optional
        Year(s) to plot. If None, plots all available years
    outfile : str or Path, optional
        Output PDF filename (root if multiple years). If None, no PDF is saved.
    fixed_ylim : bool
        Use same y-axis limits for all months
    figsize : tuple
        Figure size (width, height)
    show : bool
        If True (default), display figures inline (Jupyter). If False, only save PDF.
    **plot_kwargs
        Additional arguments passed to plt.plot()
    """
    if outfile is not None:
        outfile = Path(outfile)
    
    df = data.copy()
    df[datetime_col] = pd.to_datetime(df[datetime_col])
    df["year"] = df[datetime_col].dt.year
    df["month"] = df[datetime_col].dt.month
    
    if year is None:
        years = sorted(df["year"].unique())
    elif isinstance(year, int):
        years = [year]
    else:
        years = sorted(year)
    
    ylabel = f"{var_name} [{units}]" if units else var_name
    
    for yr in years:
        year_data = df[df["year"] == yr]
        
        if len(year_data) == 0:
            print(f"No data for year {yr}")
            continue
        
        # Determine output file for this year
        yr_outfile = None
        if outfile is not None:
            if len(years) > 1:
                yr_outfile = outfile.parent / f"{outfile.stem}.{yr}.pdf"
            else:
                yr_outfile = outfile
            if yr_outfile.suffix != ".pdf":
                yr_outfile = yr_outfile.with_suffix(".pdf")

        # Determine y-limits
        if fixed_ylim:
            ylim = (year_data[value_col].min(), year_data[value_col].max())
        else:
            ylim = None

        fig, axes = plt.subplots(4, 3, figsize=figsize)
        axes = axes.flatten()

        for month in range(1, 13):
            month_data = year_data[year_data["month"] == month]
            ax = axes[month - 1]

            if len(month_data) > 0:
                ax.plot(month_data[datetime_col], month_data[value_col],
                        **plot_kwargs)

                if ylim is not None:
                    ax.set_ylim(ylim)

                # Grid
                ax.grid(True, alpha=0.3, which="both")

                # Format x-axis
                ax.xaxis.set_major_formatter(mdates.DateFormatter("%d"))
                ax.xaxis.set_major_locator(mdates.DayLocator([1, 10, 20]))

            ax.set_title(calendar.month_name[month])
            ax.set_ylabel(ylabel if month % 3 == 1 else "")

        fig.suptitle(f"{var_name} - {yr}", fontsize=14, fontweight="bold")
        plt.tight_layout()

        if yr_outfile is not None:
            from matplotlib.backends.backend_pdf import PdfPages
            with PdfPages(yr_outfile) as pdf:
                pdf.savefig(fig)
            print(f"Plot saved in {yr_outfile}")

        if show:
            plt.show()
        else:
            plt.close(fig)


def plot_weekly_cycle(
    data: pd.DataFrame,
    date_col: str = "Date",
    precip_col: str = "rr",
    station_id: str = "station",
    outpath: Optional[Union[str, Path]] = None,
    threshold: float = 1.0,
    confidence_level: float = 0.95,
    figsize: Tuple[int, int] = (7, 5),
    show: bool = True
):
    """
    Test and visualize weekly cycle in daily precipitation data using binomial test.
    
    Parameters
    ----------
    data : pd.DataFrame
        DataFrame with date and precipitation columns
    date_col : str
        Name of date column
    precip_col : str
        Name of precipitation column
    station_id : str
        Station identifier for output filename
    outpath : str or Path, optional
        Output directory for PDF. If None, no PDF is saved.
    threshold : float
        Precipitation threshold for wet day (mm, default: 1.0)
    confidence_level : float
        Confidence level for binomial test (default: 0.95)
    figsize : tuple
        Figure size (width, height)
    show : bool
        If True (default), display figures inline (Jupyter). If False, only save PDF.
        
    References
    ----------
    Hunziker et al., 2017: Int. J. Climatol, 37: 4131-4145.
    https://doi.org/10.1002/joc.5037

    Notes
    -----
    For each weekday, the function tests whether the observed number of wet days
    differs from the expected count under a binomial model with success
    probability equal to the overall wet-day fraction, phi, computed from all
    valid observations. A two-sided exact binomial test is applied separately to
    each weekday, and a weekday is flagged as significant when
    p_value < (1 - confidence_level). This is intended as a screening test and
    does not correct for multiple comparisons across weekdays. In a quality
    control context, phi is the baseline wet-day probability for the full
    series, and large weekday departures from this baseline can indicate
    reporting artifacts such as delayed observations, weekend effects, or
    day-shifting of precipitation totals rather than a purely meteorological
    signal.
    """
    from scipy.stats import binomtest

    if outpath is not None:
        outpath = Path(outpath)
        outpath.mkdir(parents=True, exist_ok=True)
    
    df = data.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.dropna(subset=[date_col, precip_col])

    if df.empty:
        raise ValueError("No valid precipitation data available for weekly cycle plot.")

    df["weekday"] = df[date_col].dt.day_name()
    df["is_wet"] = (df[precip_col] >= threshold).astype(int)
    
    # Count wet days per weekday
    weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", 
                     "Friday", "Saturday", "Sunday"]
    
    wet_counts = []
    total_counts = []
    
    for wd in weekday_order:
        wd_data = df[df["weekday"] == wd]
        wet_counts.append(wd_data["is_wet"].sum())
        total_counts.append(len(wd_data))
    
    # Calculate overall wet day probability
    total_observations = sum(total_counts)
    if total_observations == 0:
        raise ValueError("No valid weekday observations available for weekly cycle plot.")

    phi = sum(wet_counts) / total_observations
    
    # Wet day fraction per weekday
    fractions = [w / t if t > 0 else 0 for w, t in zip(wet_counts, total_counts)]
    
    # Binomial test for each weekday
    significant = []
    p_values = []
    for wet, total in zip(wet_counts, total_counts):
        if total > 0:
            p_value = binomtest(wet, total, phi, alternative="two-sided").pvalue
            p_values.append(p_value)
            significant.append(p_value < (1 - confidence_level))
        else:
            p_values.append(np.nan)
            significant.append(False)
    
    # Plot
    fig, ax = plt.subplots(figsize=figsize)
    
    x = np.arange(7)
    colors = ["red" if sig else "black" for sig in significant]
    
    ax.bar(x, fractions, color=colors, width=0.7)
    ax.axhline(phi, linestyle="--", color="black", label=f"Overall φ = {phi:.3f}")
    
    # Annotate bars with sample size to make the denominator explicit.
    for i, (frac, count) in enumerate(zip(fractions, total_counts)):
        ax.text(i, frac, f"n={count}", ha="center", va="bottom", fontsize=11)
    
    ax.set_xticks(x)
    ax.set_xticklabels([wd[:3] for wd in weekday_order], fontsize=12)
    ax.set_ylabel(f"WD fraction (rr ≥ {threshold} mm)", fontsize=13)
    ax.set_title(station_id, fontsize=16, fontweight="bold")
    ax.legend()
    
    plt.tight_layout()

    if outpath is not None:
        outfile = outpath / f"weekly_{station_id}.pdf"
        plt.savefig(outfile)
        print(f"Plot saved in {outfile}")

    if show:
        plt.show()
    else:
        plt.close()
    
    # Return test results
    return {
        "weekdays": weekday_order,
        "wet_counts": wet_counts,
        "total_counts": total_counts,
        "fractions": fractions,
        "phi": phi,
        "p_values": p_values,
        "significant": significant
    }
