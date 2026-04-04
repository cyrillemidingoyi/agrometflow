# Data Quality Control Plots

Python implementation of plotting functions from the R package [dataresqc](https://cran.r-project.org/package=dataresqc) for climate data quality control.

## Overview

The `agrometflow.dataquality.plots` module provides visualization tools to identify data quality issues in meteorological observations:

1. **plot_decimals** - Detect artificial reporting resolution patterns
2. **plot_daily** - Visualize daily time series with missing data indicators
3. **plot_subdaily** - Display sub-daily observations in monthly panels
4. **plot_weekly_cycle** - Test for systematic biases by day of week

## Installation

```python
from agrometflow.dataquality.plots import (
    plot_decimals,
    plot_daily,
    plot_subdaily,
    plot_weekly_cycle
)
```

## Functions

### 1. plot_decimals()

**Purpose:** Investigate reporting resolution by analyzing the distribution of decimal digits year by year.

**Use case:** Detect changes in measurement precision, rounding practices, or data transcription errors.

**Example:**
```python
import pandas as pd
from agrometflow.dataquality.plots import plot_decimals

# Load your data with Date and value columns
df = pd.read_csv("temperature_data.csv")
df['Date'] = pd.to_datetime(df['Date'])

plot_decimals(
    data=df,
    date_col='Date',
    value_col='Tmax',
    outfile='decimals_analysis.pdf',
    startyear=1950,
    endyear=2020
)
```

**Output:** Multi-page PDF with stacked bar charts showing the distribution of decimal digits (0-9) per year.

**Interpretation:**
- **Uniform distribution** → Good resolution (e.g., instrument measures to 0.1°C)
- **Dominance of .0 and .5** → Data rounded to 0.5°C
- **Only .0 present** → Integer values only
- **Sudden changes** → Equipment change or transcription method change

---

### 2. plot_daily()

**Purpose:** Create time series plots of daily data with visual indicators for missing observations.

**Use case:** Inspect data completeness and identify gaps, outliers, or suspicious patterns.

**Example:**
```python
from agrometflow.dataquality.plots import plot_daily

plot_daily(
    data=df,
    date_col='Date',
    value_col='Precipitation',
    var_name='Daily Precipitation',
    units='mm',
    outfile='daily_precip_plot.pdf',
    startyear=2010,
    endyear=2015,
    years_per_panel=2,
    show_missing=True,
    marker='o',
    markersize=3,
    color='blue',
    alpha=0.7
)
```

**Output:** PDF with time series panels. Missing dates are marked with red crosses at the bottom.

**Parameters:**
- `years_per_panel`: Number of years displayed per subplot (default: 1)
- `show_missing`: Mark dates with no observations (default: True)
- `**plot_kwargs`: Any matplotlib plot() parameters (marker, color, etc.)

---

### 3. plot_subdaily()

**Purpose:** Display sub-daily (hourly, 3-hourly, etc.) observations organized by month.

**Use case:** Inspect diurnal cycles, identify missing observation times, detect timing errors.

**Example:**
```python
from agrometflow.dataquality.plots import plot_subdaily

# Data must have datetime column
subdaily_data = pd.read_csv("hourly_temp.csv")
subdaily_data['datetime'] = pd.to_datetime(subdaily_data['datetime'])

plot_subdaily(
    data=subdaily_data,
    datetime_col='datetime',
    value_col='temperature',
    var_name='Air Temperature',
    units='°C',
    year=2020,  # Single year or list of years
    outfile='subdaily_temp.pdf',
    fixed_ylim=True,  # Same y-axis for all months
    marker='.',
    markersize=2,
    linestyle='',
    alpha=0.6
)
```

**Output:** PDF with 4x3 grid of monthly panels showing all observations within each month.

**Tip:** Set `fixed_ylim=False` to auto-scale each month independently.

---

### 4. plot_weekly_cycle()

**Purpose:** Test for systematic biases in daily precipitation by day of week using binomial tests.

**Use case:** Detect observer biases (e.g., weekend under-reporting), or validate data homogeneity.

**Example:**
```python
from agrometflow.dataquality.plots import plot_weekly_cycle

# Data must have Date and precipitation columns
precip_data = pd.read_csv("daily_precip.csv")
precip_data['Date'] = pd.to_datetime(precip_data['Date'])

results = plot_weekly_cycle(
    data=precip_data,
    date_col='Date',
    precip_col='precip_mm',
    station_id='Station_XYZ',
    outpath='qc_plots/',
    threshold=1.0,  # mm (wet day definition)
    confidence_level=0.95
)

print(f"Overall wet day probability: {results['phi']:.3f}")
print(f"Significant weekdays: {[d for d, s in zip(results['weekdays'], results['significant']) if s]}")
```

**Output:** 
- PDF bar chart showing wet day fraction per weekday
- Red bars indicate statistically significant deviations
- Dashed line shows overall wet day probability
- Numbers on bars show total observation count per weekday

**Interpretation:**
- **Red bars** → That weekday differs significantly from expected
- **Higher on weekends** → Possible accumulation/delayed reporting
- **Lower on Mondays** → Possible weekend under-observation

**Returns:** Dictionary with test statistics including:
- `phi`: Overall wet day probability
- `weekdays`: Day names
- `wet_counts`: Number of wet days per weekday
- `total_counts`: Total days per weekday
- `fractions`: Wet day fraction per weekday
- `significant`: Boolean per weekday (True if significant deviation)

---

## Complete Workflow Example

```python
import pandas as pd
from pathlib import Path
from agrometflow.dataquality.plots import (
    plot_decimals,
    plot_daily,
    plot_weekly_cycle
)

# 1. Load data
df = pd.read_csv("station_data.csv")
df['Date'] = pd.to_datetime(df['Date'])

# 2. Create output directory
output_dir = Path("qc_analysis")
output_dir.mkdir(exist_ok=True)

# 3. Analyze decimal patterns (for Tmax)
if 'Tmax' in df.columns:
    plot_decimals(
        data=df[['Date', 'Tmax']].rename(columns={'Tmax': 'value'}),
        date_col='Date',
        value_col='value',
        outfile=output_dir / 'decimals_Tmax.pdf'
    )

# 4. Plot time series
plot_daily(
    data=df[['Date', 'Tmax']].rename(columns={'Tmax': 'value'}),
    date_col='Date',
    value_col='value',
    var_name='Tmax',
    units='°C',
    outfile=output_dir / 'timeseries_Tmax.pdf',
    years_per_panel=5,
    marker='o',
    markersize=2
)

# 5. Test weekly cycle (for precipitation)
if 'Precip' in df.columns:
    weekly_results = plot_weekly_cycle(
        data=df[['Date', 'Precip']].rename(columns={'Precip': 'rr'}),
        date_col='Date',
        precip_col='rr',
        station_id='MyStation',
        outpath=output_dir,
        threshold=0.1
    )
    
    # Print summary
    if any(weekly_results['significant']):
        print("⚠️ Weekly cycle detected - check data quality")
    else:
        print("✓ No significant weekly cycle")
```

## References

**Original R package:**
- Brugnara, Y., Auchmann, R., Hunziker, S. (2020). dataresqc: QC tools for weather and climate data. R package version 1.1.1.
- CRAN: https://cran.r-project.org/package=dataresqc

**Scientific background:**
- Hunziker, S. et al. (2017). Identifying, attributing, and overcoming common data quality issues of manned station observations. *International Journal of Climatology*, 37: 4131-4145. https://doi.org/10.1002/joc.5037

- Hunziker, S. et al. (2018). Effects of undetected data quality issues on climatological analyses. *Climate of the Past*, 14: 1-20. https://doi.org/10.5194/cp-14-1-2018

## Dependencies

```
numpy
pandas
matplotlib
scipy (for plot_weekly_cycle binomial test)
```

## Notes

- All plotting functions save outputs as **PDF files** for publication quality
- Use `%matplotlib inline` in Jupyter notebooks, but plots are saved to files, not displayed inline
- Missing data handling: functions automatically skip NaN values
- Date formats: use `pd.to_datetime()` to ensure proper date parsing

## Demo Notebook

See `notebooks/demo_plots.ipynb` for interactive examples with the POWER dataset.
