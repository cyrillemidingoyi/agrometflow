from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Optional, Sequence, Union

import numpy as np
import pandas as pd
import yaml


DAILY_BOUNDED_VARS = {"rr", "sd", "fs", "sc", "sw"}


def _as_dataframe(data: Union[str, Path, pd.DataFrame]) -> pd.DataFrame:
	if isinstance(data, pd.DataFrame):
		return data.copy()
	return pd.read_csv(data)


def _ensure_columns(df: pd.DataFrame, required: Sequence[str], context: str) -> None:
	missing = [c for c in required if c not in df.columns]
	if missing:
		raise ValueError(f"{context}: missing required columns {missing}")


def _build_datetime(
	df: pd.DataFrame,
	year_col: str,
	month_col: str,
	day_col: str,
	hour_col: Optional[str] = None,
	minute_col: Optional[str] = None,
) -> pd.Series:
	work = pd.DataFrame(
		{
			"year": pd.to_numeric(df[year_col], errors="coerce"),
			"month": pd.to_numeric(df[month_col], errors="coerce"),
			"day": pd.to_numeric(df[day_col], errors="coerce"),
		}
	)
	if hour_col is not None:
		work["hour"] = pd.to_numeric(df[hour_col], errors="coerce").fillna(0)
	if minute_col is not None:
		work["minute"] = pd.to_numeric(df[minute_col], errors="coerce").fillna(0)
	return pd.to_datetime(work, errors="coerce")


def _coerce_numeric(s: pd.Series) -> pd.Series:
	return pd.to_numeric(s, errors="coerce")


def check_units(values: pd.Series, var_code: str, units: str) -> pd.Series:
	"""Convert values to canonical units used by QC tests.

	Canonical units:
	- Temperature variables: C
	- Precipitation rr/sw/rrls: mm
	- Snow sd/fs: cm
	- Wind w: m/s
	"""
	x = _coerce_numeric(values.copy())
	v = str(var_code)
	u = str(units)

	temp_vars = {
		"ta",
		"tb",
		"td",
		"t_air",
		"t_wet",
		"t_dew",
		"Tx",
		"Tn",
		"dep_dew",
		"ibt",
		"atb",
		"Txs",
		"TGs",
		"Tns",
		"TGn",
		"t_snow",
		"Ts",
		"t_water",
	}
	if v in temp_vars:
		if u in {"K", "k"}:
			return (x - 273.15).round(1)
		if u in {"F", "f"}:
			return ((x - 32.0) * 5.0 / 9.0).round(1)
		if u in {"R", "r"}:
			return (x * 1.25).round(1)
		if u in {"C", "c"}:
			return x
		raise ValueError(f"Unknown units for {v}: {u}")

	if v in {"rr", "sw", "rrls"}:
		if u in {"in", '"'}:
			return (x * 25.4).round(1)
		if u == "mm":
			return x
		raise ValueError(f"Unknown units for {v}: {u}")

	if v in {"sd", "fs"}:
		if u in {"in", '"'}:
			return (x * 2.54).round(1)
		if u == "mm":
			return (x / 10.0).round(1)
		if u == "m":
			return x * 100.0
		if u == "ft":
			return (x * 30.48).round(1)
		if u == "cm":
			return x
		raise ValueError(f"Unknown units for {v}: {u}")

	if v == "w":
		if u in {"km/h", "kph"}:
			return (x / 3.6).round(1)
		if u == "mph":
			return (x / 2.2369).round(1)
		if u in {"kn", "kt"}:
			return (x / 1.9438).round(1)
		if u in {"m/s", "mps"}:
			return x
		raise ValueError(f"Unknown units for {v}: {u}")

	return x


def _append_or_write_flags(path: Path, out: pd.DataFrame, key_cols: Sequence[str]) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)
	if path.exists():
		prev = pd.read_csv(path, sep="\t")
		merged = prev.merge(out, on=list(key_cols), how="outer", suffixes=("_x", "_y"))
		test_x = merged["Test_x"].astype("string")
		test_y = merged["Test_y"].astype("string")

		def combine_flags(a: Optional[str], b: Optional[str]) -> Optional[str]:
			if pd.isna(a):
				return None if pd.isna(b) else str(b)
			if pd.isna(b):
				return str(a)
			flags = [x.strip() for x in str(a).split(";") if x.strip()]
			if str(b) not in flags:
				flags.append(str(b))
			return ";".join(flags)

		merged["Test"] = [combine_flags(a, b) for a, b in zip(test_x, test_y)]
		cols = list(key_cols) + ["Test"]
		merged = merged[cols]
		merged.to_csv(path, sep="\t", index=False)
	else:
		out.to_csv(path, sep="\t", index=False)


def _daily_output_path(outpath: Union[str, Path], station_id: str, var_name: str) -> Path:
	return Path(outpath) / f"qc_{station_id}_{var_name}_daily.txt"


def _subdaily_output_path(outpath: Union[str, Path], station_id: str, var_name: str) -> Path:
	return Path(outpath) / f"qc_{station_id}_{var_name}_subdaily.txt"


def climatic_outliers(
	data: Union[str, Path, pd.DataFrame],
	var_name: str,
	station_id: str = "station",
	units: Optional[str] = None,
	iqr: Optional[float] = None,
	bplot: bool = False,
	outfile: Optional[Union[str, Path]] = None,
	outpath: Optional[Union[str, Path]] = None,
	year_col: str = "Year",
	month_col: str = "Month",
	day_col: str = "Day",
) -> pd.DataFrame:
	"""Flag monthly climatic outliers using Tukey whiskers by month.

	Expected input: daily CSV/DataFrame with one variable per column.
	If bplot=True, save a monthly boxplot to PDF.
	"""
	df = _as_dataframe(data)
	_ensure_columns(df, [year_col, month_col, day_col, var_name], "climatic_outliers")

	outrange = iqr
	if outrange is None:
		if var_name == "rr":
			outrange = 5
		elif var_name in {"Tx", "Tn", "ta"}:
			outrange = 3
		else:
			outrange = 4

	work = df[[year_col, month_col, day_col, var_name]].copy()
	work[var_name] = _coerce_numeric(work[var_name])
	if units:
		work[var_name] = check_units(work[var_name], var_name, units)

	non_na = work[var_name].notna().sum()
	if non_na <= 5 * 365:
		return pd.DataFrame(columns=[var_name, year_col, month_col, day_col, "Value", "Test"])

	if var_name in DAILY_BOUNDED_VARS:
		work = work[work[var_name] != 0]

	if bplot:
		import matplotlib
		matplotlib.use("Agg")
		import matplotlib.pyplot as plt

		if outfile is None:
			if outpath is not None:
				plot_path = Path(outpath) / f"climatic_outliers_boxplot_{station_id}_{var_name}.pdf"
			else:
				plot_path = Path(f"climatic_outliers_boxplot_{station_id}_{var_name}.pdf")
		else:
			plot_path = Path(outfile)

		plot_path.parent.mkdir(parents=True, exist_ok=True)
		fig, ax = plt.subplots(figsize=(10, 4))
		month_series = _coerce_numeric(work[month_col])
		month_data: list[np.ndarray] = []
		labels: list[str] = []
		for m in range(1, 13):
			vals = work.loc[month_series == m, var_name].dropna().to_numpy()
			if vals.size > 0:
				month_data.append(vals)
				labels.append(str(m))
		if month_data:
			ax.boxplot(month_data, tick_labels=labels, whis=outrange)
		ax.set_title(var_name)
		ax.set_xlabel("Months")
		ax.set_ylabel(units if units else "Value")
		fig.tight_layout()
		fig.savefig(plot_path, format="pdf")
		plt.close(fig)

	def month_bounds(s: pd.Series) -> tuple[float, float]:
		q1 = s.quantile(0.25)
		q3 = s.quantile(0.75)
		i = q3 - q1
		return q1 - outrange * i, q3 + outrange * i

	bounds = work.groupby(month_col)[var_name].apply(month_bounds)
	bounds = bounds.rename("bounds").reset_index()
	bounds[["lower", "upper"]] = pd.DataFrame(bounds["bounds"].tolist(), index=bounds.index)
	merged = work.merge(bounds[[month_col, "lower", "upper"]], on=month_col, how="left")
	out = merged[(merged[var_name] < merged["lower"]) | (merged[var_name] > merged["upper"])].copy()

	if out.empty:
		return pd.DataFrame(columns=["Var", "Year", "Month", "Day", "Value", "Test"])

	out = out.rename(
		columns={year_col: "Year", month_col: "Month", day_col: "Day", var_name: "Value"}
	)[["Year", "Month", "Day", "Value"]]
	out.insert(0, "Var", var_name)
	out["Test"] = "climatic_outliers"

	if outpath:
		_append_or_write_flags(
			_daily_output_path(outpath, station_id, var_name),
			out,
			["Var", "Year", "Month", "Day", "Value"],
		)
	return out


def internal_consistency(
	data: Union[str, Path, pd.DataFrame],
	var_x: str,
	var_y: str,
	station_id: str = "station",
	units_x: Optional[str] = None,
	units_y: Optional[str] = None,
	outpath: Optional[Union[str, Path]] = None,
	year_col: str = "Year",
	month_col: str = "Month",
	day_col: str = "Day",
) -> pd.DataFrame:
	"""Apply pairwise daily internal consistency checks.

	Supported pairs: (Tx,Tn), (w,dd), (sc,sd), (fs,sd), (fs,Tn), (sd,Tn).
	Returns flagged rows for both variables.
	"""
	df = _as_dataframe(data)
	_ensure_columns(df, [year_col, month_col, day_col, var_x, var_y], "internal_consistency")

	pair = frozenset({var_x, var_y})
	supported = {
		frozenset({"Tx", "Tn"}),
		frozenset({"w", "dd"}),
		frozenset({"sc", "sd"}),
		frozenset({"fs", "sd"}),
		frozenset({"fs", "Tn"}),
		frozenset({"sd", "Tn"}),
	}
	if pair not in supported:
		raise ValueError("The variables provided are incompatible with this test")

	work = df[[year_col, month_col, day_col, var_x, var_y]].copy()
	work[var_x] = _coerce_numeric(work[var_x])
	work[var_y] = _coerce_numeric(work[var_y])
	if units_x:
		work[var_x] = check_units(work[var_x], var_x, units_x)
	if units_y:
		work[var_y] = check_units(work[var_y], var_y, units_y)

	dt = _build_datetime(work, year_col, month_col, day_col)
	work = work.assign(_dt=dt).sort_values("_dt").reset_index(drop=True)
	flagged = pd.Series(False, index=work.index)

	if pair == frozenset({"Tx", "Tn"}):
		tx = "Tx"
		tn = "Tn"
		flagged = work[tx] < work[tn]

	elif pair == frozenset({"w", "dd"}):
		w = "w"
		dd = "dd"
		flagged = (work[w] == 0) & work[dd].notna()

	elif pair == frozenset({"sc", "sd"}):
		sc = "sc"
		sd = "sd"
		flagged = (work[sc] == 0) & (work[sd] > 0)

	elif pair == frozenset({"fs", "sd"}):
		fs = "fs"
		sd = "sd"
		diffsd = work[sd].diff()
		diffdate = work["_dt"].diff().dt.days
		flagged = (diffsd > 0) & (work[fs] == 0) & (diffdate == 1)

	elif pair == frozenset({"fs", "Tn"}):
		fs = "fs"
		tn = "Tn"
		flagged = (work[fs] > 0) & (work[tn] > 3)

	elif pair == frozenset({"sd", "Tn"}):
		sd = "sd"
		tn = "Tn"
		diffsd = work[sd].diff()
		diffdate = work["_dt"].diff().dt.days
		flagged = (diffsd > 0) & (work[tn] > 2.5) & (diffdate == 1)

	out = work.loc[flagged, [year_col, month_col, day_col, var_x, var_y]].copy()
	if out.empty:
		return pd.DataFrame(columns=["Var", "Year", "Month", "Day", "Value", "Test"])

	out_x = out.rename(
		columns={year_col: "Year", month_col: "Month", day_col: "Day", var_x: "Value"}
	)[["Year", "Month", "Day", "Value"]]
	out_x.insert(0, "Var", var_x)
	out_x["Test"] = "internal_consistency"

	out_y = out.rename(
		columns={year_col: "Year", month_col: "Month", day_col: "Day", var_y: "Value"}
	)[["Year", "Month", "Day", "Value"]]
	out_y.insert(0, "Var", var_y)
	out_y["Test"] = "internal_consistency"

	out_all = pd.concat([out_x, out_y], ignore_index=True)
	if outpath:
		_append_or_write_flags(
			_daily_output_path(outpath, station_id, var_x),
			out_x,
			["Var", "Year", "Month", "Day", "Value"],
		)
		_append_or_write_flags(
			_daily_output_path(outpath, station_id, var_y),
			out_y,
			["Var", "Year", "Month", "Day", "Value"],
		)
	return out_all


def temporal_coherence(
	data: Union[str, Path, pd.DataFrame],
	var_name: str,
	station_id: str = "station",
	units: Optional[str] = None,
	outpath: Optional[Union[str, Path]] = None,
	year_col: str = "Year",
	month_col: str = "Month",
	day_col: str = "Day",
	temp_jumps: float = 20.0,
	windspeed_jumps: float = 15.0,
	snowdepth_jumps: float = 50.0,
) -> pd.DataFrame:
	df = _as_dataframe(data)
	_ensure_columns(df, [year_col, month_col, day_col, var_name], "temporal_coherence")
	if var_name not in {"Tx", "Tn", "w", "sd"}:
		raise ValueError("Variable not supported by this test")

	jumps = temp_jumps if var_name in {"Tx", "Tn"} else windspeed_jumps if var_name == "w" else snowdepth_jumps

	work = df[[year_col, month_col, day_col, var_name]].copy()
	work[var_name] = _coerce_numeric(work[var_name])
	if units:
		work[var_name] = check_units(work[var_name], var_name, units)

	dt = _build_datetime(work, year_col, month_col, day_col)
	work = work.assign(_dt=dt).sort_values("_dt").reset_index(drop=True)
	diff_val = work[var_name].diff()
	diff_day = work["_dt"].diff().dt.days
	flags = ((diff_val.abs() > jumps) & (diff_day == 1)).fillna(False)
	selected = flags | flags.shift(-1, fill_value=False)

	out = work.loc[selected, [year_col, month_col, day_col, var_name]].drop_duplicates().copy()
	if out.empty:
		return pd.DataFrame(columns=["Var", "Year", "Month", "Day", "Value", "Test"])

	out = out.rename(
		columns={year_col: "Year", month_col: "Month", day_col: "Day", var_name: "Value"}
	)[["Year", "Month", "Day", "Value"]]
	out.insert(0, "Var", var_name)
	out["Test"] = "temporal_coherence"

	if outpath:
		_append_or_write_flags(
			_daily_output_path(outpath, station_id, var_name),
			out,
			["Var", "Year", "Month", "Day", "Value"],
		)
	return out


def _run_lengths(series: pd.Series) -> tuple[np.ndarray, np.ndarray]:
	vals = series.to_numpy()
	if len(vals) == 0:
		return np.array([], dtype=int), np.array([], dtype=int)
	change = np.empty(len(vals), dtype=bool)
	change[0] = True
	change[1:] = vals[1:] != vals[:-1]
	starts = np.flatnonzero(change)
	lengths = np.diff(np.append(starts, len(vals)))
	return starts, lengths


def daily_repetition(
	data: Union[str, Path, pd.DataFrame],
	var_name: str,
	station_id: str = "station",
	units: Optional[str] = None,
	outpath: Optional[Union[str, Path]] = None,
	year_col: str = "Year",
	month_col: str = "Month",
	day_col: str = "Day",
	n: int = 4,
) -> pd.DataFrame:
	df = _as_dataframe(data)
	_ensure_columns(df, [year_col, month_col, day_col, var_name], "daily_repetition")

	work = df[[year_col, month_col, day_col, var_name]].copy()
	work[var_name] = _coerce_numeric(work[var_name])
	if units:
		work[var_name] = check_units(work[var_name], var_name, units)
	work = work.sort_values([year_col, month_col, day_col]).reset_index(drop=True)

	starts, lengths = _run_lengths(work[var_name])
	indices: list[int] = []
	for start, length in zip(starts, lengths):
		if length >= n:
			indices.extend(range(start, start + length))

	if not indices:
		return pd.DataFrame(columns=["Var", "Year", "Month", "Day", "Value", "Test"])

	out = work.loc[indices, [year_col, month_col, day_col, var_name]].copy()
	if var_name in DAILY_BOUNDED_VARS:
		out = out[out[var_name] != 0]
	if out.empty:
		return pd.DataFrame(columns=["Var", "Year", "Month", "Day", "Value", "Test"])

	out = out.rename(
		columns={year_col: "Year", month_col: "Month", day_col: "Day", var_name: "Value"}
	)[["Year", "Month", "Day", "Value"]]
	out.insert(0, "Var", var_name)
	out["Test"] = "daily_repetition"

	if outpath:
		_append_or_write_flags(
			_daily_output_path(outpath, station_id, var_name),
			out,
			["Var", "Year", "Month", "Day", "Value"],
		)
	return out


def duplicate_dates(
	data: Union[str, Path, pd.DataFrame],
	var_name: str,
	station_id: str = "station",
	units: Optional[str] = None,
	outpath: Optional[Union[str, Path]] = None,
	year_col: str = "Year",
	month_col: str = "Month",
	day_col: str = "Day",
) -> pd.DataFrame:
	df = _as_dataframe(data)
	_ensure_columns(df, [year_col, month_col, day_col, var_name], "duplicate_dates")

	work = df[[year_col, month_col, day_col, var_name]].copy()
	work[var_name] = _coerce_numeric(work[var_name])
	if units:
		work[var_name] = check_units(work[var_name], var_name, units)

	dup = work.duplicated(subset=[year_col, month_col, day_col], keep=False)
	out = work.loc[dup, [year_col, month_col, day_col, var_name]].copy()
	if out.empty:
		return pd.DataFrame(columns=["Var", "Year", "Month", "Day", "Value", "Test"])

	out = out.rename(
		columns={year_col: "Year", month_col: "Month", day_col: "Day", var_name: "Value"}
	)[["Year", "Month", "Day", "Value"]]
	out.insert(0, "Var", var_name)
	out["Test"] = "duplicate_dates"

	if outpath:
		_append_or_write_flags(
			_daily_output_path(outpath, station_id, var_name),
			out,
			["Var", "Year", "Month", "Day", "Value"],
		)
	return out


def daily_out_of_range(
	data: Union[str, Path, pd.DataFrame],
	var_name: str,
	station_id: str = "station",
	units: Optional[str] = None,
	outpath: Optional[Union[str, Path]] = None,
	year_col: str = "Year",
	month_col: str = "Month",
	day_col: str = "Day",
	tmax_upper: float = 45,
	tmax_lower: float = -30,
	tmin_upper: float = 30,
	tmin_lower: float = -40,
	rr_upper: float = 200,
	rr_lower: float = 0,
	w_upper: float = 30,
	w_lower: float = 0,
	dd_upper: float = 360,
	dd_lower: float = 0,
	sc_upper: float = 100,
	sc_lower: float = 0,
	sd_upper: float = 200,
	sd_lower: float = 0,
	fs_upper: float = 100,
	fs_lower: float = 0,
) -> pd.DataFrame:
	df = _as_dataframe(data)
	_ensure_columns(df, [year_col, month_col, day_col, var_name], "daily_out_of_range")

	thresholds = {
		"Tx": (tmax_lower, tmax_upper),
		"Tn": (tmin_lower, tmin_upper),
		"rr": (rr_lower, rr_upper),
		"w": (w_lower, w_upper),
		"dd": (dd_lower, dd_upper),
		"sc": (sc_lower, sc_upper),
		"sd": (sd_lower, sd_upper),
		"fs": (fs_lower, fs_upper),
	}
	if var_name not in thresholds:
		raise ValueError("Variable not supported by this test")
	lower, upper = thresholds[var_name]

	work = df[[year_col, month_col, day_col, var_name]].copy()
	work[var_name] = _coerce_numeric(work[var_name])
	if units:
		work[var_name] = check_units(work[var_name], var_name, units)

	out = work[(work[var_name] > upper) | (work[var_name] < lower)].copy()
	if out.empty:
		return pd.DataFrame(columns=["Var", "Year", "Month", "Day", "Value", "Test"])

	out = out.rename(
		columns={year_col: "Year", month_col: "Month", day_col: "Day", var_name: "Value"}
	)[["Year", "Month", "Day", "Value"]]
	out.insert(0, "Var", var_name)
	out["Test"] = "daily_out_of_range"

	if outpath:
		_append_or_write_flags(
			_daily_output_path(outpath, station_id, var_name),
			out,
			["Var", "Year", "Month", "Day", "Value"],
		)
	return out


def subdaily_out_of_range(
	data: Union[str, Path, pd.DataFrame],
	var_name: str,
	station_id: str = "station",
	units: Optional[str] = None,
	outpath: Optional[Union[str, Path]] = None,
	year_col: str = "Year",
	month_col: str = "Month",
	day_col: str = "Day",
	hour_col: str = "Hour",
	minute_col: str = "Minute",
	time_offset: float = 0,
	ta_day_upper: float = 45,
	ta_day_lower: float = -35,
	ta_night_upper: float = 40,
	ta_night_lower: float = -40,
	rr_upper: float = 100,
	rr_lower: float = 0,
	w_upper: float = 50,
	w_lower: float = 0,
	dd_upper: float = 360,
	dd_lower: float = 0,
	sc_upper: float = 100,
	sc_lower: float = 0,
	sd_upper: float = 200,
	sd_lower: float = 0,
	fs_upper: float = 100,
	fs_lower: float = 0,
) -> pd.DataFrame:
	df = _as_dataframe(data)
	_ensure_columns(
		df,
		[year_col, month_col, day_col, hour_col, minute_col, var_name],
		"subdaily_out_of_range",
	)
	if var_name not in {"ta", "rr", "w", "dd", "sc", "sd", "fs"}:
		raise ValueError("Variable not supported by this test")

	work = df[[year_col, month_col, day_col, hour_col, minute_col, var_name]].copy()
	work[var_name] = _coerce_numeric(work[var_name])
	if units:
		work[var_name] = check_units(work[var_name], var_name, units)

	if var_name == "ta":
		dt = _build_datetime(work, year_col, month_col, day_col, hour_col, minute_col)
		local_dt = dt + pd.to_timedelta(time_offset, unit="h")
		local_hour = local_dt.dt.hour
		day_mask = (local_hour >= 8) & (local_hour <= 19)
		flags = (day_mask & ((work[var_name] < ta_day_lower) | (work[var_name] > ta_day_upper))) | (
			(~day_mask) & ((work[var_name] < ta_night_lower) | (work[var_name] > ta_night_upper))
		)
		out = work.loc[flags].copy()
	else:
		thresholds = {
			"rr": (rr_lower, rr_upper),
			"w": (w_lower, w_upper),
			"dd": (dd_lower, dd_upper),
			"sc": (sc_lower, sc_upper),
			"sd": (sd_lower, sd_upper),
			"fs": (fs_lower, fs_upper),
		}
		lower, upper = thresholds[var_name]
		out = work[(work[var_name] > upper) | (work[var_name] < lower)].copy()

	if out.empty:
		return pd.DataFrame(columns=["Var", "Year", "Month", "Day", "Hour", "Minute", "Value", "Test"])

	out = out.rename(
		columns={
			year_col: "Year",
			month_col: "Month",
			day_col: "Day",
			hour_col: "Hour",
			minute_col: "Minute",
			var_name: "Value",
		}
	)[["Year", "Month", "Day", "Hour", "Minute", "Value"]]
	out.insert(0, "Var", var_name)
	out["Test"] = "subdaily_out_of_range"

	if outpath:
		_append_or_write_flags(
			_subdaily_output_path(outpath, station_id, var_name),
			out,
			["Var", "Year", "Month", "Day", "Hour", "Minute", "Value"],
		)
	return out


def subdaily_repetition(
	data: Union[str, Path, pd.DataFrame],
	var_name: str,
	station_id: str = "station",
	units: Optional[str] = None,
	outpath: Optional[Union[str, Path]] = None,
	year_col: str = "Year",
	month_col: str = "Month",
	day_col: str = "Day",
	hour_col: str = "Hour",
	minute_col: str = "Minute",
	n: int = 6,
) -> pd.DataFrame:
	df = _as_dataframe(data)
	_ensure_columns(
		df,
		[year_col, month_col, day_col, hour_col, minute_col, var_name],
		"subdaily_repetition",
	)

	work = df[[year_col, month_col, day_col, hour_col, minute_col, var_name]].copy()
	work[var_name] = _coerce_numeric(work[var_name])
	if units:
		work[var_name] = check_units(work[var_name], var_name, units)

	work = work.sort_values([year_col, month_col, day_col, hour_col, minute_col]).reset_index(drop=True)
	starts, lengths = _run_lengths(work[var_name])
	indices: list[int] = []
	for start, length in zip(starts, lengths):
		if length >= n:
			indices.extend(range(start, start + length))

	if not indices:
		return pd.DataFrame(columns=["Var", "Year", "Month", "Day", "Hour", "Minute", "Value", "Test"])

	out = work.loc[indices, [year_col, month_col, day_col, hour_col, minute_col, var_name]].copy()
	if var_name in DAILY_BOUNDED_VARS:
		out = out[out[var_name] != 0]
	if out.empty:
		return pd.DataFrame(columns=["Var", "Year", "Month", "Day", "Hour", "Minute", "Value", "Test"])

	out = out.rename(
		columns={
			year_col: "Year",
			month_col: "Month",
			day_col: "Day",
			hour_col: "Hour",
			minute_col: "Minute",
			var_name: "Value",
		}
	)[["Year", "Month", "Day", "Hour", "Minute", "Value"]]
	out.insert(0, "Var", var_name)
	out["Test"] = "subdaily_repetition"

	if outpath:
		_append_or_write_flags(
			_subdaily_output_path(outpath, station_id, var_name),
			out,
			["Var", "Year", "Month", "Day", "Hour", "Minute", "Value"],
		)
	return out


def duplicate_times(
	data: Union[str, Path, pd.DataFrame],
	var_name: str,
	station_id: str = "station",
	units: Optional[str] = None,
	outpath: Optional[Union[str, Path]] = None,
	year_col: str = "Year",
	month_col: str = "Month",
	day_col: str = "Day",
	hour_col: str = "Hour",
	minute_col: str = "Minute",
) -> pd.DataFrame:
	df = _as_dataframe(data)
	_ensure_columns(
		df,
		[year_col, month_col, day_col, hour_col, minute_col, var_name],
		"duplicate_times",
	)

	work = df[[year_col, month_col, day_col, hour_col, minute_col, var_name]].copy()
	work[var_name] = _coerce_numeric(work[var_name])
	if units:
		work[var_name] = check_units(work[var_name], var_name, units)
	work = work.sort_values([year_col, month_col, day_col, hour_col, minute_col]).reset_index(drop=True)

	dup = work.duplicated(subset=[year_col, month_col, day_col, hour_col, minute_col], keep=False)
	out = work.loc[dup, [year_col, month_col, day_col, hour_col, minute_col, var_name]].copy()
	if out.empty:
		return pd.DataFrame(columns=["Var", "Year", "Month", "Day", "Hour", "Minute", "Value", "Test"])

	out = out.rename(
		columns={
			year_col: "Year",
			month_col: "Month",
			day_col: "Day",
			hour_col: "Hour",
			minute_col: "Minute",
			var_name: "Value",
		}
	)[["Year", "Month", "Day", "Hour", "Minute", "Value"]]
	out.insert(0, "Var", var_name)
	out["Test"] = "duplicate_times"

	if outpath:
		_append_or_write_flags(
			_subdaily_output_path(outpath, station_id, var_name),
			out,
			["Var", "Year", "Month", "Day", "Hour", "Minute", "Value"],
		)
	return out


def run_qc_pipeline(
	data: Union[str, Path, pd.DataFrame],
	station_id: str = "station",
	units_map: Optional[dict[str, str]] = None,
	frequency: str = "auto",
	variable_cols: Optional[Iterable[str]] = None,
	outpath: Optional[Union[str, Path]] = None,
	year_col: str = "Year",
	month_col: str = "Month",
	day_col: str = "Day",
	hour_col: str = "Hour",
	minute_col: str = "Minute",
	climatic_outliers_params: Optional[dict] = None,
	daily_out_of_range_params: Optional[dict] = None,
	temporal_coherence_params: Optional[dict] = None,
	daily_repetition_params: Optional[dict] = None,
	subdaily_out_of_range_params: Optional[dict] = None,
	subdaily_repetition_params: Optional[dict] = None,
) -> dict[str, Union[pd.DataFrame, dict[str, pd.DataFrame]]]:
	"""Run all applicable QC checks for a station dataset.

	Inputs are CSV/DataFrame with variables in columns.
	Returns:
	- results_by_test: dict[test_name -> flagged rows DataFrame]
	- all_flags: concatenated flags
	- summary: counts by test and variable
	"""
	df = _as_dataframe(data)
	_ensure_columns(df, [year_col, month_col, day_col], "run_qc_pipeline")

	if frequency not in {"auto", "daily", "subdaily"}:
		raise ValueError("frequency must be one of: auto, daily, subdaily")

	if frequency == "auto":
		is_subdaily = hour_col in df.columns and minute_col in df.columns
	elif frequency == "subdaily":
		is_subdaily = True
	else:
		is_subdaily = False

	if is_subdaily:
		_ensure_columns(df, [hour_col, minute_col], "run_qc_pipeline")

	excluded = {year_col, month_col, day_col, hour_col, minute_col}
	if variable_cols is None:
		variable_list = [c for c in df.columns if c not in excluded]
	else:
		variable_list = list(variable_cols)
		_ensure_columns(df, variable_list, "run_qc_pipeline")

	units_map = units_map or {}
	climatic_outliers_params = climatic_outliers_params or {}
	daily_out_of_range_params = daily_out_of_range_params or {}
	temporal_coherence_params = temporal_coherence_params or {}
	daily_repetition_params = daily_repetition_params or {}
	subdaily_out_of_range_params = subdaily_out_of_range_params or {}
	subdaily_repetition_params = subdaily_repetition_params or {}
	results: dict[str, list[pd.DataFrame]] = {}

	def _add_result(test_name: str, out: pd.DataFrame) -> None:
		if out is None or out.empty:
			return
		results.setdefault(test_name, []).append(out)

	if is_subdaily:
		for var in variable_list:
			units = units_map.get(var)
			try:
				_add_result(
					"subdaily_out_of_range",
					subdaily_out_of_range(
						df,
						var_name=var,
						station_id=station_id,
						units=units,
						outpath=outpath,
						year_col=year_col,
						month_col=month_col,
						day_col=day_col,
						hour_col=hour_col,
						minute_col=minute_col,
						**subdaily_out_of_range_params,
					),
				)
			except ValueError:
				pass

			_add_result(
				"subdaily_repetition",
				subdaily_repetition(
					df,
					var_name=var,
					station_id=station_id,
					units=units,
					outpath=outpath,
					year_col=year_col,
					month_col=month_col,
					day_col=day_col,
					hour_col=hour_col,
					minute_col=minute_col,
					**subdaily_repetition_params,
				),
			)

			_add_result(
				"duplicate_times",
				duplicate_times(
					df,
					var_name=var,
					station_id=station_id,
					units=units,
					outpath=outpath,
					year_col=year_col,
					month_col=month_col,
					day_col=day_col,
					hour_col=hour_col,
					minute_col=minute_col,
				),
			)

	else:
		for var in variable_list:
			units = units_map.get(var)

			_add_result(
				"climatic_outliers",
				climatic_outliers(
					df,
					var_name=var,
					station_id=station_id,
					units=units,
					outpath=outpath,
					year_col=year_col,
					month_col=month_col,
					day_col=day_col,
					**climatic_outliers_params,
				),
			)

			try:
				_add_result(
					"daily_out_of_range",
					daily_out_of_range(
						df,
						var_name=var,
						station_id=station_id,
						units=units,
						outpath=outpath,
						year_col=year_col,
						month_col=month_col,
						day_col=day_col,
						**daily_out_of_range_params,
					),
				)
			except ValueError:
				pass

			try:
				_add_result(
					"temporal_coherence",
					temporal_coherence(
						df,
						var_name=var,
						station_id=station_id,
						units=units,
						outpath=outpath,
						year_col=year_col,
						month_col=month_col,
						day_col=day_col,
						**temporal_coherence_params,
					),
				)
			except ValueError:
				pass

			_add_result(
				"daily_repetition",
				daily_repetition(
					df,
					var_name=var,
					station_id=station_id,
					units=units,
					outpath=outpath,
					year_col=year_col,
					month_col=month_col,
					day_col=day_col,
					**daily_repetition_params,
				),
			)

			_add_result(
				"duplicate_dates",
				duplicate_dates(
					df,
					var_name=var,
					station_id=station_id,
					units=units,
					outpath=outpath,
					year_col=year_col,
					month_col=month_col,
					day_col=day_col,
				),
			)

		consistency_pairs = [
			("Tx", "Tn"),
			("w", "dd"),
			("sc", "sd"),
			("fs", "sd"),
			("fs", "Tn"),
			("sd", "Tn"),
		]
		for var_x, var_y in consistency_pairs:
			if var_x in variable_list and var_y in variable_list:
				_add_result(
					"internal_consistency",
					internal_consistency(
						df,
						var_x=var_x,
						var_y=var_y,
						station_id=station_id,
						units_x=units_map.get(var_x),
						units_y=units_map.get(var_y),
						outpath=outpath,
						year_col=year_col,
						month_col=month_col,
						day_col=day_col,
					),
				)

	results_by_test: dict[str, pd.DataFrame] = {}
	for test_name, out_list in results.items():
		if not out_list:
			continue
		results_by_test[test_name] = pd.concat(out_list, ignore_index=True).drop_duplicates()

	if results_by_test:
		all_flags = pd.concat(results_by_test.values(), ignore_index=True)
		summary = (
			all_flags.groupby(["Test", "Var"], dropna=False)
			.size()
			.rename("n_flags")
			.reset_index()
			.sort_values(["Test", "Var"])
			.reset_index(drop=True)
		)
	else:
		all_flags = pd.DataFrame()
		summary = pd.DataFrame(columns=["Test", "Var", "n_flags"])

	return {
		"results_by_test": results_by_test,
		"all_flags": all_flags,
		"summary": summary,
	}


def run_qc_pipeline_from_config(
	config_path: Union[str, Path],
	data: Optional[Union[str, Path, pd.DataFrame]] = None,
) -> dict[str, Union[pd.DataFrame, dict[str, pd.DataFrame]]]:
	"""Run QC pipeline from YAML/JSON config.

	Supported config keys:
	- data or input_csv: path to CSV (ignored if `data` arg is provided)
	- station_id: station id used in output filenames
	- units_map: mapping {var: units}
	- frequency: auto|daily|subdaily
	- variable_cols: list of variable column names
	- outpath: folder for intermediate QC files
	- columns: mapping for date/time column names
	  : year, month, day, hour, minute
	- tests: optional parameter dictionaries per test
	  : climatic_outliers, daily_out_of_range, temporal_coherence,
	    daily_repetition, subdaily_out_of_range, subdaily_repetition
	- outputs: optional output file paths
	  : summary_csv, flags_csv
	"""
	path = Path(config_path)
	if not path.exists():
		raise FileNotFoundError(f"Config file not found: {config_path}")

	with path.open("r", encoding="utf-8") as f:
		if path.suffix.lower() == ".json":
			cfg = json.load(f)
		else:
			cfg = yaml.safe_load(f)

	if not isinstance(cfg, dict):
		raise ValueError("Config must be a mapping/dictionary")

	columns = cfg.get("columns", {}) or {}
	tests = cfg.get("tests", {}) or {}

	data_input = data if data is not None else cfg.get("data", cfg.get("input_csv"))
	if data_input is None:
		raise ValueError("Config must provide 'data' (or 'input_csv') when no data argument is passed")

	result = run_qc_pipeline(
		data=data_input,
		station_id=cfg.get("station_id", "station"),
		units_map=cfg.get("units_map", {}),
		frequency=cfg.get("frequency", "auto"),
		variable_cols=cfg.get("variable_cols"),
		outpath=cfg.get("outpath"),
		year_col=columns.get("year", "Year"),
		month_col=columns.get("month", "Month"),
		day_col=columns.get("day", "Day"),
		hour_col=columns.get("hour", "Hour"),
		minute_col=columns.get("minute", "Minute"),
		climatic_outliers_params=tests.get("climatic_outliers", {}),
		daily_out_of_range_params=tests.get("daily_out_of_range", {}),
		temporal_coherence_params=tests.get("temporal_coherence", {}),
		daily_repetition_params=tests.get("daily_repetition", {}),
		subdaily_out_of_range_params=tests.get("subdaily_out_of_range", {}),
		subdaily_repetition_params=tests.get("subdaily_repetition", {}),
	)

	outputs = cfg.get("outputs", {}) or {}
	summary_csv = outputs.get("summary_csv")
	flags_csv = outputs.get("flags_csv")
	if summary_csv:
		summary_path = Path(summary_csv)
		summary_path.parent.mkdir(parents=True, exist_ok=True)
		result["summary"].to_csv(summary_path, index=False)
	if flags_csv:
		flags_path = Path(flags_csv)
		flags_path.parent.mkdir(parents=True, exist_ok=True)
		result["all_flags"].to_csv(flags_path, index=False)

	return result


__all__ = [
	"check_units",
	"climatic_outliers",
	"internal_consistency",
	"temporal_coherence",
	"daily_repetition",
	"duplicate_dates",
	"daily_out_of_range",
	"subdaily_out_of_range",
	"subdaily_repetition",
	"duplicate_times",
	"run_qc_pipeline",
	"run_qc_pipeline_from_config",
]
