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
	- Pressure p/mslp/pppp: hPa
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

	if v in {"p", "mslp", "pppp"}:
		if u in {"Pa", "pa"}:
			return (x / 100.0).round(1)
		if u in {"mm", "mmHg", "mmhg"}:
			return (x * 1013.25 / 760.0).round(1)
		if u in {"in", '"'}:
			return (x * 25.4 * 1013.25 / 760.0).round(1)
		if u in {"hPa", "hpa"}:
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


def _lookup_station_value(mapping: dict, key: object) -> Optional[object]:
	candidates: list[object] = [key, str(key)]
	key_num = pd.to_numeric(pd.Series([key]), errors="coerce").iloc[0]
	if not pd.isna(key_num):
		candidates.append(key_num.item() if hasattr(key_num, "item") else key_num)
		if float(key_num).is_integer():
			candidates.append(int(key_num))
			candidates.append(f"{int(key_num)}")
		candidates.append(float(key_num))
	for candidate in candidates:
		if candidate in mapping:
			return mapping[candidate]
	return None


def climatic_outliers(
	data: Union[str, Path, pd.DataFrame],
	var_name: str,
	station_id: Union[str, Sequence[object]] = "station",
	units: Optional[str] = None,
	iqr: Optional[float] = None,
	bplot: bool = False,
	show: bool = False,
	outfile: Optional[Union[str, Path]] = None,
	outpath: Optional[Union[str, Path]] = None,
	year_col: str = "Year",
	month_col: str = "Month",
	day_col: str = "Day",
	station_col: Optional[str] = None,
) -> pd.DataFrame:
	"""Flag monthly climatic outliers using Tukey whiskers by month.

	Expected input: daily CSV/DataFrame with one variable per column.
	If bplot=True, save a monthly boxplot to PDF.
	If show=True, display the boxplot inline (e.g. in notebooks).
	If ``station_col`` is provided, applies the test to every station in that column.
	Otherwise, if ``station_id`` names one or more stations and a ``station`` column
	exists in the data, the test is clipped to those station ids before computing bounds.
	"""
	df = _as_dataframe(data)

	def _as_station_ids(value: Union[str, Sequence[object]]) -> list[object]:
		if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
			return list(value)
		return [value]

	def _station_mask(series: pd.Series, sid: object) -> pd.Series:
		mask = series.astype("string") == str(sid)
		sid_num = pd.to_numeric(pd.Series([sid]), errors="coerce").iloc[0]
		if not pd.isna(sid_num):
			mask = mask | (pd.to_numeric(series, errors="coerce") == float(sid_num))
		return mask.fillna(False)

	selected_station_col: Optional[str] = None
	selected_station_ids: Optional[list[object]] = None
	if station_col is None and "station" in df.columns:
		candidate_station_ids = _as_station_ids(station_id)
		if not (len(candidate_station_ids) == 1 and str(candidate_station_ids[0]) == "station"):
			selected_station_col = "station"
			selected_station_ids = candidate_station_ids

	group_station_col = station_col if station_col is not None else selected_station_col
	required_cols = [year_col, month_col, day_col, var_name]
	if group_station_col is not None:
		required_cols.append(group_station_col)
	_ensure_columns(df, required_cols, "climatic_outliers")

	outrange = iqr
	if outrange is None:
		if var_name == "rr":
			outrange = 5
		elif var_name in {"Tx", "Tn", "ta"}:
			outrange = 3
		else:
			outrange = 4

	wrk_cols = [year_col, month_col, day_col, var_name]
	if group_station_col is not None:
		wrk_cols.insert(0, group_station_col)
	work = df[wrk_cols].copy()
	work[var_name] = _coerce_numeric(work[var_name])
	if units:
		work[var_name] = check_units(work[var_name], var_name, units)

	def month_bounds(s: pd.Series) -> tuple[float, float]:
		q1 = s.quantile(0.25)
		q3 = s.quantile(0.75)
		i = q3 - q1
		return q1 - outrange * i, q3 + outrange * i

	def _empty_result() -> pd.DataFrame:
		if group_station_col is not None:
			return pd.DataFrame(columns=[group_station_col, "Var", "Year", "Month", "Day", "Value", "Test"])
		return pd.DataFrame(columns=["Var", "Year", "Month", "Day", "Value", "Test"])

	def _plot_group(group: pd.DataFrame, sid: object, titled: bool) -> None:
		import matplotlib.pyplot as plt

		if outfile is None:
			if outpath is not None:
				plot_path = Path(outpath) / f"climatic_outliers_boxplot_{sid}_{var_name}.pdf"
			else:
				plot_path = Path(f"climatic_outliers_boxplot_{sid}_{var_name}.pdf")
		else:
			plot_path = Path(outfile)
			if titled and str(outfile).count("_") == 0:
				plot_path = Path(str(outfile).replace(".pdf", f"_{sid}.pdf"))
		plot_path.parent.mkdir(parents=True, exist_ok=True)
		fig, ax = plt.subplots(figsize=(10, 4))
		month_series = _coerce_numeric(group[month_col])
		month_data: list[np.ndarray] = []
		labels: list[str] = []
		for m in range(1, 13):
			vals = group.loc[month_series == m, var_name].dropna().to_numpy()
			if vals.size > 0:
				month_data.append(vals)
				labels.append(str(m))
		if month_data:
			ax.boxplot(month_data, tick_labels=labels, whis=outrange)
		ax.set_title(f"{var_name} - {sid}" if titled else var_name)
		ax.set_xlabel("Months")
		ax.set_ylabel(units if units else "Value")
		fig.tight_layout()
		fig.savefig(plot_path, format="pdf")
		if show:
			plt.show()
		else:
			plt.close(fig)

	def _flag_group(group: pd.DataFrame, sid: object) -> pd.DataFrame:
		station_work = group.copy()
		non_na = station_work[var_name].notna().sum()
		if non_na <= 5 * 365:
			return _empty_result()
		if var_name in DAILY_BOUNDED_VARS:
			station_work = station_work[station_work[var_name] != 0]
		if station_work.empty:
			return _empty_result()
		if bplot:
			_plot_group(station_work, sid, titled=group_station_col is not None)
		bounds = station_work.groupby(month_col)[var_name].apply(month_bounds)
		bounds = bounds.rename("bounds").reset_index()
		bounds[["lower", "upper"]] = pd.DataFrame(bounds["bounds"].tolist(), index=bounds.index)
		merged = station_work.merge(bounds[[month_col, "lower", "upper"]], on=month_col, how="left")
		out = merged[(merged[var_name] < merged["lower"]) | (merged[var_name] > merged["upper"])].copy()
		if out.empty:
			return _empty_result()
		out = out.rename(
			columns={year_col: "Year", month_col: "Month", day_col: "Day", var_name: "Value"}
		)[["Year", "Month", "Day", "Value"]]
		out.insert(0, "Var", var_name)
		if group_station_col is not None:
			out.insert(0, group_station_col, sid)
		out["Test"] = "climatic_outliers"
		if outpath:
			_append_or_write_flags(
				_daily_output_path(outpath, sid, var_name),
				out,
				["Var", "Year", "Month", "Day", "Value"],
			)
		return out

	if group_station_col is not None:
		if station_col is not None:
			station_groups = [(sid, group.copy()) for sid, group in work.groupby(group_station_col)]
		else:
			station_groups = []
			for sid in selected_station_ids or []:
				group = work.loc[_station_mask(work[group_station_col], sid)].copy()
				if not group.empty:
					station_groups.append((sid, group))
		all_out = [_flag_group(group, sid) for sid, group in station_groups]
		all_out = [out for out in all_out if not out.empty]
		if all_out:
			return pd.concat(all_out, ignore_index=True)
		return _empty_result()

	non_na = work[var_name].notna().sum()
	if non_na <= 5 * 365:
		print("Not enough data for outliers test",
		      "(minimum 5 years of non-zero values for daily data)")
		return _empty_result()
	if var_name in DAILY_BOUNDED_VARS:
		work = work[work[var_name] != 0]
	if work.empty:
		return _empty_result()
	if bplot:
		_plot_group(work, station_id, titled=False)
	bounds = work.groupby(month_col)[var_name].apply(month_bounds)
	bounds = bounds.rename("bounds").reset_index()
	bounds[["lower", "upper"]] = pd.DataFrame(bounds["bounds"].tolist(), index=bounds.index)
	merged = work.merge(bounds[[month_col, "lower", "upper"]], on=month_col, how="left")
	out = merged[(merged[var_name] < merged["lower"]) | (merged[var_name] > merged["upper"])].copy()
	if out.empty:
		return _empty_result()
	out = out.rename(
		columns={year_col: "Year", month_col: "Month", day_col: "Day", var_name: "Value"}
	)[["Year", "Month", "Day", "Value"]]
	out.insert(0, "Var", var_name)
	out["Test"] = "climatic_outliers"
	if outpath:
		_append_or_write_flags(
			_daily_output_path(outpath, str(station_id), var_name),
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
	station_col: Optional[str] = None,
) -> pd.DataFrame:
	df = _as_dataframe(data)
	required_cols = [year_col, month_col, day_col, var_name]
	if station_col is not None:
		required_cols.append(station_col)
	_ensure_columns(df, required_cols, "temporal_coherence")
	if var_name not in {"Tx", "Tn", "w", "sd"}:
		raise ValueError("Variable not supported by this test")

	jumps = temp_jumps if var_name in {"Tx", "Tn"} else windspeed_jumps if var_name == "w" else snowdepth_jumps

	wrk_cols = [year_col, month_col, day_col, var_name]
	if station_col is not None:
		wrk_cols.insert(0, station_col)
	work = df[wrk_cols].copy()
	work[var_name] = _coerce_numeric(work[var_name])
	if units:
		work[var_name] = check_units(work[var_name], var_name, units)

	if station_col is not None and station_col in work.columns:
		# Process each station separately
		all_out = []
		for sid, group in work.groupby(station_col):
			dt = _build_datetime(group, year_col, month_col, day_col)
			work_s = group.assign(_dt=dt).sort_values("_dt").reset_index(drop=True)
			diff_val = work_s[var_name].diff()
			diff_day = work_s["_dt"].diff().dt.days
			flags = ((diff_val.abs() > jumps) & (diff_day == 1)).fillna(False)
			selected = flags | flags.shift(-1, fill_value=False)
			
			out = work_s.loc[selected, [year_col, month_col, day_col, var_name]].drop_duplicates().copy()
			if not out.empty:
				out = out.rename(
					columns={year_col: "Year", month_col: "Month", day_col: "Day", var_name: "Value"}
				)[["Year", "Month", "Day", "Value"]]
				out.insert(0, "Var", var_name)
				out.insert(0, station_col, sid)
				out["Test"] = "temporal_coherence"
				all_out.append(out)
			if outpath and not out.empty:
				_append_or_write_flags(
					_daily_output_path(outpath, sid, var_name),
					out,
					["Var", "Year", "Month", "Day", "Value"],
				)
		if all_out:
			return pd.concat(all_out, ignore_index=True)
		else:
			return pd.DataFrame(columns=[station_col, "Var", "Year", "Month", "Day", "Value", "Test"])
	else:
		# Single station processing
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
	station_col: Optional[str] = None,
) -> pd.DataFrame:
	df = _as_dataframe(data)
	required_cols = [year_col, month_col, day_col, var_name]
	if station_col is not None:
		required_cols.append(station_col)
	_ensure_columns(df, required_cols, "daily_repetition")

	wrk_cols = [year_col, month_col, day_col, var_name]
	if station_col is not None:
		wrk_cols.insert(0, station_col)
	work = df[wrk_cols].copy()
	work[var_name] = _coerce_numeric(work[var_name])
	if units:
		work[var_name] = check_units(work[var_name], var_name, units)

	if station_col is not None and station_col in work.columns:
		# Process each station separately
		all_out = []
		for sid, group in work.groupby(station_col):
			group = group.sort_values([year_col, month_col, day_col]).reset_index(drop=True)
			starts, lengths = _run_lengths(group[var_name])
			indices: list[int] = []
			for start, length in zip(starts, lengths):
				if length >= n:
					indices.extend(range(start, start + length))
			if indices:
				out = group.iloc[indices, :].copy()
				if var_name in DAILY_BOUNDED_VARS:
					out = out[out[var_name] != 0]
				if not out.empty:
					out = out.rename(
						columns={year_col: "Year", month_col: "Month", day_col: "Day", var_name: "Value"}
					)[["Year", "Month", "Day", "Value"]]
					out.insert(0, "Var", var_name)
					out.insert(0, station_col, sid)
					out["Test"] = "daily_repetition"
					all_out.append(out)
				if outpath and not out.empty:
					_append_or_write_flags(
						_daily_output_path(outpath, sid, var_name),
						out,
						["Var", "Year", "Month", "Day", "Value"],
					)
		if all_out:
			return pd.concat(all_out, ignore_index=True)
		else:
			return pd.DataFrame(columns=[station_col, "Var", "Year", "Month", "Day", "Value", "Test"])
	else:
		# Single station processing
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
	station_col: Optional[str] = None,
) -> pd.DataFrame:
	df = _as_dataframe(data)
	required_cols = [year_col, month_col, day_col, var_name]
	if station_col is not None:
		required_cols.append(station_col)
	_ensure_columns(df, required_cols, "duplicate_dates")

	wrk_cols = [year_col, month_col, day_col, var_name]
	if station_col is not None:
		wrk_cols.insert(0, station_col)
	work = df[wrk_cols].copy()
	work[var_name] = _coerce_numeric(work[var_name])
	if units:
		work[var_name] = check_units(work[var_name], var_name, units)

	if station_col is not None and station_col in work.columns:
		# Process each station separately
		all_out = []
		for sid, group in work.groupby(station_col):
			dup = group.duplicated(subset=[year_col, month_col, day_col], keep=False)
			out = group.loc[dup, [year_col, month_col, day_col, var_name]].copy()
			if not out.empty:
				out = out.rename(
					columns={year_col: "Year", month_col: "Month", day_col: "Day", var_name: "Value"}
				)[["Year", "Month", "Day", "Value"]]
				out.insert(0, "Var", var_name)
				out.insert(0, station_col, sid)
				out["Test"] = "duplicate_dates"
				all_out.append(out)
			if outpath and not out.empty:
				# Each station has its own file, so don't include station_col in merge keys
				_append_or_write_flags(
					_daily_output_path(outpath, sid, var_name),
					out,
					["Var", "Year", "Month", "Day", "Value"],
				)
		if all_out:
			return pd.concat(all_out, ignore_index=True)
		else:
			return pd.DataFrame(columns=[station_col, "Var", "Year", "Month", "Day", "Value", "Test"])
	else:
		# Single station processing (original logic)
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
	station_col: Optional[str] = None,
) -> pd.DataFrame:
	df = _as_dataframe(data)
	required_cols = [year_col, month_col, day_col, var_name]
	if station_col is not None:
		required_cols.append(station_col)
	_ensure_columns(df, required_cols, "daily_out_of_range")

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

	wrk_cols = [year_col, month_col, day_col, var_name]
	if station_col is not None:
		wrk_cols.insert(0, station_col)
	work = df[wrk_cols].copy()
	work[var_name] = _coerce_numeric(work[var_name])
	if units:
		work[var_name] = check_units(work[var_name], var_name, units)

	if station_col is not None and station_col in work.columns:
		# Process each station separately
		all_out = []
		for sid, group in work.groupby(station_col):
			out = group[(group[var_name] > upper) | (group[var_name] < lower)].copy()
			if not out.empty:
				out = out.rename(
					columns={year_col: "Year", month_col: "Month", day_col: "Day", var_name: "Value"}
				)[["Year", "Month", "Day", "Value"]]
				out.insert(0, "Var", var_name)
				out.insert(0, station_col, sid)
				out["Test"] = "daily_out_of_range"
				all_out.append(out)
			if outpath and not out.empty:
				_append_or_write_flags(
					_daily_output_path(outpath, sid, var_name),
					out,
					["Var", "Year", "Month", "Day", "Value"],
				)
		if all_out:
			return pd.concat(all_out, ignore_index=True)
		else:
			return pd.DataFrame(columns=[station_col, "Var", "Year", "Month", "Day", "Value", "Test"])
	else:
		# Single station processing
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


def wmo_time_consistency(
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
	"""WMO time consistency test for pressure, temperature, and dew point.

	Applicable to a sub-daily series of air pressure (``p``, ``mslp``), air
	temperature (``ta``), or dew point temperature (``td``) observations with at
	least some time intervals between observations less than or equal to twelve
	hours. The test flags records where the observations exceed the WMO suggested
	tolerances for temperatures and pressure tendency as a function of the time
	period between consecutive reports.

	Parameters
	----------
	data : str or pathlib.Path or pandas.DataFrame
		Input series as a CSV path or DataFrame. The data must contain sub-daily
		date-time columns plus one variable column.
	var_name : str
		Variable to test. Supported values are ``"ta"``, ``"td"``, ``"p"``, and
		``"mslp"``.
	station_id : str, default "station"
		Station identifier used in output filenames.
	units : str, optional
		Units of ``var_name``. When provided, values are converted to the canonical
		units used by the test before thresholding.
	outpath : str or pathlib.Path, optional
		Output directory for flagged observations. Results are appended to the
		station-variable sub-daily QC file.
	year_col, month_col, day_col, hour_col, minute_col : str
		Column names for the date-time components.

	Returns
	-------
	pandas.DataFrame
		Flagged observations with columns ``Var, Year, Month, Day, Hour, Minute,
		Value, Test``. The test column is ``"wmo_time_consistency"``.

	Notes
	-----
	This function is a port of ``dataresqc::wmo_time_consistency``.

	Input expectations:

	- A CSV file path or a DataFrame containing sub-daily observations.
	- The DataFrame must contain year, month, day, hour, minute, and one
	  observation column.

	The WMO time consistency test uses tolerances for temperatures and pressure
	tendency as a function of the time period between consecutive reports
	(WMO, 1993: VI.21):

	- ``ta`` tolerance for ``dt = 1, 2, 3, 6, 12`` hours:
	  ``4, 7, 9, 15, 25`` C
	- ``td`` tolerance for ``dt = 1, 2, 3, 6, 12`` hours:
	  ``4, 6, 8, 12, 20`` C
	- ``p`` and ``mslp`` tolerance for ``dt = 1, 2, 3, 6, 12`` hours:
	  ``3, 6, 9, 18, 36`` hPa

	Implementation details:

	- The temperature tolerances for ``ta`` and ``td`` are extended piecewise to
	  all intervals in ``(0, 12]`` hours using the stepwise limits defined in the
	  original R code.
	- The pressure tolerance for ``p`` and ``mslp`` is determined for time
	  intervals in ``[1, 12]`` hours by assuming a linear variation of ``3`` hPa
	  per hour, following the original R implementation.
	- The test applied is:
	  ``|obs(t) - obs(t - dt)| > tol``
	- When this condition is met, both consecutive observations are flagged.
	- The flag, corresponding to suspect values, is therefore always associated
	  with two consecutive observations within twelve hours.

	References
	----------
	WMO, 1993. Chapter 6: Quality Control Procedures. Guide on the Global
	Data-processing System. World Meteorological Organization, Geneva, No. 305,
	VI.1-VI.27.

	Examples
	--------
	>>> wmo_time_consistency(df, var_name="p", station_id="Bern", units="hPa")
	"""
	df = _as_dataframe(data)
	_ensure_columns(
		df,
		[year_col, month_col, day_col, hour_col, minute_col, var_name],
		"wmo_time_consistency",
	)
	vcode = str(var_name)
	if vcode not in {"ta", "p", "td", "mslp"}:
		raise ValueError("Variable not supported by this test")

	work = df[[year_col, month_col, day_col, hour_col, minute_col, var_name]].copy()
	work[var_name] = _coerce_numeric(work[var_name])
	if units:
		work[var_name] = check_units(work[var_name], vcode, units)
	work = work.sort_values([year_col, month_col, day_col, hour_col, minute_col]).reset_index(drop=True)

	if len(work) < 2:
		return pd.DataFrame(columns=["Var", "Year", "Month", "Day", "Hour", "Minute", "Value", "Test"])

	times = _build_datetime(work, year_col, month_col, day_col, hour_col, minute_col)
	dt = times.diff().dt.total_seconds().div(3600.0).iloc[1:].reset_index(drop=True)
	dsubd = work[var_name].diff().abs().iloc[1:].reset_index(drop=True)

	if vcode in {"p", "mslp"}:
		tol = 3.0 * dt
	else:
		tol = pd.Series(np.nan, index=dt.index, dtype="float64")
		if vcode == "ta":
			limits = [4, 7, 9, 11, 13, 15, 17, 18, 20, 22, 23, 25]
		else:
			limits = [4, 6, 8, 9, 11, 12, 13, 15, 16, 17, 19, 20]
		for hour, limit in enumerate(limits, start=1):
			lower = hour - 1
			mask = (dt > lower) & (dt <= hour) if lower > 0 else (dt <= hour)
			tol.loc[mask] = limit

	flags = (dt <= 12) & ((dsubd - tol) > 0)
	if not flags.any():
		return pd.DataFrame(columns=["Var", "Year", "Month", "Day", "Hour", "Minute", "Value", "Test"])

	flagged_positions = sorted(set(flags[flags].index.tolist() + (flags[flags].index + 1).tolist()))
	out = work.loc[flagged_positions, [year_col, month_col, day_col, hour_col, minute_col, var_name]].copy()
	out = out.drop_duplicates()
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
	out.insert(0, "Var", vcode)
	out["Test"] = "wmo_time_consistency"

	if outpath:
		_append_or_write_flags(
			_subdaily_output_path(outpath, station_id, vcode),
			out,
			["Var", "Year", "Month", "Day", "Hour", "Minute", "Value"],
		)
	return out


def wmo_gross_errors(
	data: Union[str, Path, pd.DataFrame],
	var_name: str,
	station_id: str = "station",
	units: Optional[str] = None,
	lat: Optional[float] = None,
	lat_col: Optional[str] = None,
	lat_map: Optional[dict[str, float]] = None,
	lat_df: Optional[pd.DataFrame] = None,
	lat_df_id_col: str = "station_id",
	lat_df_lat_col: str = "lat",
	station_col: Optional[str] = None,
	outpath: Optional[Union[str, Path]] = None,
	year_col: str = "Year",
	month_col: str = "Month",
	day_col: str = "Day",
	hour_col: str = "Hour",
	minute_col: str = "Minute",
) -> pd.DataFrame:
	"""Flag WMO gross errors for pressure, temperature, dew point, and wind.

	Applicable to daily or sub-daily series for station-level pressure (`p`),
	mean sea level pressure (`mslp`), air temperature (`ta`), dew point
	temperature (`td`), and wind speed (`w`). Input may be a CSV path or a
	DataFrame with date columns and one variable column. When hour and minute
	columns are present, the function treats the input as sub-daily; otherwise it
	uses daily records.

	Parameters
	----------
	data : str or pathlib.Path or pandas.DataFrame
		Input series as a CSV path or DataFrame.
	var_name : str
		Variable to test. Supported values are ``"p"``, ``"mslp"``, ``"ta"``,
		``"td"``, and ``"w"``.
	station_id : str, default "station"
		Station identifier used in output filenames when ``station_col`` is not
		provided.
	units : str, optional
		Units of ``var_name``. When provided, values are converted to the canonical
		units used by the test before thresholding.
	lat : float, optional
		Single latitude applied to all records or all stations.
	lat_col : str, optional
		Name of a latitude column already present in ``data``.
	lat_map : dict[str, float], optional
		Mapping from station identifier to latitude.
	lat_df : pandas.DataFrame, optional
		DataFrame containing station identifiers and latitudes. This is converted
		into ``lat_map`` using ``lat_df_id_col`` and ``lat_df_lat_col``.
	lat_df_id_col : str, default "station_id"
		Station identifier column in ``lat_df``.
	lat_df_lat_col : str, default "lat"
		Latitude column in ``lat_df``.
	station_col : str, optional
		Station identifier column in ``data``. If provided, the test is applied
		separately to each station.
	outpath : str or pathlib.Path, optional
		Output directory for flagged observations. Results are appended to
		station-variable QC files using the same naming convention as the other QC
		functions.
	year_col, month_col, day_col, hour_col, minute_col : str
		Column names for the date-time components.

	Returns
	-------
	pandas.DataFrame
		Flagged observations with columns ``Var, Year, Month, Day, Value, Test``
		for daily data, or ``Var, Year, Month, Day, Hour, Minute, Value, Test``
		for sub-daily data. If ``station_col`` is provided, it is included as the
		first column. The output reports all values crossing WMO suspect or
		erroneous limits under the single test label ``"wmo_gross_errors"``.

	Notes
	-----
	This function implements the thresholds used in
	``dataresqc::wmo_gross_errors`` and based on WMO (1993). For station-level
	pressure (``p``), limits are independent of latitude and season:

	- Suspect: ``300 <= p < 400`` hPa or ``1080 < p <= 1100`` hPa
	- Erroneous: ``p < 300`` hPa or ``p > 1100`` hPa

	For ``mslp``, ``ta``, ``td``, and ``w``, limits depend on absolute latitude
	and on a two-season split. The implementation uses:

	- Northern Hemisphere winter / Southern Hemisphere summer:
	  January, February, March, October, November, December
	- Northern Hemisphere summer / Southern Hemisphere winter:
	  April, May, June, July, August, September

	Gross-error limits used by this implementation:

	- ``|lat| <= 45``, winter
	  ``mslp`` suspect: ``870 <= mslp < 910`` or ``1080 < mslp <= 1100`` hPa;
	  erroneous: ``mslp < 870`` or ``mslp > 1100`` hPa
	  ``ta`` suspect: ``-40 <= ta < -30`` or ``50 < ta <= 55`` C;
	  erroneous: ``ta < -40`` or ``ta > 55`` C
	  ``td`` suspect: ``-45 <= td < -35`` or ``35 < td <= 40`` C;
	  erroneous: ``td < -45`` or ``td > 40`` C
	  ``w`` suspect: ``60 < w <= 125`` m/s; erroneous: ``w > 125`` m/s

	- ``|lat| <= 45``, summer
	  ``mslp`` suspect: ``850 <= mslp < 900`` or ``1080 < mslp <= 1100`` hPa;
	  erroneous: ``mslp < 850`` or ``mslp > 1100`` hPa
	  ``ta`` suspect: ``-30 <= ta < -20`` or ``50 < ta <= 60`` C;
	  erroneous: ``ta < -30`` or ``ta > 60`` C
	  ``td`` suspect: ``-35 <= td < -25`` or ``35 < td <= 40`` C;
	  erroneous: ``td < -35`` or ``td > 40`` C
	  ``w`` suspect: ``90 < w <= 150`` m/s; erroneous: ``w > 150`` m/s

	- ``|lat| > 45``, winter
	  ``mslp`` suspect: ``910 <= mslp < 940`` or ``1080 < mslp <= 1100`` hPa;
	  erroneous: ``mslp < 910`` or ``mslp > 1100`` hPa
	  ``ta`` suspect: ``-90 <= ta < -80`` or ``35 < ta <= 40`` C;
	  erroneous: ``ta < -90`` or ``ta > 40`` C
	  ``td`` suspect: ``-99 <= td < -85`` or ``30 < td <= 35`` C;
	  erroneous: ``td < -99`` or ``td > 35`` C
	  ``w`` suspect: ``50 < w <= 100`` m/s; erroneous: ``w > 100`` m/s

	- ``|lat| > 45``, summer
	  ``mslp`` suspect: ``920 <= mslp < 950`` or ``1080 < mslp <= 1100`` hPa;
	  erroneous: ``mslp < 920`` or ``mslp > 1100`` hPa
	  ``ta`` suspect: ``-40 <= ta < -30`` or ``40 < ta <= 50`` C;
	  erroneous: ``ta < -40`` or ``ta > 50`` C
	  ``td`` suspect: ``-45 <= td < -35`` or ``35 < td <= 40`` C;
	  erroneous: ``td < -45`` or ``td > 40`` C
	  ``w`` suspect: ``40 < w <= 75`` m/s; erroneous: ``w > 75`` m/s

	Latitude is resolved in the following priority order:

	- ``lat_df``: lookup table built from station id and latitude columns
	- ``lat_map``: explicit ``{station_id: latitude}`` mapping
	- ``lat_col``: latitude column in ``data``
	- ``lat``: single latitude applied to all records

	A latitude is required for ``mslp``, ``ta``, ``td``, and ``w``. Current
	output does not distinguish suspect from erroneous values; both are returned
	as flagged observations under the same test name.

	References
	----------
	WMO, 1993. Guide on the Global Data-processing System, Chapter 6: Quality
	Control Procedures. World Meteorological Organization, Geneva, No. 305,
	VI.1-VI.27.
	"""
	vcode = str(var_name)
	if vcode not in {"mslp", "p", "ta", "td", "w"}:
		raise ValueError("Variable not supported by this test")

	# Build lat_map from lat_df when provided
	if lat_df is not None:
		if lat_df_id_col not in lat_df.columns or lat_df_lat_col not in lat_df.columns:
			raise ValueError(
				f"lat_df must contain columns '{lat_df_id_col}' and '{lat_df_lat_col}'. "
				f"Found: {list(lat_df.columns)}"
			)
		lat_map = (
			lat_df[[lat_df_id_col, lat_df_lat_col]]
			.drop_duplicates(lat_df_id_col)
			.set_index(lat_df_id_col)[lat_df_lat_col]
			.to_dict()
		)

	df = _as_dataframe(data)
	has_time = hour_col in df.columns and minute_col in df.columns
	required_cols = [year_col, month_col, day_col, var_name]
	if has_time:
		required_cols.extend([hour_col, minute_col])
	if station_col is not None:
		required_cols.append(station_col)
	if lat_col is not None:
		required_cols.append(lat_col)
	_ensure_columns(df, required_cols, "wmo_gross_errors")

	flag_name = "wmo_gross_errors"

	wrk_cols = [year_col, month_col, day_col, var_name]
	if has_time:
		wrk_cols.extend([hour_col, minute_col])
	if station_col is not None:
		wrk_cols.insert(0, station_col)
	if lat_col is not None:
		wrk_cols.append(lat_col)
	work = df[wrk_cols].copy()
	work[var_name] = _coerce_numeric(work[var_name])
	if units:
		work[var_name] = check_units(work[var_name], vcode, units)

	if station_col is not None and station_col in work.columns:
		station_groups = work.groupby(station_col, dropna=False)
	else:
		station_groups = [(station_id, work)]

	def _limits_for(var: str, lat_value: float, winter: bool) -> tuple[tuple[float, float], tuple[float, float], tuple[float, float], tuple[float, float]]:
		"""Return (suspect_low, suspect_high, erroneous_low, erroneous_high)."""
		abs_lat = abs(lat_value)
		if var == "mslp":
			if abs_lat <= 45:
				if winter:
					return (870.0, 910.0), (1080.0, 1100.0), (-np.inf, 870.0), (1100.0, np.inf)
				return (850.0, 900.0), (1080.0, 1100.0), (-np.inf, 850.0), (1100.0, np.inf)
			if winter:
				return (910.0, 940.0), (1080.0, 1100.0), (-np.inf, 910.0), (1100.0, np.inf)
			return (920.0, 950.0), (1080.0, 1100.0), (-np.inf, 920.0), (1100.0, np.inf)

		if var == "ta":
			if abs_lat <= 45:
				if winter:
					return (-40.0, -30.0), (50.0, 55.0), (-np.inf, -40.0), (55.0, np.inf)
				return (-30.0, -20.0), (50.0, 60.0), (-np.inf, -30.0), (60.0, np.inf)
			if winter:
				return (-90.0, -80.0), (35.0, 40.0), (-np.inf, -90.0), (40.0, np.inf)
			return (-40.0, -30.0), (40.0, 50.0), (-np.inf, -40.0), (50.0, np.inf)

		if var == "td":
			if abs_lat <= 45:
				if winter:
					return (-45.0, -35.0), (35.0, 40.0), (-np.inf, -45.0), (40.0, np.inf)
				return (-35.0, -25.0), (35.0, 40.0), (-np.inf, -35.0), (40.0, np.inf)
			if winter:
				return (-99.0, -85.0), (30.0, 35.0), (-np.inf, -99.0), (35.0, np.inf)
			return (-45.0, -35.0), (35.0, 40.0), (-np.inf, -45.0), (40.0, np.inf)

		# var == "w"
		if abs_lat <= 45:
			if winter:
				return (-np.inf, -np.inf), (60.0, 125.0), (-np.inf, -np.inf), (125.0, np.inf)
			return (-np.inf, -np.inf), (90.0, 150.0), (-np.inf, -np.inf), (150.0, np.inf)
		if winter:
			return (-np.inf, -np.inf), (50.0, 100.0), (-np.inf, -np.inf), (100.0, np.inf)
		return (-np.inf, -np.inf), (40.0, 75.0), (-np.inf, -np.inf), (75.0, np.inf)

	all_out: list[pd.DataFrame] = []
	for sid, group in station_groups:
		g = group.copy()

		# Resolve latitude for this station/group.
		if lat_col is not None and lat_col in g.columns:
			lat_candidates = pd.to_numeric(g[lat_col], errors="coerce").dropna()
			lat_value = float(lat_candidates.iloc[0]) if not lat_candidates.empty else np.nan
		elif lat_map is not None:
			lat_lookup = _lookup_station_value(lat_map, sid)
			lat_value = float(lat_lookup) if lat_lookup is not None else np.nan
		elif lat is not None:
			lat_value = float(lat)
		else:
			lat_value = np.nan

		if vcode != "p" and np.isnan(lat_value):
			raise ValueError(
				"wmo_gross_errors: latitude is required for mslp/ta/td/w. "
				"Provide lat, lat_col, or lat_map."
			)

		if vcode == "p":
			susp = ((g[var_name] >= 300) & (g[var_name] < 400)) | ((g[var_name] > 1080) & (g[var_name] <= 1100))
			erro = (g[var_name] < 300) | (g[var_name] > 1100)
			flagged = g[susp | erro].copy()
		else:
			winter_months = {1, 2, 3, 10, 11, 12} if lat_value >= 0 else {4, 5, 6, 7, 8, 9}
			summer_months = {4, 5, 6, 7, 8, 9} if lat_value >= 0 else {1, 2, 3, 10, 11, 12}
			months = pd.to_numeric(g[month_col], errors="coerce")
			winter_mask = months.isin(winter_months)
			summer_mask = months.isin(summer_months)

			sl_w, sh_w, el_w, eh_w = _limits_for(vcode, lat_value, winter=True)
			sl_s, sh_s, el_s, eh_s = _limits_for(vcode, lat_value, winter=False)

			def _season_flags(
				x: pd.Series,
				sus_low: tuple[float, float],
				sus_high: tuple[float, float],
				err_low: tuple[float, float],
				err_high: tuple[float, float],
			) -> pd.Series:
				sus = ((x >= sus_low[0]) & (x < sus_low[1])) | ((x > sus_high[0]) & (x <= sus_high[1]))
				err = ((x > err_high[0]) & (x < err_high[1])) | ((x > err_low[0]) & (x < err_low[1]))
				return sus | err

			flags_w = _season_flags(g[var_name], sl_w, sh_w, el_w, eh_w) & winter_mask
			flags_s = _season_flags(g[var_name], sl_s, sh_s, el_s, eh_s) & summer_mask
			flagged = g[flags_w | flags_s].copy()

		if flagged.empty:
			continue

		if has_time:
			out = flagged.rename(
				columns={
					year_col: "Year",
					month_col: "Month",
					day_col: "Day",
					hour_col: "Hour",
					minute_col: "Minute",
					var_name: "Value",
				}
			)[["Year", "Month", "Day", "Hour", "Minute", "Value"]]
			key_cols = ["Var", "Year", "Month", "Day", "Hour", "Minute", "Value"]
		else:
			out = flagged.rename(
				columns={
					year_col: "Year",
					month_col: "Month",
					day_col: "Day",
					var_name: "Value",
				}
			)[["Year", "Month", "Day", "Value"]]
			key_cols = ["Var", "Year", "Month", "Day", "Value"]

		out.insert(0, "Var", vcode)
		if station_col is not None:
			out.insert(0, station_col, sid)
		out["Test"] = flag_name
		all_out.append(out)

		if outpath:
			if has_time:
				_append_or_write_flags(_subdaily_output_path(outpath, str(sid), vcode), out, key_cols)
			else:
				_append_or_write_flags(_daily_output_path(outpath, str(sid), vcode), out, key_cols)

	if not all_out:
		if has_time:
			cols = ["Var", "Year", "Month", "Day", "Hour", "Minute", "Value", "Test"]
		else:
			cols = ["Var", "Year", "Month", "Day", "Value", "Test"]
		if station_col is not None:
			cols = [station_col] + cols
		return pd.DataFrame(columns=cols)

	return pd.concat(all_out, ignore_index=True).drop_duplicates()


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
	station_col: Optional[str] = "station",
	climatic_outliers_params: Optional[dict] = None,
	daily_out_of_range_params: Optional[dict] = None,
	temporal_coherence_params: Optional[dict] = None,
	daily_repetition_params: Optional[dict] = None,
	subdaily_out_of_range_params: Optional[dict] = None,
	subdaily_repetition_params: Optional[dict] = None,
	bplot: bool = False,
) -> dict[str, Union[pd.DataFrame, dict[str, pd.DataFrame]]]:
	"""Run all applicable QC checks for a station dataset.

	Inputs are CSV/DataFrame with variables in columns.
	If station_col is provided and exists in data, will group by station and apply tests per-station.
	Returns:
	- results_by_test: dict[test_name -> flagged rows DataFrame]
	- all_flags: concatenated flags
	- summary: counts by test and variable (and station if multi-station)
	"""
	df = _as_dataframe(data)
	required_cols = [year_col, month_col, day_col]
	if station_col is not None and station_col in df.columns:
		required_cols.append(station_col)
		station_col_actual = station_col
	else:
		station_col_actual = None
	_ensure_columns(df, required_cols, "run_qc_pipeline")

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
					bplot=bplot,
					var_name=var,
					station_id=station_id,
					units=units,
					outpath=outpath,
					year_col=year_col,
					month_col=month_col,
					day_col=day_col,
					station_col=station_col_actual,
					**climatic_outliers_params,
				),
			)

			try:
				_add_result(
					"daily_out_of_range",
					daily_out_of_range(
						df,
											station_col=station_col_actual,
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
											station_col=station_col_actual,
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
								station_col=station_col_actual,
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
								station_col=station_col_actual,
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
	"wmo_time_consistency",
	"wmo_gross_errors",
	"daily_out_of_range",
	"subdaily_out_of_range",
	"subdaily_repetition",
	"duplicate_times",
	"run_qc_pipeline",
	"run_qc_pipeline_from_config",
]
