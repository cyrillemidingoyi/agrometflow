from __future__ import annotations

import io
import logging
import tempfile
import traceback
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
import xarray as xr
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from agrometflow.dataquality.qc import (
    climatic_outliers,
    daily_out_of_range,
    daily_repetition,
    duplicate_dates,
    duplicate_times,
    internal_consistency,
    run_qc_pipeline,
    subdaily_out_of_range,
    subdaily_repetition,
    temporal_coherence,
    wmo_gross_errors,
)


@dataclass
class DatasetState:
    name: str
    dataframe: pd.DataFrame


@dataclass
class OutputState:
    label: str
    filepath: str
    media_type: str
    filename: str


app = FastAPI(title="AgrometFlow QC Studio", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")

DATASETS: dict[str, DatasetState] = {}
OUTPUTS: dict[str, OutputState] = {}

# --------------------------------------------------------------------------- #
# Catalogue of all available QC tests
# --------------------------------------------------------------------------- #
DAILY_TESTS = [
    "climatic_outliers",
    "daily_out_of_range",
    "temporal_coherence",
    "daily_repetition",
    "duplicate_dates",
    "internal_consistency",
]
SUBDAILY_TESTS = [
    "subdaily_out_of_range",
    "subdaily_repetition",
    "duplicate_times",
]
ALL_TESTS = DAILY_TESTS + SUBDAILY_TESTS + ["wmo_gross_errors", "run_qc_pipeline"]

QC_VAR_ALIASES = {
    "tmax": "Tx",
    "tx": "Tx",
    "tmin": "Tn",
    "tn": "Tn",
    "prcp": "rr",
    "rr": "rr",
    "awnd": "w",
    "w": "w",
    "wd": "dd",
    "dd": "dd",
    "snow": "fs",
    "fs": "fs",
    "snwd": "sd",
    "sd": "sd",
    "sc": "sc",
    "ta": "ta",
    "td": "td",
    "mslp": "mslp",
    "p": "p",
}

PIPELINE_DAILY_SUPPORTED = {"Tx", "Tn", "rr", "w", "dd", "sc", "sd", "fs"}
PIPELINE_SUBDAILY_SUPPORTED = {"ta", "rr", "w", "dd", "sc", "sd", "fs"}
TEMPORAL_SUPPORTED = {"Tx", "Tn", "w", "sd"}
DAILY_RANGE_SUPPORTED = {"Tx", "Tn", "rr", "w", "dd", "sc", "sd", "fs"}
SUBDAILY_RANGE_SUPPORTED = {"ta", "rr", "w", "dd", "sc", "sd", "fs"}


class FillMissingRequest(BaseModel):
    method: str = Field(default="linear", pattern="^(linear|ffill|bfill|mean|median)$")
    columns: list[str]
    group_by: list[str] = []
    sort_by: list[str] = []


class ExportRequest(BaseModel):
    format: str = Field(default="csv", pattern="^(csv|xlsx|netcdf)$")
    filename: Optional[str] = None


class WMORequest(BaseModel):
    enabled: bool = False
    input_column: Optional[str] = None
    wmo_code: Optional[str] = None
    units: Optional[str] = None
    station_col: Optional[str] = "station"
    lat_col: Optional[str] = None
    lat: Optional[float] = None


class InternalConsistencyRequest(BaseModel):
    enabled: bool = False
    var_x: Optional[str] = None
    var_y: Optional[str] = None
    units_x: Optional[str] = None
    units_y: Optional[str] = None


class QCRequest(BaseModel):
    frequency: str = Field(default="daily", pattern="^(auto|daily|subdaily)$")
    station_col: Optional[str] = "station"
    station_id: str = "station"
    year_col: str = "Year"
    month_col: str = "Month"
    day_col: str = "Day"
    hour_col: str = "Hour"
    minute_col: str = "Minute"
    variable_cols: list[str] = []
    units_map: dict[str, str] = {}
    selected_tests: list[str] = Field(default_factory=list)
    run_pipeline: bool = False
    run_wmo: WMORequest = Field(default_factory=WMORequest)
    internal_consistency_req: InternalConsistencyRequest = Field(
        default_factory=InternalConsistencyRequest
    )


def _to_json_rows(df: pd.DataFrame, max_rows: int = 200) -> list[dict[str, Any]]:
    out = df.head(max_rows).copy()
    out = out.replace({np.nan: None})
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    return out.to_dict(orient="records")


def _read_uploaded_table(file: UploadFile) -> pd.DataFrame:
    suffix = Path(file.filename or "").suffix.lower()
    if suffix in {".csv", ".txt"}:
        raw = file.file.read()
        file.file.seek(0)
        
        # Try multiple encodings in order of likelihood
        encodings = ["utf-8", "latin-1", "iso-8859-1", "cp1252", "utf-16"]
        df = None
        
        for encoding in encodings:
            try:
                return pd.read_csv(io.BytesIO(raw), encoding=encoding)
            except (UnicodeDecodeError, pd.errors.ParserError):
                pass
            try:
                return pd.read_csv(io.BytesIO(raw), encoding=encoding, sep=";")
            except (UnicodeDecodeError, pd.errors.ParserError):
                pass
        
        # If all encodings fail, raise the original error
        raise HTTPException(
            status_code=400,
            detail="Cannot read CSV file. Try saving with UTF-8 encoding. "
                   "Common encodings: UTF-8, Latin-1 (European), cp1252 (Windows)."
        )

    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(file.file)

    if suffix in {".nc", ".nc4", ".cdf"}:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(file.file.read())
            tmp_path = tmp.name
        ds = xr.open_dataset(tmp_path)
        try:
            var_names = list(ds.data_vars)
            if not var_names:
                raise ValueError("NetCDF has no data variables")
            df = ds[var_names[0]].to_dataframe().reset_index()
            return df
        finally:
            ds.close()
            Path(tmp_path).unlink(missing_ok=True)

    raise HTTPException(status_code=400, detail="Unsupported file type. Use CSV, Excel, or NetCDF.")


def _save_output(df: pd.DataFrame, label: str, fmt: str = "csv") -> str:
    """Persist a DataFrame as a temp file, register in OUTPUTS, return output_id."""
    output_id = str(uuid.uuid4())
    if fmt == "csv":
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        df.to_csv(tmp.name, index=False)
        tmp.close()
        OUTPUTS[output_id] = OutputState(
            label=label,
            filepath=tmp.name,
            media_type="text/csv",
            filename=f"{label}.csv",
        )
    elif fmt == "xlsx":
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        with pd.ExcelWriter(tmp.name, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="flags")
        tmp.close()
        OUTPUTS[output_id] = OutputState(
            label=label,
            filepath=tmp.name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"{label}.xlsx",
        )
    return output_id


def _ensure_temporal_columns(df: pd.DataFrame, req: QCRequest) -> tuple[pd.DataFrame, list[str]]:
    """Ensure required temporal columns exist, deriving them from datetime-like columns when possible."""
    wrk = df.copy()
    notes: list[str] = []

    needed_daily = [req.year_col, req.month_col, req.day_col]
    missing_daily = [c for c in needed_daily if c not in wrk.columns]
    if not missing_daily:
        return wrk, notes

    candidates = [
        "date",
        "Date",
        "datetime",
        "Datetime",
        "timestamp",
        "Timestamp",
        "time",
        "Time",
    ]
    source_col = next((c for c in candidates if c in wrk.columns), None)

    if source_col is None:
        for c in wrk.columns:
            lc = str(c).lower()
            if "date" in lc or "time" in lc:
                source_col = c
                break

    if source_col is None:
        return wrk, notes

    # Try ISO format first (dayfirst=False), fall back to European (dayfirst=True)
    parsed = pd.to_datetime(wrk[source_col], errors="coerce", utc=False, dayfirst=False)
    if parsed.notna().sum() == 0:
        parsed = pd.to_datetime(wrk[source_col], errors="coerce", utc=False, dayfirst=True)

    if parsed.notna().sum() == 0:
        return wrk, notes

    # Drop rows where date parsing failed entirely
    valid = parsed.notna()
    if not valid.all():
        wrk = wrk.loc[valid].reset_index(drop=True)
        parsed = parsed.loc[valid].reset_index(drop=True)

    if req.year_col not in wrk.columns:
        wrk[req.year_col] = parsed.dt.year.astype(int)
    if req.month_col not in wrk.columns:
        wrk[req.month_col] = parsed.dt.month.astype(int)
    if req.day_col not in wrk.columns:
        wrk[req.day_col] = parsed.dt.day.astype(int)

    if req.hour_col not in wrk.columns:
        wrk[req.hour_col] = parsed.dt.hour.astype(int)
    if req.minute_col not in wrk.columns:
        wrk[req.minute_col] = parsed.dt.minute.astype(int)

    notes.append(
        f"Temporal columns auto-derived from '{source_col}' -> "
        f"{req.year_col},{req.month_col},{req.day_col}"
    )
    return wrk, notes


def _ensure_qc_alias_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add canonical QC variable aliases (Tx/Tn/rr/...) from common raw names when needed."""
    out = df.copy()
    lower_to_col = {str(c).lower(): c for c in out.columns}
    for raw_lc, canonical in QC_VAR_ALIASES.items():
        src = lower_to_col.get(raw_lc)
        if src is not None and canonical not in out.columns:
            out[canonical] = out[src]
    return out


def _canonicalize_vars(vars_in: list[str], available_cols: list[str]) -> list[str]:
    """Map user variable names to QC canonical names and keep only available columns."""
    available = set(available_cols)
    out: list[str] = []
    for v in vars_in:
        vv = str(v).strip()
        if not vv:
            continue
        mapped = QC_VAR_ALIASES.get(vv.lower(), vv)
        if mapped in available and mapped not in out:
            out.append(mapped)
    return out


# =========================================================================== #
# Page routes
# =========================================================================== #

@app.get("/", response_class=RedirectResponse)
def root() -> RedirectResponse:
    return RedirectResponse(url="/data")


@app.get("/data", response_class=HTMLResponse)
def page_data() -> str:
    return (Path(__file__).parent / "templates" / "data.html").read_text(encoding="utf-8")


@app.get("/qc", response_class=HTMLResponse)
def page_qc() -> str:
    return (Path(__file__).parent / "templates" / "qc.html").read_text(encoding="utf-8")


@app.get("/outputs", response_class=HTMLResponse)
def page_outputs() -> str:
    return (Path(__file__).parent / "templates" / "outputs.html").read_text(encoding="utf-8")


# =========================================================================== #
# API – meta
# =========================================================================== #

@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/qc/tests")
def list_tests() -> dict[str, Any]:
    return {
        "daily": DAILY_TESTS,
        "subdaily": SUBDAILY_TESTS,
        "other": ["wmo_gross_errors", "run_qc_pipeline"],
        "all": ALL_TESTS,
    }


# =========================================================================== #
# API – datasets
# =========================================================================== #

@app.post("/api/datasets/upload")
def upload_dataset(file: UploadFile = File(...)) -> dict[str, Any]:
    df = _read_uploaded_table(file)
    dataset_id = str(uuid.uuid4())
    DATASETS[dataset_id] = DatasetState(name=file.filename or "uploaded", dataframe=df)
    return {
        "dataset_id": dataset_id,
        "name": file.filename,
        "rows": int(len(df)),
        "columns": list(df.columns),
        "preview": _to_json_rows(df),
    }


@app.get("/api/datasets/{dataset_id}/preview")
def preview_dataset(dataset_id: str, limit: int = 100) -> dict[str, Any]:
    state = DATASETS.get(dataset_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return {
        "dataset_id": dataset_id,
        "name": state.name,
        "rows": int(len(state.dataframe)),
        "columns": list(state.dataframe.columns),
        "preview": _to_json_rows(state.dataframe, max_rows=limit),
    }


@app.post("/api/datasets/{dataset_id}/fill-missing")
def fill_missing(dataset_id: str, req: FillMissingRequest) -> dict[str, Any]:
    state = DATASETS.get(dataset_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    df = state.dataframe.copy()
    missing_columns = [c for c in req.columns if c not in df.columns]
    if missing_columns:
        raise HTTPException(status_code=400, detail=f"Missing columns: {missing_columns}")

    if req.sort_by:
        df = df.sort_values(req.sort_by)

    def _fill_frame(frame: pd.DataFrame) -> pd.DataFrame:
        out = frame.copy()
        for col in req.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
            if req.method == "linear":
                out[col] = out[col].interpolate(limit_direction="both")
            elif req.method == "ffill":
                out[col] = out[col].ffill().bfill()
            elif req.method == "bfill":
                out[col] = out[col].bfill().ffill()
            elif req.method == "mean":
                out[col] = out[col].fillna(out[col].mean())
            elif req.method == "median":
                out[col] = out[col].fillna(out[col].median())
        return out

    if req.group_by:
        missing_groups = [c for c in req.group_by if c not in df.columns]
        if missing_groups:
            raise HTTPException(status_code=400, detail=f"Missing group_by columns: {missing_groups}")
        df = df.groupby(req.group_by, dropna=False, group_keys=False).apply(_fill_frame)
    else:
        df = _fill_frame(df)

    state.dataframe = df
    na_counts = {c: int(df[c].isna().sum()) for c in req.columns}
    return {
        "dataset_id": dataset_id,
        "method": req.method,
        "na_counts_after": na_counts,
        "preview": _to_json_rows(df),
    }


@app.post("/api/datasets/{dataset_id}/export")
def export_dataset(dataset_id: str, req: ExportRequest) -> FileResponse:
    state = DATASETS.get(dataset_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    df = state.dataframe.copy()
    base = req.filename or Path(state.name).stem or "dataset"

    if req.format == "csv":
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        df.to_csv(tmp.name, index=False)
        tmp.close()
        output_id = str(uuid.uuid4())
        OUTPUTS[output_id] = OutputState(
            label=f"export_{base}",
            filepath=tmp.name,
            media_type="text/csv",
            filename=f"{base}.csv",
        )
        return FileResponse(tmp.name, media_type="text/csv", filename=f"{base}.csv")

    if req.format == "xlsx":
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        with pd.ExcelWriter(tmp.name, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="data")
        tmp.close()
        output_id = str(uuid.uuid4())
        OUTPUTS[output_id] = OutputState(
            label=f"export_{base}",
            filepath=tmp.name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"{base}.xlsx",
        )
        return FileResponse(
            tmp.name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"{base}.xlsx",
        )

    if req.format == "netcdf":
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".nc")
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if not numeric_cols:
            raise HTTPException(status_code=400, detail="No numeric columns available for NetCDF export")
        xr.Dataset.from_dataframe(df[numeric_cols]).to_netcdf(tmp.name)
        tmp.close()
        output_id = str(uuid.uuid4())
        OUTPUTS[output_id] = OutputState(
            label=f"export_{base}",
            filepath=tmp.name,
            media_type="application/x-netcdf",
            filename=f"{base}.nc",
        )
        return FileResponse(tmp.name, media_type="application/x-netcdf", filename=f"{base}.nc")

    raise HTTPException(status_code=400, detail="Unsupported export format")


# =========================================================================== #
# API – QC
# =========================================================================== #

@app.post("/api/datasets/{dataset_id}/run-qc")
def run_qc(dataset_id: str, req: QCRequest) -> dict[str, Any]:  # noqa: C901
    state = DATASETS.get(dataset_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    df = state.dataframe.copy()
    df, time_notes = _ensure_temporal_columns(df, req)
    df = _ensure_qc_alias_columns(df)
    warnings: list[str] = []
    results_by_test: dict[str, Any] = {}
    all_flags_parts: list[pd.DataFrame] = []

    selected = set(req.selected_tests) if req.selected_tests else set()
    # If nothing explicitly selected, fall back to the pipeline
    run_pipeline = req.run_pipeline or (not selected and not req.run_wmo.enabled)

    def _record(test: str, flags: pd.DataFrame) -> None:
        if flags is None or flags.empty:
            results_by_test[test] = {"flag_count": 0, "flag_preview": []}
            return
        results_by_test[test] = {
            "flag_count": int(len(flags)),
            "flag_preview": _to_json_rows(flags, max_rows=500),
        }
        all_flags_parts.append(flags.assign(_test=test))

    # ---- auto-detect variable_cols when not provided ---------------------- #
    effective_vars = _canonicalize_vars(req.variable_cols, list(df.columns))
    if not effective_vars:
        supported = PIPELINE_SUBDAILY_SUPPORTED if req.frequency == "subdaily" else PIPELINE_DAILY_SUPPORTED
        effective_vars = [v for v in supported if v in df.columns]
    if not effective_vars:
        # Fallback: numeric columns only, excluding non-data columns
        skip = {req.year_col, req.month_col, req.day_col, req.hour_col, req.minute_col}
        if req.station_col:
            skip.add(req.station_col)
        for c in df.columns:
            lc = str(c).lower()
            if "date" in lc or "time" in lc or lc in {"lat", "latitude", "lon", "longitude", "elevation", "alt"}:
                skip.add(c)
        effective_vars = [
            c for c in df.columns
            if c not in skip and pd.api.types.is_numeric_dtype(df[c])
        ]

    # ---- full pipeline ---------------------------------------------------- #
    if run_pipeline or "run_qc_pipeline" in selected:
        try:
            result = run_qc_pipeline(
                data=df,
                station_id=req.station_id,
                station_col=req.station_col,
                units_map=req.units_map,
                frequency=req.frequency,
                variable_cols=effective_vars or None,
                year_col=req.year_col,
                month_col=req.month_col,
                day_col=req.day_col,
                hour_col=req.hour_col,
                minute_col=req.minute_col,
                outpath=None,
                bplot=False,
            )
            all_flags = result.get("all_flags", pd.DataFrame())
            summary = result.get("summary", {})
            summary_rows = (
                summary.reset_index().to_dict(orient="records")
                if hasattr(summary, "reset_index")
                else summary
            )
            results_by_test["run_qc_pipeline"] = {
                "flag_count": int(len(all_flags)),
                "flag_preview": _to_json_rows(all_flags, max_rows=500),
                "summary": summary_rows,
            }
            if not all_flags.empty:
                all_flags_parts.append(all_flags)
        except Exception as exc:
            logging.exception("run_qc_pipeline failed")
            warnings.append(f"run_qc_pipeline skipped: {exc}")

    # ---- individual tests ------------------------------------------------- #
    vars_ = effective_vars or []

    if "climatic_outliers" in selected:
        for var in vars_:
            try:
                flags = climatic_outliers(
                    data=df, var_name=var,
                    station_id=req.station_id,
                    units=req.units_map.get(var),
                    year_col=req.year_col, month_col=req.month_col, day_col=req.day_col,
                    station_col=req.station_col if req.station_col in df.columns else None,
                )
                _record(f"climatic_outliers[{var}]", flags)
            except Exception as exc:
                warnings.append(f"climatic_outliers[{var}] skipped: {exc}")

    if "daily_out_of_range" in selected:
        for var in [v for v in vars_ if v in DAILY_RANGE_SUPPORTED]:
            try:
                flags = daily_out_of_range(
                    data=df, var_name=var,
                    station_id=req.station_id,
                    units=req.units_map.get(var),
                    year_col=req.year_col, month_col=req.month_col, day_col=req.day_col,
                    station_col=req.station_col if req.station_col in df.columns else None,
                )
                _record(f"daily_out_of_range[{var}]", flags)
            except Exception as exc:
                warnings.append(f"daily_out_of_range[{var}] skipped: {exc}")

    if "temporal_coherence" in selected:
        for var in [v for v in vars_ if v in TEMPORAL_SUPPORTED]:
            try:
                flags = temporal_coherence(
                    data=df, var_name=var,
                    station_id=req.station_id,
                    units=req.units_map.get(var),
                    year_col=req.year_col, month_col=req.month_col, day_col=req.day_col,
                    station_col=req.station_col if req.station_col in df.columns else None,
                )
                _record(f"temporal_coherence[{var}]", flags)
            except Exception as exc:
                warnings.append(f"temporal_coherence[{var}] skipped: {exc}")

    if "daily_repetition" in selected:
        for var in vars_:
            try:
                flags = daily_repetition(
                    data=df, var_name=var,
                    station_id=req.station_id,
                    units=req.units_map.get(var),
                    year_col=req.year_col, month_col=req.month_col, day_col=req.day_col,
                    station_col=req.station_col if req.station_col in df.columns else None,
                )
                _record(f"daily_repetition[{var}]", flags)
            except Exception as exc:
                warnings.append(f"daily_repetition[{var}] skipped: {exc}")

    if "duplicate_dates" in selected:
        for var in vars_:
            try:
                flags = duplicate_dates(
                    data=df, var_name=var,
                    station_id=req.station_id,
                    year_col=req.year_col, month_col=req.month_col, day_col=req.day_col,
                    station_col=req.station_col if req.station_col in df.columns else None,
                )
                _record(f"duplicate_dates[{var}]", flags)
            except Exception as exc:
                warnings.append(f"duplicate_dates[{var}] skipped: {exc}")

    if "internal_consistency" in selected:
        ic = req.internal_consistency_req
        if ic.enabled and ic.var_x and ic.var_y:
            try:
                flags = internal_consistency(
                    data=df, var_x=ic.var_x, var_y=ic.var_y,
                    station_id=req.station_id,
                    units_x=ic.units_x, units_y=ic.units_y,
                    year_col=req.year_col, month_col=req.month_col, day_col=req.day_col,
                )
                _record("internal_consistency", flags)
            except Exception as exc:
                warnings.append(f"internal_consistency skipped: {exc}")
        else:
            warnings.append("internal_consistency skipped: var_x and var_y required")

    if "subdaily_out_of_range" in selected:
        for var in [v for v in vars_ if v in SUBDAILY_RANGE_SUPPORTED]:
            try:
                flags = subdaily_out_of_range(
                    data=df, var_name=var,
                    station_id=req.station_id,
                    units=req.units_map.get(var),
                    year_col=req.year_col, month_col=req.month_col, day_col=req.day_col,
                    hour_col=req.hour_col, minute_col=req.minute_col,
                )
                _record(f"subdaily_out_of_range[{var}]", flags)
            except Exception as exc:
                warnings.append(f"subdaily_out_of_range[{var}] skipped: {exc}")

    if "subdaily_repetition" in selected:
        for var in vars_:
            try:
                flags = subdaily_repetition(
                    data=df, var_name=var,
                    station_id=req.station_id,
                    units=req.units_map.get(var),
                    year_col=req.year_col, month_col=req.month_col, day_col=req.day_col,
                    hour_col=req.hour_col, minute_col=req.minute_col,
                )
                _record(f"subdaily_repetition[{var}]", flags)
            except Exception as exc:
                warnings.append(f"subdaily_repetition[{var}] skipped: {exc}")

    if "duplicate_times" in selected:
        for var in vars_:
            try:
                flags = duplicate_times(
                    data=df, var_name=var,
                    station_id=req.station_id,
                    year_col=req.year_col, month_col=req.month_col, day_col=req.day_col,
                    hour_col=req.hour_col, minute_col=req.minute_col,
                )
                _record(f"duplicate_times[{var}]", flags)
            except Exception as exc:
                warnings.append(f"duplicate_times[{var}] skipped: {exc}")

    # ---- WMO gross errors ------------------------------------------------- #
    if req.run_wmo.enabled or "wmo_gross_errors" in selected:
        w = req.run_wmo
        if not w.input_column or not w.wmo_code:
            warnings.append("wmo_gross_errors skipped: input_column and wmo_code required")
        elif w.input_column not in df.columns:
            warnings.append(f"wmo_gross_errors skipped: column '{w.input_column}' not found")
        else:
            wdf = df.rename(columns={w.input_column: w.wmo_code})
            try:
                flags = wmo_gross_errors(
                    data=wdf, var_name=w.wmo_code,
                    station_id=req.station_id,
                    units=w.units,
                    station_col=w.station_col,
                    lat_col=w.lat_col,
                    lat=w.lat,
                    year_col=req.year_col, month_col=req.month_col, day_col=req.day_col,
                    hour_col=req.hour_col, minute_col=req.minute_col,
                )
                _record("wmo_gross_errors", flags)
            except Exception as exc:
                warnings.append(f"wmo_gross_errors skipped: {exc}")

    # ---- aggregate all flags and save output ------------------------------ #
    output_id: Optional[str] = None
    total_flags = 0
    dataset_label = Path(state.name).stem
    if all_flags_parts:
        combined = pd.concat(all_flags_parts, ignore_index=True)
        total_flags = int(len(combined))
        output_id = _save_output(combined, label=f"qc_flags_{dataset_label}")
    elif results_by_test:
        # Even when 0 flags, save a summary so the user gets a downloadable result
        summary_rows = []
        for tname, tdata in results_by_test.items():
            summary_rows.append({"test": tname, "flag_count": tdata.get("flag_count", 0)})
        summary_df = pd.DataFrame(summary_rows)
        output_id = _save_output(summary_df, label=f"qc_summary_{dataset_label}")

    return {
        "results": results_by_test,
        "total_flags": total_flags,
        "output_id": output_id,
        "notes": time_notes,
        "warnings": warnings,
    }


# =========================================================================== #
# API – outputs
# =========================================================================== #

@app.get("/api/outputs")
def list_outputs() -> dict[str, Any]:
    outputs = []
    for oid, s in OUTPUTS.items():
        if not Path(s.filepath).exists():
            continue
        outputs.append({
            "output_id": oid,
            "name": s.label,
            "filename": s.filename,
            "download_url": f"/api/outputs/{oid}/download",
        })
    return {"outputs": outputs}


@app.get("/api/outputs/{output_id}/download")
def download_output(output_id: str) -> FileResponse:
    out = OUTPUTS.get(output_id)
    if out is None or not Path(out.filepath).exists():
        raise HTTPException(status_code=404, detail="Output not found")
    return FileResponse(out.filepath, media_type=out.media_type, filename=out.filename)


@app.delete("/api/outputs/{output_id}")
def delete_output(output_id: str) -> dict[str, str]:
    out = OUTPUTS.pop(output_id, None)
    if out is None:
        raise HTTPException(status_code=404, detail="Output not found")
    Path(out.filepath).unlink(missing_ok=True)
    return {"status": "deleted", "id": output_id}

