const api = {
  datasetId: null,
};

const statusEl = document.getElementById("status");
const datasetBadge = document.getElementById("datasetBadge");

function setStatus(msg, isError = false) {
  statusEl.textContent = msg;
  statusEl.style.color = isError ? "#a11c1c" : "#547164";
}

function parseCsvList(text) {
  return text
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function renderTable(tableEl, rows) {
  tableEl.innerHTML = "";
  if (!rows || rows.length === 0) {
    tableEl.innerHTML = "<tr><td>No rows</td></tr>";
    return;
  }

  const columns = Object.keys(rows[0]);
  const thead = document.createElement("thead");
  const headTr = document.createElement("tr");
  for (const col of columns) {
    const th = document.createElement("th");
    th.textContent = col;
    headTr.appendChild(th);
  }
  thead.appendChild(headTr);

  const tbody = document.createElement("tbody");
  for (const row of rows) {
    const tr = document.createElement("tr");
    for (const col of columns) {
      const td = document.createElement("td");
      td.textContent = row[col] == null ? "" : String(row[col]);
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }

  tableEl.appendChild(thead);
  tableEl.appendChild(tbody);
}

async function refreshPreview() {
  if (!api.datasetId) return;
  const res = await fetch(`/api/datasets/${api.datasetId}/preview?limit=120`);
  if (!res.ok) {
    throw new Error(await res.text());
  }
  const data = await res.json();
  document.getElementById("previewMeta").textContent =
    `${data.name} | ${data.rows} rows | ${data.columns.length} columns`;
  renderTable(document.getElementById("previewTable"), data.preview);
}

document.getElementById("uploadBtn").addEventListener("click", async () => {
  try {
    const input = document.getElementById("fileInput");
    if (!input.files || input.files.length === 0) {
      setStatus("Select a file first.", true);
      return;
    }

    const fd = new FormData();
    fd.append("file", input.files[0]);

    setStatus("Uploading dataset...");
    const res = await fetch("/api/datasets/upload", {
      method: "POST",
      body: fd,
    });
    if (!res.ok) {
      throw new Error(await res.text());
    }

    const data = await res.json();
    api.datasetId = data.dataset_id;
    datasetBadge.textContent = `Loaded: ${data.name}`;
    await refreshPreview();
    setStatus("Dataset uploaded successfully.");
  } catch (err) {
    setStatus(`Upload failed: ${err.message}`, true);
  }
});

document.getElementById("fillBtn").addEventListener("click", async () => {
  try {
    if (!api.datasetId) {
      setStatus("Upload a dataset first.", true);
      return;
    }

    const payload = {
      method: document.getElementById("fillMethod").value,
      columns: parseCsvList(document.getElementById("fillCols").value),
      group_by: parseCsvList(document.getElementById("fillGroup").value),
      sort_by: ["Year", "Month", "Day"],
    };

    setStatus("Filling missing values...");
    const res = await fetch(`/api/datasets/${api.datasetId}/fill-missing`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      throw new Error(await res.text());
    }

    const data = await res.json();
    await refreshPreview();
    setStatus(`Fill completed. Remaining NA counts: ${JSON.stringify(data.na_counts_after)}`);
  } catch (err) {
    setStatus(`Fill failed: ${err.message}`, true);
  }
});

document.getElementById("runQcBtn").addEventListener("click", async () => {
  try {
    if (!api.datasetId) {
      setStatus("Upload a dataset first.", true);
      return;
    }

    const wmoEnabled = document.getElementById("wmoEnabled").checked;
    const payload = {
      frequency: document.getElementById("frequency").value,
      station_col: document.getElementById("stationCol").value || null,
      variable_cols: parseCsvList(document.getElementById("variables").value),
      run_pipeline: true,
      run_wmo: {
        enabled: wmoEnabled,
        input_column: document.getElementById("wmoInput").value || null,
        wmo_code: document.getElementById("wmoCode").value || null,
        units: "C",
        station_col: document.getElementById("stationCol").value || null,
        lat_col: document.getElementById("wmoLatCol").value || null,
      },
    };

    setStatus("Running QC...");
    const res = await fetch(`/api/datasets/${api.datasetId}/run-qc`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      throw new Error(await res.text());
    }

    const data = await res.json();
    const pipelineCount = data.pipeline ? data.pipeline.flag_count : 0;
    const wmoCount = data.wmo ? data.wmo.flag_count : 0;

    document.getElementById("qcSummary").textContent =
      `Pipeline flags: ${pipelineCount} | WMO flags: ${wmoCount}`;

    let rows = [];
    if (data.pipeline && data.pipeline.flag_preview) {
      rows = rows.concat(data.pipeline.flag_preview);
    }
    if (data.wmo && data.wmo.flag_preview) {
      rows = rows.concat(data.wmo.flag_preview);
    }
    renderTable(document.getElementById("qcTable"), rows);

    if (Array.isArray(data.warnings) && data.warnings.length > 0) {
      setStatus(`QC completed with warnings: ${data.warnings.join(" | ")}`, true);
    } else {
      setStatus("QC completed.");
    }
  } catch (err) {
    setStatus(`QC failed: ${err.message}`, true);
  }
});

document.getElementById("exportBtn").addEventListener("click", async () => {
  try {
    if (!api.datasetId) {
      setStatus("Upload a dataset first.", true);
      return;
    }

    const format = document.getElementById("exportFormat").value;
    setStatus(`Exporting as ${format}...`);

    const res = await fetch(`/api/datasets/${api.datasetId}/export`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ format }),
    });
    if (!res.ok) {
      throw new Error(await res.text());
    }

    const blob = await res.blob();
    const href = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = href;
    a.download = `dataset_export.${format === "netcdf" ? "nc" : format}`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(href);

    setStatus("Export ready.");
  } catch (err) {
    setStatus(`Export failed: ${err.message}`, true);
  }
});
