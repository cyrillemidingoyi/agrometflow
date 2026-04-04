const statusEl = document.getElementById("status");
const datasetBadge = document.getElementById("datasetBadge");

function clearPreview() {
  document.getElementById("previewMeta").textContent = "";
  window.qcStudio.renderTable(document.getElementById("previewTable"), []);
  datasetBadge.textContent = "No dataset";
}

async function refreshPreview() {
  const datasetId = window.qcStudio.getDatasetId();
  if (!datasetId) return;
  const res = await fetch(`/api/datasets/${datasetId}/preview?limit=120`);
  if (!res.ok) throw new Error(await res.text());
  const data = await res.json();
  document.getElementById("previewMeta").textContent = `${data.name} | ${data.rows} rows | ${data.columns.length} columns`;
  window.qcStudio.renderTable(document.getElementById("previewTable"), data.preview);
  datasetBadge.textContent = `Loaded: ${data.name}`;
}

document.getElementById("uploadBtn").addEventListener("click", async () => {
  try {
    const input = document.getElementById("fileInput");
    if (!input.files || input.files.length === 0) {
      window.qcStudio.setStatus(statusEl, "Select a file first.", true);
      return;
    }

    const fd = new FormData();
    fd.append("file", input.files[0]);
    window.qcStudio.setStatus(statusEl, "Uploading dataset...");

    const res = await fetch("/api/datasets/upload", { method: "POST", body: fd });
    if (!res.ok) throw new Error(await res.text());
    const data = await res.json();

    window.qcStudio.setDatasetId(data.dataset_id);
    datasetBadge.textContent = `Loaded: ${data.name}`;
    await refreshPreview();
    window.qcStudio.setStatus(statusEl, "Dataset uploaded.");
  } catch (err) {
    window.qcStudio.setStatus(statusEl, `Upload failed: ${err.message}`, true);
  }
});

document.getElementById("fillBtn").addEventListener("click", async () => {
  try {
    const datasetId = window.qcStudio.getDatasetId();
    if (!datasetId) {
      window.qcStudio.setStatus(statusEl, "Upload a dataset first.", true);
      return;
    }

    const payload = {
      method: document.getElementById("fillMethod").value,
      columns: window.qcStudio.parseCsvList(document.getElementById("fillCols").value),
      group_by: window.qcStudio.parseCsvList(document.getElementById("fillGroup").value),
      sort_by: ["Year", "Month", "Day"],
    };

    window.qcStudio.setStatus(statusEl, "Filling missing values...");
    const res = await fetch(`/api/datasets/${datasetId}/fill-missing`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!res.ok) throw new Error(await res.text());

    const data = await res.json();
    await refreshPreview();
    window.qcStudio.setStatus(statusEl, `Fill done. Output saved: ${data.output.output_id}`);
  } catch (err) {
    window.qcStudio.setStatus(statusEl, `Fill failed: ${err.message}`, true);
  }
});

document.getElementById("exportBtn").addEventListener("click", async () => {
  try {
    const datasetId = window.qcStudio.getDatasetId();
    if (!datasetId) {
      window.qcStudio.setStatus(statusEl, "Upload a dataset first.", true);
      return;
    }

    const format = document.getElementById("exportFormat").value;
    const res = await fetch(`/api/datasets/${datasetId}/export`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ format }),
    });
    if (!res.ok) throw new Error(await res.text());

    const out = await res.json();
    window.open(out.download_url, "_blank");
    window.qcStudio.setStatus(statusEl, `Export created: ${out.output_id}`);
  } catch (err) {
    window.qcStudio.setStatus(statusEl, `Export failed: ${err.message}`, true);
  }
});

// Always start the Data page in a clean state to avoid showing stale preview data.
window.qcStudio.clearDatasetId();
clearPreview();
window.qcStudio.setStatus(statusEl, "No dataset loaded yet.");
