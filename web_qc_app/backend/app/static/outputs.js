const statusEl = document.getElementById("status");

async function refreshOutputs() {
  const res = await fetch("/api/outputs");
  if (!res.ok) throw new Error(await res.text());
  const data = await res.json();
  const list = data.outputs || [];

  const table = document.getElementById("outputsTable");
  if (list.length === 0) {
    table.innerHTML = "<tr><td>No outputs yet. Run QC or export a dataset first.</td></tr>";
    window.qcStudio.setStatus(statusEl, "No outputs.");
    return;
  }

  // Build simple display rows
  const displayRows = list.map((o) => ({
    name: o.name,
    filename: o.filename,
    download: "",
    delete: "",
  }));

  window.qcStudio.renderTable(table, displayRows);

  // Wire download links and delete buttons into the last 2 columns
  table.querySelectorAll("tbody tr").forEach((tr, idx) => {
    const row = list[idx];
    const downloadTd = tr.children[2];
    const deleteTd = tr.children[3];

    const a = document.createElement("a");
    a.href = row.download_url;
    a.textContent = "Download";
    a.className = "btn primary";
    a.target = "_blank";
    downloadTd.innerHTML = "";
    downloadTd.appendChild(a);

    const btn = document.createElement("button");
    btn.textContent = "Delete";
    btn.className = "btn";
    btn.onclick = async () => {
      if (!confirm(`Delete '${row.name}'?`)) return;
      const del = await fetch(`/api/outputs/${row.output_id}`, { method: "DELETE" });
      if (!del.ok) {
        window.qcStudio.setStatus(statusEl, `Delete failed: ${await del.text()}`, true);
        return;
      }
      await refreshOutputs();
      window.qcStudio.setStatus(statusEl, "Output deleted.");
    };
    deleteTd.innerHTML = "";
    deleteTd.appendChild(btn);
  });

  window.qcStudio.setStatus(statusEl, `${list.length} output(s) listed.`);
}

document.getElementById("refreshBtn").addEventListener("click", () => {
  refreshOutputs().catch((e) => window.qcStudio.setStatus(statusEl, `Error: ${e.message}`, true));
});

refreshOutputs().catch((e) => window.qcStudio.setStatus(statusEl, `Error: ${e.message}`, true));
