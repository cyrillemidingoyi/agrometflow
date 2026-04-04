window.qcStudio = {
  getDatasetId() {
    return localStorage.getItem("qc_dataset_id") || "";
  },
  setDatasetId(id) {
    localStorage.setItem("qc_dataset_id", id);
  },
  clearDatasetId() {
    localStorage.removeItem("qc_dataset_id");
  },
  parseCsvList(text) {
    return (text || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  },
  setStatus(el, msg, isError = false) {
    if (!el) return;
    el.textContent = msg;
    el.style.color = isError ? "#a11c1c" : "#547164";
  },
  renderTable(tableEl, rows) {
    tableEl.innerHTML = "";
    if (!rows || rows.length === 0) {
      tableEl.innerHTML = "<tr><td>No rows</td></tr>";
      return;
    }
    const cols = Object.keys(rows[0]);
    const thead = document.createElement("thead");
    const trh = document.createElement("tr");
    cols.forEach((c) => {
      const th = document.createElement("th");
      th.textContent = c;
      trh.appendChild(th);
    });
    thead.appendChild(trh);

    const tbody = document.createElement("tbody");
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      cols.forEach((c) => {
        const td = document.createElement("td");
        td.textContent = row[c] == null ? "" : String(row[c]);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    tableEl.appendChild(thead);
    tableEl.appendChild(tbody);
  },
};
