const statusEl = document.getElementById("status");
const datasetInput = document.getElementById("datasetId");
datasetInput.value = window.qcStudio.getDatasetId();

function selectedTests() {
  return Array.from(document.querySelectorAll("input[name='qc_test']:checked")).map((x) => x.value);
}

async function loadTests() {
  const res = await fetch("/api/qc/tests");
  const data = await res.json();
  const box = document.getElementById("testsContainer");
  box.innerHTML = "";

  // Group label → list of tests
  const groups = [
    { label: "Daily tests", tests: data.daily || [] },
    { label: "Sub-daily tests", tests: data.subdaily || [] },
    { label: "Other", tests: data.other || [] },
  ];

  groups.forEach(({ label, tests }) => {
    if (!tests.length) return;
    const heading = document.createElement("p");
    heading.className = "tests-group-label";
    heading.textContent = label;
    box.appendChild(heading);

    tests.forEach((t) => {
      const lbl = document.createElement("label");
      lbl.className = "test-item";
      lbl.innerHTML = `<input type='checkbox' name='qc_test' value='${t}' /> ${t}`;
      box.appendChild(lbl);
    });
  });
}

document.getElementById("runQcBtn").addEventListener("click", async () => {
  try {
    const datasetId = window.qcStudio.getDatasetId();
    if (!datasetId) {
      window.qcStudio.setStatus(statusEl, "No dataset loaded. Go to Data page first.", true);
      return;
    }

    const tests = selectedTests();
    const wmoLatRaw = document.getElementById("wmoLat").value;
    const wmoInput = document.getElementById("wmoInput").value || null;
    const wmoCode = document.getElementById("wmoCode").value || null;
    const wmoEnabled = tests.includes("wmo_gross_errors") && !!wmoInput && !!wmoCode;

    const payload = {
      frequency: document.getElementById("frequency").value,
      station_col: document.getElementById("stationCol").value || null,
      variable_cols: window.qcStudio.parseCsvList(document.getElementById("variables").value),
      selected_tests: tests,
      run_pipeline: tests.includes("run_qc_pipeline"),
      run_wmo: {
        enabled: wmoEnabled,
        input_column: wmoInput,
        wmo_code: wmoCode,
        station_col: document.getElementById("stationCol").value || null,
        lat_col: document.getElementById("wmoLatCol").value || null,
        lat: wmoLatRaw ? Number(wmoLatRaw) : null,
      },
    };

    window.qcStudio.setStatus(statusEl, "Running QC tests…");
    const res = await fetch(`/api/datasets/${datasetId}/run-qc`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!res.ok) throw new Error(await res.text());

    const out = await res.json();

    // Collect all flag rows from results dict
    const rows = [];
    if (out.results) {
      Object.entries(out.results).forEach(([testKey, entry]) => {
        if (entry.flag_preview) {
          entry.flag_preview.forEach((r) => rows.push({ _test: testKey, ...r }));
        }
      });
    }

    document.getElementById("qcSummary").textContent =
      `Total flags: ${out.total_flags ?? rows.length}` +
      (out.output_id ? `  ·  Output saved (id: ${out.output_id})` : "");

    window.qcStudio.renderTable(document.getElementById("qcTable"), rows.slice(0, 500));

    // Show notes (informational) and warnings (errors) separately
    const infoNotes = out.notes && out.notes.length > 0 ? out.notes.join(" | ") : "";
    if (out.warnings && out.warnings.length > 0) {
      const msg = (infoNotes ? infoNotes + " · " : "") +
        `Warnings: ${out.warnings.join(" | ")}`;
      window.qcStudio.setStatus(statusEl, msg, true);
    } else {
      const msg = infoNotes
        ? `QC completed. ${infoNotes}`
        : "QC completed. Check Outputs page for generated files.";
      window.qcStudio.setStatus(statusEl, msg);
    }
  } catch (err) {
    window.qcStudio.setStatus(statusEl, `QC failed: ${err.message}`, true);
  }
});

loadTests().catch((e) => window.qcStudio.setStatus(statusEl, `Cannot load tests: ${e.message}`, true));

