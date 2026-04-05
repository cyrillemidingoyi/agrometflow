function setupZoomableImages() {
  var modal = document.getElementById("zoom-modal");

  if (!modal) {
    modal = document.createElement("div");
    modal.id = "zoom-modal";
    modal.className = "zoom-modal";
    modal.innerHTML = '<img alt="Image agrandie" />';
    document.body.appendChild(modal);

    modal.addEventListener("click", function () {
      modal.classList.remove("is-open");
    });

    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape") {
        modal.classList.remove("is-open");
      }
    });
  }

  var modalImage = modal.querySelector("img");
  var zoomables = document.querySelectorAll("img.zoomable");

  zoomables.forEach(function (img) {
    if (img.dataset.zoomBound === "1") {
      return;
    }

    img.dataset.zoomBound = "1";
    img.addEventListener("click", function () {
      modalImage.src = img.currentSrc || img.src;
      modalImage.alt = img.alt || "Image agrandie";
      modal.classList.add("is-open");
    });
  });
}

if (typeof window.document$ !== "undefined") {
  window.document$.subscribe(function () {
    setupZoomableImages();
  });
} else {
  document.addEventListener("DOMContentLoaded", setupZoomableImages);
}
