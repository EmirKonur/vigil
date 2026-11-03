// Loaded blocking in <head> so a stored theme applies before first paint.
(function () {
  try {
    var t = localStorage.getItem("vigil-theme");
    if (t === "light" || t === "dark") document.documentElement.dataset.theme = t;
  } catch (e) {
    // Storage can be unavailable (private mode); the OS preference applies.
  }
})();
