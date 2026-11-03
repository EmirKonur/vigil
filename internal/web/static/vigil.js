// Small enhancements on top of server-rendered pages. Everything here is
// optional: without it, pages still render, links work, and every value is
// readable from tables and labels.
(function () {
  "use strict";
  var root = document.documentElement;

  // Theme toggle ---------------------------------------------------------
  function effectiveTheme() {
    if (root.dataset.theme) return root.dataset.theme;
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }
  function toggleTheme() {
    var next = effectiveTheme() === "dark" ? "light" : "dark";
    root.dataset.theme = next;
    try { localStorage.setItem("vigil-theme", next); } catch (e) { /* not persisted */ }
  }
  document.addEventListener("click", function (e) {
    var t = e.target.closest("[data-action]");
    if (!t) return;
    if (t.dataset.action === "theme") toggleTheme();
    if (t.dataset.action === "keys") openKeys();
  });

  // Keyboard shortcuts ---------------------------------------------------
  var keys = document.getElementById("keys");
  function openKeys() { if (keys && !keys.open) keys.showModal(); }
  var pendingG = 0;
  var jumps = { o: "/admin", i: "/admin/incidents", p: "/admin/probes" };
  document.addEventListener("keydown", function (e) {
    if (e.metaKey || e.ctrlKey || e.altKey) return;
    var el = document.activeElement;
    if (el && (el.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName))) {
      if (e.key === "Escape") el.blur();
      return;
    }
    if (pendingG && Date.now() - pendingG < 1200 && jumps[e.key] && document.body.dataset.admin) {
      window.location.href = jumps[e.key];
      pendingG = 0;
      return;
    }
    pendingG = 0;
    switch (e.key) {
      case "g": pendingG = Date.now(); break;
      case "t": toggleTheme(); break;
      case "?": openKeys(); break;
      case "/":
        var f = document.getElementById("filter");
        if (f) { e.preventDefault(); f.focus(); }
        break;
    }
  });

  // Live connection state (htmx SSE extension events) --------------------
  function setLive(state, text) {
    document.body.dataset.live = state;
    var label = document.querySelector(".live span");
    if (label) label.textContent = text;
  }
  document.body.addEventListener("htmx:sseOpen", function () { setLive("open", "Live"); });
  document.body.addEventListener("htmx:sseError", function () { setLive("error", "Reconnecting"); });
  window.addEventListener("offline", function () { setLive("error", "Offline"); });

  // Monitor filter -------------------------------------------------------
  function applyFilter() {
    var f = document.getElementById("filter");
    if (!f) return;
    var q = f.value.trim().toLowerCase();
    var shown = 0;
    document.querySelectorAll("tr[data-name]").forEach(function (tr) {
      var match = !q || tr.dataset.name.indexOf(q) !== -1;
      tr.hidden = !match;
      if (match) shown++;
    });
    var none = document.getElementById("filter-empty");
    if (none) none.hidden = shown !== 0;
  }
  document.addEventListener("input", function (e) { if (e.target.id === "filter") applyFilter(); });
  document.body.addEventListener("htmx:afterSwap", function () { applyFilter(); bindCharts(); });

  // Tooltip shared by charts and uptime bars ----------------------------
  var tip = document.createElement("div");
  tip.className = "tip";
  tip.hidden = true;
  tip.setAttribute("role", "status");
  document.body.appendChild(tip);
  function place(x, y) {
    var w = tip.offsetWidth, h = tip.offsetHeight;
    var left = Math.min(x + 14, window.scrollX + document.documentElement.clientWidth - w - 8);
    var top = y - h - 12 < window.scrollY ? y + 16 : y - h - 12;
    tip.style.left = Math.max(8, left) + "px";
    tip.style.top = top + "px";
  }
  function row(label, value, slot) {
    var r = document.createElement("div");
    r.className = "tip-row";
    var name = document.createElement("span");
    if (slot !== undefined) {
      var k = document.createElement("span");
      k.className = "tip-key";
      k.style.background = "var(--s" + (slot % 3) + ")";
      name.appendChild(k);
    }
    name.appendChild(document.createTextNode(label));
    var v = document.createElement("b");
    v.textContent = value;
    r.appendChild(name);
    r.appendChild(v);
    return r;
  }

  document.addEventListener("pointerover", function (e) {
    var bar = e.target.closest && e.target.closest("svg.bars [data-tip]");
    if (!bar) return;
    tip.replaceChildren(document.createTextNode(bar.dataset.tip));
    tip.hidden = false;
    var b = bar.getBoundingClientRect();
    place(b.left + window.scrollX, b.top + window.scrollY);
  });
  document.addEventListener("pointerout", function (e) {
    if (e.target.closest && e.target.closest("svg.bars [data-tip]")) tip.hidden = true;
  });

  // Latency chart crosshair ---------------------------------------------
  function fmtTime(ms) {
    var d = new Date(ms);
    var months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    var p = function (n) { return (n < 10 ? "0" : "") + n; };
    return months[d.getUTCMonth()] + " " + d.getUTCDate() + ", " + p(d.getUTCHours()) + ":" + p(d.getUTCMinutes()) + " UTC";
  }
  function fmtMS(v) { return v >= 100 ? Math.round(v) + " ms" : v.toFixed(1) + " ms"; }

  function bindChart(wrap) {
    if (wrap.dataset.bound) return;
    wrap.dataset.bound = "1";
    var data;
    try { data = JSON.parse(wrap.dataset.chart); } catch (e) { return; }
    var svg = wrap.querySelector("svg.chart");
    var hair = svg && svg.querySelector(".xhair");
    if (!svg || !hair || !data.series.length) return;
    var n = data.series[0].p50.length;
    var index = -1;

    function show(i, px, py) {
      index = Math.max(0, Math.min(n - 1, i));
      var x = data.x0 + (data.x1 - data.x0) * (index + 0.5) / n;
      hair.setAttribute("x1", x);
      hair.setAttribute("x2", x);
      hair.setAttribute("visibility", "visible");
      var head = document.createElement("div");
      head.className = "tip-time";
      head.textContent = fmtTime(data.start + index * data.step);
      var rows = [head];
      data.series.forEach(function (s) {
        var v = s.p50[index];
        var text = v == null ? (s.fail[index] ? "failed" : "no data") : fmtMS(v) + " / p95 " + fmtMS(s.p95[index]);
        if (v != null && s.fail[index]) text += ", some failed";
        rows.push(row(s.name, text, s.slot));
      });
      tip.replaceChildren.apply(tip, rows);
      tip.hidden = false;
      var b = svg.getBoundingClientRect();
      if (px === undefined) {
        px = b.left + window.scrollX + b.width * x / data.w;
        py = b.top + window.scrollY + 20;
      }
      place(px, py);
    }
    function hide() { hair.setAttribute("visibility", "hidden"); tip.hidden = true; }

    svg.addEventListener("pointermove", function (e) {
      var b = svg.getBoundingClientRect();
      var vx = (e.clientX - b.left) * data.w / b.width;
      if (vx < data.x0 || vx > data.x1) { hide(); return; }
      show(Math.floor((vx - data.x0) / (data.x1 - data.x0) * n), e.pageX, e.pageY);
    });
    svg.addEventListener("pointerleave", hide);
    svg.setAttribute("tabindex", "0");
    svg.addEventListener("keydown", function (e) {
      if (e.key === "ArrowRight" || e.key === "ArrowLeft") {
        e.preventDefault();
        show(index < 0 ? n - 1 : index + (e.key === "ArrowRight" ? 1 : -1));
      } else if (e.key === "Escape") {
        hide();
      }
    });
    svg.addEventListener("blur", hide);
  }
  function bindCharts() { document.querySelectorAll(".chart-wrap[data-chart]").forEach(bindChart); }
  bindCharts();
})();
