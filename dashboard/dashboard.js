const API_BASE = "http://localhost:8000/api";

const state = {
  count: { balls: null, strikes: null, outs: null, sort: "xwoba", batterHand: "all", pitcherHand: "all" },
  batter: { selectedId: null, window: "season", balls: 0, strikes: 0 },
  pitcher: { selectedId: null, window: "season", balls: 0, strikes: 0 },
  leaderboard: { type: "batting", window: "season", balls: null, strikes: null, outs: null, sort: "xwoba", limit: 10, season: null, minPa: 25 },
  sequence: { pitches: [], playerType: "pitcher", selectedId: null },
  scope: { mode: "single", season: null, seasonStart: null, seasonEnd: null },
  season: null,
  latestGameDate: null,
  earliestSeason: null,
  players: { batters: [], pitchers: [] },
  page: "count",
};

const cache = {
  countMatrix: null,
  batterOverview: new Map(),
  pitcherOverview: new Map(),
};

let outcomeChart;
const apiErrors = {};

function showApiError(source, message) {
  apiErrors[source] = message;
  const banner = document.getElementById("api-error-banner");
  const body = document.getElementById("api-error-message");
  const entries = Object.entries(apiErrors).map(([key, value]) => `<div>${key}: ${value}</div>`).join("");
  body.innerHTML = entries;
  banner.style.display = entries ? "block" : "none";
}

function clearApiError(source) {
  delete apiErrors[source];
  const banner = document.getElementById("api-error-banner");
  const body = document.getElementById("api-error-message");
  const entries = Object.entries(apiErrors).map(([key, value]) => `<div>${key}: ${value}</div>`).join("");
  body.innerHTML = entries;
  banner.style.display = entries ? "block" : "none";
}

async function apiFetch(path, source) {
  try {
    const response = await fetch(`${API_BASE}${path}`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    clearApiError(source);
    return await response.json();
  } catch (error) {
    showApiError(source, error.message);
    throw error;
  }
}

function fmtPct(value, digits = 1) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return `${(Number(value) * 100).toFixed(digits)}%`;
}

function fmtRate(value, digits = 3) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return Number(value).toFixed(digits);
}

function fmtNum(value, digits = 0) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return Number(value).toFixed(digits);
}

function calcSlash(summary) {
  const atBats = Number(summary.at_bats || 0);
  const hits = Number(summary.hits || 0);
  const walks = Number(summary.walks || 0);
  const totalBases = Number(summary.total_bases || 0);
  return {
    avg: atBats ? (hits / atBats) : 0,
    obp: (Number(summary.pa || 0)) ? ((hits + walks) / Number(summary.pa || 0)) : 0,
    slg: atBats ? (totalBases / atBats) : 0,
  };
}

function buildQuery(params) {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== null && value !== undefined && value !== "") {
      search.set(key, value);
    }
  });
  const query = search.toString();
  return query ? `?${query}` : "";
}

function buildCountGrid(containerId, stateRef, onSelect) {
  const grid = document.getElementById(containerId);
  if (!grid) return;
  grid.innerHTML = "";

  const corner = document.createElement("div");
  corner.className = "count-cell hdr";
  grid.appendChild(corner);

  for (let b = 0; b <= 3; b += 1) {
    const head = document.createElement("div");
    head.className = "count-cell hdr";
    head.textContent = `${b}B`;
    grid.appendChild(head);
  }

  for (let s = 0; s <= 2; s += 1) {
    const rowHead = document.createElement("div");
    rowHead.className = "count-cell hdr";
    rowHead.textContent = `${s}S`;
    grid.appendChild(rowHead);

    for (let b = 0; b <= 3; b += 1) {
      const cell = document.createElement("div");
      const active = stateRef.balls === b && stateRef.strikes === s;
      cell.className = `count-cell${active ? " active" : ""}`;
      cell.textContent = `${b}-${s}`;
      cell.addEventListener("click", () => {
        stateRef.balls = b;
        stateRef.strikes = s;
        onSelect();
      });
      grid.appendChild(cell);
    }
  }
}

function buildZoneChart(containerId, data, colorVar) {
  const grid = document.getElementById(containerId);
  if (!grid) return;
  grid.innerHTML = "";
  const flat = data.flat();
  const max = Math.max(1, ...flat);
  data.forEach((row) => {
    row.forEach((value) => {
      const cell = document.createElement("div");
      cell.className = "zone-cell";
      const pct = value / max;
      const rgb = colorVar === "accent2" ? "232,93,58" : "91,143,255";
      cell.style.background = `rgba(${rgb},${0.08 + pct * 0.72})`;
      cell.style.color = pct > 0.55 ? "#fff" : "var(--muted)";
      cell.textContent = value ? `${value}` : "0";
      grid.appendChild(cell);
    });
  });
}

function populatePlayerLists() {
  const batterList = document.getElementById("batter-list");
  const pitcherList = document.getElementById("pitcher-list");
  const seqList = document.getElementById("seq-player-list");
  batterList.innerHTML = state.players.batters.map((player) => `<option value="${playerOptionLabel(player)}"></option>`).join("");
  pitcherList.innerHTML = state.players.pitchers.map((player) => `<option value="${playerOptionLabel(player)}"></option>`).join("");
  seqList.innerHTML = state.players.pitchers.map((player) => `<option value="${playerOptionLabel(player)}"></option>`).join("");
}

function playerOptionLabel(player) {
  return player?.team ? `${player.name} · ${player.team}` : player?.name || "";
}

function lookupPlayerByName(role, name) {
  const players = role === "batter" ? state.players.batters : state.players.pitchers;
  return players.find((player) => {
    const optionLabel = playerOptionLabel(player);
    return player.name === name || optionLabel === name;
  }) || null;
}

function lookupPlayerById(role, id) {
  const players = role === "batter" ? state.players.batters : state.players.pitchers;
  return players.find((player) => player.id === id) || null;
}

function playerName(role, id) {
  return lookupPlayerById(role, id)?.name || `${role === "batter" ? "Batter" : "Pitcher"} #${id}`;
}

function playerLabel(role, id, fallbackName = null) {
  const player = lookupPlayerById(role, id);
  const name = player?.name || fallbackName || playerName(role, id);
  const team = player?.team;
  return team ? `${name} · ${team}` : name;
}

function selectedSeason() {
  return Number(document.getElementById("lb-season").value) || state.season;
}

function selectedSeasonStart() {
  return Number(document.getElementById("lb-season-start").value) || state.earliestSeason || state.season;
}

function selectedSeasonEnd() {
  return Number(document.getElementById("lb-season-end").value) || state.season;
}

function activeSeasonQuery(windowName = "season") {
  const mode = document.getElementById("scope-mode")?.value || state.scope.mode;
  if (windowName === "last7" || windowName === "career") {
    return { window: windowName };
  }

  if (mode === "range") {
    const start = Math.min(selectedSeasonStart(), selectedSeasonEnd());
    const end = Math.max(selectedSeasonStart(), selectedSeasonEnd());
    return { window: "season", season_start: start, season_end: end };
  }

  return { window: "season", season: selectedSeason() };
}

function activeSeasonLabel(windowName = "season") {
  const mode = document.getElementById("scope-mode")?.value || state.scope.mode;
  if (windowName === "career") return "All loaded seasons";
  if (windowName === "last7") return "Last 7 days";
  if (mode === "range") {
    const start = Math.min(selectedSeasonStart(), selectedSeasonEnd());
    const end = Math.max(selectedSeasonStart(), selectedSeasonEnd());
    return `${start}-${end}`;
  }
  return `${selectedSeason()}`;
}

function syncScopeControls() {
  const mode = document.getElementById("scope-mode").value;
  state.scope.mode = mode;
  document.getElementById("single-season-wrap").style.display = mode === "single" ? "flex" : "none";
  document.getElementById("range-season-wrap").style.display = mode === "range" ? "flex" : "none";
  document.getElementById("season-scope-label").textContent = activeSeasonLabel("season");
}

async function loadMetaContext() {
  const context = await apiFetch("/meta/context", "context");
  const hadScope = state.scope.season !== null || state.scope.seasonStart !== null || state.scope.seasonEnd !== null;
  const previousBatterId = state.batter.selectedId;
  const previousPitcherId = state.pitcher.selectedId;
  const previousSequenceId = state.sequence.selectedId;
  state.season = context.latest_season;
  state.latestGameDate = context.latest_game_date;
  state.earliestSeason = context.earliest_season;
  state.players.batters = context.batters || [];
  state.players.pitchers = context.pitchers || [];
  if (!hadScope) {
    state.scope.mode = context.earliest_season !== context.latest_season ? "range" : "single";
    state.scope.season = context.latest_season;
    state.scope.seasonStart = context.earliest_season;
    state.scope.seasonEnd = context.latest_season;
  }

  const seasonInputs = ["lb-season", "lb-season-start", "lb-season-end"];
  seasonInputs.forEach((id) => {
    const input = document.getElementById(id);
    input.min = context.earliest_season || 2015;
    input.max = context.latest_season || new Date().getFullYear();
  });

  document.getElementById("scope-mode").value = state.scope.mode;
  document.getElementById("lb-season").value = state.scope.season || context.latest_season;
  document.getElementById("lb-season-start").value = state.scope.seasonStart || context.earliest_season;
  document.getElementById("lb-season-end").value = state.scope.seasonEnd || context.latest_season;
  populatePlayerLists();
  syncScopeControls();

  const activeBatter = lookupPlayerById("batter", previousBatterId) || state.players.batters[0] || null;
  if (activeBatter) {
    state.batter.selectedId = activeBatter.id;
    document.getElementById("batter-select").value = playerOptionLabel(activeBatter);
  }

  const activePitcher = lookupPlayerById("pitcher", previousPitcherId) || state.players.pitchers[0] || null;
  if (activePitcher) {
    state.pitcher.selectedId = activePitcher.id;
    document.getElementById("pitcher-select").value = playerOptionLabel(activePitcher);
  }

  const activeSequencePitcher = lookupPlayerById("pitcher", previousSequenceId) || activePitcher;
  if (activeSequencePitcher) {
    state.sequence.selectedId = activeSequencePitcher.id;
    document.getElementById("seq-player-search").value = playerOptionLabel(activeSequencePitcher);
  }
}

async function fetchCountMatrix() {
  const query = buildQuery({
    ...activeSeasonQuery("season"),
    stand: state.count.batterHand,
    p_throws: state.count.pitcherHand,
  });
  const rows = await apiFetch(`/count-state/outcome-matrix${query}`, "count-matrix");
  cache.countMatrix = rows;
  return rows;
}

async function fetchCountZoneMap() {
  const query = buildQuery({
    balls: state.count.balls,
    strikes: state.count.strikes,
    outs: state.count.outs,
    ...activeSeasonQuery("season"),
    stand: state.count.batterHand,
    p_throws: state.count.pitcherHand,
  });
  return apiFetch(`/count-state/zone-map${query}`, "count-zone");
}

async function fetchCountLeaderboard() {
  const query = buildQuery({
    ...activeSeasonQuery("season"),
    limit: Number(document.getElementById("lb-limit").value) || state.leaderboard.limit,
    min_pa: Number(document.getElementById("lb-min-pa").value) || state.leaderboard.minPa,
    balls: state.count.balls,
    strikes: state.count.strikes,
    outs: state.count.outs,
    stand: state.count.batterHand,
    p_throws: state.count.pitcherHand,
  });
  const payload = state.count.balls === null || state.count.strikes === null
    ? await apiFetch(`/leaderboard/batting${query}`, "count-leaderboard")
    : (await apiFetch(`/count-state/batter-splits${query}`, "count-leaderboard")).results;
  return payload || [];
}

function buildOutcomeMatrix(rows) {
  const container = document.getElementById("outcome-matrix");
  container.innerHTML = "";

  const corner = document.createElement("div");
  corner.className = "matrix-axis-label";
  container.appendChild(corner);

  ["0B", "1B", "2B", "3B"].forEach((label) => {
    const head = document.createElement("div");
    head.className = "matrix-axis-label";
    head.style.cssText = "font-family:var(--mono);font-size:9px;color:var(--muted)";
    head.textContent = label;
    container.appendChild(head);
  });

  for (let s = 0; s <= 2; s += 1) {
    const head = document.createElement("div");
    head.className = "matrix-axis-label";
    head.style.cssText = "font-family:var(--mono);font-size:9px;color:var(--muted)";
    head.textContent = `${s}S`;
    container.appendChild(head);

    for (let b = 0; b <= 3; b += 1) {
      const row = rows.find((entry) => entry.balls === b && entry.strikes === s) || {};
      const value = Number(row.k_pct || 0);
      const active = state.count.balls === b && state.count.strikes === s;
      const cell = document.createElement("div");
      cell.className = "matrix-cell";
      cell.style.background = active ? "var(--accent)" : `rgba(91,143,255,${0.12 + value * 0.55})`;
      cell.style.color = active ? "#000" : "var(--text)";
      cell.innerHTML = `<span class="cell-val">${fmtPct(value, 0)}</span>`;
      cell.addEventListener("click", () => {
        state.count.balls = b;
        state.count.strikes = s;
        buildCountGrid("count-grid", state.count, renderCountPage);
        renderCountPage();
      });
      container.appendChild(cell);
    }
  }
}

function buildOutcomeChart(rows) {
  const ctx = document.getElementById("outcome-chart").getContext("2d");
  const ordered = [];
  for (let s = 0; s <= 2; s += 1) {
    for (let b = 0; b <= 3; b += 1) {
      ordered.push(rows.find((entry) => entry.balls === b && entry.strikes === s) || { count: `${b}-${s}`, k_pct: 0 });
    }
  }

  if (outcomeChart) outcomeChart.destroy();
  outcomeChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels: ordered.map((row) => row.count),
      datasets: [{
        data: ordered.map((row) => Number(row.k_pct || 0) * 100),
        backgroundColor: ordered.map((row) => (row.balls === state.count.balls && row.strikes === state.count.strikes ? "rgba(200,240,78,0.9)" : "rgba(91,143,255,0.4)")),
        borderRadius: 2,
        borderSkipped: false,
      }],
    },
    options: {
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: "#5a5f72", font: { family: "DM Mono", size: 9 } }, grid: { color: "#1f2332" } },
        y: { ticks: { color: "#5a5f72", font: { family: "DM Mono", size: 9 } }, grid: { color: "#1f2332" }, title: { display: true, text: "K%", color: "#5a5f72", font: { family: "DM Mono", size: 9 } } },
      },
    },
  });
}

async function renderCountPage() {
  buildCountGrid("count-grid", state.count, renderCountPage);
  const [matrix, zonePayload, leaderboard] = await Promise.all([
    fetchCountMatrix(),
    fetchCountZoneMap(),
    fetchCountLeaderboard(),
  ]);

  const selected = state.count.balls === null || state.count.strikes === null
    ? {
      count: "All",
      at_bats: matrix.reduce((sum, row) => sum + Number(row.at_bats || 0), 0),
      k_pct: matrix.reduce((sum, row) => sum + Number(row.k_pct || 0) * Number(row.at_bats || 0), 0) / Math.max(1, matrix.reduce((sum, row) => sum + Number(row.at_bats || 0), 0)),
      bb_pct: matrix.reduce((sum, row) => sum + Number(row.bb_pct || 0) * Number(row.at_bats || 0), 0) / Math.max(1, matrix.reduce((sum, row) => sum + Number(row.at_bats || 0), 0)),
      avg_xwoba: matrix.reduce((sum, row) => sum + Number(row.avg_xwoba || 0) * Number(row.at_bats || 0), 0) / Math.max(1, matrix.reduce((sum, row) => sum + Number(row.at_bats || 0), 0)),
    }
    : matrix.find((row) => row.balls === state.count.balls && row.strikes === state.count.strikes) || { count: `${state.count.balls}-${state.count.strikes}`, at_bats: 0, k_pct: 0, bb_pct: 0, avg_xwoba: 0 };

  document.getElementById("count-display").textContent = selected.count === "All" ? "All" : `${state.count.balls}–${state.count.strikes}`;
  document.getElementById("count-meta").innerHTML = `${state.count.outs === null ? "All outs" : `${state.count.outs} out${state.count.outs === 1 ? "" : "s"}`} · Instances: <span id="count-instances">${fmtNum(selected.at_bats, 0)}</span>`;
  document.getElementById("count-stat-cards").innerHTML = `
    <div class="stat-card">
      <div class="stat-label">League xwOBA</div>
      <div class="stat-value" style="color:var(--accent)">${fmtRate(selected.avg_xwoba)}</div>
      <div class="stat-delta">${fmtNum(selected.at_bats, 0)} tracked plate appearances</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Strikeout Rate</div>
      <div class="stat-value" style="color:var(--accent2)">${fmtPct(selected.k_pct, 1)}</div>
      <div class="stat-delta">final outcome after reaching this count</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Walk Rate</div>
      <div class="stat-value" style="color:var(--accent3)">${fmtPct(selected.bb_pct, 1)}</div>
      <div class="stat-delta">including intentional walks</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Handedness Split</div>
      <div class="stat-value sm">${state.count.batterHand}/${state.count.pitcherHand}</div>
      <div class="stat-delta">${activeSeasonLabel("season")}</div>
    </div>
  `;

  buildOutcomeMatrix(matrix);
  buildOutcomeChart(matrix);
  buildZoneChart("count-zone", zonePayload.zones || [[0, 0, 0], [0, 0, 0], [0, 0, 0]], "accent3");

  const sortField = state.count.sort;
  const sorted = [...leaderboard].sort((a, b) => Number(b[sortField] || 0) - Number(a[sortField] || 0));
  const maxValue = Math.max(1, ...sorted.map((row) => Number(row.xwoba || 0)));
  document.getElementById("count-lb-title").textContent = state.count.balls === null ? "Batter Leaderboard — All Counts" : `Batter Leaderboard — ${state.count.balls}-${state.count.strikes}`;
  document.getElementById("season-scope-label").textContent = activeSeasonLabel("season");
  document.getElementById("count-lb-body").innerHTML = sorted.map((row, index) => `
    <tr>
      <td class="td-rank">${index + 1}</td>
      <td class="td-name">${playerLabel("batter", row.batter_id, row.batter_name || row.name)}</td>
      <td class="td-stat" style="color:var(--muted)">${fmtNum(row.pa, 0)}</td>
      <td class="td-stat td-highlight">${fmtRate(row.xwoba)}</td>
      <td class="td-stat">${fmtRate(row.avg)}</td>
      <td class="td-stat" style="color:var(--accent2)">${fmtPct(row.k_pct || row.k, 1)}</td>
      <td class="td-stat" style="color:var(--muted)">${fmtPct(row.whiff_pct || 0, 1)}</td>
      <td class="td-bar-cell"><div class="bar-bg"><div class="bar-fill" style="width:${(Number(row.xwoba || 0) / maxValue) * 100}%"></div></div></td>
    </tr>
  `).join("");
}

async function fetchBatterOverview() {
  const seasonQuery = activeSeasonQuery(state.batter.window);
  const cacheKey = `${state.batter.selectedId}:${state.batter.window}:${JSON.stringify(seasonQuery)}`;
  if (cache.batterOverview.has(cacheKey)) return cache.batterOverview.get(cacheKey);
  const data = await apiFetch(`/batter/${state.batter.selectedId}/overview${buildQuery(seasonQuery)}`, "batter-overview");
  cache.batterOverview.set(cacheKey, data);
  return data;
}

async function renderBatterProfile() {
  const lookup = lookupPlayerByName("batter", document.getElementById("batter-select").value);
  if (lookup) state.batter.selectedId = lookup.id;
  if (!state.batter.selectedId) return;

  const profile = await fetchBatterOverview();
  const slash = calcSlash(profile.summary);
  document.getElementById("batter-stat-cards").innerHTML = `
    <div class="stat-card">
      <div class="stat-label">AVG / OBP / SLG</div>
      <div class="stat-value sm" style="color:var(--accent)">${fmtRate(slash.avg)} / ${fmtRate(slash.obp)} / ${fmtRate(slash.slg)}</div>
      <div class="stat-delta">${fmtNum(profile.summary.pa, 0)} PA in ${activeSeasonLabel(state.batter.window)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">xwOBA</div>
      <div class="stat-value" style="color:var(--accent)">${fmtRate(profile.summary.xwoba)}</div>
      <div class="stat-delta">expected production</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">K%</div>
      <div class="stat-value" style="color:var(--accent2)">${fmtPct(profile.summary.k_pct, 1)}</div>
      <div class="stat-delta">BB% ${fmtPct(profile.summary.bb_pct, 1)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Home Runs</div>
      <div class="stat-value">${fmtNum(profile.summary.hr, 0)}</div>
      <div class="stat-delta">${playerLabel("batter", state.batter.selectedId)}</div>
    </div>
  `;

  document.getElementById("batter-season-thead").innerHTML = `<tr><th>Season</th><th class="td-stat">PA</th><th class="td-stat">xwOBA</th><th class="td-stat">K%</th><th class="td-stat">BB%</th><th class="td-stat">HR</th></tr>`;
  document.getElementById("batter-season-body").innerHTML = profile.seasons.map((row) => `
    <tr>
      <td class="td-name">${row.season}</td>
      <td class="td-stat">${fmtNum(row.pa, 0)}</td>
      <td class="td-stat td-highlight">${fmtRate(row.xwoba)}</td>
      <td class="td-stat">${fmtPct(row.k_pct, 1)}</td>
      <td class="td-stat">${fmtPct(row.bb_pct, 1)}</td>
      <td class="td-stat">${fmtNum(row.hr, 0)}</td>
    </tr>
  `).join("");

  buildZoneChart("batter-zone", profile.zones || [[0, 0, 0], [0, 0, 0], [0, 0, 0]], "accent3");
  buildCountGrid("batter-count-grid", state.batter, renderBatterCountStats);
  renderBatterCountStats(profile);
}

function renderBatterCountStats(profileData = null) {
  const render = (profile) => {
    const selected = profile.counts.find((row) => row.balls === state.batter.balls && row.strikes === state.batter.strikes) || { pa: 0, avg: 0, xwoba: 0, k_pct: 0 };
    document.getElementById("batter-count-stats").innerHTML = `
      <div class="cstat"><div class="cstat-val">${fmtNum(selected.pa, 0)}</div><div class="cstat-lbl">PA</div></div>
      <div class="cstat"><div class="cstat-val">${fmtRate(selected.avg)}</div><div class="cstat-lbl">AVG</div></div>
      <div class="cstat"><div class="cstat-val">${fmtRate(selected.xwoba)}</div><div class="cstat-lbl">xwOBA</div></div>
      <div class="cstat"><div class="cstat-val">${fmtPct(selected.k_pct, 1)}</div><div class="cstat-lbl">K%</div></div>
    `;
    buildCountGrid("batter-count-grid", state.batter, renderBatterCountStats);
  };

  if (profileData) {
    render(profileData);
    return;
  }

  fetchBatterOverview().then(render);
}

async function fetchPitcherOverview() {
  const seasonQuery = activeSeasonQuery(state.pitcher.window);
  const cacheKey = `${state.pitcher.selectedId}:${state.pitcher.window}:${JSON.stringify(seasonQuery)}`;
  if (cache.pitcherOverview.has(cacheKey)) return cache.pitcherOverview.get(cacheKey);
  const data = await apiFetch(`/pitcher/${state.pitcher.selectedId}/overview${buildQuery(seasonQuery)}`, "pitcher-overview");
  cache.pitcherOverview.set(cacheKey, data);
  return data;
}

async function renderPitcherProfile() {
  const lookup = lookupPlayerByName("pitcher", document.getElementById("pitcher-select").value);
  if (lookup) state.pitcher.selectedId = lookup.id;
  if (!state.pitcher.selectedId) return;

  const profile = await fetchPitcherOverview();
  document.getElementById("pitcher-stat-cards").innerHTML = `
    <div class="stat-card">
      <div class="stat-label">Pitches</div>
      <div class="stat-value" style="color:var(--accent)">${fmtNum(profile.summary.pitches, 0)}</div>
      <div class="stat-delta">${playerLabel("pitcher", state.pitcher.selectedId)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Avg Velo</div>
      <div class="stat-value" style="color:var(--accent3)">${fmtNum(profile.summary.avg_velo, 1)}</div>
      <div class="stat-delta">Spin ${fmtNum(profile.summary.avg_spin, 0)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Whiff%</div>
      <div class="stat-value" style="color:var(--accent2)">${fmtPct(profile.summary.whiff_pct, 1)}</div>
      <div class="stat-delta">CSW ${fmtPct(profile.summary.csw_pct, 1)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">xwOBA Allowed</div>
      <div class="stat-value">${fmtRate(profile.summary.xwoba_allowed)}</div>
      <div class="stat-delta">K ${fmtPct(profile.summary.k_pct, 1)} · BB ${fmtPct(profile.summary.bb_pct, 1)}</div>
    </div>
  `;

  document.getElementById("pitcher-season-thead").innerHTML = `<tr><th>Season</th><th class="td-stat">Pitches</th><th class="td-stat">Velo</th><th class="td-stat">Spin</th><th class="td-stat">Whiff%</th></tr>`;
  document.getElementById("pitcher-season-body").innerHTML = profile.seasons.map((row) => `
    <tr>
      <td class="td-name">${row.season}</td>
      <td class="td-stat">${fmtNum(row.pitches, 0)}</td>
      <td class="td-stat">${fmtNum(row.avg_velo, 1)}</td>
      <td class="td-stat">${fmtNum(row.avg_spin, 0)}</td>
      <td class="td-stat">${fmtPct(row.whiff_pct, 1)}</td>
    </tr>
  `).join("");

  buildZoneChart("pitcher-zone", profile.zones || [[0, 0, 0], [0, 0, 0], [0, 0, 0]], "accent2");
  buildCountGrid("pitcher-count-grid", state.pitcher, renderPitcherCountStats);
  renderPitcherCountStats(profile);
}

function renderPitcherCountStats(profileData = null) {
  const render = (profile) => {
    const rows = profile.counts.filter((row) => row.balls === state.pitcher.balls && row.strikes === state.pitcher.strikes);
    const best = rows[0] || { whiff_pct: 0, avg_velo: 0 };
    const usage = rows.slice(0, 3).map((row) => `${row.pitch_type} ${fmtPct(row.usage_pct, 0)}`).join(" · ") || "—";
    document.getElementById("pitcher-count-stats").innerHTML = `
      <div class="cstat"><div class="cstat-val">${fmtPct(best.whiff_pct, 1)}</div><div class="cstat-lbl">Whiff%</div></div>
      <div class="cstat"><div class="cstat-val">${fmtNum(best.avg_velo, 1)}</div><div class="cstat-lbl">Avg Velo</div></div>
      <div class="cstat" style="flex:1;min-width:180px"><div class="cstat-val" style="font-size:13px;font-family:var(--mono);letter-spacing:.04em;color:var(--text)">${usage}</div><div class="cstat-lbl">Pitch Mix</div></div>
    `;
    buildCountGrid("pitcher-count-grid", state.pitcher, renderPitcherCountStats);
  };

  if (profileData) {
    render(profileData);
    return;
  }
  fetchPitcherOverview().then(render);
}

async function renderLeaderboard() {
  const isBatting = state.leaderboard.type === "batting";
  const sortField = state.leaderboard.sort;
  const seasonQuery = activeSeasonQuery(state.leaderboard.window);
  buildCountGrid("lb-count-grid", state.leaderboard, renderLeaderboard);

  let rows;
  if (isBatting) {
    rows = await apiFetch(`/leaderboard/batting${buildQuery({
      ...seasonQuery,
      limit: state.leaderboard.limit,
      min_pa: state.leaderboard.minPa,
      balls: state.leaderboard.balls,
      strikes: state.leaderboard.strikes,
      outs: state.leaderboard.outs,
    })}`, "lb-batting");
    rows = [...rows].sort((a, b) => Number(b[sortField] || 0) - Number(a[sortField] || 0));
    const maxValue = Math.max(1, ...rows.map((row) => Number(row.xwoba || 0)));
    document.getElementById("lb-thead").innerHTML = `<tr><th>#</th><th>Player</th><th class="td-stat">PA</th><th class="td-stat">xwOBA</th><th class="td-stat">AVG</th><th class="td-stat">K%</th><th class="td-stat">BB%</th><th class="td-bar-cell"></th></tr>`;
    document.getElementById("lb-body").innerHTML = rows.map((row, index) => `
      <tr>
        <td class="td-rank">${index + 1}</td>
        <td class="td-name">${playerLabel("batter", row.batter_id, row.batter_name)}</td>
        <td class="td-stat">${fmtNum(row.pa, 0)}</td>
        <td class="td-stat td-highlight">${fmtRate(row.xwoba)}</td>
        <td class="td-stat">${fmtRate(row.avg)}</td>
        <td class="td-stat">${fmtPct(row.k_pct, 1)}</td>
        <td class="td-stat">${fmtPct(row.bb_pct, 1)}</td>
        <td class="td-bar-cell"><div class="bar-bg"><div class="bar-fill" style="width:${(Number(row.xwoba || 0) / maxValue) * 100}%"></div></div></td>
      </tr>
    `).join("");
  } else {
    rows = await apiFetch(`/leaderboard/stuff${buildQuery({
      ...seasonQuery,
      limit: state.leaderboard.limit,
      min_pitches: 20,
    })}`, "lb-pitching");
    rows = [...rows].sort((a, b) => Number(b[sortField] || 0) - Number(a[sortField] || 0));
    const maxValue = Math.max(1, ...rows.map((row) => Number(row.whiff_pct || 0)));
    document.getElementById("lb-thead").innerHTML = `<tr><th>#</th><th>Pitcher</th><th class="td-stat">Pitch</th><th class="td-stat">Whiff%</th><th class="td-stat">CSW%</th><th class="td-stat">Velo</th><th class="td-stat">Spin</th><th class="td-bar-cell"></th></tr>`;
    document.getElementById("lb-body").innerHTML = rows.map((row, index) => `
      <tr>
        <td class="td-rank">${index + 1}</td>
        <td class="td-name">${playerLabel("pitcher", row.pitcher_id, row.pitcher_name)}</td>
        <td class="td-stat">${row.pitch_type}</td>
        <td class="td-stat td-highlight">${fmtPct(row.whiff_pct, 1)}</td>
        <td class="td-stat">${fmtPct(row.csw_pct, 1)}</td>
        <td class="td-stat">${fmtNum(row.avg_velo, 1)}</td>
        <td class="td-stat">${fmtNum(row.avg_spin, 0)}</td>
        <td class="td-bar-cell"><div class="bar-bg"><div class="bar-fill" style="width:${(Number(row.whiff_pct || 0) / maxValue) * 100}%"></div></div></td>
      </tr>
    `).join("");
  }

  document.getElementById("lb-title").textContent = `${isBatting ? "Batting" : "Pitching"} Leaderboard — ${activeSeasonLabel(state.leaderboard.window)}`;
  document.getElementById("lb-count-display").textContent = state.leaderboard.balls === null ? "All" : `${state.leaderboard.balls}–${state.leaderboard.strikes}`;
  document.getElementById("lb-count-meta").textContent = state.leaderboard.balls === null ? "no count filter" : `${state.leaderboard.outs === null ? "all outs" : `${state.leaderboard.outs} outs`} filter active`;

  const pills = isBatting
    ? ["xwoba", "avg", "k_pct", "bb_pct"]
    : ["whiff_pct", "csw_pct", "avg_velo", "avg_spin"];
  document.getElementById("lb-pills").innerHTML = pills.map((pill) => `<div class="filter-pill${state.leaderboard.sort === pill ? " active" : ""}" onclick="setLBSort('${pill}')">${pill.toUpperCase()}</div>`).join("");
}

async function renderSequencePage() {
  document.getElementById("seq-pitch-count").textContent = state.sequence.pitches.length;
  document.getElementById("sequence-builder").innerHTML = state.sequence.pitches.map((pitch, index) => `<div class="filter-pill">${index + 1}. ${pitch}</div>`).join("");

  const league = await apiFetch(`/sequences${buildQuery({ ...activeSeasonQuery("season"), min_occurrences: 3 })}`, "seq-league");
  document.getElementById("sequence-league-stats").innerHTML = `
    <div class="stat-card">
      <div class="stat-label">Top League Sequence</div>
      <div class="stat-value sm" style="color:var(--accent)">${league[0]?.sequence || "—"}</div>
      <div class="stat-delta">${fmtNum(league[0]?.occurrences, 0)} occurrences</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">League Whiff</div>
      <div class="stat-value" style="color:var(--accent2)">${fmtPct(league[0]?.whiff_pct, 1)}</div>
      <div class="stat-delta">on most common sequence</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">League K Rate</div>
      <div class="stat-value" style="color:var(--accent3)">${fmtPct(league[0]?.k_pct, 1)}</div>
      <div class="stat-delta">sequence-level finish rate</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Live Scope</div>
      <div class="stat-value sm">Pitch Type</div>
      <div class="stat-delta">using real two-pitch transitions</div>
    </div>
  `;

  if (state.sequence.playerType !== "pitcher") {
    document.getElementById("sequence-player-stats").innerHTML = `
      <div class="stat-card"><div class="stat-label">Note</div><div class="stat-value sm">Pitcher View</div><div class="stat-delta">Sequencing is currently pitcher-owned in the live API.</div></div>
    `;
    return;
  }

  const lookup = lookupPlayerByName("pitcher", document.getElementById("seq-player-search").value);
  if (lookup) state.sequence.selectedId = lookup.id;
  if (!state.sequence.selectedId) return;

  const playerRows = await apiFetch(`/pitcher/${state.sequence.selectedId}/sequences${buildQuery({ ...activeSeasonQuery("season"), min_occurrences: 2 })}`, "seq-player");
  const topRows = playerRows.slice(0, 10);
  const bottomRows = [...playerRows].sort((a, b) => Number(a.whiff_pct || 0) - Number(b.whiff_pct || 0)).slice(0, 10);
  const top = topRows[0];

  document.getElementById("sequence-player-stats").innerHTML = `
    <div class="stat-card">
      <div class="stat-label">Pitcher</div>
      <div class="stat-value sm" style="color:var(--accent)">${playerLabel("pitcher", state.sequence.selectedId)}</div>
      <div class="stat-delta">${fmtNum(playerRows.length, 0)} tracked sequences</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Best Sequence</div>
      <div class="stat-value sm" style="color:var(--accent2)">${top?.sequence || "—"}</div>
      <div class="stat-delta">Whiff ${fmtPct(top?.whiff_pct, 1)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">K Rate</div>
      <div class="stat-value" style="color:var(--accent3)">${fmtPct(top?.k_pct, 1)}</div>
      <div class="stat-delta">top sequence finish rate</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Occurrences</div>
      <div class="stat-value">${fmtNum(top?.occurrences, 0)}</div>
      <div class="stat-delta">for best sequence</div>
    </div>
  `;

  const renderTable = (rows) => rows.map((row, index) => `
    <tr>
      <td class="td-rank">${index + 1}</td>
      <td class="td-name">${row.sequence}</td>
      <td class="td-stat">${fmtPct(row.whiff_pct, 1)}</td>
      <td class="td-stat">${fmtPct(row.k_pct, 1)}</td>
    </tr>
  `).join("");

  document.getElementById("seq-top-thead").innerHTML = `<tr><th>#</th><th>Sequence</th><th class="td-stat">Whiff%</th><th class="td-stat">K%</th></tr>`;
  document.getElementById("seq-top-body").innerHTML = renderTable(topRows);
  document.getElementById("seq-bottom-thead").innerHTML = `<tr><th>#</th><th>Sequence</th><th class="td-stat">Whiff%</th><th class="td-stat">K%</th></tr>`;
  document.getElementById("seq-bottom-body").innerHTML = renderTable(bottomRows);

  document.getElementById("sequence-pitch-mix").innerHTML = topRows.slice(0, 6).map((row) => `
    <div style="padding:16px;border:1px solid var(--border);border-radius:2px">
      <div style="font-family:var(--mono);font-size:11px;color:var(--muted);text-transform:uppercase;margin-bottom:8px">${row.first_pitch} to ${row.second_pitch}</div>
      <div style="font-size:13px;color:var(--text);line-height:1.6">Whiff ${fmtPct(row.whiff_pct, 1)} · K ${fmtPct(row.k_pct, 1)} · ${fmtNum(row.occurrences, 0)} uses</div>
    </div>
  `).join("");
}

function setPage(page, navEl) {
  document.querySelectorAll(".nav-item").forEach((el) => el.classList.remove("active"));
  if (navEl) navEl.classList.add("active");
  document.querySelectorAll(".page-section").forEach((el) => el.classList.remove("active"));
  document.getElementById(`page-${page}`).classList.add("active");
  state.page = page;
  if (page === "count") renderCountPage();
  if (page === "batter") renderBatterProfile();
  if (page === "pitcher") renderPitcherProfile();
  if (page === "leaderboard") renderLeaderboard();
  if (page === "pitch-sequence") renderSequencePage();
}

function setCountAll() {
  state.count.balls = null;
  state.count.strikes = null;
  buildCountGrid("count-grid", state.count, renderCountPage);
  renderCountPage();
}

function setCountFilter(type, value, el) {
  const group = el.parentElement;
  group.querySelectorAll(".out-btn").forEach((btn) => btn.classList.remove("active"));
  el.classList.add("active");
  if (type === "batter-hand") state.count.batterHand = value;
  if (type === "pitcher-hand") state.count.pitcherHand = value;
  renderCountPage();
}

function setBatterWindow(window, el) {
  document.querySelectorAll("#batter-window .window-tab").forEach((tab) => tab.classList.remove("active"));
  el.classList.add("active");
  state.batter.window = window;
  renderBatterProfile();
}

function setPitcherWindow(window, el) {
  document.querySelectorAll("#pitcher-window .window-tab").forEach((tab) => tab.classList.remove("active"));
  el.classList.add("active");
  state.pitcher.window = window;
  renderPitcherProfile();
}

function setLBType(type, el) {
  document.querySelectorAll(".type-btn").forEach((btn) => btn.classList.remove("active"));
  el.classList.add("active");
  state.leaderboard.type = type;
  state.leaderboard.sort = type === "batting" ? "xwoba" : "whiff_pct";
  renderLeaderboard();
}

function setLBWindow(window, el) {
  document.querySelectorAll("#lb-window .window-tab").forEach((tab) => tab.classList.remove("active"));
  el.classList.add("active");
  state.leaderboard.window = window;
  renderLeaderboard();
}

function setLBSort(sortField) {
  state.leaderboard.sort = sortField;
  renderLeaderboard();
}

function clearLBCount() {
  state.leaderboard.balls = null;
  state.leaderboard.strikes = null;
  buildCountGrid("lb-count-grid", state.leaderboard, () => {
    renderLeaderboard();
  });
  renderLeaderboard();
}

function clearLBOuts() {
  state.leaderboard.outs = null;
  document.querySelectorAll("#lb-outs .out-btn").forEach((btn) => btn.classList.remove("active"));
  renderLeaderboard();
}

function addSequencePitch(result) {
  if (state.sequence.pitches.length < 25) {
    state.sequence.pitches.push(result);
    renderSequencePage();
  }
}

function clearSequence() {
  state.sequence.pitches = [];
  renderSequencePage();
}

function updateSequencePlayer() {
  renderSequencePage();
}

function toggleSequenceType() {
  state.sequence.playerType = document.getElementById("seq-type-toggle").value;
  renderSequencePage();
}

async function refreshData() {
  cache.countMatrix = null;
  cache.batterOverview.clear();
  cache.pitcherOverview.clear();
  await loadMetaContext();
  if (state.page === "batter") await renderBatterProfile();
  if (state.page === "pitcher") await renderPitcherProfile();
  if (state.page === "leaderboard") await renderLeaderboard();
  if (state.page === "pitch-sequence") await renderSequencePage();
  await renderCountPage();
}

document.addEventListener("DOMContentLoaded", async () => {
  await loadMetaContext();

  buildCountGrid("count-grid", state.count, renderCountPage);
  buildCountGrid("lb-count-grid", state.leaderboard, () => {
    renderLeaderboard();
  });

  document.getElementById("scope-mode").addEventListener("change", () => {
    syncScopeControls();
    refreshData();
  });

  document.getElementById("count-outs").addEventListener("click", (event) => {
    const button = event.target.closest(".out-btn");
    if (!button) return;
    document.querySelectorAll("#count-outs .out-btn").forEach((btn) => btn.classList.remove("active"));
    button.classList.add("active");
    state.count.outs = button.dataset.outs === "all" ? null : Number(button.dataset.outs);
    renderCountPage();
  });

  document.getElementById("count-lb-pills").addEventListener("click", (event) => {
    const pill = event.target.closest(".filter-pill");
    if (!pill) return;
    document.querySelectorAll("#count-lb-pills .filter-pill").forEach((node) => node.classList.remove("active"));
    pill.classList.add("active");
    state.count.sort = pill.dataset.s === "k" ? "k_pct" : pill.dataset.s === "whiff" ? "whiff_pct" : pill.dataset.s;
    renderCountPage();
  });

  document.getElementById("lb-outs").addEventListener("click", (event) => {
    const button = event.target.closest(".out-btn");
    if (!button) return;
    document.querySelectorAll("#lb-outs .out-btn").forEach((btn) => btn.classList.remove("active"));
    button.classList.add("active");
    state.leaderboard.outs = Number(button.dataset.outs);
    renderLeaderboard();
  });

  ["lb-limit", "lb-season", "lb-season-start", "lb-season-end", "lb-min-pa"].forEach((id) => {
    document.getElementById(id).addEventListener("change", () => {
      state.leaderboard.limit = Number(document.getElementById("lb-limit").value) || 10;
      state.leaderboard.season = selectedSeason();
      state.leaderboard.minPa = Number(document.getElementById("lb-min-pa").value) || 25;
      state.scope.season = selectedSeason();
      state.scope.seasonStart = selectedSeasonStart();
      state.scope.seasonEnd = selectedSeasonEnd();
      syncScopeControls();
      refreshData();
    });
  });

  await renderCountPage();
});
