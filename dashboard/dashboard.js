const API_BASE = "http://localhost:8000/api";
const ACTIVE_PAGE_KEY = "basecount.activePage";

const state = {
  count: { balls: null, strikes: null, outs: null, sort: "xwoba", batterHand: "all", pitcherHand: "all" },
  batter: { selectedId: null, window: "season", balls: 0, strikes: 0, team: "all" },
  pitcher: { selectedId: null, window: "season", balls: 0, strikes: 0, team: "all" },
  leaderboard: { type: "batting", window: "season", balls: null, strikes: null, outs: null, sort: "xwoba", limit: 10, season: null, minPa: 25 },
  sequence: {
    pitches: [],
    playerType: "pitcher",
    selectedId: null,
    activeSlot: 1,
    slots: [
      { key: "league", mode: "league", id: null, label: "League Average" },
      { key: "pitcher-1", mode: "pitcher", id: null, label: "" },
    ],
  },
  team: { selected: null },
  scope: { mode: "single", season: null, seasonStart: null, seasonEnd: null, seasonType: "regular" },
  season: null,
  latestGameDate: null,
  earliestSeason: null,
  teams: [],
  players: { batters: [], pitchers: [] },
  page: "count",
};

const cache = {
  countMatrix: null,
  batterOverview: new Map(),
  pitcherOverview: new Map(),
  leaguePitchingOverview: new Map(),
  teamOverview: new Map(),
};

const playerSearchTimers = {};
const PITCH_TYPE_LABELS = {
  FF: "4-Seam Fastball",
  FT: "2-Seam Fastball",
  SI: "Sinker",
  FC: "Cutter",
  FS: "Splitter",
  FO: "Forkball",
  FA: "Fastball",
  SL: "Slider",
  ST: "Sweeper",
  SV: "Slurve",
  CU: "Curveball",
  KC: "Knuckle Curve",
  CS: "Slow Curve",
  CH: "Changeup",
  SC: "Screwball",
  EP: "Eephus",
  KN: "Knuckleball",
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

function fmtInt(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return Number(value).toLocaleString();
}

function pitchTypeLabel(code) {
  if (!code) return "Unknown";
  return PITCH_TYPE_LABELS[code] ? `${PITCH_TYPE_LABELS[code]} (${code})` : code;
}

function pitchKeyMarkup(rows) {
  const codes = Array.from(new Set(
    rows.flatMap((row) => [row.first_pitch, row.second_pitch]).filter(Boolean)
  ));
  if (!codes.length) return "";
  return codes
    .sort()
    .map((code) => `<span style="display:inline-block;margin-right:14px">${code}: ${PITCH_TYPE_LABELS[code] || code}</span>`)
    .join("");
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

function aggregateSeasonRows(rows, fields, defaultWeightKey = "pa") {
  const grouped = new Map();
  rows.forEach((row) => {
    const season = row.season;
    if (!grouped.has(season)) grouped.set(season, []);
    grouped.get(season).push(row);
  });

  const output = [];
  Array.from(grouped.keys()).sort((a, b) => Number(b) - Number(a)).forEach((season) => {
    const seasonRows = grouped.get(season);
    seasonRows.forEach((row) => output.push({ ...row, rowType: "team" }));
    if (seasonRows.length > 1) {
      const total = { season, team: "Total", rowType: "total" };
      fields.forEach(({ key, mode }) => {
        if (mode === "sum") {
          total[key] = seasonRows.reduce((sum, row) => sum + Number(row[key] || 0), 0);
        } else if (mode === "weighted" || typeof mode === "object") {
          const weightKey = mode.weightKey || defaultWeightKey;
          const weight = seasonRows.reduce((sum, row) => sum + Number(row[weightKey] || 0), 0);
          total[key] = weight ? seasonRows.reduce((sum, row) => sum + Number(row[key] || 0) * Number(row[weightKey] || 0), 0) / weight : 0;
        }
      });
      output.push(total);
    }
  });
  return output;
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

function deriveSequenceCount(pitches) {
  return pitches.reduce((count, pitch) => {
    if (count.terminal) return count;
    if (pitch === "ball") {
      const balls = Math.min(count.balls + 1, 4);
      return { ...count, balls, terminal: balls >= 4, terminalLabel: balls >= 4 ? "Walk" : null };
    }
    if (pitch === "strike") {
      const strikes = Math.min(count.strikes + 1, 3);
      return { ...count, strikes, terminal: strikes >= 3, terminalLabel: strikes >= 3 ? "Strikeout" : null };
    }
    if (pitch === "foul") {
      const strikes = count.strikes < 2 ? count.strikes + 1 : 2;
      return { ...count, strikes };
    }
    return count;
  }, { balls: 0, strikes: 0, terminal: false, terminalLabel: null });
}

function sequencePitchAllowed(result) {
  const count = deriveSequenceCount(state.sequence.pitches);
  if (count.terminal || count.balls >= 4) {
    return { allowed: false, message: `Sequence already ended with ${count.terminalLabel || "a walk"}.` };
  }
  if (result === "ball" && count.balls >= 3) {
    return { allowed: false, message: "You cannot add a fifth ball." };
  }
  if (result === "strike" && count.strikes >= 2) {
    return { allowed: false, message: "You cannot add another strike once the count already has two strikes." };
  }
  return { allowed: true, message: null };
}

function updateSequenceCountState(message = null) {
  const node = document.getElementById("sequence-count-state");
  if (!node) return;
  const count = deriveSequenceCount(state.sequence.pitches);
  const base = `Count: ${count.balls}-${Math.min(count.strikes, 2)}`;
  if (message) {
    node.textContent = `${base} · ${message}`;
    node.style.color = "var(--accent2)";
    return;
  }
  if (count.terminalLabel) {
    node.textContent = `${base} · ${count.terminalLabel}`;
    node.style.color = "var(--accent)";
    return;
  }
  node.textContent = base;
  node.style.color = "var(--muted)";
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

function pitcherSlotLabel(slot) {
  if (!slot) return "Unselected";
  if (slot.mode === "league") return "League Average";
  if (!slot.id) return "Select a pitcher";
  return playerLabel("pitcher", slot.id, slot.label || null);
}

function normalizePitcherSlotValue(value) {
  if (!value) return null;
  return value.trim().toLowerCase() === "league average" ? "League Average" : value.trim();
}

function getActivePitcherSlot() {
  return state.sequence.slots[state.sequence.activeSlot] || state.sequence.slots[0];
}

function renderPitcherCompareControls() {
  const container = document.getElementById("pitcher-compare-controls");
  if (!container) return;
  container.innerHTML = state.sequence.slots.map((slot, index) => `
    <div data-slot-index="${index}" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
      <div style="min-width:54px;font-family:var(--mono);font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em">Slot ${index + 1}</div>
      <input
        type="text"
        class="player-select pitcher-slot-input"
        data-slot-index="${index}"
        value="${pitcherSlotLabel(slot)}"
        placeholder="${index === 0 ? "League Average or pitcher" : "Search pitcher..."}"
        list="seq-player-list"
        style="background:var(--bg);color:var(--text);min-width:240px"
      />
      <button class="window-tab pitcher-slot-focus${index === state.sequence.activeSlot ? " active" : ""}" data-slot-index="${index}" type="button">Focus Details</button>
    </div>
  `).join("");
  document.getElementById("pitcher-add-slot").disabled = state.sequence.slots.length >= 5;
  document.getElementById("pitcher-remove-slot").disabled = state.sequence.slots.length <= 2;
}

function mergePlayers(role, incoming) {
  const key = role === "batter" ? "batters" : "pitchers";
  const existing = state.players[key];
  const byId = new Map(existing.map((player) => [player.id, player]));
  incoming.forEach((player) => {
    if (!player?.id) return;
    byId.set(player.id, { ...byId.get(player.id), ...player });
  });
  state.players[key] = Array.from(byId.values()).sort((a, b) => playerOptionLabel(a).localeCompare(playerOptionLabel(b)));
}

async function searchPlayers(role, query, limit = 25) {
  if (!query || query.trim().length < 2) return [];
  const payload = await apiFetch(`/players/search${buildQuery({ role, q: query.trim(), limit })}`, `player-search-${role}`);
  const results = payload.results || [];
  mergePlayers(role, results);
  populatePlayerLists();
  return results;
}

async function ensurePlayerLookup(role, value) {
  if (!value) return null;
  let lookup = lookupPlayerByName(role, value);
  if (lookup) return lookup;
  await searchPlayers(role, value, 15);
  lookup = lookupPlayerByName(role, value);
  return lookup;
}

function bindPlayerSearch(inputId, role) {
  const input = document.getElementById(inputId);
  if (!input) return;
  input.addEventListener("input", () => {
    const query = input.value.trim();
    clearTimeout(playerSearchTimers[inputId]);
    playerSearchTimers[inputId] = window.setTimeout(() => {
      searchPlayers(role, query).catch(() => {});
    }, 180);
  });
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
  const seasonType = document.getElementById("season-type")?.value || state.scope.seasonType || "regular";
  if (windowName === "career") {
    return { window: windowName, season_type: seasonType };
  }

  if (mode === "range") {
    const start = Math.min(selectedSeasonStart(), selectedSeasonEnd());
    const end = Math.max(selectedSeasonStart(), selectedSeasonEnd());
    return { window: "season", season_start: start, season_end: end, season_type: seasonType };
  }

  return { window: "season", season: selectedSeason(), season_type: seasonType };
}

function activeSeasonLabel(windowName = "season") {
  const mode = document.getElementById("scope-mode")?.value || state.scope.mode;
  if (windowName === "career") return "All loaded seasons";
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
  const singleWrap = document.getElementById("single-season-wrap");
  const rangeWrap = document.getElementById("range-season-wrap");
  const singleInput = document.getElementById("lb-season");
  const rangeStart = document.getElementById("lb-season-start");
  const rangeEnd = document.getElementById("lb-season-end");

  singleWrap.style.display = mode === "single" ? "flex" : "none";
  rangeWrap.style.display = mode === "range" ? "flex" : "none";
  singleInput.disabled = mode !== "single";
  rangeStart.disabled = mode !== "range";
  rangeEnd.disabled = mode !== "range";

  const label = activeSeasonLabel("season");
  document.getElementById("season-scope-label").textContent = label;
  document.getElementById("topbar-scope-label").textContent = label;
  const countScope = document.getElementById("count-scope-label");
  if (countScope) countScope.textContent = label;
}

async function loadMetaContext() {
  const context = await apiFetch("/meta/context", "context");
  const hadScope = state.scope.season !== null || state.scope.seasonStart !== null || state.scope.seasonEnd !== null;
  const previousBatterId = state.batter.selectedId;
  const previousPitcherId = state.pitcher.selectedId;
  const previousSequenceSlots = state.sequence.slots.map((slot) => ({ ...slot }));
  const previousSequenceId = state.sequence.selectedId;
  state.season = context.latest_season;
  state.latestGameDate = context.latest_game_date;
  state.earliestSeason = context.earliest_season;
  state.teams = context.teams || [];
  state.players.batters = context.batters || [];
  state.players.pitchers = context.pitchers || [];
  if (!hadScope) {
    state.scope.mode = context.earliest_season !== context.latest_season ? "range" : "single";
    state.scope.season = context.latest_season;
    state.scope.seasonStart = context.earliest_season;
    state.scope.seasonEnd = context.latest_season;
    state.scope.seasonType = "regular";
  }

  const seasonInputs = ["lb-season", "lb-season-start", "lb-season-end"];
  seasonInputs.forEach((id) => {
    const input = document.getElementById(id);
    input.min = context.earliest_season || 2015;
    input.max = context.latest_season || new Date().getFullYear();
  });

  document.getElementById("scope-mode").value = state.scope.mode;
  document.getElementById("season-type").value = state.scope.seasonType || "regular";
  document.getElementById("lb-season").value = state.scope.season || context.latest_season;
  document.getElementById("lb-season-start").value = state.scope.seasonStart || context.earliest_season;
  document.getElementById("lb-season-end").value = state.scope.seasonEnd || context.latest_season;
  populatePlayerLists();
  syncScopeControls();
  const batterTeam = document.getElementById("batter-team-filter");
  const pitcherTeam = document.getElementById("pitcher-team-filter");
  if (batterTeam) {
    batterTeam.innerHTML = [`<option value="all">All Teams</option>`]
      .concat(state.teams.map((team) => `<option value="${team}">${team}</option>`))
      .join("");
    batterTeam.value = state.batter.team || "all";
  }
  if (pitcherTeam) {
    pitcherTeam.innerHTML = [`<option value="all">All Teams</option>`]
      .concat(state.teams.map((team) => `<option value="${team}">${team}</option>`))
      .join("");
    pitcherTeam.value = state.pitcher.team || "all";
  }
  const teamSelect = document.getElementById("team-select");
  if (teamSelect) {
    if (!state.team.selected) state.team.selected = state.teams[0] || null;
    teamSelect.innerHTML = state.teams.map((team) => `<option value="${team}">${team}</option>`).join("");
    if (state.team.selected) teamSelect.value = state.team.selected;
  }

  const activeBatter = lookupPlayerById("batter", previousBatterId) || state.players.batters[0] || null;
  if (activeBatter) {
    state.batter.selectedId = activeBatter.id;
    document.getElementById("batter-select").value = playerOptionLabel(activeBatter);
  }

  const activePitcher = lookupPlayerById("pitcher", previousPitcherId) || state.players.pitchers[0] || null;
  if (activePitcher) {
    state.pitcher.selectedId = activePitcher.id;
    const pitcherSelect = document.getElementById("pitcher-select");
    if (pitcherSelect) pitcherSelect.value = playerOptionLabel(activePitcher);
  }

  state.sequence.slots = previousSequenceSlots.map((slot, index) => {
    if (slot.mode === "league") return { key: slot.key || `slot-${index}`, mode: "league", id: null, label: "League Average" };
    const resolved = lookupPlayerById("pitcher", slot.id) || (index === 1 ? state.players.pitchers[0] || null : null);
    return {
      key: slot.key || `pitcher-${index}`,
      mode: "pitcher",
      id: resolved?.id || null,
      label: resolved ? playerOptionLabel(resolved) : "",
    };
  });
  if (!state.sequence.slots.length) {
    state.sequence.slots = [
      { key: "league", mode: "league", id: null, label: "League Average" },
      { key: "pitcher-1", mode: "pitcher", id: state.players.pitchers[0]?.id || null, label: state.players.pitchers[0] ? playerOptionLabel(state.players.pitchers[0]) : "" },
    ];
  }
  state.sequence.selectedId = state.sequence.slots[state.sequence.activeSlot]?.id || state.sequence.slots.find((slot) => slot.id)?.id || null;
  renderPitcherCompareControls();

  const activeSequencePitcher = lookupPlayerById("pitcher", previousSequenceId) || activePitcher;
  if (activeSequencePitcher) {
    state.sequence.selectedId = activeSequencePitcher.id;
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
  const countScope = document.getElementById("count-scope-label");
  if (countScope) countScope.textContent = activeSeasonLabel("season");
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
  const team = state.batter.team && state.batter.team !== "all" ? state.batter.team : null;
  const cacheKey = `${state.batter.selectedId}:${state.batter.window}:${team || "all"}:${JSON.stringify(seasonQuery)}`;
  if (cache.batterOverview.has(cacheKey)) return cache.batterOverview.get(cacheKey);
  const data = await apiFetch(`/batter/${state.batter.selectedId}/overview${buildQuery({ ...seasonQuery, team })}`, "batter-overview");
  cache.batterOverview.set(cacheKey, data);
  return data;
}

async function renderBatterProfile() {
  const lookup = await ensurePlayerLookup("batter", document.getElementById("batter-select").value);
  if (lookup) state.batter.selectedId = lookup.id;
  if (!state.batter.selectedId) return;

  const profile = await fetchBatterOverview();
  const batterTeamSelect = document.getElementById("batter-team-filter");
  if (batterTeamSelect) batterTeamSelect.value = state.batter.team || "all";
  const slash = calcSlash(profile.summary);
  const seasonRows = aggregateSeasonRows(profile.seasons || [], [
    { key: "g", mode: "sum" },
    { key: "pa", mode: "sum" },
    { key: "ab", mode: "sum" },
    { key: "h", mode: "sum" },
    { key: "doubles", mode: "sum" },
    { key: "triples", mode: "sum" },
    { key: "hr", mode: "sum" },
    { key: "bb", mode: "sum" },
    { key: "so", mode: "sum" },
    { key: "avg", mode: { weightKey: "ab" } },
    { key: "obp", mode: { weightKey: "pa" } },
    { key: "slg", mode: { weightKey: "ab" } },
    { key: "xwoba", mode: { weightKey: "pa" } },
  ]);
  document.getElementById("batter-stat-cards").innerHTML = `
    <div class="stat-card">
      <div class="stat-label">AVG / OBP / SLG</div>
      <div class="stat-value sm" style="color:var(--accent)">${fmtRate(slash.avg)} / ${fmtRate(slash.obp)} / ${fmtRate(slash.slg)}</div>
      <div class="stat-delta">${fmtInt(profile.summary.pa)} PA${profile.summary.team_display ? ` · ${profile.summary.team_display}` : ""}</div>
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
      <div class="stat-label">Games / AB / H</div>
      <div class="stat-value sm">${fmtInt(profile.summary.g)} / ${fmtInt(profile.summary.at_bats)} / ${fmtInt(profile.summary.hits)}</div>
      <div class="stat-delta">${playerLabel("batter", state.batter.selectedId)}</div>
    </div>
  `;

  document.getElementById("batter-season-thead").innerHTML = `<tr><th>Season</th><th>Team</th><th class="td-stat">G</th><th class="td-stat">PA</th><th class="td-stat">AB</th><th class="td-stat">H</th><th class="td-stat">2B</th><th class="td-stat">3B</th><th class="td-stat">HR</th><th class="td-stat">BB</th><th class="td-stat">SO</th><th class="td-stat">AVG</th><th class="td-stat">OBP</th><th class="td-stat">SLG</th></tr>`;
  document.getElementById("batter-season-body").innerHTML = seasonRows.map((row) => `
    <tr class="${row.rowType === "total" ? "summary-row" : ""}">
      <td class="td-name">${row.season}</td>
      <td class="td-name" style="font-size:12px;color:${row.rowType === "total" ? "var(--text)" : "var(--accent3)"}">${row.team || "—"}</td>
      <td class="td-stat">${fmtInt(row.g)}</td>
      <td class="td-stat">${fmtInt(row.pa)}</td>
      <td class="td-stat">${fmtInt(row.ab)}</td>
      <td class="td-stat">${fmtInt(row.h)}</td>
      <td class="td-stat">${fmtInt(row.doubles)}</td>
      <td class="td-stat">${fmtInt(row.triples)}</td>
      <td class="td-stat">${fmtInt(row.hr)}</td>
      <td class="td-stat">${fmtInt(row.bb)}</td>
      <td class="td-stat">${fmtInt(row.so)}</td>
      <td class="td-stat td-highlight">${fmtRate(row.avg)}</td>
      <td class="td-stat">${fmtRate(row.obp)}</td>
      <td class="td-stat">${fmtRate(row.slg)}</td>
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

async function fetchPitcherOverviewForSlot(slot) {
  const seasonQuery = activeSeasonQuery(state.pitcher.window);
  const team = state.pitcher.team && state.pitcher.team !== "all" ? state.pitcher.team : null;
  if (slot.mode === "league") {
    const cacheKey = `league:${state.pitcher.window}:${team || "all"}:${JSON.stringify(seasonQuery)}`;
    if (cache.leaguePitchingOverview.has(cacheKey)) return cache.leaguePitchingOverview.get(cacheKey);
    const data = await apiFetch(`/pitching/overview${buildQuery({ ...seasonQuery, team })}`, "pitching-overview");
    cache.leaguePitchingOverview.set(cacheKey, data);
    return data;
  }
  if (!slot.id) return null;
  const cacheKey = `${slot.id}:${state.pitcher.window}:${team || "all"}:${JSON.stringify(seasonQuery)}`;
  if (cache.pitcherOverview.has(cacheKey)) return cache.pitcherOverview.get(cacheKey);
  const data = await apiFetch(`/pitcher/${slot.id}/overview${buildQuery({ ...seasonQuery, team })}`, "pitcher-overview");
  cache.pitcherOverview.set(cacheKey, data);
  return data;
}

async function renderPitcherProfile() {
  const lookup = await ensurePlayerLookup("pitcher", document.getElementById("pitcher-select").value);
  if (lookup) state.pitcher.selectedId = lookup.id;
  if (!state.pitcher.selectedId) return;
  const pitcherTeamSelect = document.getElementById("pitcher-team-filter");
  if (pitcherTeamSelect) pitcherTeamSelect.value = state.pitcher.team || "all";
  const profile = await fetchPitcherOverviewForSlot({ mode: "pitcher", id: state.pitcher.selectedId, label: playerLabel("pitcher", state.pitcher.selectedId) });
  if (!profile) return;
  const seasonRows = aggregateSeasonRows(profile.seasons || [], [
    { key: "g", mode: "sum" },
    { key: "pitches", mode: "sum" },
    { key: "avg_velo", mode: { weightKey: "pitches" } },
    { key: "avg_spin", mode: { weightKey: "pitches" } },
    { key: "whiff_pct", mode: { weightKey: "pitches" } },
  ], "pitches");

  const container = document.getElementById("pitcher-stat-cards");
  container.style.gridTemplateColumns = "repeat(4, minmax(0, 1fr))";
  container.innerHTML = `
    <div class="stat-card">
      <div class="stat-label">Pitches</div>
      <div class="stat-value sm" style="color:var(--accent)">${fmtInt(profile.summary.pitches)}</div>
      <div class="stat-delta">${playerLabel("pitcher", state.pitcher.selectedId)}${profile.summary.team_display ? ` · ${profile.summary.team_display}` : ""}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Avg Velocity</div>
      <div class="stat-value" style="color:var(--accent2)">${fmtNum(profile.summary.avg_velo, 1)}</div>
      <div class="stat-delta">Spin ${fmtNum(profile.summary.avg_spin, 0)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Whiff / K</div>
      <div class="stat-value sm" style="color:var(--accent3)">${fmtPct(profile.summary.whiff_pct, 1)} / ${fmtPct(profile.summary.k_pct, 1)}</div>
      <div class="stat-delta">BB ${fmtPct(profile.summary.bb_pct, 1)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">xwOBA Allowed</div>
      <div class="stat-value">${fmtRate(profile.summary.xwoba_allowed)}</div>
      <div class="stat-delta">${fmtInt(profile.summary.g)} games</div>
    </div>
  `;

  document.getElementById("pitcher-detail-title").textContent = `Season-by-Season Summary — ${playerLabel("pitcher", state.pitcher.selectedId)}`;
  document.getElementById("pitcher-season-thead").innerHTML = `<tr><th>Season</th><th>Team</th><th class="td-stat">G</th><th class="td-stat">Pitches</th><th class="td-stat">Velo</th><th class="td-stat">Spin</th><th class="td-stat">Whiff%</th></tr>`;
  document.getElementById("pitcher-season-body").innerHTML = seasonRows.map((row) => `
    <tr class="${row.rowType === "total" ? "summary-row" : ""}">
      <td class="td-name">${row.season}</td>
      <td class="td-name" style="font-size:12px;color:${row.rowType === "total" ? "var(--text)" : "var(--accent3)"}">${row.team || "—"}</td>
      <td class="td-stat">${fmtInt(row.g)}</td>
      <td class="td-stat">${fmtInt(row.pitches)}</td>
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
  fetchPitcherOverviewForSlot(getActivePitcherSlot()).then((profile) => {
    if (profile) render(profile);
  });
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
  state.sequence.playerType = "pitcher";
  document.getElementById("seq-pitch-count").textContent = state.sequence.pitches.length;
  document.getElementById("sequence-builder").innerHTML = state.sequence.pitches.map((pitch, index) => `<div class="filter-pill">${index + 1}. ${pitch}</div>`).join("");
  updateSequenceCountState();
  renderPitcherCompareControls();

  const league = await apiFetch(`/sequences${buildQuery({ ...activeSeasonQuery("season"), min_occurrences: 3 })}`, "seq-league");
  const selectedOutcomeSummary = state.sequence.pitches.length
    ? await apiFetch(`/sequences/outcomes${buildQuery({ ...activeSeasonQuery("season"), outcomes: state.sequence.pitches.join(",") })}`, "seq-outcomes")
    : null;
  const summaryLabel = selectedOutcomeSummary?.sequence || "No sequence selected";
  const summaryOccurrences = selectedOutcomeSummary?.occurrences ?? 0;
  const summaryWhiff = selectedOutcomeSummary?.whiff_pct ?? null;
  const summaryK = selectedOutcomeSummary?.k_pct ?? null;
  document.getElementById("sequence-league-stats").innerHTML = `
    <div class="stat-card">
      <div class="stat-label">Selected Sequence</div>
      <div class="stat-value sm" style="color:var(--accent)">${summaryLabel}</div>
      <div class="stat-delta">${fmtNum(summaryOccurrences, 0)} matches in ${activeSeasonLabel("season")}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">League Whiff</div>
      <div class="stat-value" style="color:var(--accent2)">${fmtPct(summaryWhiff, 1)}</div>
      <div class="stat-delta">on final pitch of selected sequence</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">League K Rate</div>
      <div class="stat-value" style="color:var(--accent3)">${fmtPct(summaryK, 1)}</div>
      <div class="stat-delta">strikeout finish rate for selected sequence</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Live Scope</div>
      <div class="stat-value sm">${state.sequence.pitches.length ? `${state.sequence.pitches.length} pitches` : "Awaiting input"}</div>
      <div class="stat-delta">${state.sequence.pitches.length ? "using real ball/strike/foul sequences" : "add balls, strikes, or fouls to evaluate"}</div>
    </div>
  `;

  const slotResults = await Promise.all(state.sequence.slots.map(async (slot) => {
    if (slot.mode === "league") {
      return { slot, summary: selectedOutcomeSummary, rows: league };
    }
    if (!slot.id) return { slot, summary: null, rows: [] };
    const [summary, rows] = await Promise.all([
      state.sequence.pitches.length
        ? apiFetch(`/sequences/outcomes${buildQuery({
          ...activeSeasonQuery("season"),
          outcomes: state.sequence.pitches.join(","),
          pitcher_id: slot.id,
        })}`, `seq-player-outcomes-${slot.id}`).catch(() => null)
        : Promise.resolve(null),
      apiFetch(`/pitcher/${slot.id}/sequences${buildQuery({ ...activeSeasonQuery("season"), min_occurrences: 2 })}`, `seq-player-${slot.id}`).catch(() => []),
    ]);
    return { slot, summary, rows };
  }));

  const activeResult = slotResults[state.sequence.activeSlot] || slotResults.find((entry) => entry.rows.length || entry.summary) || slotResults[0];
  const activeRows = activeResult?.rows || [];
  const topRows = activeRows.slice(0, 10);
  const bottomRows = [...activeRows].sort((a, b) => Number(a.whiff_pct || 0) - Number(b.whiff_pct || 0)).slice(0, 10);

  document.getElementById("sequence-player-stats").style.gridTemplateColumns = `repeat(${Math.max(2, Math.min(5, state.sequence.slots.length))}, minmax(0, 1fr))`;
  document.getElementById("sequence-player-stats").innerHTML = slotResults.map(({ slot, summary, rows }, index) => {
    const best = rows[0] || null;
    const occurrences = state.sequence.pitches.length ? summary?.occurrences : best?.occurrences;
    const whiff = state.sequence.pitches.length ? summary?.whiff_pct : best?.whiff_pct;
    const kRate = state.sequence.pitches.length ? summary?.k_pct : best?.k_pct;
    const label = slot.mode === "league" ? "League Average" : pitcherSlotLabel(slot);
    const line = state.sequence.pitches.length
      ? `${fmtInt(occurrences)} occurrences of selected sequence`
      : `${fmtInt(rows.length)} tracked sequence patterns`;
    return `
      <div class="stat-card" style="${index === state.sequence.activeSlot ? "border-color:var(--accent2);" : ""}">
        <div class="stat-label">${label}</div>
        <div class="stat-value sm" style="color:var(--accent)">${state.sequence.pitches.length ? (summary?.sequence || "—") : (best?.sequence || "—")}</div>
        <div class="stat-delta">${line}</div>
        <div class="stat-delta">Whiff ${fmtPct(whiff, 1)} · K ${fmtPct(kRate, 1)}</div>
      </div>
    `;
  }).join("");

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
      <div style="font-family:var(--mono);font-size:11px;color:var(--muted);text-transform:uppercase;margin-bottom:8px">${pitchTypeLabel(row.first_pitch)} to ${pitchTypeLabel(row.second_pitch)}</div>
      <div style="font-size:13px;color:var(--text);line-height:1.6">Whiff ${fmtPct(row.whiff_pct, 1)} · K ${fmtPct(row.k_pct, 1)} · ${fmtNum(row.occurrences, 0)} uses</div>
    </div>
  `).join("");
  document.getElementById("sequence-pitch-key").innerHTML = pitchKeyMarkup(topRows);
}

function setPage(page, navEl) {
  document.querySelectorAll(".nav-item").forEach((el) => el.classList.remove("active"));
  if (navEl) {
    navEl.classList.add("active");
  } else {
    const fallback = Array.from(document.querySelectorAll(".nav-item")).find((el) => el.getAttribute("onclick")?.includes(`'${page}'`));
    if (fallback) fallback.classList.add("active");
  }
  document.querySelectorAll(".page-section").forEach((el) => el.classList.remove("active"));
  document.getElementById(`page-${page}`).classList.add("active");
  state.page = page;
  try {
    window.localStorage.setItem(ACTIVE_PAGE_KEY, page);
  } catch (_) {}
  if (page === "count") renderCountPage();
  if (page === "batter") renderBatterProfile();
  if (page === "pitcher") renderPitcherProfile();
  if (page === "team") renderTeamProfile();
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

function setBatterTeam(team) {
  state.batter.team = team || "all";
  renderBatterProfile();
}

function setPitcherWindow(window, el) {
  document.querySelectorAll("#pitcher-window .window-tab").forEach((tab) => tab.classList.remove("active"));
  el.classList.add("active");
  state.pitcher.window = window;
  renderPitcherProfile();
}

function setPitcherTeam(team) {
  state.pitcher.team = team || "all";
  renderPitcherProfile();
}

async function fetchTeamOverview() {
  if (!state.team.selected) return null;
  const seasonQuery = activeSeasonQuery("season");
  const cacheKey = `${state.team.selected}:${JSON.stringify(seasonQuery)}`;
  if (cache.teamOverview.has(cacheKey)) return cache.teamOverview.get(cacheKey);
  const data = await apiFetch(`/team/${state.team.selected}/overview${buildQuery(seasonQuery)}`, "team-overview");
  cache.teamOverview.set(cacheKey, data);
  return data;
}

async function renderTeamProfile() {
  const teamSelect = document.getElementById("team-select");
  if (teamSelect?.value) state.team.selected = teamSelect.value;
  if (!state.team.selected) return;
  const profile = await fetchTeamOverview();
  if (!profile) return;
  const summary = profile.summary || {};
  document.getElementById("team-stat-cards").innerHTML = `
    <div class="stat-card">
      <div class="stat-label">Team / Games</div>
      <div class="stat-value sm" style="color:var(--accent)">${summary.team || state.team.selected}</div>
      <div class="stat-delta">${fmtInt(summary.games)} games in ${activeSeasonLabel("season")}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Team Slash</div>
      <div class="stat-value sm" style="color:var(--accent2)">${fmtRate(summary.avg)} / ${fmtRate(summary.obp)} / ${fmtRate(summary.slg)}</div>
      <div class="stat-delta">${fmtInt(summary.pa)} PA · xwOBA ${fmtRate(summary.xwoba)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Pitching</div>
      <div class="stat-value sm" style="color:var(--accent3)">${fmtPct(summary.whiff_pct, 1)}</div>
      <div class="stat-delta">team whiff · xwOBA allowed ${fmtRate(summary.xwoba_allowed)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Run-Creation Events</div>
      <div class="stat-value sm">${fmtInt(summary.hr)} HR</div>
      <div class="stat-delta">${fmtInt(summary.walks)} BB · ${fmtInt(summary.strikeouts)} SO</div>
    </div>
  `;

  document.getElementById("team-batting-thead").innerHTML = `<tr><th>Player</th><th class="td-stat">G</th><th class="td-stat">PA</th><th class="td-stat">AB</th><th class="td-stat">H</th><th class="td-stat">2B</th><th class="td-stat">3B</th><th class="td-stat">HR</th><th class="td-stat">BB</th><th class="td-stat">SO</th><th class="td-stat">AVG</th><th class="td-stat">OBP</th><th class="td-stat">SLG</th><th class="td-stat">xwOBA</th></tr>`;
  document.getElementById("team-batting-body").innerHTML = (profile.batting || []).map((row) => `
    <tr>
      <td class="td-name">${row.batter_name || playerName("batter", row.batter_id)}</td>
      <td class="td-stat">${fmtInt(row.g)}</td>
      <td class="td-stat">${fmtInt(row.pa)}</td>
      <td class="td-stat">${fmtInt(row.ab_count)}</td>
      <td class="td-stat">${fmtInt(row.h)}</td>
      <td class="td-stat">${fmtInt(row.doubles)}</td>
      <td class="td-stat">${fmtInt(row.triples)}</td>
      <td class="td-stat">${fmtInt(row.hr)}</td>
      <td class="td-stat">${fmtInt(row.bb)}</td>
      <td class="td-stat">${fmtInt(row.so)}</td>
      <td class="td-stat td-highlight">${fmtRate(row.avg)}</td>
      <td class="td-stat">${fmtRate(row.obp)}</td>
      <td class="td-stat">${fmtRate(row.slg)}</td>
      <td class="td-stat">${fmtRate(row.xwoba)}</td>
    </tr>
  `).join("");

  document.getElementById("team-pitching-thead").innerHTML = `<tr><th>Pitcher</th><th class="td-stat">G</th><th class="td-stat">Pitches</th><th class="td-stat">Velo</th><th class="td-stat">Spin</th><th class="td-stat">Whiff%</th><th class="td-stat">K%</th><th class="td-stat">BB%</th><th class="td-stat">xwOBA Allowed</th></tr>`;
  document.getElementById("team-pitching-body").innerHTML = (profile.pitching || []).map((row) => `
    <tr>
      <td class="td-name">${row.pitcher_name || playerName("pitcher", row.pitcher_id)}</td>
      <td class="td-stat">${fmtInt(row.g)}</td>
      <td class="td-stat">${fmtInt(row.pitches)}</td>
      <td class="td-stat">${fmtNum(row.avg_velo, 1)}</td>
      <td class="td-stat">${fmtNum(row.avg_spin, 0)}</td>
      <td class="td-stat td-highlight">${fmtPct(row.whiff_pct, 1)}</td>
      <td class="td-stat">${fmtPct(row.k_pct, 1)}</td>
      <td class="td-stat">${fmtPct(row.bb_pct, 1)}</td>
      <td class="td-stat">${fmtRate(row.xwoba_allowed)}</td>
    </tr>
  `).join("");
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
  const ruleCheck = sequencePitchAllowed(result);
  if (!ruleCheck.allowed) {
    updateSequenceCountState(ruleCheck.message);
    return;
  }
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
  state.sequence.playerType = "pitcher";
  renderSequencePage();
}

async function setPitcherSlotValue(index, rawValue) {
  const slot = state.sequence.slots[index];
  if (!slot) return;
  const value = normalizePitcherSlotValue(rawValue);
  if (!value) {
    slot.mode = index === 0 ? "league" : "pitcher";
    slot.id = null;
    slot.label = index === 0 ? "League Average" : "";
    renderPitcherCompareControls();
    await renderSequencePage();
    return;
  }
  if (index === 0 && value === "League Average") {
    slot.mode = "league";
    slot.id = null;
    slot.label = "League Average";
    state.sequence.activeSlot = 0;
    renderPitcherCompareControls();
    await renderSequencePage();
    return;
  }
  const lookup = await ensurePlayerLookup("pitcher", value);
  if (!lookup) return;
  slot.mode = "pitcher";
  slot.id = lookup.id;
  slot.label = playerOptionLabel(lookup);
  if (state.sequence.activeSlot === index || index === 1) {
    state.sequence.activeSlot = index;
  }
  state.sequence.selectedId = lookup.id;
  renderPitcherCompareControls();
  await renderSequencePage();
}

function addPitcherComparisonSlot() {
  if (state.sequence.slots.length >= 5) return;
  state.sequence.slots.push({ key: `pitcher-${Date.now()}`, mode: "pitcher", id: null, label: "" });
  renderPitcherCompareControls();
}

function removePitcherComparisonSlot() {
  if (state.sequence.slots.length <= 2) return;
  state.sequence.slots.pop();
  state.sequence.activeSlot = Math.min(state.sequence.activeSlot, state.sequence.slots.length - 1);
  renderPitcherCompareControls();
  renderSequencePage();
}

async function refreshData() {
  cache.countMatrix = null;
  cache.batterOverview.clear();
  cache.pitcherOverview.clear();
  cache.leaguePitchingOverview.clear();
  cache.teamOverview.clear();
  await loadMetaContext();
  if (state.page === "count") await renderCountPage();
  if (state.page === "batter") await renderBatterProfile();
  if (state.page === "pitcher") await renderPitcherProfile();
  if (state.page === "team") await renderTeamProfile();
  if (state.page === "leaderboard") await renderLeaderboard();
  if (state.page === "pitch-sequence") await renderSequencePage();
}

document.addEventListener("DOMContentLoaded", async () => {
  try {
    state.page = window.localStorage.getItem(ACTIVE_PAGE_KEY) || state.page;
  } catch (_) {}
  await loadMetaContext();

  bindPlayerSearch("batter-select", "batter");
  bindPlayerSearch("pitcher-select", "pitcher");

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

  ["lb-limit", "lb-season", "lb-season-start", "lb-season-end", "lb-min-pa", "season-type"].forEach((id) => {
    document.getElementById(id).addEventListener("change", () => {
      state.leaderboard.limit = Number(document.getElementById("lb-limit").value) || 10;
      state.leaderboard.season = selectedSeason();
      state.leaderboard.minPa = Number(document.getElementById("lb-min-pa").value) || 25;
      state.scope.season = selectedSeason();
      state.scope.seasonStart = selectedSeasonStart();
      state.scope.seasonEnd = selectedSeasonEnd();
      state.scope.seasonType = document.getElementById("season-type").value || "regular";
      syncScopeControls();
      refreshData();
    });
  });

  document.getElementById("pitcher-add-slot").addEventListener("click", () => {
    addPitcherComparisonSlot();
  });

  document.getElementById("pitcher-remove-slot").addEventListener("click", () => {
    removePitcherComparisonSlot();
  });

  document.getElementById("pitcher-compare-controls").addEventListener("change", async (event) => {
    const input = event.target.closest(".pitcher-slot-input");
    if (!input) return;
    await setPitcherSlotValue(Number(input.dataset.slotIndex), input.value);
  });

  document.getElementById("pitcher-compare-controls").addEventListener("input", (event) => {
    const input = event.target.closest(".pitcher-slot-input");
    if (!input) return;
    const key = `pitcher-slot-${input.dataset.slotIndex}`;
    clearTimeout(playerSearchTimers[key]);
    playerSearchTimers[key] = window.setTimeout(() => {
      searchPlayers("pitcher", input.value).catch(() => {});
    }, 180);
  });

  document.getElementById("pitcher-compare-controls").addEventListener("click", (event) => {
    const button = event.target.closest(".pitcher-slot-focus");
    if (!button) return;
    state.sequence.activeSlot = Number(button.dataset.slotIndex);
    renderPitcherCompareControls();
    renderSequencePage();
  });

  setPage(state.page || "count");
});
