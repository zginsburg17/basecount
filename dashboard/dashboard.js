// ══════════════════════════════════════
// STATE
// ══════════════════════════════════════
const state = {
  count:       { balls:0, strikes:0, outs:0, sort:'xwoba' },
  batter:      { selected:'F. Freeman', window:'season', balls:0, strikes:0 },
  pitcher:     { selected:'C. Burnes',  window:'season', balls:0, strikes:0 },
  leaderboard: { type:'batting', window:'season', balls:null, strikes:null, outs:0, sort:'xwoba' },
  page: 'count',
};

// ══════════════════════════════════════
// DATA
// ══════════════════════════════════════
const countData = {
  '0-0':{ k:5,  xwoba:.330, whiff:14.2, bb:8.1  },
  '1-0':{ k:7,  xwoba:.345, whiff:16.1, bb:0    },
  '2-0':{ k:4,  xwoba:.380, whiff:11.8, bb:0    },
  '3-0':{ k:1,  xwoba:.420, whiff:5.2,  bb:0    },
  '0-1':{ k:12, xwoba:.290, whiff:22.4, bb:0    },
  '1-1':{ k:14, xwoba:.305, whiff:24.1, bb:0    },
  '2-1':{ k:9,  xwoba:.350, whiff:18.3, bb:0    },
  '3-1':{ k:5,  xwoba:.400, whiff:12.1, bb:0    },
  '0-2':{ k:32, xwoba:.220, whiff:38.4, bb:0    },
  '1-2':{ k:28, xwoba:.240, whiff:35.2, bb:0    },
  '2-2':{ k:34, xwoba:.260, whiff:40.1, bb:0    },
  '3-2':{ k:26, xwoba:.310, whiff:31.8, bb:14.2 },
};

const countZones = {
  '0-0':[[10,16,9],[18,26,20],[12,17,10]], '1-0':[[11,17,10],[19,28,22],[13,18,11]],
  '2-0':[[12,19,11],[22,30,24],[14,20,12]], '3-0':[[8,12,7],[14,20,15],[9,13,8]],
  '0-1':[[9,15,8],[17,24,18],[11,16,9]],  '1-1':[[10,16,9],[18,25,19],[12,17,10]],
  '2-1':[[11,18,10],[20,28,22],[13,19,11]],'3-1':[[10,17,9],[19,27,21],[12,18,10]],
  '0-2':[[8,13,7],[15,22,16],[10,14,8]],  '1-2':[[9,14,8],[16,23,17],[10,15,9]],
  '2-2':[[10,15,9],[17,24,18],[11,16,10]],'3-2':[[12,18,11],[20,28,22],[13,19,12]],
};

function genBatterByCount(baseXwoba) {
  const out = {};
  for (const key of Object.keys(countData)) {
    const cd = countData[key];
    const delta = (baseXwoba - 0.320) * 0.5;
    out[key] = {
      avg:   +(0.250 + delta + (cd.xwoba - 0.300) * 0.3).toFixed(3),
      xwoba: +(cd.xwoba + delta * 0.8).toFixed(3),
      k:     +(cd.k * (1 + (0.320 - baseXwoba) * 0.5)).toFixed(1),
      whiff: +(cd.whiff * (1 + (0.320 - baseXwoba) * 0.4)).toFixed(1),
    };
  }
  return out;
}

function genPitcherByCount(baseEra) {
  const out = {};
  const delta = (baseEra - 3.5) * 0.1;
  for (const key of Object.keys(countData)) {
    const cd = countData[key];
    const [b, s] = key.split('-').map(Number);
    let usage;
    if (s >= 2)            usage = 'SL 42% · FF 32% · CH 26%';
    else if (b >= 3)       usage = 'FF 48% · SL 28% · CH 24%';
    else if (b===0&&s===0) usage = 'FF 52% · SL 26% · CH 22%';
    else                   usage = 'FF 44% · SL 32% · CH 24%';
    out[key] = {
      k:     +(cd.k * (1 - delta * 0.3)).toFixed(1),
      xwoba: +(cd.xwoba + delta * 0.05).toFixed(3),
      usage,
    };
  }
  return out;
}

function makeBatter(cAvg,cObp,cSlg,cHr,cXw,cK,cBb, sAvg,sObp,sSlg,sHr,sXw,sK,sBb, lAvg,lObp,lSlg,lHr,lXw,lK,lBb, zC,zS,zL) {
  return {
    career:{ avg:cAvg,obp:cObp,slg:cSlg,hr:cHr,xwoba:cXw,k:cK,bb:cBb },
    season:{ avg:sAvg,obp:sObp,slg:sSlg,hr:sHr,xwoba:sXw,k:sK,bb:sBb },
    last7: { avg:lAvg,obp:lObp,slg:lSlg,hr:lHr,xwoba:lXw,k:lK,bb:lBb },
    zone:{ career:zC, season:zS, last7:zL },
    byCount: genBatterByCount(sXw),
  };
}

function makePitcher(cEra,cFip,cWhip,cK9,cBb9,cHr9, sEra,sFip,sWhip,sK9,sBb9,sHr9, lEra,lFip,lWhip,lK9,lBb9,lHr9, zC,zS,zL) {
  return {
    career:{ era:cEra,fip:cFip,whip:cWhip,k9:cK9,bb9:cBb9,hr9:cHr9 },
    season:{ era:sEra,fip:sFip,whip:sWhip,k9:sK9,bb9:sBb9,hr9:sHr9 },
    last7: { era:lEra,fip:lFip,whip:lWhip,k9:lK9,bb9:lBb9,hr9:lHr9 },
    zone:{ career:zC, season:zS, last7:zL },
    byCount: genPitcherByCount(sEra),
  };
}

const batterProfiles = {
  'F. Freeman':     makeBatter(.298,.386,.503,220,.372,18.2,12.1, .311,.401,.521,21,.391,17.1,13.4, .333,.421,.556,3,.418,15.2,14.1, [[18,24,14],[28,35,26],[16,22,12]],[[20,26,15],[30,38,28],[18,24,13]],[[22,29,17],[33,41,30],[20,26,15]]),
  'P. Goldschmidt': makeBatter(.292,.381,.512,210,.368,22.4,11.8, .298,.379,.508,18,.374,21.8,11.4, .310,.392,.524,2,.389,20.4,12.1, [[16,22,13],[26,34,25],[15,21,12]],[[18,24,14],[28,36,27],[16,22,13]],[[20,26,15],[30,38,29],[18,24,14]]),
  'M. Olson':       makeBatter(.268,.364,.497,196,.362,26.8,10.4, .274,.367,.499,22,.366,25.4,10.8, .288,.378,.513,3,.381,24.8,11.4, [[15,20,12],[24,32,23],[14,19,11]],[[17,22,13],[26,34,25],[15,21,12]],[[19,25,14],[28,36,27],[17,23,13]]),
  'T. Turner':      makeBatter(.302,.372,.471,148,.356,18.8,9.8,  .309,.378,.482,14,.362,18.4,10.1, .322,.388,.495,2,.375,17.2,10.8, [[17,23,13],[27,34,25],[16,22,12]],[[19,25,14],[29,36,27],[17,23,13]],[[21,27,16],[31,38,29],[19,25,15]]),
  'A. Judge':       makeBatter(.284,.394,.601,254,.391,28.4,14.2, .291,.398,.614,42,.402,28.1,14.8, .305,.410,.631,6,.418,26.8,15.4, [[14,19,11],[22,30,22],[13,18,10]],[[16,21,12],[24,32,24],[14,19,11]],[[18,24,13],[26,34,26],[16,21,12]]),
  'P. Alonso':      makeBatter(.259,.352,.521,178,.358,31.4,9.8,  .262,.354,.524,31,.361,30.8,10.1, .271,.362,.538,4,.374,29.4,10.8, [[12,17,10],[20,28,21],[11,16,9]], [[14,19,11],[22,30,22],[12,17,10]],[[16,21,12],[24,32,24],[14,19,11]]),
  'K. Tucker':      makeBatter(.278,.361,.498,136,.349,22.2,10.1, .281,.364,.502,18,.353,22.0,10.4, .292,.374,.516,2,.364,21.2,11.2, [[16,22,13],[25,33,24],[15,21,12]],[[18,24,14],[27,35,26],[16,22,13]],[[20,26,15],[29,37,28],[18,24,14]]),
  'G. Stanton':     makeBatter(.254,.344,.516,148,.348,34.2,9.2,  .258,.347,.519,28,.352,33.8,9.6,  .265,.353,.530,4,.361,32.4,10.1, [[11,16,9],[19,27,20],[10,14,8]], [[13,18,10],[21,29,22],[11,16,9]], [[15,20,11],[23,31,24],[13,18,10]]),
};

const pitcherProfiles = {
  'C. Burnes':    makePitcher(2.94,2.88,1.00,10.8,2.1,0.8, 2.72,2.65,0.94,11.2,1.9,0.7, 1.88,2.10,0.82,12.4,1.6,0.4, [[14,20,12],[22,30,24],[12,18,10]],[[15,22,13],[24,32,26],[13,20,11]],[[16,24,14],[26,34,28],[14,22,12]]),
  'S. Alcantara': makePitcher(2.28,2.41,0.98,8.6,2.8,0.7,  2.58,2.70,1.02,8.9,2.6,0.8,  2.12,2.30,0.90,9.4,2.2,0.5,  [[16,22,14],[24,32,26],[14,20,12]],[[17,23,15],[25,33,27],[15,21,13]],[[18,25,16],[27,35,29],[16,22,14]]),
  'Z. Wheeler':   makePitcher(3.16,3.08,1.08,11.0,2.4,0.9, 3.24,3.14,1.10,11.2,2.3,1.0, 2.88,2.94,0.98,11.8,2.0,0.8, [[15,21,13],[23,31,25],[13,19,11]],[[16,22,14],[24,32,26],[14,20,12]],[[17,23,15],[25,33,27],[15,21,13]]),
  'K. Gausman':   makePitcher(3.01,2.96,1.04,10.2,2.6,0.9, 3.14,3.08,1.06,10.4,2.5,1.0, 2.72,2.81,0.96,11.0,2.2,0.7, [[13,19,11],[21,29,23],[11,17,9]], [[14,20,12],[22,30,24],[12,18,10]],[[15,21,13],[23,31,25],[13,19,11]]),
  'G. Cole':      makePitcher(3.20,3.14,1.06,11.4,2.2,1.0, 3.41,3.28,1.08,11.8,2.1,1.1, 3.08,2.98,1.00,12.2,1.9,0.9, [[14,20,12],[22,30,24],[12,18,10]],[[15,21,13],[23,31,25],[13,19,11]],[[16,22,14],[24,32,26],[14,20,12]]),
  'M. Fried':     makePitcher(3.02,2.98,1.12,9.8,2.8,0.8,  3.18,3.10,1.14,10.0,2.7,0.9, 2.76,2.84,1.04,10.6,2.4,0.7, [[15,21,13],[23,31,25],[13,19,11]],[[16,22,14],[24,32,26],[14,20,12]],[[17,23,15],[25,33,27],[15,21,13]]),
  'L. Webb':      makePitcher(3.25,3.18,1.10,9.4,3.0,0.9,  3.38,3.22,1.12,9.8,2.8,1.0,  3.01,2.96,1.02,10.4,2.5,0.8, [[14,20,12],[22,30,24],[12,18,10]],[[15,21,13],[23,31,25],[13,19,11]],[[16,22,14],[24,32,26],[14,20,12]]),
  'P. Corbin':    makePitcher(4.88,4.62,1.38,7.4,4.2,1.4,  5.12,4.84,1.42,7.0,4.4,1.5,  5.44,5.10,1.50,6.8,4.8,1.6,  [[10,14,8],[16,22,18],[8,12,7]],  [[10,14,8],[16,22,18],[8,12,7]],  [[9,13,7],[15,21,17],[7,11,6]]),
};

const lbBatting = {
  career:[
    {name:'F. Freeman',     pa:3421,avg:.298,obp:.386,slg:.503,hr:220,xwoba:.372,k:18.2,bb:12.1},
    {name:'A. Judge',       pa:2814,avg:.284,obp:.394,slg:.601,hr:254,xwoba:.391,k:28.4,bb:14.2},
    {name:'P. Goldschmidt', pa:3188,avg:.292,obp:.381,slg:.512,hr:210,xwoba:.368,k:22.4,bb:11.8},
    {name:'M. Olson',       pa:2944,avg:.268,obp:.364,slg:.497,hr:196,xwoba:.362,k:26.8,bb:10.4},
    {name:'T. Turner',      pa:3012,avg:.302,obp:.372,slg:.471,hr:148,xwoba:.356,k:18.8,bb:9.8 },
    {name:'P. Alonso',      pa:2812,avg:.259,obp:.352,slg:.521,hr:178,xwoba:.358,k:31.4,bb:9.8 },
    {name:'K. Tucker',      pa:2288,avg:.278,obp:.361,slg:.498,hr:136,xwoba:.349,k:22.2,bb:10.1},
    {name:'G. Stanton',     pa:2244,avg:.254,obp:.344,slg:.516,hr:148,xwoba:.348,k:34.2,bb:9.2 },
  ],
  season:[
    {name:'F. Freeman',     pa:624,avg:.311,obp:.401,slg:.521,hr:21,xwoba:.391,k:17.1,bb:13.4},
    {name:'A. Judge',       pa:584,avg:.291,obp:.398,slg:.614,hr:42,xwoba:.402,k:28.1,bb:14.8},
    {name:'P. Goldschmidt', pa:572,avg:.298,obp:.379,slg:.508,hr:18,xwoba:.374,k:21.8,bb:11.4},
    {name:'M. Olson',       pa:601,avg:.274,obp:.367,slg:.499,hr:22,xwoba:.366,k:25.4,bb:10.8},
    {name:'T. Turner',      pa:618,avg:.309,obp:.378,slg:.482,hr:14,xwoba:.362,k:18.4,bb:10.1},
    {name:'P. Alonso',      pa:598,avg:.262,obp:.354,slg:.524,hr:31,xwoba:.361,k:30.8,bb:10.1},
    {name:'K. Tucker',      pa:512,avg:.281,obp:.364,slg:.502,hr:18,xwoba:.353,k:22.0,bb:10.4},
    {name:'G. Stanton',     pa:488,avg:.258,obp:.347,slg:.519,hr:28,xwoba:.352,k:33.8,bb:9.6 },
  ],
  last7:[
    {name:'F. Freeman',     pa:28,avg:.333,obp:.421,slg:.556,hr:3,xwoba:.418,k:15.2,bb:14.1},
    {name:'A. Judge',       pa:26,avg:.305,obp:.410,slg:.631,hr:6,xwoba:.418,k:26.8,bb:15.4},
    {name:'T. Turner',      pa:30,avg:.322,obp:.388,slg:.495,hr:2,xwoba:.375,k:17.2,bb:10.8},
    {name:'P. Goldschmidt', pa:25,avg:.310,obp:.392,slg:.524,hr:2,xwoba:.389,k:20.4,bb:12.1},
    {name:'K. Tucker',      pa:24,avg:.292,obp:.374,slg:.516,hr:2,xwoba:.364,k:21.2,bb:11.2},
    {name:'M. Olson',       pa:27,avg:.288,obp:.378,slg:.513,hr:3,xwoba:.381,k:24.8,bb:11.4},
    {name:'P. Alonso',      pa:26,avg:.271,obp:.362,slg:.538,hr:4,xwoba:.374,k:29.4,bb:10.8},
    {name:'G. Stanton',     pa:22,avg:.265,obp:.353,slg:.530,hr:4,xwoba:.361,k:32.4,bb:10.1},
  ],
};

const lbPitching = {
  career:[
    {name:'S. Alcantara',ip:1042,era:2.28,fip:2.41,whip:0.98,k9:8.6, bb9:2.8,hr9:0.7},
    {name:'C. Burnes',   ip:764, era:2.94,fip:2.88,whip:1.00,k9:10.8,bb9:2.1,hr9:0.8},
    {name:'M. Fried',    ip:1088,era:3.02,fip:2.98,whip:1.12,k9:9.8, bb9:2.8,hr9:0.8},
    {name:'K. Gausman',  ip:1188,era:3.01,fip:2.96,whip:1.04,k9:10.2,bb9:2.6,hr9:0.9},
    {name:'Z. Wheeler',  ip:1248,era:3.16,fip:3.08,whip:1.08,k9:11.0,bb9:2.4,hr9:0.9},
    {name:'G. Cole',     ip:1124,era:3.20,fip:3.14,whip:1.06,k9:11.4,bb9:2.2,hr9:1.0},
    {name:'L. Webb',     ip:844, era:3.25,fip:3.18,whip:1.10,k9:9.4, bb9:3.0,hr9:0.9},
    {name:'P. Corbin',   ip:1344,era:4.88,fip:4.62,whip:1.38,k9:7.4, bb9:4.2,hr9:1.4},
  ],
  season:[
    {name:'C. Burnes',   ip:198,era:2.72,fip:2.65,whip:0.94,k9:11.2,bb9:1.9,hr9:0.7},
    {name:'S. Alcantara',ip:228,era:2.58,fip:2.70,whip:1.02,k9:8.9, bb9:2.6,hr9:0.8},
    {name:'M. Fried',    ip:202,era:3.18,fip:3.10,whip:1.14,k9:10.0,bb9:2.7,hr9:0.9},
    {name:'K. Gausman',  ip:206,era:3.14,fip:3.08,whip:1.06,k9:10.4,bb9:2.5,hr9:1.0},
    {name:'Z. Wheeler',  ip:212,era:3.24,fip:3.14,whip:1.10,k9:11.2,bb9:2.3,hr9:1.0},
    {name:'G. Cole',     ip:198,era:3.41,fip:3.28,whip:1.08,k9:11.8,bb9:2.1,hr9:1.1},
    {name:'L. Webb',     ip:194,era:3.38,fip:3.22,whip:1.12,k9:9.8, bb9:2.8,hr9:1.0},
    {name:'P. Corbin',   ip:162,era:5.12,fip:4.84,whip:1.42,k9:7.0, bb9:4.4,hr9:1.5},
  ],
  last7:[
    {name:'C. Burnes',   ip:48,era:1.88,fip:2.10,whip:0.82,k9:12.4,bb9:1.6,hr9:0.4},
    {name:'S. Alcantara',ip:44,era:2.12,fip:2.30,whip:0.90,k9:9.4, bb9:2.2,hr9:0.5},
    {name:'M. Fried',    ip:44,era:2.76,fip:2.84,whip:1.04,k9:10.6,bb9:2.4,hr9:0.7},
    {name:'K. Gausman',  ip:43,era:2.72,fip:2.81,whip:0.96,k9:11.0,bb9:2.2,hr9:0.7},
    {name:'Z. Wheeler',  ip:46,era:2.88,fip:2.94,whip:0.98,k9:11.8,bb9:2.0,hr9:0.8},
    {name:'G. Cole',     ip:44,era:3.08,fip:2.98,whip:1.00,k9:12.2,bb9:1.9,hr9:0.9},
    {name:'L. Webb',     ip:42,era:3.01,fip:2.96,whip:1.02,k9:10.4,bb9:2.5,hr9:0.8},
    {name:'P. Corbin',   ip:38,era:5.44,fip:5.10,whip:1.50,k9:6.8, bb9:4.8,hr9:1.6},
  ],
};

// ══════════════════════════════════════
// GENERIC HELPERS
// ══════════════════════════════════════
function buildCountGrid(containerId, stateRef, onSelect) {
  const grid = document.getElementById(containerId);
  if (!grid) return;
  grid.innerHTML = '';
  const corner = document.createElement('div');
  corner.className = 'count-cell hdr';
  grid.appendChild(corner);
  for (let b = 0; b <= 3; b++) {
    const el = document.createElement('div');
    el.className = 'count-cell hdr';
    el.textContent = b + 'B';
    grid.appendChild(el);
  }
  for (let s = 0; s <= 2; s++) {
    const lbl = document.createElement('div');
    lbl.className = 'count-cell hdr';
    lbl.textContent = s + 'S';
    grid.appendChild(lbl);
    for (let b = 0; b <= 3; b++) {
      const cell = document.createElement('div');
      cell.className = 'count-cell' + (stateRef.balls === b && stateRef.strikes === s ? ' active' : '');
      cell.textContent = b + '-' + s;
      cell.addEventListener('click', () => {
        stateRef.balls = b;
        stateRef.strikes = s;
        onSelect();
      });
      grid.appendChild(cell);
    }
  }
}

function buildZoneChart(containerId, data, color) {
  const grid = document.getElementById(containerId);
  if (!grid) return;
  grid.innerHTML = '';
  const max = Math.max(...data.flat());
  data.forEach(row => row.forEach(f => {
    const cell = document.createElement('div');
    cell.className = 'zone-cell';
    const pct = f / max;
    cell.style.background = color === 'accent2'
      ? `rgba(232,93,58,${0.05 + pct * 0.75})`
      : `rgba(91,143,255,${0.05 + pct * 0.75})`;
    cell.style.color = pct > 0.6 ? '#fff' : 'var(--muted)';
    cell.textContent = f + '%';
    grid.appendChild(cell);
  }));
}

// ══════════════════════════════════════
// COUNT STATE PAGE
// ══════════════════════════════════════
function renderCountPage() {
  const { balls, strikes, outs } = state.count;
  const key = balls + '-' + strikes;
  const d = countData[key] || countData['0-0'];
  const avgX = 0.312, avgK = 16.8, avgW = 24.2;

  document.getElementById('count-display').textContent = balls + '–' + strikes;
  document.getElementById('count-meta').textContent = ['0 outs','1 out','2 outs'][outs];

  document.getElementById('count-stat-cards').innerHTML = `
    <div class="stat-card">
      <div class="stat-label">League xwOBA</div>
      <div class="stat-value" style="color:var(--accent)">${d.xwoba.toFixed(3)}</div>
      <div class="stat-delta ${d.xwoba > avgX ? 'up':'down'}">${d.xwoba > avgX ? '↑':'↓'} ${Math.abs(d.xwoba-avgX).toFixed(3)} vs avg</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">K Rate</div>
      <div class="stat-value" style="color:var(--accent2)">${d.k.toFixed(1)}%</div>
      <div class="stat-delta ${d.k > avgK ? 'down':'up'}">${d.k > avgK ? '↑':'↓'} ${Math.abs(d.k-avgK).toFixed(1)}pp vs avg</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Walk Rate</div>
      <div class="stat-value" style="color:var(--accent3)">${d.bb.toFixed(1)}%</div>
      <div class="stat-delta">— ${balls}-${strikes} context</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Whiff %</div>
      <div class="stat-value">${d.whiff.toFixed(1)}%</div>
      <div class="stat-delta ${d.whiff > avgW ? 'down':'up'}">${d.whiff > avgW ? '↑':'↓'} ${Math.abs(d.whiff-avgW).toFixed(1)}pp vs avg</div>
    </div>
  `;

  buildOutcomeMatrix();
  buildOutcomeChart();
  buildZoneChart('count-zone', countZones[key] || countZones['0-0'], 'accent3');
  renderCountLeaderboard();
  document.getElementById('count-lb-title').textContent = `Batter Leaderboard — ${balls}-${strikes}`;
}

function buildOutcomeMatrix() {
  const { balls, strikes } = state.count;
  const container = document.getElementById('outcome-matrix');
  container.innerHTML = '';
  const corner = document.createElement('div'); corner.className = 'matrix-axis-label'; container.appendChild(corner);
  ['0B','1B','2B','3B'].forEach(l => {
    const el = document.createElement('div'); el.className = 'matrix-axis-label';
    el.style.cssText = 'font-family:var(--mono);font-size:9px;color:var(--muted)';
    el.textContent = l; container.appendChild(el);
  });
  for (let s = 0; s <= 2; s++) {
    const rl = document.createElement('div'); rl.className = 'matrix-axis-label';
    rl.style.cssText = 'font-family:var(--mono);font-size:9px;color:var(--muted)';
    rl.textContent = s+'S'; container.appendChild(rl);
    for (let b = 0; b <= 3; b++) {
      const key = b+'-'+s;
      const d = countData[key] || {k:15};
      const pct = d.k / 40;
      const isSel = b === balls && s === strikes;
      const cell = document.createElement('div');
      cell.className = 'matrix-cell';
      cell.style.background = isSel ? 'var(--accent)' : `rgba(${Math.round(30+pct*200)},${Math.round(200-pct*180)},60,${0.15+pct*0.5})`;
      cell.style.color = isSel ? '#000' : 'var(--text)';
      cell.style.outline = isSel ? '2px solid var(--accent)' : 'none';
      cell.innerHTML = `<span class="cell-val">${d.k}%</span>`;
      cell.title = `${b}-${s}: K%=${d.k}%, xwOBA=${d.xwoba}`;
      cell.addEventListener('click', () => {
        state.count.balls = b; state.count.strikes = s;
        buildCountGrid('count-grid', state.count, renderCountPage);
        renderCountPage();
      });
      container.appendChild(cell);
    }
  }
}

let outcomeChart;
function buildOutcomeChart() {
  const { balls, strikes } = state.count;
  const ctx = document.getElementById('outcome-chart').getContext('2d');
  const keys = Object.keys(countData);
  const colors = keys.map(k => {
    const [b,s] = k.split('-').map(Number);
    return b===balls && s===strikes ? 'rgba(200,240,78,0.85)' : 'rgba(91,143,255,0.35)';
  });
  if (outcomeChart) outcomeChart.destroy();
  outcomeChart = new Chart(ctx, {
    type:'bar',
    data:{ labels:keys, datasets:[{ data:keys.map(k=>countData[k].k), backgroundColor:colors, borderRadius:2, borderSkipped:false }] },
    options:{
      plugins:{legend:{display:false}},
      scales:{
        x:{ticks:{font:{family:'DM Mono',size:9},color:'#5a5f72'},grid:{color:'#1f2332'}},
        y:{ticks:{font:{family:'DM Mono',size:9},color:'#5a5f72'},grid:{color:'#1f2332'},title:{display:true,text:'K%',font:{family:'DM Mono',size:9},color:'#5a5f72'}},
      },
    },
  });
}

function renderCountLeaderboard() {
  const { balls, strikes, sort } = state.count;
  const key = balls + '-' + strikes;
  let data = lbBatting.season.map(p => {
    const bc = batterProfiles[p.name]?.byCount[key] || {};
    return { ...p, xwoba: bc.xwoba ?? p.xwoba, avg: bc.avg ?? p.avg, k: bc.k ?? p.k, whiff: bc.whiff ?? 22 };
  });
  data.sort((a,b) => b[sort]-a[sort]);
  const maxX = Math.max(...data.map(p=>p.xwoba));
  const tbody = document.getElementById('count-lb-body');
  tbody.innerHTML = '';
  data.forEach((p,i) => {
    const pct = (p.xwoba/maxX*100).toFixed(0);
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="td-rank">${i+1}</td>
      <td class="td-name">${p.name}</td>
      <td class="td-stat" style="color:var(--muted)">${p.pa}</td>
      <td class="td-stat td-highlight">${p.xwoba.toFixed(3)}</td>
      <td class="td-stat">${p.avg.toFixed(3)}</td>
      <td class="td-stat" style="color:var(--accent2)">${p.k.toFixed(1)}%</td>
      <td class="td-stat" style="color:var(--muted)">${p.whiff.toFixed(1)}%</td>
      <td class="td-bar-cell"><div class="bar-bg"><div class="bar-fill" style="width:${pct}%"></div></div></td>
    `;
    tbody.appendChild(tr);
  });
}

document.getElementById('count-outs').addEventListener('click', e => {
  const btn = e.target.closest('.out-btn');
  if (!btn) return;
  document.querySelectorAll('#count-outs .out-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  state.count.outs = +btn.dataset.outs;
  renderCountPage();
});

document.getElementById('count-lb-pills').addEventListener('click', e => {
  const pill = e.target.closest('.filter-pill');
  if (!pill) return;
  document.querySelectorAll('#count-lb-pills .filter-pill').forEach(p => p.classList.remove('active'));
  pill.classList.add('active');
  state.count.sort = pill.dataset.s;
  renderCountLeaderboard();
});

// ══════════════════════════════════════
// BATTER PROFILE
// ══════════════════════════════════════
function renderBatterProfile() {
  const name = document.getElementById('batter-select').value;
  state.batter.selected = name;
  const profile = batterProfiles[name];
  if (!profile) return;
  const d = profile[state.batter.window];

  document.getElementById('batter-stat-cards').innerHTML = `
    <div class="stat-card">
      <div class="stat-label">AVG / OBP / SLG</div>
      <div class="stat-value sm" style="color:var(--accent)">${d.avg.toFixed(3)} / ${d.obp.toFixed(3)} / ${d.slg.toFixed(3)}</div>
      <div class="stat-delta">slash line — ${state.batter.window === 'last7' ? 'last 7 games' : state.batter.window}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">xwOBA</div>
      <div class="stat-value" style="color:var(--accent)">${d.xwoba.toFixed(3)}</div>
      <div class="stat-delta ${d.xwoba > 0.340 ? 'up':'down'}">${d.xwoba > 0.340 ? '↑ above':'↓ below'} .340 league avg</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">K%</div>
      <div class="stat-value" style="color:var(--accent2)">${d.k.toFixed(1)}%</div>
      <div class="stat-delta ${d.k < 22 ? 'up':'down'}">${d.k < 22 ? 'below':'above'} avg strikeout rate</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">${state.batter.window==='career' ? 'Career HR' : state.batter.window==='season' ? 'Season HR' : 'HR (Last 7)'}</div>
      <div class="stat-value">${d.hr}</div>
      <div class="stat-delta">BB%: ${d.bb.toFixed(1)}%</div>
    </div>
  `;

  buildZoneChart('batter-zone', profile.zone[state.batter.window], 'accent3');
  buildCountGrid('batter-count-grid', state.batter, renderBatterCountStats);
  renderBatterCountStats();
}

function renderBatterCountStats() {
  const profile = batterProfiles[state.batter.selected];
  if (!profile) return;
  const key = state.batter.balls + '-' + state.batter.strikes;
  const cd = profile.byCount[key] || profile.byCount['0-0'];
  document.getElementById('batter-count-stats').innerHTML = `
    <div class="cstat"><div class="cstat-val">${cd.avg.toFixed(3)}</div><div class="cstat-lbl">AVG</div></div>
    <div class="cstat"><div class="cstat-val">${cd.xwoba.toFixed(3)}</div><div class="cstat-lbl">xwOBA</div></div>
    <div class="cstat"><div class="cstat-val">${cd.k.toFixed(1)}%</div><div class="cstat-lbl">K%</div></div>
    <div class="cstat"><div class="cstat-val">${cd.whiff.toFixed(1)}%</div><div class="cstat-lbl">Whiff%</div></div>
  `;
  buildCountGrid('batter-count-grid', state.batter, renderBatterCountStats);
}

function setBatterWindow(win, el) {
  document.querySelectorAll('#batter-window .window-tab').forEach(t => t.classList.remove('active'));
  el.classList.add('active');
  state.batter.window = win;
  renderBatterProfile();
}

// ══════════════════════════════════════
// PITCHER PROFILE
// ══════════════════════════════════════
function renderPitcherProfile() {
  const name = document.getElementById('pitcher-select').value;
  state.pitcher.selected = name;
  const profile = pitcherProfiles[name];
  if (!profile) return;
  const d = profile[state.pitcher.window];

  document.getElementById('pitcher-stat-cards').innerHTML = `
    <div class="stat-card">
      <div class="stat-label">ERA</div>
      <div class="stat-value" style="color:var(--accent)">${d.era.toFixed(2)}</div>
      <div class="stat-delta ${d.era < 3.50 ? 'up':'down'}">${d.era < 3.50 ? 'elite' : 'below avg'} — ${state.pitcher.window === 'last7' ? 'last 7' : state.pitcher.window}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">FIP</div>
      <div class="stat-value" style="color:var(--accent3)">${d.fip.toFixed(2)}</div>
      <div class="stat-delta">WHIP: ${d.whip.toFixed(2)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">K/9</div>
      <div class="stat-value" style="color:var(--accent2)">${d.k9.toFixed(1)}</div>
      <div class="stat-delta ${d.k9 > 10 ? 'up':''}">${d.k9 > 10 ? '↑ elite swing-and-miss' : 'above average'}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">BB/9</div>
      <div class="stat-value">${d.bb9.toFixed(1)}</div>
      <div class="stat-delta">HR/9: ${d.hr9.toFixed(1)}</div>
    </div>
  `;

  buildZoneChart('pitcher-zone', profile.zone[state.pitcher.window], 'accent2');
  buildCountGrid('pitcher-count-grid', state.pitcher, renderPitcherCountStats);
  renderPitcherCountStats();
}

function renderPitcherCountStats() {
  const profile = pitcherProfiles[state.pitcher.selected];
  if (!profile) return;
  const key = state.pitcher.balls + '-' + state.pitcher.strikes;
  const cd = profile.byCount[key] || profile.byCount['0-0'];
  document.getElementById('pitcher-count-stats').innerHTML = `
    <div class="cstat"><div class="cstat-val">${cd.k.toFixed(1)}%</div><div class="cstat-lbl">K%</div></div>
    <div class="cstat"><div class="cstat-val">${cd.xwoba.toFixed(3)}</div><div class="cstat-lbl">xwOBA Against</div></div>
    <div class="cstat" style="flex:1;min-width:180px">
      <div class="cstat-val" style="font-size:13px;font-family:var(--mono);letter-spacing:.04em;color:var(--text)">${cd.usage}</div>
      <div class="cstat-lbl">Pitch Mix</div>
    </div>
  `;
  buildCountGrid('pitcher-count-grid', state.pitcher, renderPitcherCountStats);
}

function setPitcherWindow(win, el) {
  document.querySelectorAll('#pitcher-window .window-tab').forEach(t => t.classList.remove('active'));
  el.classList.add('active');
  state.pitcher.window = win;
  renderPitcherProfile();
}

// ══════════════════════════════════════
// LEADERBOARD
// ══════════════════════════════════════
const lbCountState = { balls: -1, strikes: -1 };

function renderLeaderboard() {
  const { type, window: win, balls, strikes, sort } = state.leaderboard;
  const isBatting = type === 'batting';
  let data = isBatting ? [...lbBatting[win]] : [...lbPitching[win]];

  if (isBatting && balls !== null) {
    const key = balls + '-' + strikes;
    data = data.map(p => {
      const bc = batterProfiles[p.name]?.byCount[key];
      return bc ? { ...p, xwoba:bc.xwoba, avg:bc.avg, k:bc.k, whiff:bc.whiff } : p;
    });
  }

  const sortField = sort || (isBatting ? 'xwoba' : 'era');
  data.sort((a,b) => ['era','fip','whip','bb9','hr9'].includes(sortField) ? a[sortField]-b[sortField] : b[sortField]-a[sortField]);

  const winLabel = {career:'Career',season:'2024 Season',last7:'Last 7 Games'}[win];
  const countLabel = balls !== null ? ` · ${balls}-${strikes}` : '';
  document.getElementById('lb-title').textContent = `${isBatting?'Batting':'Pitching'} Leaderboard — ${winLabel}${countLabel}`;
  document.getElementById('lb-count-display').textContent = balls !== null ? `${balls}–${strikes}` : 'All';
  document.getElementById('lb-count-meta').textContent = balls !== null ? `${['0 outs','1 out','2 outs'][state.leaderboard.outs]} filter active` : 'no count filter';

  const pills = isBatting
    ? ['xwoba','avg','obp','k','hr'].map(s => `<div class="filter-pill${sortField===s?' active':''}" onclick="setLBSort('${s}')">${s.toUpperCase()}</div>`).join('')
    : ['era','fip','k9','bb9','whip'].map(s => `<div class="filter-pill${sortField===s?' active':''}" onclick="setLBSort('${s}')">${s.toUpperCase()}</div>`).join('');
  document.getElementById('lb-pills').innerHTML = pills;

  if (isBatting) {
    document.getElementById('lb-thead').innerHTML = `<tr><th>#</th><th>Player</th><th class="td-stat">PA</th><th class="td-stat">xwOBA</th><th class="td-stat">AVG</th><th class="td-stat">OBP</th><th class="td-stat">K%</th><th class="td-stat">BB%</th><th class="td-bar-cell"></th></tr>`;
    const maxX = Math.max(...data.map(p=>p.xwoba));
    document.getElementById('lb-body').innerHTML = data.map((p,i) => `
      <tr>
        <td class="td-rank">${i+1}</td>
        <td class="td-name">${p.name}</td>
        <td class="td-stat" style="color:var(--muted)">${p.pa}</td>
        <td class="td-stat td-highlight">${p.xwoba.toFixed(3)}</td>
        <td class="td-stat">${p.avg.toFixed(3)}</td>
        <td class="td-stat">${p.obp.toFixed(3)}</td>
        <td class="td-stat" style="color:var(--accent2)">${p.k.toFixed(1)}%</td>
        <td class="td-stat">${p.bb.toFixed(1)}%</td>
        <td class="td-bar-cell"><div class="bar-bg"><div class="bar-fill" style="width:${(p.xwoba/maxX*100).toFixed(0)}%"></div></div></td>
      </tr>`).join('');
  } else {
    document.getElementById('lb-thead').innerHTML = `<tr><th>#</th><th>Pitcher</th><th class="td-stat">IP</th><th class="td-stat">ERA</th><th class="td-stat">FIP</th><th class="td-stat">K/9</th><th class="td-stat">BB/9</th><th class="td-stat">WHIP</th><th class="td-bar-cell"></th></tr>`;
    const maxEra = Math.max(...data.map(p=>p.era));
    const minEra = Math.min(...data.map(p=>p.era));
    document.getElementById('lb-body').innerHTML = data.map((p,i) => `
      <tr>
        <td class="td-rank">${i+1}</td>
        <td class="td-name">${p.name}</td>
        <td class="td-stat" style="color:var(--muted)">${p.ip}</td>
        <td class="td-stat td-highlight">${p.era.toFixed(2)}</td>
        <td class="td-stat" style="color:var(--accent3)">${p.fip.toFixed(2)}</td>
        <td class="td-stat" style="color:var(--accent2)">${p.k9.toFixed(1)}</td>
        <td class="td-stat">${p.bb9.toFixed(1)}</td>
        <td class="td-stat">${p.whip.toFixed(2)}</td>
        <td class="td-bar-cell"><div class="bar-bg"><div class="bar-fill" style="width:${((maxEra-p.era)/(maxEra-minEra)*100).toFixed(0)}%"></div></div></td>
      </tr>`).join('');
  }
}

function setLBType(type, el) {
  document.querySelectorAll('.type-btn').forEach(b => b.classList.remove('active'));
  el.classList.add('active');
  state.leaderboard.type = type;
  state.leaderboard.sort = type === 'batting' ? 'xwoba' : 'era';
  renderLeaderboard();
}

function setLBWindow(win, el) {
  document.querySelectorAll('#lb-window .window-tab').forEach(t => t.classList.remove('active'));
  el.classList.add('active');
  state.leaderboard.window = win;
  renderLeaderboard();
}

function setLBSort(s) {
  state.leaderboard.sort = s;
  renderLeaderboard();
}

function clearLBCount() {
  state.leaderboard.balls = null;
  state.leaderboard.strikes = null;
  lbCountState.balls = -1;
  lbCountState.strikes = -1;
  buildCountGrid('lb-count-grid', lbCountState, onLBCountSelect);
  renderLeaderboard();
}

function onLBCountSelect() {
  state.leaderboard.balls = lbCountState.balls;
  state.leaderboard.strikes = lbCountState.strikes;
  renderLeaderboard();
}

document.getElementById('lb-outs').addEventListener('click', e => {
  const btn = e.target.closest('.out-btn');
  if (!btn) return;
  document.querySelectorAll('#lb-outs .out-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  state.leaderboard.outs = +btn.dataset.outs;
  renderLeaderboard();
});

// ══════════════════════════════════════
// PAGE NAV
// ══════════════════════════════════════
function setPage(page, navEl) {
  document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
  navEl.classList.add('active');
  document.querySelectorAll('.page-section').forEach(el => el.classList.remove('active'));
  document.getElementById('page-' + page).classList.add('active');
  state.page = page;
  if (page === 'batter')      renderBatterProfile();
  if (page === 'pitcher')     renderPitcherProfile();
  if (page === 'leaderboard') renderLeaderboard();
}

// ══════════════════════════════════════
// INIT
// ══════════════════════════════════════
buildCountGrid('count-grid', state.count, renderCountPage);
buildCountGrid('lb-count-grid', lbCountState, onLBCountSelect);
renderCountPage();
