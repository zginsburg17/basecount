# BaseCount

A local, pitch-first baseball analytics platform built on MLB Statcast data. Stores pitch-by-pitch data in a local DuckDB database, serves it through a FastAPI backend, and renders it in a browser dashboard.

---

## Architecture

```
[MLB Statcast API]  ──via pybaseball──►  [ETL Pipeline]
                                               │
                                        exports/*.parquet  ◄── source of truth
                                               │
                                        baseball.duckdb    ◄── local query cache
                                               │
                              [FastAPI server  ─  localhost:8000]
                                               │
                              [Browser dashboard (HTML / JS)]
```

| Layer | File | Role |
|-------|------|------|
| Ingestion | `etl/pipeline.py` | Downloads Statcast, transforms, loads DuckDB |
| Parquet bundles | `exports/season=*/` | Portable season snapshots — never re-pull the API |
| Database | `baseball.duckdb` | Local DuckDB — always reconstructable from Parquet |
| API | `api/main.py` | FastAPI server, read-only queries |
| Dashboard | `dashboard/dashboard.html` | Browser SPA |

---

## Data philosophy

**The Statcast API is called exactly once per season.** After that, everything comes from Parquet.

| Situation | What the ETL does |
|-----------|-------------------|
| Season bundle exists in `exports/` | Import from Parquet — zero API calls |
| No bundle exists | Pull from Statcast API → save Parquet → done forever |
| New games since last load | `auto-update`: pull only new dates, overwrite current season bundle |

This means:
- The first full historical load is slow (~hours). Every load after that is fast.
- `baseball.duckdb` is disposable. Lose it, run `./run.sh import-all`, done.
- Only `auto-update` ever touches the Statcast API after initial setup.

---

## Quick Start

### Prerequisites

- Python 3.9+
- Terminal + internet access (only needed for the initial load and daily updates)

### First time

```bash
./run.sh setup    # pulls from Statcast; saves Parquet bundles as it goes
./run.sh status   # confirm everything looks right
./run.sh api      # start the API
open dashboard/dashboard.html
```

> **Note:** `setup` downloads all Statcast data from 2015 to today. This takes several hours the first time. Every run after that skips completed seasons and loads from Parquet.

### New machine (Parquet bundles already exist)

```bash
git lfs pull      # download Parquet bundles
./run.sh restore  # rebuild database from Parquet — no API calls, takes minutes
./run.sh api
open dashboard/dashboard.html
```

### Day-to-day

```bash
./run.sh update   # pull new games, refresh derived tables, update Parquet bundle
./run.sh api
open dashboard/dashboard.html
```

The dashboard will also show a banner automatically when data is stale, with an **Update Now** button that triggers `auto-update` in the background.

---

## Commands

| Command | What it does |
|---------|-------------|
| `./run.sh setup` | First-time load. Imports from Parquet if bundles exist, pulls Statcast API otherwise. Safe to re-run after gaps. |
| `./run.sh restore` | Rebuild the database from local Parquet bundles. No API calls. Use after `git lfs pull` on a new machine. |
| `./run.sh update` | Pull games since the last load for the current season. Refreshes derived tables and overwrites the current season bundle. |
| `./run.sh api` | Start the API server on `http://localhost:8000`. |
| `./run.sh status` | Show loaded seasons, completeness, row counts, player-name coverage. |
| `./run.sh export [year]` | Export Parquet bundles. Omit year for all seasons; pass a year for one. |
| `./run.sh rebuild <year>` | Force re-pull one season from the Statcast API, replacing existing data and bundle. |
| `./run.sh report <year>` | Season validation report: game counts, postseason breakdown, derived-table row counts. |
| `./run.sh enrich` | Resolve missing player names, handedness, and position. Safe to re-run. |

---

## Git LFS (storing Parquet in the repo)

Each season bundle is ~40–70 MB. The `.gitattributes` file already configures `exports/**/*.parquet` for Git LFS.

```bash
# One-time setup
git lfs install

# After a full load, commit all bundles
./run.sh export
git add exports/
git commit -m "Add season Parquet bundles"
git push

# New machine: pull bundles and rebuild DB
git lfs pull
./run.sh restore
```

---

## Season Validation

| Season | Regular games | Notes |
|--------|--------------|-------|
| 2020 | 898 | COVID-shortened season |
| 2021–2024 | 2430 | Standard |
| 2025 | 2430 + 47 postseason | Reference season |
| 2026 | Partial | Ongoing |

```bash
./run.sh season-report 2025
```

---

## API Reference

Server: `http://localhost:8000` — Interactive docs: `http://localhost:8000/docs`

### Metadata & admin

| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Database connectivity check |
| `GET /api/meta/context` | Seasons, players, teams, completeness |
| `GET /api/meta/coverage` | Season completeness and player-name coverage |
| `GET /api/players/search` | Player autocomplete (`?role=batter\|pitcher&q=<term>`) |
| `GET /api/admin/freshness` | Latest game date and days-stale for the current season |
| `POST /api/admin/sync` | Trigger background auto-update (non-blocking) |
| `GET /api/admin/sync-status` | Sync job state: `idle \| running \| done \| error` |

### Analytics

| Endpoint | Description |
|----------|-------------|
| `GET /api/pitch-state/league` | League pitch outcome rates by count/outs/handedness |
| `GET /api/pitch-state/player` | Per-player pitch outcome rates |
| `GET /api/predict/next-pitch` | Next-pitch probabilities given count + previous pitch type |
| `GET /api/predict/outcome-by-pitch` | Expected outcomes by pitch type in a given count |
| `GET /api/count-state/outcome-matrix` | Full count-state probability matrix |
| `GET /api/count-state/batter-splits` | Batter performance in a specific count |
| `GET /api/count-state/zone-map` | Pitch location frequency (3×3 grid) by count |
| `GET /api/batter/{id}/overview` | Batter summary, season splits, count splits, zone map |
| `GET /api/pitcher/{id}/overview` | Pitcher summary, pitch usage, velocity and whiff trends |
| `GET /api/team/{code}/overview` | Team batting/pitching aggregates and per-player leaderboards |
| `GET /api/leaderboard/batting` | Top batters by xwOBA |
| `GET /api/leaderboard/stuff` | Top pitchers by whiff rate |
| `GET /api/sequences` | Pitch-to-pitch sequence patterns and outcomes |
| `GET /api/at-bat/{game_pk}/{at_bat_number}` | Full pitch timeline of a single at-bat |

### Common query parameters

| Parameter | Description |
|-----------|-------------|
| `season` | Single season (e.g. `2025`) |
| `season_start` / `season_end` | Season range |
| `season_type` | `regular`, `postseason`, or `both` |
| `stand` | Batter handedness: `L`, `R`, `S` |
| `p_throws` | Pitcher handedness: `L`, `R` |
| `balls` / `strikes` / `outs` | Count state filters |

---

## Database Schema

### Core tables

| Table | Description |
|-------|-------------|
| `pitches` | One row per pitch. Count state, pitch type, velocity, movement, location, outcome. |
| `at_bats` | Pitch rollup per at-bat. Pre-aggregated for query speed. |
| `games` | Game metadata: teams, date, venue, season, game type. |
| `players` | Player directory: name, bats, throws, position, team. |

### Derived tables (pre-materialized per season)

| Table | Description |
|-------|-------------|
| `league_pitch_state_summary` | Pitch outcome counts by season, count, outs, handedness, pitch type |
| `player_pitch_state_summary` | Same, per player (batter and pitcher perspectives) |
| `pitch_transition_summary` | Pitch-to-pitch transition counts and outcome rates per player |

### Supplemental stats (from Fangraphs)

| Table | Description |
|-------|-------------|
| `batting_standard_stats` | Traditional batting stats per player/season |
| `batting_value_stats` | wOBA, wRC+, WAR |
| `pitching_standard_stats` | Traditional pitching stats per player/season |
| `pitching_value_stats` | xFIP, SIERA, ERA-, FIP-, WAR |

---

## Troubleshooting

**`permission denied: ./run.sh`** → `chmod +x run.sh`

**Player names show as `Batter #657656`** → `./run.sh enrich`

**Dashboard is blank** → confirm the API is running (`./run.sh api`), check `http://localhost:8000/docs`, and verify `./run.sh status` shows loaded seasons.

**Only one season appears** → the database was seeded with only recent data. Run `./run.sh setup` to fill all seasons.

**First load is slow** → expected. Statcast history is large and the pipeline is conservative with API requests to avoid rate-limiting. Every load after the first is fast (Parquet).

**Lost the database** → `./run.sh restore` rebuilds it from Parquet in minutes.
