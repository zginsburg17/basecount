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
# 1. Pull from Statcast for the first time (saves Parquet bundles as it goes)
./run.sh ensure-history

# 2. Confirm everything looks right
./run.sh status

# 3. Start the API
./run.sh api

# 4. Open the dashboard
open dashboard/dashboard.html
```

> **Note:** The first run downloads all Statcast data from 2015 to today. This takes several hours. Every subsequent run skips completed seasons and loads from Parquet.

### Subsequent runs (new machine or lost database)

```bash
git lfs pull          # download Parquet bundles if stored in repo
./run.sh import-all   # rebuild baseball.duckdb from local Parquet — no API calls
./run.sh api
open dashboard/dashboard.html
```

### Day-to-day

```bash
# Pull games played since the last load, refresh derived tables, update Parquet
./run.sh auto-update

# Start the API
./run.sh api

open dashboard/dashboard.html
```

The dashboard will also show a banner automatically when data is stale, with an **Update Now** button that triggers `auto-update` in the background.

---

## Command Reference

### Common workflows

| Command | When to use |
|---------|-------------|
| `./run.sh ensure-history` | Initial load, or after a gap. Imports from Parquet if bundles exist, pulls from API otherwise. |
| `./run.sh import-all [export_root]` | Rebuild the database from existing Parquet bundles. No API calls. |
| `./run.sh auto-update [export_root]` | Pull new games since last load, refresh derived tables, overwrite current season bundle. |
| `./run.sh api` | Start the API server on `http://localhost:8000`. |
| `./run.sh status` | Show loaded seasons, completeness, row counts, player coverage. |

### Loading data

| Command | Description |
|---------|-------------|
| `./run.sh season <year>` | Load one season (Parquet if bundle exists, API otherwise). |
| `./run.sh rebuild-season <year>` | Force re-pull one season from the API, replacing existing data. |
| `./run.sh range <start> <end>` | Load a range of seasons, preferring Parquet for each. |
| `./run.sh recent [days]` | Load the last N days of data (default 7). Not a backfill. |

### Parquet bundles

| Command | Description |
|---------|-------------|
| `./run.sh export-season <year> [export_root]` | Export one season to a Parquet bundle. |
| `./run.sh export-all [export_root]` | Export all loaded seasons to Parquet. |
| `./run.sh import-season <bundle_dir>` | Import one season bundle into the database. |
| `./run.sh import-all [export_root]` | Import all bundles and rebuild the full database. |

### Utilities

| Command | Description |
|---------|-------------|
| `./run.sh enrich` | Resolve missing player names, handedness, position. Safe to re-run. |
| `./run.sh season-report <year>` | Game counts, postseason breakdown, derived-table row counts. |
| `./run.sh reference-build` | Rebuild the canonical 2025 season from scratch. |
| `./run.sh reference-report` | Print validation report for 2025. |
| `./run.sh all` | `ensure-history` then start the API. |

Default export root is `exports/`. Pass a second argument to override: `./run.sh export-all /data/bundles`.

---

## Git LFS (storing Parquet in the repo)

Each season bundle is ~40–70 MB. The `.gitattributes` file already configures `exports/**/*.parquet` for Git LFS.

```bash
# One-time setup
git lfs install

# After a full load, commit all bundles
./run.sh export-all
git add exports/
git commit -m "Add season Parquet bundles"
git push

# New machine: pull bundles and rebuild DB
git lfs pull
./run.sh import-all
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

**Only one season appears** → the database was seeded with `recent` instead of a full load. Run `./run.sh ensure-history`.

**First load is slow** → expected. Statcast history is large and the pipeline is conservative with API requests to avoid rate-limiting. Every load after the first is fast (Parquet).

**Lost the database** → `./run.sh import-all` rebuilds it from Parquet in minutes.
