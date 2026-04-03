# BaseCount

A local, pitch-first baseball analytics platform built on MLB Statcast data.

BaseCount downloads pitch-by-pitch Statcast data through `pybaseball`, stores it in a local DuckDB database, serves analytics through a FastAPI application, and renders the results in a browser-based dashboard.

---

## Architecture

```
[MLB Statcast API]  ──via pybaseball──►  [ETL Pipeline]
                                               │
                                        baseball.duckdb
                                               │
                              [FastAPI server  ─  localhost:8000]
                                               │
                              [Browser dashboard (HTML / JS)]
```

Four layers, each dependent on the one below:

| Layer | File | Role |
|-------|------|------|
| Ingestion | `etl/pipeline.py` | Downloads Statcast data, transforms it, loads DuckDB |
| Database | `baseball.duckdb` | Local DuckDB file — created on first load |
| API | `api/main.py` | FastAPI server, read-only queries, JSON responses |
| Dashboard | `dashboard/dashboard.html` | Browser SPA, calls the local API |

---

## Design Principles

- **Pitch as atomic unit.** All higher-level analytics are derived from pitch-level state, not pre-aggregated at-bat summaries.
- **Reference season first.** The canonical reference season is `2025`. A season should be validated before scaling to other years.
- **Materialized derived tables.** Pitch-state and pitch-transition summary tables are pre-built per season so the API can serve count-state and prediction features from fast, stable data.
- **Reproducible season bundles.** Any season can be exported to Parquet and re-imported on another machine without hitting the Statcast API again.
- **Spring training excluded.** Only regular season (`R`) and postseason (`F`, `D`, `L`, `W`) game types are retained.

---

## Repository Layout

```
basecount/
├── analytics/
│   └── queries.py          # Reusable analytical SQL functions
├── api/
│   └── main.py             # FastAPI application
├── dashboard/
│   ├── dashboard.html      # Browser SPA entry point
│   ├── dashboard.js        # Frontend logic
│   └── dashboard.css       # Styling
├── etl/
│   └── pipeline.py         # ETL pipeline and CLI
├── requirements.txt
└── run.sh                  # Primary CLI wrapper
```

Files created on first use:

```
basecount/
├── baseball.duckdb         # Created after the first data load
└── venv/                   # Created automatically by run.sh
```

---

## Prerequisites

- Python 3.9 or later
- Terminal access
- Internet access (required for Statcast data, player enrichment, and pip)

---

## Quick Start

### First-time setup

```bash
# 1. Clone and enter the repo
cd /path/to/basecount

# 2. Build the canonical 2025 reference season
./run.sh reference-build

# 3. Confirm the season report looks correct
./run.sh reference-report

# 4. Export a reproducible bundle (optional but recommended)
./run.sh reference-export

# 5. Start the API server (keep this terminal open)
./run.sh api

# 6. Open the dashboard
open dashboard/dashboard.html
```

### Daily use

```bash
# Refresh the current season with the last 2 days of data
./run.sh current-season-update 2

# Start the API
./run.sh api

# Open the dashboard
open dashboard/dashboard.html
```

### Full historical load

To load all Statcast history from 2015 to the current season:

```bash
./run.sh ensure-history
./run.sh status
./run.sh api
open dashboard/dashboard.html
```

---

## run.sh Command Reference

`run.sh` handles virtual-environment creation, dependency installation, and routing to the correct Python command. Pass no arguments (or `all`) to ensure full history is loaded and then start the API.

### Data loading

| Command | Description |
|---------|-------------|
| `./run.sh recent [days]` | Load the last N days of data (default: 7). Not a historical backfill. |
| `./run.sh current-season-update [days]` | Load a rolling window for the active season. Recommended for daily refreshes (default: 2 days). |
| `./run.sh season <year> [chunk_days]` | Load a single season. |
| `./run.sh rebuild-season <year> [chunk_days]` | Delete a season from the database and reload it cleanly. Use this to fix one bad season without touching the rest. |
| `./run.sh range <start> <end> [chunk_days]` | Load an inclusive range of seasons. |
| `./run.sh all-history [chunk_days]` | Load all Statcast history from 2015 to the current year. |
| `./run.sh ensure-history [chunk_days]` | Verify all seasons from 2015 to the current year are present; backfill only missing seasons. Fails if coverage is still incomplete after backfill. |

### Reference season shortcuts

| Command | Description |
|---------|-------------|
| `./run.sh reference-build [chunk_days]` | Rebuild the canonical 2025 season from scratch. |
| `./run.sh reference-report` | Print a validation report for the 2025 season. |
| `./run.sh reference-export [export_root]` | Export the 2025 season bundle to disk. |

### Export and import

| Command | Description |
|---------|-------------|
| `./run.sh export-season <year> [export_root]` | Export one season to a Parquet bundle (default output: `exports/`). |
| `./run.sh import-season <bundle_dir>` | Import a previously exported Parquet bundle into the database. |

### Operations

| Command | Description |
|---------|-------------|
| `./run.sh enrich` | Fill in missing player names, handedness, position, and team. Safe to run multiple times. |
| `./run.sh status` | Report loaded seasons, completeness, date span, row counts, and player-name coverage. |
| `./run.sh season-report <year>` | Print game counts, postseason breakdown, and derived-table row counts for one season. |
| `./run.sh api` | Start the API server on `http://localhost:8000`. |
| `./run.sh all [chunk_days]` | Ensure full history is loaded, then start the API. |
| `./run.sh help` | Print usage. |

---

## ETL Direct Reference

If you prefer to bypass `run.sh` and call the pipeline directly (with the virtual environment already active):

```bash
python etl/pipeline.py recent --days 7
python etl/pipeline.py season --season 2025
python etl/pipeline.py rebuild-season --season 2025
python etl/pipeline.py range --season-start 2018 --season-end 2025
python etl/pipeline.py all-history
python etl/pipeline.py ensure-history
python etl/pipeline.py season-report --season 2025
python etl/pipeline.py export-season --season 2025 --export-root exports
python etl/pipeline.py import-season --import-dir exports/season=2025
python etl/pipeline.py enrich
python etl/pipeline.py status

# Optional flags
python etl/pipeline.py season --season 2025 --skip-enrich --skip-season-stats
python etl/pipeline.py status --db-path custom.duckdb
```

---

## Season Bundle Format

Exporting a season creates a directory with one Parquet file per table:

```
exports/season=2025/
├── pitches.parquet
├── at_bats.parquet
├── games.parquet
├── players.parquet
├── league_pitch_state_summary.parquet
├── player_pitch_state_summary.parquet
├── pitch_transition_summary.parquet
├── batting_standard_stats.parquet
├── batting_value_stats.parquet
├── pitching_standard_stats.parquet
├── pitching_value_stats.parquet
└── metadata.json
```

To import on another machine:

```bash
./run.sh import-season exports/season=2025
```

Derived summary tables (`league_pitch_state_summary`, `player_pitch_state_summary`, `pitch_transition_summary`) are regenerated automatically if their Parquet files are absent from the bundle.

---

## Season Validation Expectations

The following game counts are used as correctness checkpoints:

| Season | Regular games | Postseason total |
|--------|--------------|-----------------|
| 2020 | 898 (shortened COVID season) | — |
| 2025 | 2430 | 47 (F: 11, D: 18, L: 11, W: 7) |

Verify with:

```bash
./run.sh season-report 2025
```

---

## API Reference

The API runs at `http://localhost:8000`. Interactive documentation is at `http://localhost:8000/docs`.

### Metadata

| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Database connectivity check |
| `GET /api/meta/context` | Full data context: seasons, players, teams, completeness |
| `GET /api/meta/coverage` | Season completeness and player-name coverage summary |
| `GET /api/reference/report` | Validation report for the reference season (default: 2025) |
| `GET /api/players/search` | Player autocomplete search (`?role=batter\|pitcher&q=<term>`) |

### Pitch-first analytics

| Endpoint | Description |
|----------|-------------|
| `GET /api/pitch-state/league` | League-wide pitch outcome rates for a given count/outs/handedness state |
| `GET /api/pitch-state/player` | Per-player pitch outcome rates (batter or pitcher perspective) |
| `GET /api/predict/next-pitch` | Next-pitch probability distribution given the current count and previous pitch type |
| `GET /api/predict/outcome-by-pitch` | Expected outcomes if a specific pitch type is thrown in the current count state |

### Count state

| Endpoint | Description |
|----------|-------------|
| `GET /api/count-state/batter-splits` | Batter performance in a specific count situation |
| `GET /api/count-state/outcome-matrix` | Full count-state outcome probability matrix (K%, BB%, HR%, AVG, xwOBA) |
| `GET /api/count-state/zone-map` | Pitch location zone frequency map (3×3 grid) for a given count state |

### Player profiles

| Endpoint | Description |
|----------|-------------|
| `GET /api/batter/{id}/overview` | Batter summary, per-season splits, count splits, zone contact map |
| `GET /api/batter/{id}/splits` | Batter situational splits by a given dimension |
| `GET /api/pitcher/{id}/overview` | Pitcher summary, per-season splits, pitch usage by count, zone map |
| `GET /api/pitcher/{id}/count-profile` | Pitcher pitch mix broken out by every count |
| `GET /api/pitcher/{id}/splits` | Pitcher situational splits by a given dimension |
| `GET /api/pitcher/{id}/sequences` | Most common pitch-to-pitch sequences for a pitcher |

### Teams and leaderboards

| Endpoint | Description |
|----------|-------------|
| `GET /api/team/{code}/overview` | Team batting and pitching aggregates with per-player leaderboards |
| `GET /api/pitching/overview` | League-wide aggregate pitching summary |
| `GET /api/leaderboard/batting` | Top batters ranked by xwOBA |
| `GET /api/leaderboard/stuff` | Top pitchers ranked by whiff rate (Stuff+ proxy) |

### Sequences and at-bats

| Endpoint | Description |
|----------|-------------|
| `GET /api/sequences` | Pitch-to-pitch sequence patterns and outcomes |
| `GET /api/sequences/outcomes` | Outcome statistics for a specific ball/strike/foul sequence |
| `GET /api/at-bat/{game_pk}/{at_bat_number}` | Full pitch-by-pitch timeline of a single at-bat |

### Common query parameters

Most endpoints accept these filters:

| Parameter | Values | Description |
|-----------|--------|-------------|
| `season` | `2015`–present | Single season |
| `season_start` / `season_end` | `2015`–present | Season range |
| `season_type` | `regular`, `postseason`, `both` | Game type filter |
| `window` | `season`, `career`, `last7` | Time window shorthand |
| `stand` | `L`, `R`, `S` | Batter handedness |
| `p_throws` | `L`, `R` | Pitcher handedness |
| `balls` / `strikes` | `0`–`3` / `0`–`2` | Count filter |
| `outs` | `0`–`2` | Outs filter |

---

## Dashboard Guide

Open `dashboard/dashboard.html` in a browser while the API server is running.

### Season controls

At the top of the page, select:

- **Single Season** — analyze one year
- **Season Range** — combine multiple years
- **Regular Season** or **Postseason** — filter game type

The dashboard can only query seasons loaded in the database. Use `./run.sh status` to confirm what is available.

### Pages

| Page | Description |
|------|-------------|
| **Count State** | Interactive count selector (4×3 grid), outcome probability matrix, and batter splits for the selected count |
| **Batter Profile** | Select any batter by name. Shows career/season summary, count-state breakdown, and zone contact rate map |
| **Pitcher Profile** | Select any pitcher by name. Shows pitch usage, per-count tendencies, velocity and whiff trends, and zone map |
| **Team Profile** | Select a team by code. Shows batting and pitching aggregates plus per-player leaderboards |
| **Leaderboard** | Top batters by xwOBA and top pitchers by whiff rate for the current scope |
| **Pitch Sequence** | Select one or more pitchers and compare pitch-to-pitch sequence patterns and outcomes |

### Player labels

Player names display as:

```
Player Name · TEAM
```

If a name cannot be resolved, the label falls back to:

```
Batter #657656
```

Fix this by running `./run.sh enrich`.

---

## Database Schema

### Core tables

| Table | Description |
|-------|-------------|
| `pitches` | Atomic pitch-level data. Every row is one pitch. Includes count state, pitch type, velocity, movement, location, and outcome. This is the source of truth. |
| `at_bats` | Derived rollup of pitches into at-bats. Pre-aggregated for query speed. |
| `games` | Game-level metadata: teams, date, venue, season, game type. |
| `players` | Player directory: full name, bats, throws, position, team. |
| `ingestion_log` | ETL run history. |
| `season_backfill_status` | Completion status for each season's backfill. |

### Derived / summary tables

| Table | Description |
|-------|-------------|
| `league_pitch_state_summary` | Pre-aggregated pitch outcome counts by season, count, outs, handedness, and pitch type. Powers the pitch-state API endpoints. |
| `player_pitch_state_summary` | Same as above, per player (both batter and pitcher perspectives). |
| `pitch_transition_summary` | Pitch-to-pitch transition counts and outcome rates per player. Powers the next-pitch prediction endpoints. |

### Supplemental stats tables

| Table | Source | Description |
|-------|--------|-------------|
| `batting_standard_stats` | Fangraphs | Traditional batting stats per player per season |
| `batting_value_stats` | Fangraphs | wOBA, wRC+, WAR, and other value metrics |
| `pitching_standard_stats` | Fangraphs | Traditional pitching stats per player per season |
| `pitching_value_stats` | Fangraphs | xFIP, SIERA, ERA-, FIP-, WAR, and other value metrics |

---

## Player Enrichment

Player names and metadata are resolved from two sources in order:

1. `pybaseball.playerid_reverse_lookup()` — bulk MLBAM ID lookup
2. MLB Stats API (`statsapi.mlb.com`) — individual fallback for any IDs not resolved in step 1

Handedness (`bats`, `throws`), position, and team are derived from the pitch data itself (most recent values). Enrichment is safe to run multiple times — it only updates rows where `full_name IS NULL`.

---

## Troubleshooting

### `permission denied: ./run.sh`

```bash
chmod +x run.sh
```

### Player names show as `Batter #657656`

```bash
./run.sh enrich
```

If names remain incomplete, verify internet access and run enrichment again.

### Dashboard opens but is blank

Check in order:
1. Is the API terminal still running? (`./run.sh api`)
2. Does `http://localhost:8000/docs` open in a browser?
3. Does `./run.sh status` show loaded seasons?
4. Is the correct `dashboard.html` file open?

### Dashboard shows the wrong season

1. Confirm the season is loaded: `./run.sh status`
2. Check the season scope selector at the top of the dashboard
3. Refresh the page

### Only one season appears

The database was likely populated with `recent` instead of a full historical load. Fix:

```bash
./run.sh ensure-history
./run.sh status
```

### Historical loading is slow

This is expected. Full Statcast history is large and the pipeline is intentionally conservative with external API requests to avoid rate limiting. Each season is loaded in weekly chunks with delays between requests.

### API does not start

Check:
1. Dependencies installed: `pip install -r requirements.txt`
2. Python 3.9+ is available
3. Port 8000 is not in use: `lsof -i :8000`
4. Database file exists: `ls -lh baseball.duckdb`

Start directly if needed:

```bash
source venv/bin/activate
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000
```

---

## Documentation Maintenance

The README is part of the product and must be updated whenever the codebase changes in a way that affects:

- setup, installation, or prerequisites
- commands, their names, arguments, or defaults
- API endpoints or their behavior
- dashboard pages, controls, or workflow
- repository file layout
- data loading behavior or season support

Code changes that make this README inaccurate require a README update in the same body of work.
