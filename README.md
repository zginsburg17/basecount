# BaseCount — Baseball Analytics Platform

A pitch-level baseball analytics platform built on MLB Statcast data. Ingests every pitch from Baseball Savant, stores it in a local DuckDB analytical database, exposes a FastAPI REST layer, and renders insights through a dark-themed interactive dashboard — all with zero external infrastructure.

---

## What It Does

BaseCount treats the **individual pitch** as the atomic unit of analysis. Every pitch is stored with full game-state context: count, outs, base state, location, velocity, spin, and movement. At-bats and player-level stats are derived from pitches, never pre-aggregated in ways that lock out filters.

From that foundation the platform answers questions like:

- Who hits best in 2-2 counts with runners on base?
- How does a pitcher's pitch mix shift when he falls behind 2-0 vs. ahead 0-2?
- What are the most common two-pitch sequences for a given starter, and how often does each end in a whiff?
- Which pitchers have the highest whiff rate on their slider this season?

---

## Architecture

```
basecount/
├── etl/
│   └── pipeline.py       # Statcast ingestion → DuckDB (players, games, pitches, at_bats)
├── analytics/
│   └── queries.py        # DuckDB query functions returning pandas DataFrames
├── api/
│   └── main.py           # FastAPI REST layer (CORS-enabled, read-only DB access)
├── dashboard/
│   └── dashboard.html    # Standalone dark-themed dashboard (Chart.js, no build step)
├── baseball.duckdb       # Local analytical database (gitignored)
└── requirements.txt
```

### Stack

| Layer | Technology |
| --- | --- |
| Data ingestion | `pybaseball` → Baseball Savant / Statcast |
| Database | DuckDB (in-process, columnar, zero infra) |
| Data processing | pandas, numpy |
| API | FastAPI + Uvicorn |
| Frontend | Vanilla HTML/CSS/JS + Chart.js |

---

## Database Schema

### `pitches` (atomic unit)
Every pitch from Statcast. ~60 columns covering participants, game state, physical pitch characteristics, batted-ball data, and advanced metrics.

| Column group | Examples |
| --- | --- |
| Identity | `pitch_id`, `game_pk`, `at_bat_number`, `pitch_number` |
| Participants | `pitcher_id`, `batter_id`, `stand`, `p_throws` |
| Game state | `inning`, `outs_when_up`, `balls`, `strikes`, `base_state` |
| Physics | `release_speed`, `release_spin_rate`, `pfx_x`, `pfx_z`, `plate_x`, `plate_z` |
| Outcome | `description`, `zone`, `type`, `events` |
| Batted ball | `launch_speed`, `launch_angle`, `bb_type`, `hc_x`, `hc_y` |
| Advanced | `xBA`, `xwOBA`, `woba_value`, `launch_speed_angle` |

### `at_bats` (rollup)

Pre-aggregated from pitches for query speed. Tracks pitch count, final count, whether the AB reached 2-strike / 3-ball / full count, final event, and xwOBA.

### `players` / `games` / `ingestion_log`
Dimension tables and run tracking. Player metadata (name, bats, throws, position, team) is enriched via `pybaseball.playerid_reverse_lookup` after each ingestion run.

---

## Analytics Layer

All query functions live in `analytics/queries.py` and return pandas DataFrames. The API and any script can call them directly.

| Function | What it returns |
| --- | --- |
| `count_state_splits(con, balls, strikes, outs, base_state, season, min_pa)` | Batter performance (xwOBA, wOBA, swing%, whiff%, contact%) ranked for a specific count situation |
| `pitcher_count_profile(con, pitcher_id, season)` | Pitch mix, velocity, and whiff rate by every count in the balls × strikes grid |
| `at_bat_outcome_by_count(con, season)` | Full count-state matrix — K%, BB%, HR%, hit%, xwOBA for every count combination |
| `pitch_sequence_patterns(con, pitcher_id, min_occurrences, season)` | Most common pitch-to-pitch transitions with whiff and K rate per sequence |
| `situational_splits(con, player_id, role, split_by, season)` | xwOBA and outcome rates split by base state, outs, handedness, or final count |
| `full_at_bat_timeline(con, game_pk, at_bat_number)` | Pitch-by-pitch reconstruction of a single at-bat |
| `batting_leaderboard(con, season, limit, min_pa)` | Top batters by xwOBA with wOBA, K%, BB%, HR%, AVG |
| `stuff_plus_proxy(con, season, pitch_type, min_pitches)` | Pitch quality leaderboard ranked by whiff rate with velo, spin, and break |

---

## REST API

Start the server:

```bash
uvicorn api.main:app --reload --port 8000
```

Interactive docs available at `http://localhost:8000/docs`.

### Endpoints

| Method | Path | Description |
| --- | --- | --- |
| GET | `/api/count-state/batter-splits` | Batter splits for a specific count (`balls`, `strikes`, optional `outs`, `base_state`, `season`, `min_pa`) |
| GET | `/api/count-state/outcome-matrix` | Full count × outcome probability matrix |
| GET | `/api/pitcher/{pitcher_id}/count-profile` | Pitch mix by count for a given pitcher |
| GET | `/api/pitcher/{pitcher_id}/sequences` | Most common pitch-to-pitch sequences |
| GET | `/api/pitcher/{pitcher_id}/splits` | Situational splits (base state, handedness, etc.) |
| GET | `/api/batter/{batter_id}/splits` | Situational splits for a batter |
| GET | `/api/at-bat/{game_pk}/{at_bat_number}` | Full pitch-by-pitch timeline of one at-bat |
| GET | `/api/leaderboard/batting` | Season batting leaderboard (`season`, `limit`, `min_pa`) |
| GET | `/api/leaderboard/stuff` | Pitch quality leaderboard (`season`, `pitch_type`, `min_pitches`) |

---

## Dashboard

Open `dashboard/dashboard.html` directly in a browser (requires the API running on port 8000). No build step or bundler needed.

Features:

- **Count State Explorer** — interactive balls × strikes grid; click any count to load batter splits and a Chart.js pitch-mix visualization
- **Batter Leaderboard** — filterable by season, PA minimum, and result limit
- **Pitch Quality Leaderboard** — stuff proxy rankings by pitch type
- Dark editorial design (DM Mono / Playfair Display / Syne typefaces, noise texture overlay)

---

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Initialize the database and load recent data
python etl/pipeline.py

# To backfill a full season (runs in weekly chunks, ~3 s delay between each):
# python -c "from etl.pipeline import init_db, backfill_season, enrich_players; \
#             con = init_db(); backfill_season(con, 2024); enrich_players(con)"

# 3. Start the API
uvicorn api.main:app --reload --port 8000

# 4. Open the dashboard
open dashboard/dashboard.html
```

The database path defaults to `baseball.duckdb` in the working directory. Override with the `DB_PATH` environment variable.

---

## Query Examples

```python
from analytics.queries import count_state_splits, pitch_sequence_patterns, full_at_bat_timeline
import duckdb

con = duckdb.connect("baseball.duckdb")

# Who hits best with a full count, 2 outs?
df = count_state_splits(con, balls=3, strikes=2, outs=2, min_pa=30)
print(df.head(10))

# What does Gerrit Cole throw after a fastball in two-strike counts?
df = pitch_sequence_patterns(con, pitcher_id=543243, season=2024)
print(df[df["first_pitch"] == "FF"])

# Walk through a specific at-bat pitch by pitch
df = full_at_bat_timeline(con, game_pk=745456, at_bat_number=23)
print(df)
```

---

## Design Decisions

**Pitch as atomic unit** — storing every pitch individually with full game-state context means any filter combination (count + base state + outs + handedness + ...) is possible without re-ingesting or pre-aggregating.

**DuckDB for local analytics** — in-process columnar queries over millions of pitch rows with zero infrastructure. Can be swapped for Postgres by changing the connection string.

**At-bat rollup table** — `at_bats` is pre-aggregated from pitches so leaderboard and situational queries don't need to re-aggregate millions of rows on every request.

**Sport-silo architecture** — schema conventions (player, team, game abstractions) are designed to extend sport-by-sport without cross-contamination. Baseball lives in its own table set; a future NBA module would do the same.

---

## Data Sources

| Source | Access | Coverage |
| --- | --- | --- |
| Baseball Savant / Statcast | Free via `pybaseball` | Pitch-level since 2015 |
| FanGraphs | Free via `pybaseball` | Advanced metrics |
| MLB Stats API | Free (unofficial) | Real-time game data |

---

## Roadmap

- [ ] Park factor normalization layer
- [ ] Umpire zone tendency maps
- [ ] Pitch arsenal clustering (k-means on velocity + movement + spin)
- [ ] Player aging curve overlays
- [ ] NBA module (sport #2)
