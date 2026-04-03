# BaseCount

## Overview

BaseCount is a local baseball analytics application built on MLB Statcast pitch-level data. It downloads Statcast data through `pybaseball`, stores the results in a local DuckDB database, serves analytics through a FastAPI application, and renders the results in a browser-based dashboard.

The system is designed to support:

- single-season analysis
- multi-season range analysis
- pitch-by-pitch count-state analysis
- handedness and outs filtering
- regular season vs postseason filtering
- batter, pitcher, leaderboard, and sequence views

Statcast pitch-level support begins in `2015`, so the historical coverage standard for this project starts with the `2015` season and extends through the current season. As of April 2, 2026, that expected range is `2015-2026`.

## Purpose

This document is intended to be a complete operating manual for the project. It is written to be precise, formal, and step-by-step so that a user with limited technical background can still install, run, verify, and troubleshoot the application successfully.

## Documentation Maintenance Requirement

The README is part of the product documentation and must be maintained whenever the codebase changes in a way that affects setup, operation, behavior, commands, architecture, file layout, user workflow, or troubleshooting.

The maintenance standard for this repository is:

1. any code change that makes the README inaccurate requires a README update in the same body of work
2. new commands, renamed commands, removed commands, or changed defaults must be reflected in the README
3. UI workflow changes that affect what the user sees or clicks must be reflected in the README
4. changes to the repository structure that affect the documented file tree must be reflected in the README
5. changes to data-loading behavior, season support, API behavior, or player-enrichment behavior must be reflected in the README

In practical terms, the README should be treated as a required deliverable, not optional cleanup.

## System Architecture

BaseCount consists of four major layers:

1. Data ingestion
   The ETL pipeline downloads Statcast data and writes it into a local DuckDB database.

2. Local database
   DuckDB stores pitch-level, at-bat, player, and game data on disk in a single local file.

3. Local API
   FastAPI reads the DuckDB database and exposes analytics endpoints on `http://localhost:8000`.

4. Browser dashboard
   The dashboard reads from the local API and displays the analytics interface.

The dashboard depends on the API. The API depends on the DuckDB database. If the database is empty, the API cannot serve meaningful data. If the API is not running, the dashboard cannot display meaningful data.

## Current Repository Directory

The repository currently contains the following files and directories:

```text
basecount/
├── .claude/
│   └── worktrees/
│       ├── compassionate-galileo/
│       └── goofy-knuth/
├── .git/
├── .gitignore
├── README.md
├── analytics/
│   └── queries.py
├── api/
│   └── main.py
├── dashboard/
│   ├── dashboard.css
│   ├── dashboard.html
│   └── dashboard.js
├── etl/
│   └── pipeline.py
├── requirements.txt
└── run.sh
```

Files that are created later during normal use:

```text
basecount/
├── baseball.duckdb
└── venv/
```

Notes:

- `baseball.duckdb` is created after data is loaded.
- `venv/` is created automatically by [run.sh](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/run.sh) if it does not already exist.
- `.claude/` and `.git/` are repository support directories and are not part of the application runtime.

## Key Files

- [run.sh](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/run.sh)
  Primary entry point for installation, ETL commands, enrichment, status checks, and starting the API.

- [etl/pipeline.py](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/etl/pipeline.py)
  ETL pipeline that pulls Statcast data, writes DuckDB tables, and enriches player information.

- [api/main.py](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/api/main.py)
  FastAPI application that exposes the analytics endpoints.

- [analytics/queries.py](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/analytics/queries.py)
  Analytical SQL/query logic used by the API.

- [dashboard/dashboard.html](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/dashboard/dashboard.html)
  Main dashboard HTML file opened in the browser.

- [dashboard/dashboard.js](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/dashboard/dashboard.js)
  Frontend logic for dashboard interactivity and API requests.

- [dashboard/dashboard.css](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/dashboard/dashboard.css)
  Dashboard styling.

- [requirements.txt](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/requirements.txt)
  Python package dependencies.

## Prerequisites

Before using BaseCount, confirm the following:

1. Python 3 is installed.
2. Terminal access is available.
3. Internet access is available.
4. The repository has been downloaded locally.

Internet access is required because:

- `run.sh` installs Python dependencies
- the ETL downloads Statcast data through `pybaseball`
- player-name enrichment uses online lookups from both `pybaseball` and the MLB Stats API fallback

## Supported Data Modes

BaseCount currently supports the following data-loading modes:

- `recent`
  Loads only a recent window of data. This is useful for refreshing current-season data, but it is not a historical backfill.

- `current-season-update`
  Loads a short rolling window for the active season and is intended for scheduled daily refreshes.

- `season`
  Loads a single season.

- `range`
  Loads an inclusive range of seasons.

- `all-history`
  Loads all supported Statcast seasons beginning in `2015`.

- `ensure-history`
  Verifies that every supported Statcast season from `2015` through the current season is present, backfills any missing seasons, reruns player enrichment, and fails if player-name coverage is still incomplete.

- `export-season`
  Exports one season from DuckDB into a reusable Parquet bundle on disk.

- `import-season`
  Imports a previously exported Parquet season bundle into DuckDB without calling the Statcast API again.

- `enrich`
  Fills in missing player metadata such as names, handedness, position, and team when possible.

- `status`
  Reports what data is currently loaded in the DuckDB database.

Important:

- a season is only considered complete after a full season backfill finishes successfully
- partial data for a season does not count as complete history coverage
- spring training is excluded from ingestion
- regular season and postseason are retained
- if a Statcast multi-day response is malformed, the ETL automatically retries and splits the date range into smaller windows

## Standard Workflow

For most users, the standard operating procedure is:

1. Load historical data into DuckDB.
2. Confirm the database contains the expected seasons.
3. Enrich player names and metadata.
4. Start the API.
5. Open the dashboard.

## Important Default Behavior

If you run:

```bash
./run.sh
```

the script defaults to `all`, which means:

1. it verifies that the full required historical range is loaded
2. it backfills any missing seasons
3. it starts the API

This is stricter than the older recent-only behavior.

If you need historical seasons, you must explicitly use one of the following:

```bash
./run.sh all-history
./run.sh ensure-history
./run.sh season 2025
./run.sh range 2015 2025
```

## Installation and First-Time Setup

This section provides the recommended first-time setup procedure.

### Step 1: Open a terminal in the project directory

Change into the repository directory:

```bash
cd /path/to/basecount
```

Replace `/path/to/basecount` with the actual path to the repository on your machine.

### Step 2: Ensure the full historical dataset is present

Run:

```bash
./run.sh ensure-history
```

What this command does:

1. creates a Python virtual environment if one does not already exist
2. activates the virtual environment
3. installs the packages from `requirements.txt`
4. checks whether every season from `2015` through the current year is already loaded
5. downloads only the missing seasons when history is incomplete
6. writes the data into `baseball.duckdb`
7. runs player enrichment automatically
8. verifies that historical season coverage is complete
9. stops with an error if player-name coverage is still incomplete

This is the preferred command for regular use because it verifies completeness rather than assuming completeness.

If you want to force a full historical backfill from scratch instead of verifying and filling gaps, use:

```bash
./run.sh all-history
```

What `./run.sh all-history` does:

1. downloads the entire supported Statcast range season by season
2. writes the data into `baseball.duckdb`
3. runs player enrichment automatically unless skipped internally

What to expect:

- the process may take a long time
- the terminal will display many data-loading messages
- this is normal for historical pitch-level data

### Step 3: Verify what seasons were loaded

Run:

```bash
./run.sh status
```

This command reports:

- the list of loaded seasons
- the list of completed seasons
- the earliest game date in the database
- the latest game date in the database
- a total row count summary
- player-name coverage information

This is the most reliable way to confirm what data the application can query.

### Step 4: Run enrichment again if player names are incomplete

Run:

```bash
./run.sh enrich
```

This command is safe to run multiple times.

Use it when:

- player names are missing
- labels such as `Batter #657656` appear
- team information looks incomplete

### Step 5: Start the API server

Open a second terminal window or tab in the same project directory and run:

```bash
./run.sh api
```

Keep this terminal open while using the dashboard.

If the API starts correctly, it should run locally at:

```text
http://localhost:8000
```

### Step 6: Open the dashboard in a browser

Open the dashboard file:

[dashboard/dashboard.html](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/dashboard/dashboard.html)

On macOS, you can also run:

```bash
open dashboard/dashboard.html
```

### Step 7: Confirm the dashboard is connected

After the dashboard opens, confirm the following:

1. the page loads visually
2. the top season controls are visible
3. the dashboard pages are visible in the sidebar
4. tables and charts populate from the API

If the page opens but remains empty, the most common causes are:

- the API is not running
- the database contains no data
- the wrong HTML file was opened

## Daily Use Workflow

After the initial historical backfill is complete, the typical day-to-day workflow is:

### 1. Refresh recent data

```bash
./run.sh recent
```

### 2. Refresh player enrichment

```bash
./run.sh enrich
```

### 3. Start the API

```bash
./run.sh api
```

### 4. Open the dashboard

```bash
open dashboard/dashboard.html
```

## Command Reference

This section documents the supported [run.sh](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/run.sh) commands.

### `./run.sh api`

Starts the API only.

Use this when:

- the database already contains data
- you want to browse the dashboard

### `./run.sh status`

Displays what data is currently loaded.

Use this when:

- you are not sure which seasons are in the database
- you need to verify a backfill completed correctly
- season filtering appears incorrect

The status output now also reports:

- expected seasons
- completed seasons
- missing seasons
- incomplete seasons
- whether full historical coverage is complete
- player-name coverage counts

### `./run.sh enrich`

Enriches player metadata.

Use this when:

- names are missing
- team information is missing
- fallback player ID labels are appearing

### `./run.sh recent`

Loads recent data only.

Important:

- this is not a historical backfill
- this usually loads only current-season data

Optional example:

```bash
./run.sh recent 7
```

### `./run.sh current-season-update [days]`

Updates the current season using a short rolling date window.

Example:

```bash
./run.sh current-season-update
./run.sh current-season-update 2
```

Recommended use:

- run this once each morning
- use `2` days instead of `1` day as a small safety buffer
- follow it with the API or dashboard as needed

### `./run.sh season <year>`

Loads a single season.

Example:

```bash
./run.sh season 2025
```

### `./run.sh range <start_year> <end_year>`

Loads an inclusive range of seasons.

Example:

```bash
./run.sh range 2018 2025
```

### `./run.sh all-history`

Loads all supported Statcast history starting from `2015`.

Example:

```bash
./run.sh all-history
```

### `./run.sh ensure-history`

Verifies that all required seasons from `2015` through the current season are present.

If any seasons are missing, it backfills only those missing seasons and then reruns player enrichment.

This mode is intentionally strict:

- it fails if historical season coverage is still incomplete
- it fails if player-name coverage is still incomplete after enrichment

Example:

```bash
./run.sh ensure-history
```

### `./run.sh export-season <year> [export_root]`

Exports a season from your local DuckDB database into a reusable Parquet bundle.

Example:

```bash
./run.sh export-season 2015
./run.sh export-season 2016 season_exports
```

This creates a folder like:

```text
exports/season=2015/
├── pitches.parquet
├── at_bats.parquet
├── games.parquet
├── players.parquet
└── metadata.json
```

This is the recommended way to avoid repeatedly pulling the same season from the Statcast API.

### `./run.sh import-season <bundle_dir>`

Imports a previously exported season bundle into DuckDB.

Example:

```bash
./run.sh import-season exports/season=2015
```

This allows you to rebuild or move a database using local Parquet files instead of live API pulls.

### `./run.sh all`

Ensures the full required historical range is loaded and then starts the API.

This is the same mode used by default when you run `./run.sh` with no arguments.

## ETL Reference

If you prefer to run the ETL directly instead of the shell wrapper, the raw commands are:

```bash
python etl/pipeline.py recent --days 7
python etl/pipeline.py season --season 2025
python etl/pipeline.py range --season-start 2018 --season-end 2025
python etl/pipeline.py all-history
python etl/pipeline.py ensure-history
python etl/pipeline.py export-season --season 2015 --export-root exports
python etl/pipeline.py import-season --import-dir exports/season=2015
python etl/pipeline.py enrich
python etl/pipeline.py status
```

Optional custom database path:

```bash
python etl/pipeline.py status --db-path custom.duckdb
```

## API Reference

The local API runs at:

```text
http://localhost:8000
```

Interactive API documentation is available at:

```text
http://localhost:8000/docs
```

Representative endpoints include:

- `/api/meta/context`
- `/api/meta/coverage`
- `/api/count-state/outcome-matrix`
- `/api/count-state/batter-splits`
- `/api/batter/{batter_id}/overview`
- `/api/pitcher/{pitcher_id}/overview`
- `/api/leaderboard/batting`
- `/api/leaderboard/stuff`
- `/api/sequences`

Many endpoints support either:

- `season=2025`

or:

- `season_start=2018&season_end=2025`

## Dashboard Usage

The dashboard supports both single-season and multi-season analysis.

The backend also exposes explicit completeness information so the application can report whether:

- all expected seasons are loaded
- any seasons are missing
- any player names remain unresolved

### Season Controls

At the top of the dashboard, select either:

- `Single Season`
- `Season Range`

You can also select the season type:

- `Regular Season`
- `Postseason`
- `Both`

Use `Single Season` when you want one year only.

Use `Season Range` when you want multiple years together.

Important:

- the dashboard can only query seasons that are actually loaded in the database
- if a season is not loaded, filtering to that season will not produce meaningful results
- spring training data is intentionally excluded from the dataset
- postseason data is retained and can be filtered separately from regular season

Always verify available data with:

```bash
./run.sh status
```

### Player Labels

Player labels are intended to display as:

```text
Player Name · TEAM
```

If the application cannot resolve the player name, it may temporarily fall back to a label such as:

```text
Batter #657656
```

If this occurs, run:

```bash
./run.sh enrich
```

## Database Overview

The major DuckDB tables are:

### `pitches`

The atomic pitch-level table. This table stores:

- pitch identifiers
- pitcher and batter IDs
- count state
- outs
- runners on base
- pitch type
- velocity
- spin
- movement
- location
- result information

### `at_bats`

A derived table created from pitch-level data for more efficient query patterns.

### `players`

Stores player metadata such as:

- full name
- handedness
- position
- team

### `games`

Stores game-level information.

### `ingestion_log`

Stores ETL run history.

## Verification Checklist

After setup, the following checks should all succeed:

1. `./run.sh status` lists the expected seasons
2. `./run.sh api` starts without error
3. `http://localhost:8000/docs` opens successfully
4. [dashboard/dashboard.html](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/dashboard/dashboard.html) opens successfully
5. the dashboard displays data-driven charts and tables
6. player names appear correctly after enrichment

## Troubleshooting

### Problem: `./run.sh` returns `permission denied`

Example:

```text
zsh: permission denied: ./run.sh
```

Resolution:

```bash
chmod +x run.sh
```

Then run the command again.

### Problem: only one season is available

Most common cause:

- `./run.sh` or `./run.sh all` was used, which loads recent data only

The project now uses a stricter standard: `./run.sh all` should ensure complete historical coverage before starting the API. If the database was created under older behavior, run:

```bash
./run.sh ensure-history
```

Resolution:

```bash
./run.sh ensure-history
```

or:

```bash
./run.sh range 2015 2026
```

Then verify:

```bash
./run.sh status
```

### Problem: player names are missing

Resolution:

```bash
./run.sh enrich
```

If names remain incomplete:

1. verify internet access
2. run the enrichment again
3. note that the system now tries both `pybaseball` and the MLB Stats API before leaving a player unresolved

### Problem: the dashboard opens but is blank

Check the following in order:

1. confirm the API terminal is still running
2. confirm `http://localhost:8000/docs` opens in a browser
3. confirm the database contains data with `./run.sh status`
4. confirm the correct dashboard file was opened

### Problem: the dashboard is using the wrong season

Check the following:

1. confirm the season is actually loaded with `./run.sh status`
2. confirm the correct top-level season mode is selected
3. confirm the selected year falls within the loaded range
4. refresh the dashboard page

### Problem: historical loading takes a long time

This is expected.

Reasons:

- historical pitch-level data is large
- the ETL loads data in chunks
- the pipeline is intentionally conservative with external requests

### Problem: the API does not start

Check the following:

1. dependencies installed successfully
2. Python is available
3. port `8000` is not already in use
4. the repository files are present

You can also start the API directly:

```bash
python api/main.py
```

## Recommended Operating Pattern

For a new full installation:

```bash
./run.sh ensure-history
./run.sh status
./run.sh api
open dashboard/dashboard.html
```

For normal ongoing use:

```bash
./run.sh current-season-update 2
./run.sh api
open dashboard/dashboard.html
```

## Summary

The most important commands for most users are:

```bash
./run.sh ensure-history
./run.sh status
./run.sh enrich
./run.sh api
```

Then open:

```bash
open dashboard/dashboard.html
```

That is the standard, complete BaseCount operating sequence.
