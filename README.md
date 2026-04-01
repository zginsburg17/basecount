# BaseCount

BaseCount is a local baseball analytics app built on MLB Statcast data.

## TL;DR

If you want the fastest possible setup, run these commands:

```bash
./run.sh all-history
./run.sh enrich
./run.sh api
open dashboard/dashboard.html
```

What these do:

- load all supported Statcast history starting in `2015`
- fill in player names and metadata
- start the local API
- open the dashboard in your browser

If something looks wrong, run:

```bash
./run.sh status
```

That tells you what seasons are actually loaded.

It lets you:

- load pitch-by-pitch MLB data into a local database
- explore exact pitch counts like `0-2`, `2-1`, `3-2`
- filter by outs, handedness, pitch type, batter, pitcher, and season
- compare one season or a range of seasons
- view batter, pitcher, leaderboard, and pitch-sequence dashboards in the browser

This README is written for non-technical users on purpose. If you can copy/paste commands into Terminal, you can run this project.

## What This Project Actually Is

BaseCount has 4 parts:

1. `etl/pipeline.py`
Pulls Statcast data from `pybaseball` and stores it in a local DuckDB database.

2. `baseball.duckdb`
The local database file. This is where the loaded MLB data lives.

3. `api/main.py`
A local API server that reads the database and returns dashboard data.

4. `dashboard/dashboard.html`
The browser dashboard UI.

Important: the dashboard does not work by itself. It needs the local API running, and the API needs the local database to have data in it.

## What Data Is Supported

- Statcast pitch-level data is supported back to `2015`
- single-season analysis is supported
- multi-season range analysis is supported
- “all loaded seasons” analysis is supported

If you load `2015` through `2026`, the dashboard can query across that full range.

## Folder Map

```text
basecount/
├── api/
│   └── main.py
├── analytics/
│   └── queries.py
├── dashboard/
│   ├── dashboard.html
│   └── dashboard.js
├── etl/
│   └── pipeline.py
├── run.sh
├── requirements.txt
└── baseball.duckdb         # created after you load data
```

## Before You Start

You need:

- macOS, Linux, or a Unix-like terminal
- Python 3 installed
- internet access

Why internet access matters:

- BaseCount downloads Statcast data from `pybaseball`
- player-name enrichment also depends on online lookups
- `run.sh` installs Python packages the first time it runs

## The Easiest Way To Think About The Workflow

There are only 3 big steps:

1. Load baseball data
2. Start the API
3. Open the dashboard

That’s it.

## Quick Start For First-Time Users

If you want the shortest version:

### 1. Open Terminal in the project folder

If you are not already in the repo:

```bash
cd /path/to/basecount
```

### 2. Load all available Statcast history

```bash
./run.sh all-history
```

What this does:

- creates a virtual environment if needed
- installs dependencies
- downloads Statcast data starting from `2015`
- stores it in `baseball.duckdb`
- enriches player metadata when possible

Important:

- this can take a long time
- it may run for a while because it loads season by season
- this is normal

### 3. Start the API server

Open a new Terminal tab or window in the same project folder and run:

```bash
./run.sh api
```

Leave this terminal window open while you use the dashboard.

If it is working, you should see output indicating the API is running at:

```text
http://localhost:8000
```

### 4. Open the dashboard

Open this file in your browser:

[dashboard/dashboard.html](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/dashboard/dashboard.html)

On a Mac, you can also run:

```bash
open dashboard/dashboard.html
```

### 5. Use the season controls at the top of the dashboard

You can choose:

- `Single Season`
- `Season Range`

If multiple seasons are loaded, the dashboard should let you work across the full loaded range.

## What You Should See

This section is here so you know whether the app is behaving normally.

### After `./run.sh all-history`

You should expect:

- lots of log output in Terminal
- messages showing date ranges being fetched
- rows being inserted into the database
- the process to take a while

This is normal. Historical pitch-level data is large.

### After `./run.sh status`

You should see:

- a list of loaded seasons
- the earliest and latest game dates in the database
- a row count summary

If you only see `2026`, then only `2026` is loaded.

### After `./run.sh api`

You should see:

- output indicating the API started successfully
- a local address such as `http://localhost:8000`

If you open:

```text
http://localhost:8000/docs
```

you should see the FastAPI docs page.

### After opening the dashboard

You should see:

- a dark-themed dashboard
- a top season control area
- navigation for `Count State`, `Batter Profile`, `Pitcher Profile`, `Leaderboard`, and `Pitch Sequence`
- data-driven tables and charts once the API responds

If the dashboard opens but looks empty, the API is usually not running or the database is empty.

## The Main Commands You Need

BaseCount is meant to be driven through `run.sh`.

### Most important commands

```bash
./run.sh api
./run.sh status
./run.sh enrich
./run.sh all-history
./run.sh season 2025
./run.sh range 2018 2025
./run.sh recent
```

### What each command does

`./run.sh api`

- starts the API only
- use this after your database already has data

`./run.sh status`

- shows which seasons are loaded
- shows date coverage and row count
- use this if you are unsure what is in the database

`./run.sh enrich`

- tries to fill in missing player names and metadata
- use this if you still see labels like `Batter #657656`

`./run.sh all-history`

- backfills all supported Statcast seasons starting in `2015`
- this is the best first-run option if you want the full historical tool

`./run.sh season 2025`

- loads one season only

`./run.sh range 2018 2025`

- loads every season from `2018` through `2025`

`./run.sh recent`

- loads recent data only
- this is not a historical backfill
- this usually only brings in the current season

## Very Important: What `./run.sh` Does By Default

If you run:

```bash
./run.sh
```

the current default mode is `all`, which means:

1. load recent data only
2. start the API

That is not the same as loading all seasons.

So if you want historical seasons, do not rely on plain `./run.sh` alone. Use one of these:

```bash
./run.sh all-history
./run.sh season 2025
./run.sh range 2015 2025
```

## Step-By-Step Setup Guide

## Option A: Full Historical Setup

This is the recommended path if you want the complete tool.

### Step 1. Go to the project folder

```bash
cd /path/to/basecount
```

### Step 2. Load all history

```bash
./run.sh all-history
```

### Step 3. Confirm the seasons loaded

```bash
./run.sh status
```

You should see a list of loaded seasons and the date span in the database.

### Step 4. Run name enrichment again if needed

```bash
./run.sh enrich
```

This is safe to run more than once.

### Step 5. Start the API

```bash
./run.sh api
```

### Step 6. Open the dashboard

```bash
open dashboard/dashboard.html
```

## Option B: Load Only One Season

If you only want one season:

```bash
./run.sh season 2025
./run.sh api
open dashboard/dashboard.html
```

## Option C: Load A Custom Range

If you want a range like `2021` through `2025`:

```bash
./run.sh range 2021 2025
./run.sh status
./run.sh api
open dashboard/dashboard.html
```

## How The Season Filtering Works

There are 2 different ideas that matter:

1. What seasons are loaded into the database
2. What season filter you selected in the dashboard

The dashboard cannot show a season that has not been loaded.

Examples:

- if only `2026` is loaded, selecting `2024` will not return meaningful results
- if `2015-2026` are loaded, you can use `Single Season` or `Season Range`

If season filtering looks wrong, the first thing to check is:

```bash
./run.sh status
```

## How Player Names Work

Player names come from 2 places:

1. the local `players` table
2. fallback name lookups when the API can resolve MLBAM IDs

If you still see labels like:

- `Batter #657656`
- `Pitcher #123456`

then run:

```bash
./run.sh enrich
```

Why this happens:

- the ETL always stores player IDs
- names are added in a later enrichment step
- if enrichment fails or is incomplete, the app falls back to ID-based labels

Teams:

- team codes are usually inferred from pitch data even if the full player profile is incomplete
- the dashboard now prefers labels like `Player Name · TEAM`

## What Each Dashboard Page Does

## Count State

Shows how hitting changes by exact count.

Examples:

- who hits best in `2-1`
- how strikeout rate changes in `0-2`
- how zone usage changes by count

Supports filters like:

- balls
- strikes
- outs
- batter handedness
- pitcher handedness
- single season
- season range

## Batter Profile

Shows a selected batter’s overview and count-based performance.

## Pitcher Profile

Shows a selected pitcher’s overview, count profile, and splits.

## Leaderboard

Shows ranked batting or pitching leaderboards based on the current filters.

## Pitch Sequence

Shows common pitch-to-pitch patterns for a selected pitcher.

## Troubleshooting

## Problem: `./run.sh` says permission denied

Example:

```text
zsh: permission denied: ./run.sh
```

Fix:

```bash
chmod +x run.sh
```

Then try again.

## Problem: only one season is showing up

Most likely cause:

- you ran `./run.sh`
- that only loaded recent data

Fix:

```bash
./run.sh all-history
```

or:

```bash
./run.sh range 2015 2026
```

Then check:

```bash
./run.sh status
```

## Problem: player names are missing

Fix:

```bash
./run.sh enrich
```

If names are still incomplete:

- check your internet connection
- re-run `./run.sh enrich`
- some players may still temporarily fall back to IDs if lookups fail

## Problem: the dashboard opens, but looks empty

Check these in order:

1. Is the API running?
2. Does `http://localhost:8000/docs` open in your browser?
3. Does the database actually contain data?
4. Did you open the correct dashboard file?

Useful commands:

```bash
./run.sh status
./run.sh api
```

## Problem: the dashboard is using the wrong season

Check these:

1. Run `./run.sh status` and make sure the season you want is actually loaded
2. Refresh the dashboard page in the browser
3. Confirm the top filter says `Single Season` or `Season Range` as expected
4. Make sure the selected year falls inside the loaded range

## Problem: `all-history` takes forever

This is expected.

Reasons:

- it is downloading many seasons of pitch-by-pitch data
- Statcast data is large
- the ETL runs in chunks to avoid hammering the source

Best practice:

- let it finish
- use `./run.sh status` after completion
- do not assume failure just because it is slow

## Problem: the API will not start

Make sure:

- dependencies installed successfully
- no other app is already using port `8000`
- the project folder contains the expected files

You can also try starting it directly:

```bash
python api/main.py
```

## FAQ

## Do I need to run `all-history` every time?

No.

Usually:

- run `./run.sh all-history` once for the big initial load
- run `./run.sh recent` later when you want newer data
- run `./run.sh enrich` if player names still look incomplete

## Why does `./run.sh` by itself not load all years?

Because plain:

```bash
./run.sh
```

currently defaults to `all`, and `all` means:

1. load recent data only
2. start the API

It does not mean “load every historical season.”

## Why am I only seeing the current year?

Usually because only the current season has been loaded into the database.

Check:

```bash
./run.sh status
```

If you want all supported seasons, run:

```bash
./run.sh all-history
```

## Why do I see `Batter #123456` instead of a real name?

Because the player ID exists, but the name enrichment is incomplete or missing.

Run:

```bash
./run.sh enrich
```

## Can this project analyze multiple seasons at once?

Yes.

That is supported as long as those seasons are loaded into the database.

Use:

- `Single Season` for one year
- `Season Range` for multiple years

## What is the oldest supported season?

Statcast pitch-level support begins in `2015`.

## Do I need to keep the API terminal open?

Yes.

If the API is not running, the dashboard has nothing to talk to.

## Can I open the HTML file without the API?

You can open it, but it will not function correctly without the API.

The dashboard page is just the frontend. The real data comes from the local API.

## How do I know whether the database actually has data?

Run:

```bash
./run.sh status
```

That is the quickest truth check.

## How do I refresh player names after loading new data?

Run:

```bash
./run.sh enrich
```

It is safe to run more than once.

## Best Practices

- use `./run.sh all-history` for your first serious setup
- run `./run.sh status` any time you are confused about what data is loaded
- run `./run.sh enrich` if names or teams look incomplete
- keep the API terminal open while using the dashboard
- use `Single Season` for year-specific analysis
- use `Season Range` for multi-year analysis

## Advanced: Direct ETL Commands

If you prefer to run the ETL directly instead of `run.sh`, these are the raw Python commands:

```bash
python etl/pipeline.py recent --days 7
python etl/pipeline.py season --season 2025
python etl/pipeline.py range --season-start 2018 --season-end 2025
python etl/pipeline.py all-history
python etl/pipeline.py enrich
python etl/pipeline.py status
```

There is also an optional custom DB path:

```bash
python etl/pipeline.py status --db-path custom.duckdb
```

The API also respects:

```bash
DB_PATH=custom.duckdb python api/main.py
```

## API Overview

The API runs locally at:

```text
http://localhost:8000
```

Interactive docs:

```text
http://localhost:8000/docs
```

Examples of supported routes:

- `/api/meta/context`
- `/api/count-state/outcome-matrix`
- `/api/count-state/batter-splits`
- `/api/batter/{batter_id}/overview`
- `/api/pitcher/{pitcher_id}/overview`
- `/api/leaderboard/batting`
- `/api/leaderboard/stuff`
- `/api/sequences`

Many of these support:

- `season=2025`
- `season_start=2018&season_end=2025`

## Database Overview

The main tables are:

`pitches`

- the atomic pitch-level table
- includes count, outs, runners, pitch type, velo, spin, location, and result

`at_bats`

- built from the pitch table
- used for faster leaderboard and split queries

`players`

- player metadata like name, handedness, position, and team

`games`

- game-level information

`ingestion_log`

- tracks ETL runs

## If You Want The Simplest Reliable Routine

Use this exact routine:

### First time only

```bash
./run.sh all-history
./run.sh enrich
```

### Every time you want to use the app

In one terminal:

```bash
./run.sh api
```

Then in your browser:

```bash
open dashboard/dashboard.html
```

### When you want fresh data

```bash
./run.sh recent
./run.sh enrich
```

## Current Data / Behavior Notes

- historical Statcast support starts in `2015`
- `recent` mode usually brings in only the current season
- `all-history` is the correct command for full historical backfill
- the dashboard depends on the API, and the API depends on the local DuckDB file
- name enrichment may need to be rerun if player labels still appear as IDs

## For Developers

Core files:

- [run.sh](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/run.sh)
- [etl/pipeline.py](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/etl/pipeline.py)
- [api/main.py](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/api/main.py)
- [analytics/queries.py](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/analytics/queries.py)
- [dashboard/dashboard.html](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/dashboard/dashboard.html)
- [dashboard/dashboard.js](/Users/zacharyginsburg/.codex/worktrees/1abd/basecount/dashboard/dashboard.js)

## Summary

If you remember only 4 commands, remember these:

```bash
./run.sh all-history
./run.sh status
./run.sh enrich
./run.sh api
```

Then open:

```bash
dashboard/dashboard.html
```

That is the simplest complete BaseCount workflow.
