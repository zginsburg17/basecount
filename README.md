# BaseCount

Pitch-first baseball analytics platform built on MLB Statcast data. Local DuckDB database, FastAPI backend, browser dashboard.

```
[MLB Statcast API]  ──pybaseball──►  [ETL Pipeline]
                                           │
                                    exports/*.parquet  ◄── source of truth
                                           │
                                    baseball.duckdb    ◄── disposable query cache
                                           │
                          [FastAPI  ─  localhost:8000]
                                           │
                          [Browser dashboard (HTML/JS)]
```

---

## Data philosophy

**The Statcast API is called once per season.** After that, everything comes from Parquet.

- First full load is slow (~hours). Every load after that is fast.
- `baseball.duckdb` is disposable. Delete it, run `./run.sh restore`, done.
- Only `update` ever touches the API after initial setup.

---

## Quick start

```bash
# First time — pulls all Statcast history, saves Parquet bundles
./run.sh setup
./run.sh api
open dashboard/dashboard.html

# New machine (Parquet bundles already in repo via Git LFS)
git lfs pull
./run.sh restore
./run.sh api

# Day-to-day — pull new games for current season
./run.sh update
./run.sh api
```

The dashboard shows a banner when data is stale, with an **Update Now** button that syncs in the background.

---

## Commands

| Command | What it does |
|---------|-------------|
| `setup` | First-time load. Imports from Parquet if bundles exist, pulls Statcast API otherwise. |
| `restore` | Rebuild database from Parquet bundles. No API calls. |
| `update` | Pull new games for the current season. Refreshes derived tables. |
| `api` | Start the API server on `localhost:8000`. |
| `status` | Show loaded seasons, row counts, player coverage. |
| `export [year]` | Export Parquet bundles. Omit year for all seasons. |
| `rebuild <year>` | Force re-pull one season from the API. |
| `report <year>` | Season validation: game counts, derived-table coverage. |
| `enrich` | Resolve missing player names and metadata. |

All commands run as `./run.sh <command>`.

---

## Git LFS

Season bundles are ~40-70 MB each. `.gitattributes` routes `exports/**/*.parquet` through Git LFS.

```bash
git lfs install          # one-time
./run.sh export          # after a full load
git add exports/ && git commit -m "Season bundles"
git push
```

---

## API docs

Start the server and visit **http://localhost:8000/docs** for the full interactive API reference.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `permission denied: ./run.sh` | `chmod +x run.sh` |
| Player names show as `Batter #657656` | `./run.sh enrich` |
| Dashboard is blank | Confirm API is running (`./run.sh api`), check `localhost:8000/docs` |
| Only one season appears | Run `./run.sh setup` to fill all seasons |
| First load is slow | Expected — Statcast history is large. Every load after is fast. |
| Lost the database | `./run.sh restore` rebuilds from Parquet in minutes |
