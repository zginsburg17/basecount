# BaseCount — Baseball Analytics Platform

A pitch-level baseball analytics platform built on Statcast data.
Designed as a portfolio project showcasing data engineering, analytics,
and interactive visualization skills.

---

## Architecture

```
baseball_analytics/
├── etl/
│   └── pipeline.py       # Statcast ingestion → DuckDB
├── analytics/
│   └── queries.py        # Count state, sequencing, situational queries
├── api/
│   └── main.py           # FastAPI REST layer
├── dashboard/
│   └── dashboard.html    # Frontend (or Streamlit app)
├── baseball.duckdb       # Local analytical database (gitignored)
└── requirements.txt
```

---

## Setup

```bash
# 1. Install dependencies
pip install pybaseball duckdb pandas fastapi uvicorn

# 2. Initialize DB and run a sample ingestion
python etl/pipeline.py

# 3. Start the API
uvicorn api.main:app --reload --port 8000

# 4. Open dashboard.html in browser (or run Streamlit)
```

---

## Key Design Decisions

### Pitch as atomic unit
Every pitch is stored individually with full game state context — count,
outs, base state, location, movement. At-bats and game-level stats are
*derived* from pitches, not the other way around. This makes any filter
combination possible without pre-aggregating.

### DuckDB for local analytics
DuckDB runs in-process, handles columnar queries on millions of pitch rows
extremely fast, and requires zero infrastructure. Can be swapped for
Postgres in production by changing the connection string.

### Sport-silo architecture
The schema is designed to be extended sport-by-sport without cross-sport
contamination. Each sport would live in its own set of tables with a
shared conventions layer (player, team, game abstractions).

---

## Query Examples

```python
from analytics.queries import count_state_splits, full_at_bat_timeline
import duckdb

con = duckdb.connect("baseball.duckdb")

# Who hits best in 2-2, 1 out situations?
df = count_state_splits(con, balls=2, strikes=2, outs=1)
print(df.head(10))

# Walk through a specific at-bat pitch by pitch
df = full_at_bat_timeline(con, game_pk=745456, at_bat_number=23)
print(df)
```

---

## Data Sources

| Source | Access | Notes |
|--------|--------|-------|
| Baseball Savant / Statcast | Free via `pybaseball` | Pitch-level since 2015 |
| FanGraphs | Free via `pybaseball` | Advanced metrics |
| MLB Stats API | Free, unofficial | Real-time game data |
| Retrosheet | Free download | Historical play-by-play |

---

## Roadmap

- [ ] Streamlit interactive dashboard
- [ ] Park factor normalization layer
- [ ] Umpire zone tendency maps
- [ ] Pitch arsenal clustering (k-means on pitch characteristics)
- [ ] Player aging curve overlays
- [ ] NBA module (sport #2)
