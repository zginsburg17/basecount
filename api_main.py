"""
Baseball Analytics Platform - FastAPI Backend
Serves analytics data as JSON for the frontend dashboard.
"""

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import pandas as pd
from typing import Optional
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from analytics.queries import (
    count_state_splits,
    pitcher_count_profile,
    at_bat_outcome_by_count,
    pitch_sequence_patterns,
    situational_splits,
    full_at_bat_timeline,
    stuff_plus_proxy,
)

app = FastAPI(title="Baseball Analytics API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = os.getenv("DB_PATH", "baseball.duckdb")


def get_con():
    return duckdb.connect(DB_PATH, read_only=True)


def df_to_json(df: pd.DataFrame) -> list[dict]:
    return df.where(pd.notna(df), None).to_dict(orient="records")


# ---------------------------------------------------------------------------
# Count State Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/count-state/batter-splits")
def api_count_state_splits(
    balls:      int            = Query(..., ge=0, le=3),
    strikes:    int            = Query(..., ge=0, le=2),
    outs:       Optional[int]  = Query(None, ge=0, le=2),
    base_state: Optional[str]  = None,
    season:     Optional[int]  = None,
    min_pa:     int            = Query(50, ge=10),
):
    """Batter performance in a specific count situation."""
    con = get_con()
    df  = count_state_splits(con, balls, strikes, outs, base_state, season, min_pa)
    return {"count": f"{balls}-{strikes}", "results": df_to_json(df)}


@app.get("/api/count-state/outcome-matrix")
def api_outcome_matrix(season: Optional[int] = None):
    """Full count-state outcome probability matrix."""
    con = get_con()
    df  = at_bat_outcome_by_count(con, season)
    return df_to_json(df)


# ---------------------------------------------------------------------------
# Pitcher Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/pitcher/{pitcher_id}/count-profile")
def api_pitcher_count_profile(pitcher_id: int, season: Optional[int] = None):
    """Pitch mix by count for a given pitcher."""
    con = get_con()
    df  = pitcher_count_profile(con, pitcher_id, season)
    return df_to_json(df)


@app.get("/api/pitcher/{pitcher_id}/sequences")
def api_pitcher_sequences(
    pitcher_id:       int,
    min_occurrences:  int           = 20,
    season:           Optional[int] = None,
):
    """Most common pitch-to-pitch sequences for a pitcher."""
    con = get_con()
    df  = pitch_sequence_patterns(con, pitcher_id, min_occurrences, season)
    return df_to_json(df)


@app.get("/api/pitcher/{pitcher_id}/splits")
def api_pitcher_splits(
    pitcher_id: int,
    split_by:   str            = "base_state",
    season:     Optional[int]  = None,
):
    con = get_con()
    df  = situational_splits(con, pitcher_id, role="pitcher", split_by=split_by, season=season)
    return df_to_json(df)


# ---------------------------------------------------------------------------
# Batter Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/batter/{batter_id}/splits")
def api_batter_splits(
    batter_id: int,
    split_by:  str            = "base_state",
    season:    Optional[int]  = None,
):
    con = get_con()
    df  = situational_splits(con, batter_id, role="batter", split_by=split_by, season=season)
    return df_to_json(df)


# ---------------------------------------------------------------------------
# At-Bat Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/at-bat/{game_pk}/{at_bat_number}")
def api_at_bat_timeline(game_pk: int, at_bat_number: int):
    """Full pitch-by-pitch timeline of a single at-bat."""
    con = get_con()
    df  = full_at_bat_timeline(con, game_pk, at_bat_number)
    if df.empty:
        raise HTTPException(status_code=404, detail="At-bat not found")
    return df_to_json(df)


# ---------------------------------------------------------------------------
# Leaderboard Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/leaderboard/stuff")
def api_stuff_leaderboard(
    season:      int            = 2024,
    pitch_type:  Optional[str]  = None,
    min_pitches: int            = 100,
):
    """Pitch quality leaderboard proxied from whiff rate + velo + movement."""
    con = get_con()
    df  = stuff_plus_proxy(con, season, pitch_type, min_pitches)
    return df_to_json(df)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
