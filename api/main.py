"""
Baseball Analytics Platform - FastAPI Backend
Serves analytics data as JSON for the frontend dashboard.
"""

from datetime import date, datetime, timedelta
import json
import os
import subprocess
import sys
import threading
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import duckdb
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from analytics.queries import (  # noqa: E402
    at_bat_outcome_by_count,
    batting_leaderboard,
    count_state_splits,
    full_at_bat_timeline,
    pitch_sequence_patterns,
    pitcher_count_profile,
    situational_splits,
    stuff_plus_proxy,
)

app = FastAPI(title="Baseball Analytics API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
DEFAULT_DB_PATH = os.path.join(REPO_ROOT, "baseball.duckdb")
DB_PATH = os.getenv("DB_PATH", DEFAULT_DB_PATH)

HIT_EVENTS = ("single", "double", "triple", "home_run")
WALK_EVENTS = ("walk", "intent_walk")
STRIKEOUT_EVENTS = ("strikeout", "strikeout_double_play")
AB_EXCLUDED_EVENTS = ("walk", "intent_walk", "hit_by_pitch", "sac_fly", "sac_bunt", "catcher_interf")
PLAYER_NAME_CACHE: dict[int, Optional[str]] = {}
REGULAR_SEASON_GAME_TYPES = ("R",)
POSTSEASON_GAME_TYPES = ("F", "D", "L", "W")
BALL_DESCRIPTIONS = ("ball", "blocked_ball", "hit_by_pitch", "pitchout")
STRIKE_DESCRIPTIONS = ("called_strike", "swinging_strike", "swinging_strike_blocked", "foul_tip", "missed_bunt")
FOUL_DESCRIPTIONS = ("foul", "foul_bunt", "foul_pitchout")
REFERENCE_SEASON = 2025

# ---------------------------------------------------------------------------
# Sync state — tracks background auto-update runs
# ---------------------------------------------------------------------------
_SYNC_STATE: dict = {
    "status": "idle",        # idle | running | done | error
    "started_at": None,
    "completed_at": None,
    "error": None,
    "last_result": None,
}
_SYNC_LOCK = threading.Lock()


def get_con():
    return duckdb.connect(DB_PATH, read_only=True)


@app.on_event("startup")
def on_startup():
    status = db_status()
    if status["ok"]:
        print(f"[api] Connected to DuckDB at {status['db_path']} with {status['pitch_rows']} pitch rows")
    else:
        print(f"[api] Startup warning: {status}")


def db_status() -> dict:
    if not os.path.exists(DB_PATH):
        return {
            "ok": False,
            "db_path": DB_PATH,
            "error": "Database file not found.",
        }

    try:
        con = get_con()
        pitch_rows = con.execute("SELECT COUNT(*) FROM pitches").fetchone()[0]
        seasons = [
            row[0]
            for row in con.execute(
                "SELECT DISTINCT season FROM pitches WHERE season IS NOT NULL ORDER BY season DESC"
            ).fetchall()
        ]
        con.close()
        return {
            "ok": True,
            "db_path": DB_PATH,
            "pitch_rows": pitch_rows,
            "seasons": seasons,
        }
    except Exception as exc:
        return {
            "ok": False,
            "db_path": DB_PATH,
            "error": str(exc),
        }


def df_to_json(df: pd.DataFrame) -> list[dict]:
    normalized = df.replace([float("inf"), float("-inf")], pd.NA)
    normalized = normalized.astype(object).where(pd.notna(normalized), None)
    return normalized.to_dict(orient="records")


def sql_list(values: list[str]) -> str:
    return ", ".join(sql_quote(value) for value in values)


@app.get("/")
def api_root():
    status = db_status()
    return {
        "name": "Baseball Analytics API",
        "version": app.version,
        "status": "ok" if status["ok"] else "degraded",
        "db": status,
        "docs_url": "/docs",
        "meta_context_url": "/api/meta/context",
        "health_url": "/api/health",
    }


@app.get("/api/health")
def api_health():
    status = db_status()
    if not status["ok"]:
        raise HTTPException(status_code=503, detail=status)
    return status


@app.get("/api/reference/report")
def api_reference_report(season: int = Query(REFERENCE_SEASON, ge=2015)):
    con = get_con()
    report = season_report_data(con, season)
    con.close()
    return report


def latest_data_context(con: duckdb.DuckDBPyConnection) -> dict:
    earliest_game_date, latest_game_date, earliest_season, latest_season = con.execute(
        """
        SELECT
            MIN(game_date) AS earliest_game_date,
            MAX(game_date) AS latest_game_date,
            MIN(season) AS earliest_season,
            MAX(season) AS latest_season
        FROM pitches
        """
    ).fetchone()
    seasons = [
        row[0]
        for row in con.execute(
            "SELECT DISTINCT season FROM pitches WHERE season IS NOT NULL ORDER BY season DESC"
        ).fetchall()
    ]
    return {
        "earliest_game_date": earliest_game_date,
        "latest_game_date": latest_game_date,
        "earliest_season": earliest_season,
        "latest_season": latest_season,
        "seasons": seasons,
    }


def expected_statcast_seasons() -> list[int]:
    current_year = date.today().year
    return list(range(2015, current_year + 1))


def history_completeness(con: duckdb.DuckDBPyConnection) -> dict:
    context = latest_data_context(con)
    loaded = context["seasons"]
    expected = expected_statcast_seasons()
    completed = [
        row[0]
        for row in con.execute(
            """
            SELECT season
            FROM season_backfill_status
            WHERE completed = TRUE
            ORDER BY season DESC
            """
        ).fetchall()
    ]
    missing = [season for season in expected if season not in set(completed)]
    incomplete = [season for season in loaded if season not in set(completed)]
    return {
        "expected_seasons": expected,
        "loaded_seasons": loaded,
        "completed_seasons": completed,
        "missing_seasons": missing,
        "incomplete_seasons": incomplete,
        "history_complete": len(missing) == 0,
    }


def player_name_completeness(con: duckdb.DuckDBPyConnection) -> dict:
    total_players, named_players, unnamed_players, team_players = con.execute(
        """
        SELECT
            COUNT(*) AS total_players,
            COUNT(*) FILTER (WHERE full_name IS NOT NULL) AS named_players,
            COUNT(*) FILTER (WHERE full_name IS NULL) AS unnamed_players,
            COUNT(*) FILTER (WHERE team IS NOT NULL) AS team_players
        FROM players
        """
    ).fetchone()
    return {
        "total_players": total_players or 0,
        "named_players": named_players or 0,
        "unnamed_players": unnamed_players or 0,
        "team_players": team_players or 0,
        "name_complete": (unnamed_players or 0) == 0,
    }


def resolve_window(
    con: duckdb.DuckDBPyConnection,
    window: str = "season",
    season: Optional[int] = None,
    season_start: Optional[int] = None,
    season_end: Optional[int] = None,
):
    context = latest_data_context(con)
    latest_game_date = context["latest_game_date"]
    latest_season = context["latest_season"]
    earliest_season = context["earliest_season"]

    if latest_game_date is None:
        return {
            "season": season,
            "season_start": season_start,
            "season_end": season_end,
            "start_date": None,
            "end_date": None,
            **context,
        }

    if window == "career":
        return {
            "season": None,
            "season_start": earliest_season,
            "season_end": latest_season,
            "start_date": None,
            "end_date": None,
            **context,
        }

    if window == "last7":
        end_date = latest_game_date
        start_date = latest_game_date - timedelta(days=6)
        return {
            "season": None,
            "season_start": None,
            "season_end": None,
            "start_date": start_date,
            "end_date": end_date,
            **context,
        }

    if season_start is not None or season_end is not None:
        start = season_start if season_start is not None else (season if season is not None else earliest_season)
        end = season_end if season_end is not None else (season if season is not None else latest_season)
        if start > end:
            start, end = end, start
        return {
            "season": None,
            "season_start": start,
            "season_end": end,
            "start_date": None,
            "end_date": None,
            **context,
        }

    return {
        "season": season or latest_season,
        "season_start": season or latest_season,
        "season_end": season or latest_season,
        "start_date": None,
        "end_date": None,
        **context,
    }


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def season_type_to_game_types(season_type: Optional[str]) -> Optional[list[str]]:
    if season_type is None or season_type == "both":
        return list(REGULAR_SEASON_GAME_TYPES + POSTSEASON_GAME_TYPES)
    if season_type == "regular":
        return list(REGULAR_SEASON_GAME_TYPES)
    if season_type == "postseason":
        return list(POSTSEASON_GAME_TYPES)
    raise HTTPException(status_code=400, detail="season_type must be one of: regular, postseason, both")


def build_filters(
    alias: str,
    *,
    season: Optional[int] = None,
    season_start: Optional[int] = None,
    season_end: Optional[int] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    balls: Optional[int] = None,
    strikes: Optional[int] = None,
    outs_col: Optional[str] = None,
    outs: Optional[int] = None,
    base_state: Optional[str] = None,
    stand: Optional[str] = None,
    p_throws: Optional[str] = None,
    game_types: Optional[list[str]] = None,
    player_col: Optional[str] = None,
    player_id: Optional[int] = None,
    pitch_type: Optional[str] = None,
) -> str:
    filters = ["1=1"]

    if season is not None:
        filters.append(f"{alias}.season = {season}")
    else:
        if season_start is not None:
            filters.append(f"{alias}.season >= {season_start}")
        if season_end is not None:
            filters.append(f"{alias}.season <= {season_end}")
    if start_date is not None:
        filters.append(f"{alias}.game_date >= DATE {sql_quote(start_date.isoformat())}")
    if end_date is not None:
        filters.append(f"{alias}.game_date <= DATE {sql_quote(end_date.isoformat())}")
    if balls is not None:
        filters.append(f"{alias}.balls = {balls}")
    if strikes is not None:
        filters.append(f"{alias}.strikes = {strikes}")
    if outs is not None and outs_col is not None:
        filters.append(f"{alias}.{outs_col} = {outs}")
    if base_state is not None:
        filters.append(f"{alias}.base_state = {sql_quote(base_state)}")
    if stand is not None:
        filters.append(f"{alias}.stand = {sql_quote(stand)}")
    if p_throws is not None:
        filters.append(f"{alias}.p_throws = {sql_quote(p_throws)}")
    if game_types:
        quoted = ", ".join(sql_quote(game_type) for game_type in game_types)
        filters.append(f"{alias}.game_type IN ({quoted})")
    if player_col is not None and player_id is not None:
        filters.append(f"{alias}.{player_col} = {player_id}")
    if pitch_type is not None:
        filters.append(f"{alias}.pitch_type = {sql_quote(pitch_type)}")

    return " AND ".join(filters)


def build_pitch_state_filters(
    alias: str,
    *,
    season: Optional[int] = None,
    game_types: Optional[list[str]] = None,
    balls: Optional[int] = None,
    strikes: Optional[int] = None,
    outs: Optional[int] = None,
    stand: Optional[str] = None,
    p_throws: Optional[str] = None,
    pitch_type: Optional[str] = None,
    perspective: Optional[str] = None,
    player_id: Optional[int] = None,
    prev_pitch_type: Optional[str] = None,
) -> str:
    filters = ["1=1"]
    if season is not None:
        filters.append(f"{alias}.season = {season}")
    if game_types:
        filters.append(f"{alias}.game_type IN ({sql_list(game_types)})")
    if balls is not None:
        filters.append(f"{alias}.balls = {balls}")
    if strikes is not None:
        filters.append(f"{alias}.strikes = {strikes}")
    if outs is not None:
        filters.append(f"{alias}.outs_when_up = {outs}")
    if stand is not None:
        filters.append(f"{alias}.stand = {sql_quote(stand)}")
    if p_throws is not None:
        filters.append(f"{alias}.p_throws = {sql_quote(p_throws)}")
    if pitch_type is not None:
        filters.append(f"{alias}.pitch_type = {sql_quote(pitch_type)}")
    if perspective is not None:
        filters.append(f"{alias}.perspective = {sql_quote(perspective)}")
    if player_id is not None:
        filters.append(f"{alias}.player_id = {player_id}")
    if prev_pitch_type is not None:
        filters.append(f"{alias}.prev_pitch_type = {sql_quote(prev_pitch_type)}")
    return " AND ".join(filters)


def season_report_data(con: duckdb.DuckDBPyConnection, season: int) -> dict:
    breakdown_rows = con.execute(
        """
        SELECT game_type, COUNT(DISTINCT game_pk) AS games
        FROM pitches
        WHERE season = ?
        GROUP BY game_type
        ORDER BY game_type
        """,
        [season],
    ).fetchall()
    breakdown = {row[0]: int(row[1]) for row in breakdown_rows}
    postseason_breakdown = {k: v for k, v in breakdown.items() if k in POSTSEASON_GAME_TYPES}
    return {
        "season": season,
        "reference_season": season == REFERENCE_SEASON,
        "regular_games": breakdown.get("R", 0),
        "postseason_total": sum(postseason_breakdown.values()),
        "postseason_breakdown": postseason_breakdown,
        "league_pitch_state_rows": con.execute("SELECT COUNT(*) FROM league_pitch_state_summary WHERE season = ?", [season]).fetchone()[0],
        "player_pitch_state_rows": con.execute("SELECT COUNT(*) FROM player_pitch_state_summary WHERE season = ?", [season]).fetchone()[0],
        "pitch_transition_rows": con.execute("SELECT COUNT(*) FROM pitch_transition_summary WHERE season = ?", [season]).fetchone()[0],
        "batting_standard_rows": con.execute("SELECT COUNT(*) FROM batting_standard_stats WHERE season = ?", [season]).fetchone()[0],
        "batting_value_rows": con.execute("SELECT COUNT(*) FROM batting_value_stats WHERE season = ?", [season]).fetchone()[0],
        "pitching_standard_rows": con.execute("SELECT COUNT(*) FROM pitching_standard_stats WHERE season = ?", [season]).fetchone()[0],
        "pitching_value_rows": con.execute("SELECT COUNT(*) FROM pitching_value_stats WHERE season = ?", [season]).fetchone()[0],
    }


def available_teams(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute(
        """
        SELECT team
        FROM (
            SELECT home_team AS team FROM games WHERE home_team IS NOT NULL
            UNION
            SELECT away_team AS team FROM games WHERE away_team IS NOT NULL
        )
        ORDER BY team
        """
    ).fetchall()
    return [row[0] for row in rows if row[0]]


def normalize_hand(value: Optional[str]) -> Optional[str]:
    if value is None or value.lower() == "all":
        return None
    return value.upper()


def player_name_expr(prefix: str, id_col: str) -> str:
    return f"COALESCE(pl.full_name, '{prefix} #' || CAST({id_col} AS VARCHAR))"


def derived_batter_team_expr(pitch_alias: str, game_alias: str = "g") -> str:
    return f"""
        COALESCE(
            NULLIF({pitch_alias}.batter_team, ''),
            CASE
                WHEN {pitch_alias}.inning_half = 'Top' THEN {game_alias}.away_team
                ELSE {game_alias}.home_team
            END
        )
    """


def derived_pitcher_team_expr(pitch_alias: str, game_alias: str = "g") -> str:
    return f"""
        COALESCE(
            NULLIF({pitch_alias}.pitcher_team, ''),
            CASE
                WHEN {pitch_alias}.inning_half = 'Top' THEN {game_alias}.home_team
                ELSE {game_alias}.away_team
            END
        )
    """


def team_expr(prefix: str, id_col: str, team_col: str) -> str:
    derived = derived_batter_team_expr("px", "gm") if team_col == "batter_team" else derived_pitcher_team_expr("px", "gm")
    return f"""
        COALESCE(
            NULLIF(pl.team, ''),
            (
                SELECT {derived}
                FROM pitches px
                LEFT JOIN games gm ON gm.game_pk = px.game_pk
                WHERE px.{id_col} = {prefix}.{id_col}
                  AND {derived} IS NOT NULL
                ORDER BY px.game_date DESC
                LIMIT 1
            )
        )
    """


def lookup_player_names(player_ids: list[int]) -> dict[int, str]:
    missing_ids = [pid for pid in player_ids if pid not in PLAYER_NAME_CACHE]
    if not missing_ids:
        return {pid: PLAYER_NAME_CACHE[pid] for pid in player_ids if PLAYER_NAME_CACHE.get(pid)}

    try:
        from pybaseball import playerid_reverse_lookup

        lookup_df = playerid_reverse_lookup(missing_ids, key_type="mlbam")
        if lookup_df is not None and not lookup_df.empty:
            for _, row in lookup_df.iterrows():
                pid = int(row["key_mlbam"])
                PLAYER_NAME_CACHE[pid] = f"{row['name_first'].strip().title()} {row['name_last'].strip().title()}"
    except Exception:
        pass

    for pid in missing_ids:
        PLAYER_NAME_CACHE.setdefault(pid, None)

    still_missing = [pid for pid in missing_ids if not PLAYER_NAME_CACHE.get(pid)]
    for pid in still_missing:
        PLAYER_NAME_CACHE[pid] = lookup_player_name_via_mlb_api(pid)

    return {pid: PLAYER_NAME_CACHE[pid] for pid in player_ids if PLAYER_NAME_CACHE.get(pid)}


def lookup_player_name_via_mlb_api(player_id: int) -> Optional[str]:
    url = f"https://statsapi.mlb.com/api/v1/people/{player_id}"
    request = Request(url, headers={"User-Agent": "basecount/1.0"})
    try:
        with urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, ValueError):
        return None

    people = payload.get("people") or []
    if not people:
        return None
    return people[0].get("fullName")


def inject_player_names(records: list[dict], *, id_key: str, name_key: str) -> list[dict]:
    missing_ids = [
        int(record[id_key])
        for record in records
        if record.get(id_key) is not None and str(record.get(name_key, "")).endswith(f"#{record[id_key]}")
    ]
    if not missing_ids:
        return records

    resolved = lookup_player_names(missing_ids)
    for record in records:
        pid = record.get(id_key)
        if pid in resolved:
            record[name_key] = resolved[pid]
    return records


def player_search_query(role: str, search_term: str, limit: int) -> str:
    escaped = search_term.lower().replace("'", "''")
    like_term = f"'%{escaped}%'"
    starts_term = f"'{escaped}%'"
    if role == "batter":
        return f"""
            SELECT
                ab.batter_id AS id,
                {player_name_expr('Batter', 'ab.batter_id')} AS name,
                {team_expr('ab', 'batter_id', 'batter_team')} AS team,
                COUNT(*) AS activity
            FROM at_bats ab
            LEFT JOIN players pl ON pl.player_id = ab.batter_id
            GROUP BY ab.batter_id, {player_name_expr('Batter', 'ab.batter_id')}, {team_expr('ab', 'batter_id', 'batter_team')}
            HAVING
                LOWER({player_name_expr('Batter', 'ab.batter_id')}) LIKE {like_term}
                OR LOWER(COALESCE({team_expr('ab', 'batter_id', 'batter_team')}, '')) LIKE {like_term}
                OR CAST(ab.batter_id AS VARCHAR) = {sql_quote(search_term)}
            ORDER BY
                CASE
                    WHEN LOWER({player_name_expr('Batter', 'ab.batter_id')}) LIKE {starts_term} THEN 0
                    ELSE 1
                END,
                activity DESC,
                name
            LIMIT {limit}
        """
    return f"""
        SELECT
            p.pitcher_id AS id,
            {player_name_expr('Pitcher', 'p.pitcher_id')} AS name,
            {team_expr('p', 'pitcher_id', 'pitcher_team')} AS team,
            COUNT(*) AS activity
        FROM pitches p
        LEFT JOIN players pl ON pl.player_id = p.pitcher_id
        GROUP BY p.pitcher_id, {player_name_expr('Pitcher', 'p.pitcher_id')}, {team_expr('p', 'pitcher_id', 'pitcher_team')}
        HAVING
            LOWER({player_name_expr('Pitcher', 'p.pitcher_id')}) LIKE {like_term}
            OR LOWER(COALESCE({team_expr('p', 'pitcher_id', 'pitcher_team')}, '')) LIKE {like_term}
            OR CAST(p.pitcher_id AS VARCHAR) = {sql_quote(search_term)}
        ORDER BY
            CASE
                WHEN LOWER({player_name_expr('Pitcher', 'p.pitcher_id')}) LIKE {starts_term} THEN 0
                ELSE 1
            END,
            activity DESC,
            name
        LIMIT {limit}
    """


def zone_matrix_from_rows(rows: list[tuple[int, int]]) -> list[list[int]]:
    counts = {zone: count for zone, count in rows if zone is not None}
    return [
        [counts.get(1, 0), counts.get(2, 0), counts.get(3, 0)],
        [counts.get(4, 0), counts.get(5, 0), counts.get(6, 0)],
        [counts.get(7, 0), counts.get(8, 0), counts.get(9, 0)],
    ]


def outcome_case_expr(alias: str = "p") -> str:
    return f"""
        CASE
            WHEN {alias}.description IN {BALL_DESCRIPTIONS} THEN 'ball'
            WHEN {alias}.description IN {STRIKE_DESCRIPTIONS} THEN 'strike'
            WHEN {alias}.description IN {FOUL_DESCRIPTIONS} THEN 'foul'
            ELSE NULL
        END
    """


def outcome_sequence_summary(
    con: duckdb.DuckDBPyConnection,
    outcomes: list[str],
    *,
    pitcher_id: Optional[int] = None,
    season: Optional[int] = None,
    season_start: Optional[int] = None,
    season_end: Optional[int] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[list[str]] = None,
) -> dict:
    if not outcomes:
        return {"sequence": "", "occurrences": 0, "whiff_pct": None, "k_pct": None, "sequence_length": 0}

    filters = build_filters(
        "p",
        season=season,
        season_start=season_start,
        season_end=season_end,
        start_date=start_date,
        end_date=end_date,
        game_types=game_types,
        player_col="pitcher_id" if pitcher_id is not None else None,
        player_id=pitcher_id,
    )
    normalized = [outcome.strip().lower() for outcome in outcomes if outcome and outcome.strip()]
    if not normalized:
        return {"sequence": "", "occurrences": 0, "whiff_pct": None, "k_pct": None, "sequence_length": 0}

    join_clauses = []
    where_clauses = []
    select_outcome = []
    last_alias = "b0"
    for idx, outcome in enumerate(normalized):
        alias = f"b{idx}"
        select_outcome.append(f"{alias}.outcome AS outcome_{idx}")
        if idx > 0:
            prev_alias = f"b{idx - 1}"
            join_clauses.append(
                f"""
                INNER JOIN base {alias}
                    ON {alias}.at_bat_id = {prev_alias}.at_bat_id
                   AND {alias}.pitch_number = {prev_alias}.pitch_number + 1
                """
            )
        where_clauses.append(f"{alias}.outcome = {sql_quote(outcome)}")
        last_alias = alias

    join_sql = "\n".join(join_clauses)
    where_sql = " AND ".join(where_clauses)
    summary = con.execute(
        f"""
        WITH base AS (
            SELECT
                CONCAT(p.game_pk, '_', p.at_bat_number) AS at_bat_id,
                p.pitch_number,
                p.description,
                p.events,
                {outcome_case_expr('p')} AS outcome
            FROM pitches p
            WHERE {filters}
              AND {outcome_case_expr('p')} IS NOT NULL
        ),
        matched AS (
            SELECT
                {', '.join(select_outcome)},
                {last_alias}.at_bat_id AS at_bat_id,
                {last_alias}.pitch_number AS last_pitch_number,
                {last_alias}.description AS last_description,
                {last_alias}.events AS last_events
            FROM base b0
            {join_sql}
            WHERE {where_sql}
        ),
        sequenced_results AS (
            SELECT
                m.*,
                next_pitch.description AS next_description,
                ab.final_event
            FROM matched m
            LEFT JOIN base next_pitch
                ON next_pitch.at_bat_id = m.at_bat_id
               AND next_pitch.pitch_number = m.last_pitch_number + 1
            LEFT JOIN at_bats ab
                ON ab.at_bat_id = m.at_bat_id
        )
        SELECT
            COUNT(*) AS occurrences,
            ROUND(
                SUM(CASE WHEN next_description IN ('swinging_strike', 'swinging_strike_blocked') THEN 1 ELSE 0 END)::FLOAT
                / NULLIF(COUNT(*), 0),
                3
            ) AS whiff_pct,
            ROUND(
                SUM(CASE WHEN final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT
                / NULLIF(COUNT(*), 0),
                3
            ) AS k_pct
        FROM sequenced_results
        """
    ).fetchone()
    occurrences, whiff_pct, k_pct = summary
    return {
        "sequence": " -> ".join(normalized),
        "occurrences": occurrences or 0,
        "whiff_pct": whiff_pct,
        "k_pct": k_pct,
        "sequence_length": len(normalized),
    }


def batter_stat_query(where_sql: str) -> str:
    return f"""
        WITH filtered AS (
            SELECT *
            FROM at_bats ab
            WHERE {where_sql}
        )
        SELECT
            COUNT(*) AS pa,
            ROUND(AVG(xwoba), 3) AS xwoba,
            ROUND(SUM(CASE WHEN final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) AS k_pct,
            ROUND(SUM(CASE WHEN final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) AS bb_pct,
            SUM(CASE WHEN final_event = 'home_run' THEN 1 ELSE 0 END) AS hr,
            SUM(CASE WHEN final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END) AS hits,
            SUM(CASE WHEN final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END) AS walks,
            SUM(CASE WHEN final_event NOT IN {AB_EXCLUDED_EVENTS} THEN 1 ELSE 0 END) AS at_bats,
            SUM(
                CASE final_event
                    WHEN 'single' THEN 1
                    WHEN 'double' THEN 2
                    WHEN 'triple' THEN 3
                    WHEN 'home_run' THEN 4
                    ELSE 0
                END
            ) AS total_bases
        FROM filtered
    """


def pitcher_summary_query(where_sql: str) -> str:
    return f"""
        WITH filtered_pitches AS (
            SELECT *
            FROM pitches p
            WHERE {where_sql}
        ),
        filtered_abs AS (
            SELECT DISTINCT
                CONCAT(game_pk, '_', at_bat_number) AS at_bat_key,
                pitcher_id,
                game_date,
                season
            FROM filtered_pitches
        ),
        pitcher_abs AS (
            SELECT ab.*
            FROM at_bats ab
            INNER JOIN filtered_abs fa
                ON fa.at_bat_key = ab.at_bat_id
        )
        SELECT
            (SELECT COUNT(*) FROM filtered_pitches) AS pitches,
            (SELECT COUNT(DISTINCT pitcher_id) FROM filtered_pitches) AS pitchers,
            (SELECT ROUND(AVG(release_speed), 1) FROM filtered_pitches) AS avg_velo,
            (SELECT ROUND(AVG(release_spin_rate), 0) FROM filtered_pitches) AS avg_spin,
            (SELECT ROUND(SUM(CASE WHEN description IN ('swinging_strike', 'swinging_strike_blocked') THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) FROM filtered_pitches) AS whiff_pct,
            (SELECT ROUND(SUM(CASE WHEN description IN ('called_strike', 'swinging_strike', 'swinging_strike_blocked', 'foul', 'foul_tip') THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) FROM filtered_pitches) AS csw_pct,
            (SELECT ROUND(AVG(xwoba), 3) FROM pitcher_abs) AS xwoba_allowed,
            (SELECT ROUND(SUM(CASE WHEN final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) FROM pitcher_abs) AS k_pct,
            (SELECT ROUND(SUM(CASE WHEN final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) FROM pitcher_abs) AS bb_pct
    """


@app.get("/api/meta/context")
def api_meta_context():
    """Full data context: loaded seasons, completeness, player list, and team list."""
    con = get_con()
    context = latest_data_context(con)
    coverage = history_completeness(con)
    name_coverage = player_name_completeness(con)
    teams = available_teams(con)
    batter_name = player_name_expr("Batter", "ab.batter_id")
    pitcher_name = player_name_expr("Pitcher", "fp.pitcher_id")

    batters = con.execute(
        f"""
        SELECT
            ab.batter_id AS id,
            {batter_name} AS name,
            {team_expr('ab', 'batter_id', 'batter_team')} AS team,
            COUNT(*) AS pa
        FROM at_bats ab
        LEFT JOIN players pl ON pl.player_id = ab.batter_id
        GROUP BY ab.batter_id, {batter_name}, {team_expr('ab', 'batter_id', 'batter_team')}
        ORDER BY pa DESC, name
        LIMIT 200
        """
    ).df()

    pitchers = con.execute(
        f"""
        SELECT
            p.pitcher_id AS id,
            {pitcher_name} AS name,
            {team_expr('p', 'pitcher_id', 'pitcher_team')} AS team,
            COUNT(*) AS pitches
        FROM pitches p
        LEFT JOIN players pl ON pl.player_id = p.pitcher_id
        GROUP BY p.pitcher_id, {pitcher_name}, {team_expr('p', 'pitcher_id', 'pitcher_team')}
        ORDER BY pitches DESC, name
        LIMIT 200
        """
    ).df()

    batter_records = inject_player_names(df_to_json(batters), id_key="id", name_key="name")
    pitcher_records = inject_player_names(df_to_json(pitchers), id_key="id", name_key="name")

    return {
        **context,
        "history": coverage,
        "players_status": name_coverage,
        "teams": teams,
        "batters": batter_records,
        "pitchers": pitcher_records,
    }


@app.get("/api/meta/coverage")
def api_meta_coverage():
    """Season completeness and player-name coverage summary."""
    con = get_con()
    return {
        **history_completeness(con),
        "players_status": player_name_completeness(con),
    }

@app.get("/api/players/search")
def api_player_search(
    role: str = Query(..., pattern="^(batter|pitcher)$"),
    q: str = Query(..., min_length=1),
    limit: int = Query(25, ge=1, le=100),
):
    con = get_con()
    df = con.execute(player_search_query(role, q.strip(), limit)).df()
    records = inject_player_names(df_to_json(df), id_key="id", name_key="name")
    return {"results": records}


@app.get("/api/pitch-state/league")
def api_pitch_state_league(
    season: int = Query(REFERENCE_SEASON, ge=2015),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
    balls: Optional[int] = Query(None, ge=0, le=3),
    strikes: Optional[int] = Query(None, ge=0, le=2),
    outs: Optional[int] = Query(None, ge=0, le=2),
    stand: Optional[str] = Query(None, pattern="^(L|R|S)$"),
    p_throws: Optional[str] = Query(None, pattern="^(L|R)$"),
    min_pitches: int = Query(1, ge=1),
):
    """League-wide pitch outcome rates for a given count / outs / handedness state."""
    con = get_con()
    where_sql = build_pitch_state_filters(
        "s",
        season=season,
        game_types=season_type_to_game_types(season_type),
        balls=balls,
        strikes=strikes,
        outs=outs,
        stand=normalize_hand(stand),
        p_throws=normalize_hand(p_throws),
    )
    df = con.execute(
        f"""
        SELECT
            pitch_type,
            any_value(pitch_name) AS pitch_name,
            SUM(pitches) AS pitches,
            SUM(plate_appearances) AS plate_appearances,
            ROUND(SUM(pitches)::FLOAT / NULLIF(SUM(SUM(pitches)) OVER (), 0), 3) AS usage_pct,
            ROUND(SUM(ball_events)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS ball_pct,
            ROUND(SUM(called_strikes)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS called_strike_pct,
            ROUND(SUM(whiffs)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS whiff_pct,
            ROUND(SUM(fouls)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS foul_pct,
            ROUND(SUM(balls_in_play)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS in_play_pct,
            ROUND(SUM(hit_events)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS hit_event_pct,
            ROUND(SUM(walk_events)::FLOAT / NULLIF(SUM(plate_appearances), 0), 3) AS walk_rate,
            ROUND(SUM(strikeout_events)::FLOAT / NULLIF(SUM(plate_appearances), 0), 3) AS strikeout_rate,
            ROUND(AVG(avg_woba_value), 3) AS avg_woba_value,
            ROUND(AVG(avg_xwoba), 3) AS avg_xwoba
        FROM league_pitch_state_summary s
        WHERE {where_sql}
        GROUP BY pitch_type
        HAVING SUM(pitches) >= {min_pitches}
        ORDER BY pitches DESC, pitch_type
        """
    ).df()
    con.close()
    return df_to_json(df)


@app.get("/api/pitch-state/player")
def api_pitch_state_player(
    player_id: int,
    perspective: str = Query(..., pattern="^(batter|pitcher)$"),
    season: int = Query(REFERENCE_SEASON, ge=2015),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
    balls: Optional[int] = Query(None, ge=0, le=3),
    strikes: Optional[int] = Query(None, ge=0, le=2),
    outs: Optional[int] = Query(None, ge=0, le=2),
    stand: Optional[str] = Query(None, pattern="^(L|R|S)$"),
    p_throws: Optional[str] = Query(None, pattern="^(L|R)$"),
    min_pitches: int = Query(1, ge=1),
):
    """Individual player pitch outcome rates, from the batter or pitcher perspective."""
    con = get_con()
    where_sql = build_pitch_state_filters(
        "s",
        season=season,
        game_types=season_type_to_game_types(season_type),
        balls=balls,
        strikes=strikes,
        outs=outs,
        stand=normalize_hand(stand),
        p_throws=normalize_hand(p_throws),
        perspective=perspective,
        player_id=player_id,
    )
    df = con.execute(
        f"""
        SELECT
            pitch_type,
            any_value(pitch_name) AS pitch_name,
            SUM(pitches) AS pitches,
            SUM(plate_appearances) AS plate_appearances,
            ROUND(SUM(pitches)::FLOAT / NULLIF(SUM(SUM(pitches)) OVER (), 0), 3) AS usage_pct,
            ROUND(SUM(ball_events)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS ball_pct,
            ROUND(SUM(called_strikes)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS called_strike_pct,
            ROUND(SUM(whiffs)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS whiff_pct,
            ROUND(SUM(fouls)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS foul_pct,
            ROUND(SUM(balls_in_play)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS in_play_pct,
            ROUND(SUM(hit_events)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS hit_event_pct,
            ROUND(SUM(walk_events)::FLOAT / NULLIF(SUM(plate_appearances), 0), 3) AS walk_rate,
            ROUND(SUM(strikeout_events)::FLOAT / NULLIF(SUM(plate_appearances), 0), 3) AS strikeout_rate,
            ROUND(AVG(avg_woba_value), 3) AS avg_woba_value,
            ROUND(AVG(avg_xwoba), 3) AS avg_xwoba
        FROM player_pitch_state_summary s
        WHERE {where_sql}
        GROUP BY pitch_type
        HAVING SUM(pitches) >= {min_pitches}
        ORDER BY pitches DESC, pitch_type
        """
    ).df()
    con.close()
    return df_to_json(df)


@app.get("/api/predict/next-pitch")
def api_predict_next_pitch(
    season: int = Query(REFERENCE_SEASON, ge=2015),
    perspective: str = Query("pitcher", pattern="^(batter|pitcher)$"),
    player_id: Optional[int] = None,
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
    balls: int = Query(..., ge=0, le=3),
    strikes: int = Query(..., ge=0, le=2),
    outs: Optional[int] = Query(None, ge=0, le=2),
    stand: Optional[str] = Query(None, pattern="^(L|R|S)$"),
    p_throws: Optional[str] = Query(None, pattern="^(L|R)$"),
    prev_pitch_type: str = Query(...),
    min_transitions: int = Query(1, ge=1),
):
    """
    Next-pitch probability distribution given the current count and previous pitch type.
    Returns transition probabilities from the pre-materialized pitch_transition_summary table.
    """
    con = get_con()
    where_sql = build_pitch_state_filters(
        "t",
        season=season,
        game_types=season_type_to_game_types(season_type),
        balls=balls,
        strikes=strikes,
        outs=outs,
        stand=normalize_hand(stand),
        p_throws=normalize_hand(p_throws),
        perspective=perspective,
        player_id=player_id,
        prev_pitch_type=prev_pitch_type,
    )
    df = con.execute(
        f"""
        SELECT
            next_pitch_type,
            SUM(transitions) AS transitions,
            ROUND(SUM(transitions)::FLOAT / NULLIF(SUM(SUM(transitions)) OVER (), 0), 3) AS probability,
            ROUND(SUM(next_pitch_whiffs)::FLOAT / NULLIF(SUM(transitions), 0), 3) AS whiff_pct,
            ROUND(SUM(next_pitch_called_strikes)::FLOAT / NULLIF(SUM(transitions), 0), 3) AS called_strike_pct,
            ROUND(SUM(next_pitch_balls)::FLOAT / NULLIF(SUM(transitions), 0), 3) AS ball_pct,
            ROUND(SUM(next_pitch_in_play)::FLOAT / NULLIF(SUM(transitions), 0), 3) AS in_play_pct,
            ROUND(SUM(next_pitch_hits)::FLOAT / NULLIF(SUM(transitions), 0), 3) AS hit_event_pct,
            ROUND(SUM(next_pitch_walks)::FLOAT / NULLIF(SUM(transitions), 0), 3) AS walk_event_pct,
            ROUND(SUM(next_pitch_strikeouts)::FLOAT / NULLIF(SUM(transitions), 0), 3) AS strikeout_event_pct
        FROM pitch_transition_summary t
        WHERE {where_sql}
        GROUP BY next_pitch_type
        HAVING SUM(transitions) >= {min_transitions}
        ORDER BY transitions DESC, next_pitch_type
        """
    ).df()
    con.close()
    return df_to_json(df)


@app.get("/api/predict/outcome-by-pitch")
def api_predict_outcome_by_pitch(
    season: int = Query(REFERENCE_SEASON, ge=2015),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
    balls: int = Query(..., ge=0, le=3),
    strikes: int = Query(..., ge=0, le=2),
    outs: Optional[int] = Query(None, ge=0, le=2),
    stand: Optional[str] = Query(None, pattern="^(L|R|S)$"),
    p_throws: Optional[str] = Query(None, pattern="^(L|R)$"),
    pitch_type: Optional[str] = None,
    min_pitches: int = Query(1, ge=1),
):
    """Expected outcomes if a specific pitch type is thrown in the current count state."""
    con = get_con()
    where_sql = build_pitch_state_filters(
        "s",
        season=season,
        game_types=season_type_to_game_types(season_type),
        balls=balls,
        strikes=strikes,
        outs=outs,
        stand=normalize_hand(stand),
        p_throws=normalize_hand(p_throws),
        pitch_type=pitch_type,
    )
    df = con.execute(
        f"""
        SELECT
            pitch_type,
            any_value(pitch_name) AS pitch_name,
            SUM(pitches) AS pitches,
            ROUND(SUM(ball_events)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS ball_pct,
            ROUND(SUM(called_strikes)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS called_strike_pct,
            ROUND(SUM(whiffs)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS whiff_pct,
            ROUND(SUM(fouls)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS foul_pct,
            ROUND(SUM(balls_in_play)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS in_play_pct,
            ROUND(SUM(hit_events)::FLOAT / NULLIF(SUM(pitches), 0), 3) AS hit_event_pct,
            ROUND(SUM(walk_events)::FLOAT / NULLIF(SUM(plate_appearances), 0), 3) AS walk_rate,
            ROUND(SUM(strikeout_events)::FLOAT / NULLIF(SUM(plate_appearances), 0), 3) AS strikeout_rate,
            ROUND(AVG(avg_woba_value), 3) AS avg_woba_value,
            ROUND(AVG(avg_xwoba), 3) AS avg_xwoba
        FROM league_pitch_state_summary s
        WHERE {where_sql}
        GROUP BY pitch_type
        HAVING SUM(pitches) >= {min_pitches}
        ORDER BY pitches DESC, pitch_type
        """
    ).df()
    con.close()
    return df_to_json(df)


# ---------------------------------------------------------------------------
# Count State Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/count-state/batter-splits")
def api_count_state_splits(
    balls: int = Query(..., ge=0, le=3),
    strikes: int = Query(..., ge=0, le=2),
    outs: Optional[int] = Query(None, ge=0, le=2),
    base_state: Optional[str] = None,
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
    min_pa: int = Query(10, ge=1),
    stand: Optional[str] = None,
    p_throws: Optional[str] = None,
):
    """Batter performance in a specific count situation."""
    con = get_con()
    resolved = resolve_window(con, "season", season, season_start, season_end)
    game_types = season_type_to_game_types(season_type)
    df = count_state_splits(
        con,
        balls,
        strikes,
        outs,
        base_state,
        resolved["season"],
        min_pa,
        stand=normalize_hand(stand),
        p_throws=normalize_hand(p_throws),
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        game_types=game_types,
    )
    records = inject_player_names(df_to_json(df), id_key="batter_id", name_key="batter_name")
    return {"count": f"{balls}-{strikes}", "results": records}


@app.get("/api/count-state/outcome-matrix")
def api_outcome_matrix(
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
    window: str = Query("season", pattern="^(season|career|last7)$"),
    stand: Optional[str] = None,
    p_throws: Optional[str] = None,
):
    """Full count-state outcome probability matrix."""
    con = get_con()
    resolved = resolve_window(con, window, season, season_start, season_end)
    game_types = season_type_to_game_types(season_type)
    stand = normalize_hand(stand)
    p_throws = normalize_hand(p_throws)
    where_sql = build_filters(
        "p",
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        start_date=resolved["start_date"],
        end_date=resolved["end_date"],
        stand=stand,
        p_throws=p_throws,
        game_types=game_types,
    )
    df = con.execute(
        f"""
        WITH count_states AS (
            SELECT DISTINCT
                CONCAT(p.game_pk, '_', p.at_bat_number) AS at_bat_key,
                p.balls,
                p.strikes
            FROM pitches p
            WHERE {where_sql}
        )
        SELECT
            cs.balls,
            cs.strikes,
            CONCAT(cs.balls, '-', cs.strikes) AS count,
            COUNT(*) AS at_bats,
            ROUND(SUM(CASE WHEN ab.final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS k_pct,
            ROUND(SUM(CASE WHEN ab.final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS bb_pct,
            ROUND(SUM(CASE WHEN ab.final_event = 'home_run' THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS hr_pct,
            ROUND(SUM(CASE WHEN ab.final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS hit_pct,
            ROUND(AVG(ab.xwoba), 3) AS avg_xwoba
        FROM count_states cs
        INNER JOIN at_bats ab ON ab.at_bat_id = cs.at_bat_key
        GROUP BY cs.balls, cs.strikes
        ORDER BY cs.balls, cs.strikes
        """
    ).df()
    return df_to_json(df)


@app.get("/api/count-state/zone-map")
def api_count_zone_map(
    balls: Optional[int] = Query(None, ge=0, le=3),
    strikes: Optional[int] = Query(None, ge=0, le=2),
    outs: Optional[int] = Query(None, ge=0, le=2),
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
    window: str = Query("season", pattern="^(season|career|last7)$"),
    stand: Optional[str] = None,
    p_throws: Optional[str] = None,
):
    """Pitch location zone frequency map (3×3 grid, zones 1–9) for a given count state."""
    con = get_con()
    resolved = resolve_window(con, window, season, season_start, season_end)
    game_types = season_type_to_game_types(season_type)
    where_sql = build_filters(
        "p",
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        start_date=resolved["start_date"],
        end_date=resolved["end_date"],
        balls=balls,
        strikes=strikes,
        outs_col="outs_when_up",
        outs=outs,
        stand=normalize_hand(stand),
        p_throws=normalize_hand(p_throws),
        game_types=game_types,
    )
    rows = con.execute(
        f"""
        SELECT zone, COUNT(*) AS pitches
        FROM pitches p
        WHERE {where_sql}
          AND zone BETWEEN 1 AND 9
        GROUP BY zone
        ORDER BY zone
        """
    ).fetchall()
    return {"zones": zone_matrix_from_rows(rows)}


# ---------------------------------------------------------------------------
# Player Profile Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/batter/{batter_id}/overview")
def api_batter_overview(
    batter_id: int,
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
    window: str = Query("season", pattern="^(season|career|last7)$"),
    team: Optional[str] = Query(None, min_length=2, max_length=5),
):
    """
    Comprehensive batter profile: career/season summary, per-season splits,
    count-state performance, and zone contact rate map.
    """
    con = get_con()
    resolved = resolve_window(con, window, season, season_start, season_end)
    game_types = season_type_to_game_types(season_type)
    pitch_where = build_filters(
        "p",
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        start_date=resolved["start_date"],
        end_date=resolved["end_date"],
        game_types=game_types,
        player_col="batter_id",
        player_id=batter_id,
    )
    team_filter_sql = f"AND {derived_batter_team_expr('p', 'g')} = {sql_quote(team)}" if team else ""
    summary = con.execute(
        f"""
        WITH pitch_ab_team AS (
            SELECT
                CONCAT(p.game_pk, '_', p.at_bat_number) AS at_bat_id,
                any_value({derived_batter_team_expr('p', 'g')}) AS batter_team
            FROM pitches p
            LEFT JOIN games g ON g.game_pk = p.game_pk
            WHERE {pitch_where}
              {team_filter_sql}
            GROUP BY 1
        ),
        filtered AS (
            SELECT ab.*, pat.batter_team
            FROM at_bats ab
            INNER JOIN pitch_ab_team pat ON pat.at_bat_id = ab.at_bat_id
        )
        SELECT
            COUNT(*) AS pa,
            COUNT(DISTINCT game_pk) AS g,
            SUM(CASE WHEN final_event NOT IN {AB_EXCLUDED_EVENTS} THEN 1 ELSE 0 END) AS at_bats,
            SUM(CASE WHEN final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END) AS hits,
            SUM(CASE WHEN final_event = 'double' THEN 1 ELSE 0 END) AS doubles,
            SUM(CASE WHEN final_event = 'triple' THEN 1 ELSE 0 END) AS triples,
            SUM(CASE WHEN final_event = 'home_run' THEN 1 ELSE 0 END) AS hr,
            SUM(CASE WHEN final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END) AS walks,
            SUM(CASE WHEN final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END) AS strikeouts,
            SUM(CASE WHEN final_event = 'sac_fly' THEN 1 ELSE 0 END) AS sac_fly,
            SUM(CASE WHEN final_event = 'single' THEN 1 WHEN final_event = 'double' THEN 2 WHEN final_event = 'triple' THEN 3 WHEN final_event = 'home_run' THEN 4 ELSE 0 END) AS total_bases,
            ROUND(AVG(xwoba), 3) AS xwoba,
            ROUND(SUM(CASE WHEN final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) AS k_pct,
            ROUND(SUM(CASE WHEN final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) AS bb_pct,
            ROUND(SUM(CASE WHEN final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(SUM(CASE WHEN final_event NOT IN {AB_EXCLUDED_EVENTS} THEN 1 ELSE 0 END), 0), 3) AS avg,
            ROUND((SUM(CASE WHEN final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END) + SUM(CASE WHEN final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END))::FLOAT / NULLIF(SUM(CASE WHEN final_event NOT IN ('hit_by_pitch') THEN 1 ELSE 0 END), 0), 3) AS obp,
            ROUND(SUM(CASE WHEN final_event = 'single' THEN 1 WHEN final_event = 'double' THEN 2 WHEN final_event = 'triple' THEN 3 WHEN final_event = 'home_run' THEN 4 ELSE 0 END)::FLOAT / NULLIF(SUM(CASE WHEN final_event NOT IN {AB_EXCLUDED_EVENTS} THEN 1 ELSE 0 END), 0), 3) AS slg,
            string_agg(DISTINCT batter_team, '/' ORDER BY batter_team) AS team_display
        FROM filtered
        """
    ).df()
    if summary.empty or summary.iloc[0]["pa"] in (None, 0):
        raise HTTPException(status_code=404, detail="Batter not found")

    seasons_df = con.execute(
        f"""
        WITH pitch_ab_team AS (
            SELECT
                CONCAT(p.game_pk, '_', p.at_bat_number) AS at_bat_id,
                any_value({derived_batter_team_expr('p', 'g')}) AS batter_team
            FROM pitches p
            LEFT JOIN games g ON g.game_pk = p.game_pk
            WHERE {pitch_where}
              {team_filter_sql}
            GROUP BY 1
        ),
        filtered AS (
            SELECT ab.*, pat.batter_team
            FROM at_bats ab
            INNER JOIN pitch_ab_team pat ON pat.at_bat_id = ab.at_bat_id
        )
        SELECT
            season,
            batter_team AS team,
            COUNT(DISTINCT game_pk) AS g,
            COUNT(*) AS pa,
            SUM(CASE WHEN final_event NOT IN {AB_EXCLUDED_EVENTS} THEN 1 ELSE 0 END) AS ab,
            SUM(CASE WHEN final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END) AS h,
            SUM(CASE WHEN final_event = 'double' THEN 1 ELSE 0 END) AS doubles,
            SUM(CASE WHEN final_event = 'triple' THEN 1 ELSE 0 END) AS triples,
            SUM(CASE WHEN final_event = 'home_run' THEN 1 ELSE 0 END) AS hr,
            SUM(CASE WHEN final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END) AS bb,
            SUM(CASE WHEN final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END) AS so,
            ROUND(AVG(xwoba), 3) AS xwoba,
            ROUND(SUM(CASE WHEN final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(SUM(CASE WHEN final_event NOT IN {AB_EXCLUDED_EVENTS} THEN 1 ELSE 0 END), 0), 3) AS avg,
            ROUND((SUM(CASE WHEN final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END) + SUM(CASE WHEN final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END))::FLOAT / NULLIF(SUM(CASE WHEN final_event NOT IN ('hit_by_pitch') THEN 1 ELSE 0 END), 0), 3) AS obp,
            ROUND(SUM(CASE WHEN final_event = 'single' THEN 1 WHEN final_event = 'double' THEN 2 WHEN final_event = 'triple' THEN 3 WHEN final_event = 'home_run' THEN 4 ELSE 0 END)::FLOAT / NULLIF(SUM(CASE WHEN final_event NOT IN {AB_EXCLUDED_EVENTS} THEN 1 ELSE 0 END), 0), 3) AS slg
        FROM filtered
        GROUP BY season, batter_team
        ORDER BY season DESC, team
        """
    ).df()

    counts_df = con.execute(
        f"""
        WITH count_pas AS (
            SELECT DISTINCT
                CONCAT(p.game_pk, '_', p.at_bat_number) AS at_bat_key,
                p.balls,
                p.strikes
            FROM pitches p
            LEFT JOIN games g ON g.game_pk = p.game_pk
            WHERE {build_filters('p', season=resolved['season'], season_start=resolved['season_start'], season_end=resolved['season_end'], start_date=resolved['start_date'], end_date=resolved['end_date'], game_types=game_types, player_col='batter_id', player_id=batter_id)}
              {team_filter_sql}
        )
        SELECT
            cp.balls,
            cp.strikes,
            COUNT(*) AS pa,
            ROUND(AVG(ab.xwoba), 3) AS xwoba,
            ROUND(SUM(CASE WHEN ab.final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS avg,
            ROUND(SUM(CASE WHEN ab.final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS k_pct
        FROM count_pas cp
        INNER JOIN at_bats ab ON ab.at_bat_id = cp.at_bat_key
        GROUP BY cp.balls, cp.strikes
        ORDER BY cp.balls, cp.strikes
        """
    ).df()

    zone_rows = con.execute(
        f"""
        SELECT
            zone,
            ROUND(
                SUM(CASE WHEN bb_type IS NOT NULL OR description IN ('foul', 'foul_tip', 'hit_into_play') THEN 1 ELSE 0 END)::FLOAT
                / NULLIF(COUNT(*), 0) * 100,
                0
            )::INTEGER AS contact_rate
        FROM pitches p
        LEFT JOIN games g ON g.game_pk = p.game_pk
        WHERE {build_filters('p', season=resolved['season'], season_start=resolved['season_start'], season_end=resolved['season_end'], start_date=resolved['start_date'], end_date=resolved['end_date'], game_types=game_types, player_col='batter_id', player_id=batter_id)}
          {team_filter_sql}
          AND zone BETWEEN 1 AND 9
        GROUP BY zone
        ORDER BY zone
        """
    ).fetchall()

    return {
        "summary": {**df_to_json(summary)[0], "team_display": df_to_json(summary)[0].get("team_display") or team},
        "seasons": [
            {**row, "team": row.get("team") or team}
            for row in df_to_json(seasons_df)
        ],
        "team_filter": team,
        "counts": df_to_json(counts_df),
        "zones": zone_matrix_from_rows(zone_rows),
    }


@app.get("/api/pitcher/{pitcher_id}/overview")
def api_pitcher_overview(
    pitcher_id: int,
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
    window: str = Query("season", pattern="^(season|career|last7)$"),
    team: Optional[str] = Query(None, min_length=2, max_length=5),
):
    """
    Comprehensive pitcher profile: career/season summary, per-season splits,
    pitch usage by count, and zone location map.
    """
    con = get_con()
    resolved = resolve_window(con, window, season, season_start, season_end)
    game_types = season_type_to_game_types(season_type)
    pitch_where = build_filters(
        "p",
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        start_date=resolved["start_date"],
        end_date=resolved["end_date"],
        game_types=game_types,
        player_col="pitcher_id",
        player_id=pitcher_id,
    )
    team_filter_sql = f"AND {derived_pitcher_team_expr('p', 'g')} = {sql_quote(team)}" if team else ""

    summary = con.execute(
        f"""
        WITH filtered_pitches AS (
            SELECT p.*, {derived_pitcher_team_expr('p', 'g')} AS derived_pitcher_team
            FROM pitches p
            LEFT JOIN games g ON g.game_pk = p.game_pk
            WHERE {pitch_where}
              {team_filter_sql}
        ),
        filtered_abs AS (
            SELECT DISTINCT
                CONCAT(game_pk, '_', at_bat_number) AS at_bat_key,
                pitcher_id,
                game_date,
                season
            FROM filtered_pitches
        ),
        pitcher_abs AS (
            SELECT ab.*
            FROM at_bats ab
            INNER JOIN filtered_abs fa
                ON fa.at_bat_key = ab.at_bat_id
        )
        SELECT
            (SELECT COUNT(*) FROM filtered_pitches) AS pitches,
            (SELECT COUNT(DISTINCT game_pk) FROM filtered_pitches) AS g,
            (SELECT ROUND(AVG(release_speed), 1) FROM filtered_pitches) AS avg_velo,
            (SELECT ROUND(AVG(release_spin_rate), 0) FROM filtered_pitches) AS avg_spin,
            (SELECT ROUND(SUM(CASE WHEN description IN ('swinging_strike', 'swinging_strike_blocked') THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) FROM filtered_pitches) AS whiff_pct,
            (SELECT ROUND(SUM(CASE WHEN description IN ('called_strike', 'swinging_strike', 'swinging_strike_blocked', 'foul', 'foul_tip') THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) FROM filtered_pitches) AS csw_pct,
            (SELECT ROUND(AVG(xwoba), 3) FROM pitcher_abs) AS xwoba_allowed,
            (SELECT ROUND(SUM(CASE WHEN final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) FROM pitcher_abs) AS k_pct,
            (SELECT ROUND(SUM(CASE WHEN final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) FROM pitcher_abs) AS bb_pct,
            (SELECT string_agg(DISTINCT derived_pitcher_team, '/' ORDER BY derived_pitcher_team) FROM filtered_pitches) AS team_display
        """
    ).df()
    if summary.empty or summary.iloc[0]["pitches"] in (None, 0):
        raise HTTPException(status_code=404, detail="Pitcher not found")

    seasons_df = con.execute(
        f"""
        SELECT
            p.season,
            {derived_pitcher_team_expr('p', 'g')} AS team,
            COUNT(DISTINCT p.game_pk) AS g,
            COUNT(*) AS pitches,
            ROUND(AVG(p.release_speed), 1) AS avg_velo,
            ROUND(AVG(p.release_spin_rate), 0) AS avg_spin,
            ROUND(SUM(CASE WHEN p.description IN ('swinging_strike', 'swinging_strike_blocked') THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS whiff_pct
        FROM pitches p
        LEFT JOIN games g ON g.game_pk = p.game_pk
        WHERE p.pitcher_id = {pitcher_id}
          AND p.game_type IN ({', '.join(sql_quote(game_type) for game_type in game_types)})
          {team_filter_sql}
        GROUP BY p.season, team
        ORDER BY p.season DESC, team
        """
    ).df()

    counts_df = con.execute(
        f"""
        WITH pitch_counts AS (
            SELECT
                p.balls,
                p.strikes,
                p.pitch_type,
                COUNT(*) AS pitches,
                ROUND(COUNT(*)::FLOAT / SUM(COUNT(*)) OVER (PARTITION BY p.balls, p.strikes), 3) AS usage_pct,
                ROUND(AVG(p.release_speed), 1) AS avg_velo,
                ROUND(SUM(CASE WHEN p.description IN ('swinging_strike', 'swinging_strike_blocked') THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS whiff_pct
            FROM pitches p
            LEFT JOIN games g ON g.game_pk = p.game_pk
            WHERE {pitch_where}
              {team_filter_sql}
              AND p.pitch_type IS NOT NULL
            GROUP BY p.balls, p.strikes, p.pitch_type
        )
        SELECT *
        FROM pitch_counts
        ORDER BY balls, strikes, usage_pct DESC
        """
    ).df()

    zone_rows = con.execute(
        f"""
        SELECT zone, COUNT(*) AS pitches
        FROM pitches p
        LEFT JOIN games g ON g.game_pk = p.game_pk
        WHERE {pitch_where}
          {team_filter_sql}
          AND zone BETWEEN 1 AND 9
        GROUP BY zone
        ORDER BY zone
        """
    ).fetchall()

    return {
        "summary": {**df_to_json(summary)[0], "team_display": df_to_json(summary)[0].get("team_display") or team},
        "seasons": [
            {**row, "team": row.get("team") or team}
            for row in df_to_json(seasons_df)
        ],
        "team_filter": team,
        "counts": df_to_json(counts_df),
        "zones": zone_matrix_from_rows(zone_rows),
    }


@app.get("/api/pitching/overview")
def api_pitching_overview(
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
    window: str = Query("season", pattern="^(season|career|last7)$"),
):
    """League-wide pitching summary: aggregate velo, spin, whiff, and xwOBA allowed."""
    con = get_con()
    resolved = resolve_window(con, window, season, season_start, season_end)
    game_types = season_type_to_game_types(season_type)
    pitch_where = build_filters(
        "p",
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        start_date=resolved["start_date"],
        end_date=resolved["end_date"],
        game_types=game_types,
    )

    summary = con.execute(pitcher_summary_query(pitch_where)).df()
    if summary.empty or summary.iloc[0]["pitches"] in (None, 0):
        raise HTTPException(status_code=404, detail="Pitching data not found")

    seasons_df = con.execute(
        f"""
        SELECT
            p.season,
            COUNT(*) AS pitches,
            ROUND(AVG(p.release_speed), 1) AS avg_velo,
            ROUND(AVG(p.release_spin_rate), 0) AS avg_spin,
            ROUND(SUM(CASE WHEN p.description IN ('swinging_strike', 'swinging_strike_blocked') THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS whiff_pct
        FROM pitches p
        WHERE {pitch_where}
        GROUP BY p.season
        ORDER BY p.season DESC
        """
    ).df()

    counts_df = con.execute(
        f"""
        WITH pitch_counts AS (
            SELECT
                p.balls,
                p.strikes,
                p.pitch_type,
                COUNT(*) AS pitches,
                ROUND(COUNT(*)::FLOAT / SUM(COUNT(*)) OVER (PARTITION BY p.balls, p.strikes), 3) AS usage_pct,
                ROUND(AVG(p.release_speed), 1) AS avg_velo,
                ROUND(SUM(CASE WHEN p.description IN ('swinging_strike', 'swinging_strike_blocked') THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS whiff_pct
            FROM pitches p
            WHERE {pitch_where}
              AND p.pitch_type IS NOT NULL
            GROUP BY p.balls, p.strikes, p.pitch_type
        )
        SELECT *
        FROM pitch_counts
        ORDER BY balls, strikes, usage_pct DESC
        """
    ).df()

    zone_rows = con.execute(
        f"""
        SELECT zone, COUNT(*) AS pitches
        FROM pitches p
        WHERE {pitch_where}
          AND zone BETWEEN 1 AND 9
        GROUP BY zone
        ORDER BY zone
        """
    ).fetchall()

    return {
        "summary": df_to_json(summary)[0],
        "seasons": df_to_json(seasons_df),
        "counts": df_to_json(counts_df),
        "zones": zone_matrix_from_rows(zone_rows),
    }


@app.get("/api/team/{team_code}/overview")
def api_team_overview(
    team_code: str,
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    season_type: str = Query("regular", pattern="^(regular|postseason|both)$"),
    window: str = Query("season", pattern="^(season|career|last7)$"),
):
    """
    Team profile: batting and pitching aggregates plus per-player leaderboards
    for both the offense and the pitching staff.
    """
    team_code = team_code.upper()
    con = get_con()
    resolved = resolve_window(con, window, season, season_start, season_end)
    game_types = season_type_to_game_types(season_type)

    pitch_where = build_filters(
        "p",
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        start_date=resolved["start_date"],
        end_date=resolved["end_date"],
        game_types=game_types,
    )
    batting_where = build_filters(
        "ab",
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        start_date=resolved["start_date"],
        end_date=resolved["end_date"],
        game_types=game_types,
    )
    summary = con.execute(
        f"""
        WITH team_games AS (
            SELECT COUNT(DISTINCT p.game_pk) AS games
            FROM pitches p
            LEFT JOIN games g ON g.game_pk = p.game_pk
            WHERE {pitch_where}
              AND (
                {derived_batter_team_expr('p', 'g')} = {sql_quote(team_code)}
                OR {derived_pitcher_team_expr('p', 'g')} = {sql_quote(team_code)}
              )
        ),
        pitch_ab_team AS (
            SELECT
                CONCAT(p.game_pk, '_', p.at_bat_number) AS at_bat_id,
                any_value({derived_batter_team_expr('p', 'g')}) AS batter_team
            FROM pitches p
            LEFT JOIN games g ON g.game_pk = p.game_pk
            WHERE {pitch_where}
              AND {derived_batter_team_expr('p', 'g')} = {sql_quote(team_code)}
            GROUP BY 1
        ),
        batting AS (
            SELECT
                COUNT(*) AS pa,
                SUM(CASE WHEN ab.final_event NOT IN {AB_EXCLUDED_EVENTS} THEN 1 ELSE 0 END) AS at_bats,
                SUM(CASE WHEN ab.final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END) AS hits,
                SUM(CASE WHEN ab.final_event = 'home_run' THEN 1 ELSE 0 END) AS hr,
                SUM(CASE WHEN ab.final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END) AS walks,
                SUM(CASE WHEN ab.final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END) AS strikeouts,
                SUM(CASE WHEN ab.final_event = 'single' THEN 1 WHEN ab.final_event = 'double' THEN 2 WHEN ab.final_event = 'triple' THEN 3 WHEN ab.final_event = 'home_run' THEN 4 ELSE 0 END) AS total_bases,
                ROUND(AVG(ab.xwoba), 3) AS xwoba
            FROM at_bats ab
            INNER JOIN pitch_ab_team pat ON pat.at_bat_id = ab.at_bat_id
        ),
        filtered_pitches AS (
            SELECT p.*, {derived_pitcher_team_expr('p', 'g')} AS derived_pitcher_team
            FROM pitches p
            LEFT JOIN games g ON g.game_pk = p.game_pk
            WHERE {pitch_where}
              AND {derived_pitcher_team_expr('p', 'g')} = {sql_quote(team_code)}
        ),
        filtered_abs AS (
            SELECT DISTINCT CONCAT(game_pk, '_', at_bat_number) AS at_bat_key
            FROM filtered_pitches
        ),
        pitching_abs AS (
            SELECT ab.*
            FROM at_bats ab
            INNER JOIN filtered_abs fa ON fa.at_bat_key = ab.at_bat_id
        )
        SELECT
            {sql_quote(team_code)} AS team,
            (SELECT games FROM team_games) AS games,
            (SELECT pa FROM batting) AS pa,
            (SELECT at_bats FROM batting) AS at_bats,
            (SELECT hits FROM batting) AS hits,
            (SELECT hr FROM batting) AS hr,
            (SELECT walks FROM batting) AS walks,
            (SELECT strikeouts FROM batting) AS strikeouts,
            ROUND((SELECT hits::FLOAT / NULLIF(at_bats, 0) FROM batting), 3) AS avg,
            ROUND((SELECT (hits + walks)::FLOAT / NULLIF(pa, 0) FROM batting), 3) AS obp,
            ROUND((SELECT total_bases::FLOAT / NULLIF(at_bats, 0) FROM batting), 3) AS slg,
            (SELECT xwoba FROM batting) AS xwoba,
            (SELECT COUNT(*) FROM filtered_pitches) AS pitches,
            (SELECT COUNT(DISTINCT pitcher_id) FROM filtered_pitches) AS pitchers,
            (SELECT ROUND(AVG(release_speed), 1) FROM filtered_pitches) AS avg_velo,
            (SELECT ROUND(SUM(CASE WHEN description IN ('swinging_strike', 'swinging_strike_blocked') THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) FROM filtered_pitches) AS whiff_pct,
            (SELECT ROUND(AVG(xwoba), 3) FROM pitching_abs) AS xwoba_allowed,
            (SELECT ROUND(SUM(CASE WHEN final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) FROM pitching_abs) AS k_pct_allowed,
            (SELECT ROUND(SUM(CASE WHEN final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) FROM pitching_abs) AS bb_pct_allowed
        """
    ).df()
    if summary.empty or summary.iloc[0]["games"] in (None, 0):
        raise HTTPException(status_code=404, detail="Team not found")

    batter_name = player_name_expr("Batter", "ab.batter_id")
    batting_df = con.execute(
        f"""
        WITH pitch_ab_team AS (
            SELECT
                CONCAT(p.game_pk, '_', p.at_bat_number) AS at_bat_id,
                any_value({derived_batter_team_expr('p', 'g')}) AS batter_team
            FROM pitches p
            LEFT JOIN games g ON g.game_pk = p.game_pk
            WHERE {pitch_where}
              AND {derived_batter_team_expr('p', 'g')} = {sql_quote(team_code)}
            GROUP BY 1
        )
        SELECT
            ab.batter_id,
            {batter_name} AS batter_name,
            COUNT(DISTINCT ab.game_pk) AS g,
            COUNT(*) AS pa,
            SUM(CASE WHEN final_event NOT IN {AB_EXCLUDED_EVENTS} THEN 1 ELSE 0 END) AS ab_count,
            SUM(CASE WHEN final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END) AS h,
            SUM(CASE WHEN final_event = 'double' THEN 1 ELSE 0 END) AS doubles,
            SUM(CASE WHEN final_event = 'triple' THEN 1 ELSE 0 END) AS triples,
            SUM(CASE WHEN final_event = 'home_run' THEN 1 ELSE 0 END) AS hr,
            SUM(CASE WHEN final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END) AS bb,
            SUM(CASE WHEN final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END) AS so,
            ROUND(SUM(CASE WHEN final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(SUM(CASE WHEN final_event NOT IN {AB_EXCLUDED_EVENTS} THEN 1 ELSE 0 END), 0), 3) AS avg,
            ROUND((SUM(CASE WHEN final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END) + SUM(CASE WHEN final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END))::FLOAT / NULLIF(COUNT(*), 0), 3) AS obp,
            ROUND(SUM(CASE WHEN final_event = 'single' THEN 1 WHEN final_event = 'double' THEN 2 WHEN final_event = 'triple' THEN 3 WHEN final_event = 'home_run' THEN 4 ELSE 0 END)::FLOAT / NULLIF(SUM(CASE WHEN final_event NOT IN {AB_EXCLUDED_EVENTS} THEN 1 ELSE 0 END), 0), 3) AS slg,
            ROUND(AVG(xwoba), 3) AS xwoba
        FROM at_bats ab
        INNER JOIN pitch_ab_team pat ON pat.at_bat_id = ab.at_bat_id
        LEFT JOIN players pl ON pl.player_id = ab.batter_id
        WHERE {batting_where}
        GROUP BY ab.batter_id, {batter_name}
        HAVING COUNT(*) > 0
        ORDER BY pa DESC, batter_name
        LIMIT 25
        """
    ).df()

    pitcher_name = player_name_expr("Pitcher", "fp.pitcher_id")
    pitching_df = con.execute(
        f"""
        WITH filtered_pitches AS (
            SELECT p.*, {derived_pitcher_team_expr('p', 'g')} AS derived_pitcher_team
            FROM pitches p
            LEFT JOIN games g ON g.game_pk = p.game_pk
            WHERE {pitch_where}
              AND {derived_pitcher_team_expr('p', 'g')} = {sql_quote(team_code)}
        ),
        filtered_abs AS (
            SELECT DISTINCT
                CONCAT(game_pk, '_', at_bat_number) AS at_bat_key,
                pitcher_id
            FROM filtered_pitches
        ),
        pitcher_abs AS (
            SELECT ab.*, fa.pitcher_id
            FROM at_bats ab
            INNER JOIN filtered_abs fa ON fa.at_bat_key = ab.at_bat_id
        ),
        pitcher_abs_summary AS (
            SELECT
                pitcher_id,
                ROUND(AVG(xwoba), 3) AS xwoba_allowed,
                ROUND(SUM(CASE WHEN final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) AS k_pct,
                ROUND(SUM(CASE WHEN final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) AS bb_pct
            FROM pitcher_abs
            GROUP BY pitcher_id
        )
        SELECT
            fp.pitcher_id,
            {pitcher_name} AS pitcher_name,
            COUNT(DISTINCT fp.game_pk) AS g,
            COUNT(*) AS pitches,
            ROUND(AVG(fp.release_speed), 1) AS avg_velo,
            ROUND(AVG(fp.release_spin_rate), 0) AS avg_spin,
            ROUND(SUM(CASE WHEN fp.description IN ('swinging_strike', 'swinging_strike_blocked') THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) AS whiff_pct,
            any_value(pas.xwoba_allowed) AS xwoba_allowed,
            any_value(pas.k_pct) AS k_pct,
            any_value(pas.bb_pct) AS bb_pct
        FROM filtered_pitches fp
        LEFT JOIN players pl ON pl.player_id = fp.pitcher_id
        LEFT JOIN pitcher_abs_summary pas ON pas.pitcher_id = fp.pitcher_id
        GROUP BY fp.pitcher_id, {pitcher_name}
        ORDER BY pitches DESC, pitcher_name
        LIMIT 25
        """
    ).df()

    return {
        "summary": df_to_json(summary)[0],
        "batting": inject_player_names(df_to_json(batting_df), id_key="batter_id", name_key="batter_name"),
        "pitching": inject_player_names(df_to_json(pitching_df), id_key="pitcher_id", name_key="pitcher_name"),
    }


# ---------------------------------------------------------------------------
# Pitcher / Batter Existing Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/pitcher/{pitcher_id}/count-profile")
def api_pitcher_count_profile(
    pitcher_id: int,
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
):
    """Pitch mix by count for a given pitcher."""
    con = get_con()
    resolved = resolve_window(con, "season", season, season_start, season_end)
    game_types = season_type_to_game_types(season_type)
    df = pitcher_count_profile(
        con,
        pitcher_id,
        resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        game_types=game_types,
    )
    return df_to_json(df)


@app.get("/api/sequences")
def api_sequences(
    pitcher_id: Optional[int] = None,
    min_occurrences: int = Query(5, ge=1),
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
):
    con = get_con()
    resolved = resolve_window(con, "season", season, season_start, season_end)
    game_types = season_type_to_game_types(season_type)
    df = pitch_sequence_patterns(
        con,
        pitcher_id,
        min_occurrences,
        resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        game_types=game_types,
    )
    return df_to_json(df)


@app.get("/api/sequences/outcomes")
def api_sequence_outcomes(
    outcomes: str = Query(..., min_length=1),
    pitcher_id: Optional[int] = None,
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
):
    con = get_con()
    resolved = resolve_window(con, "season", season, season_start, season_end)
    game_types = season_type_to_game_types(season_type)
    parsed = [value.strip().lower() for value in outcomes.split(",") if value.strip()]
    for value in parsed:
        if value not in {"ball", "strike", "foul"}:
            raise HTTPException(status_code=400, detail="outcomes must contain only: ball, strike, foul")
    return outcome_sequence_summary(
        con,
        parsed,
        pitcher_id=pitcher_id,
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        start_date=resolved["start_date"],
        end_date=resolved["end_date"],
        game_types=game_types,
    )


@app.get("/api/pitcher/{pitcher_id}/sequences")
def api_pitcher_sequences(
    pitcher_id: int,
    min_occurrences: int = Query(5, ge=1),
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
):
    """Most common pitch-to-pitch sequences for a pitcher."""
    con = get_con()
    resolved = resolve_window(con, "season", season, season_start, season_end)
    game_types = season_type_to_game_types(season_type)
    df = pitch_sequence_patterns(
        con,
        pitcher_id,
        min_occurrences,
        resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        game_types=game_types,
    )
    return df_to_json(df)


@app.get("/api/pitcher/{pitcher_id}/splits")
def api_pitcher_splits(
    pitcher_id: int,
    split_by: str = "base_state",
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
):
    con = get_con()
    resolved = resolve_window(con, "season", season, season_start, season_end)
    game_types = season_type_to_game_types(season_type)
    df = situational_splits(
        con,
        pitcher_id,
        role="pitcher",
        split_by=split_by,
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        game_types=game_types,
    )
    return df_to_json(df)


@app.get("/api/batter/{batter_id}/splits")
def api_batter_splits(
    batter_id: int,
    split_by: str = "base_state",
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
):
    con = get_con()
    resolved = resolve_window(con, "season", season, season_start, season_end)
    game_types = season_type_to_game_types(season_type)
    df = situational_splits(
        con,
        batter_id,
        role="batter",
        split_by=split_by,
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        game_types=game_types,
    )
    return df_to_json(df)


# ---------------------------------------------------------------------------
# At-Bat Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/at-bat/{game_pk}/{at_bat_number}")
def api_at_bat_timeline(game_pk: int, at_bat_number: int):
    """Full pitch-by-pitch timeline of a single at-bat."""
    con = get_con()
    df = full_at_bat_timeline(con, game_pk, at_bat_number)
    if df.empty:
        raise HTTPException(status_code=404, detail="At-bat not found")
    return df_to_json(df)


# ---------------------------------------------------------------------------
# Leaderboard Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/leaderboard/batting")
def api_batting_leaderboard(
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    limit: int = Query(10, ge=1, le=100),
    min_pa: int = Query(10, ge=1),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
    window: str = Query("season", pattern="^(season|career|last7)$"),
    balls: Optional[int] = Query(None, ge=0, le=3),
    strikes: Optional[int] = Query(None, ge=0, le=2),
    outs: Optional[int] = Query(None, ge=0, le=2),
    stand: Optional[str] = None,
    p_throws: Optional[str] = None,
):
    """Top batters leaderboard for a given season or window."""
    con = get_con()
    resolved = resolve_window(con, window, season, season_start, season_end)
    game_types = season_type_to_game_types(season_type)

    if balls is None or strikes is None:
        if (
            window == "season"
            and resolved["season"] is not None
            and resolved["season_start"] == resolved["season_end"]
            and outs is None and stand is None and p_throws is None
            and season_type == "both"
        ):
            df = batting_leaderboard(con, resolved["season"], limit, min_pa, game_types=game_types)
            return inject_player_names(df_to_json(df), id_key="batter_id", name_key="batter_name")

        where_sql = build_filters(
            "ab",
            season=resolved["season"],
            season_start=resolved["season_start"],
            season_end=resolved["season_end"],
            start_date=resolved["start_date"],
            end_date=resolved["end_date"],
            outs_col="outs_at_start",
            outs=outs,
            stand=normalize_hand(stand),
            p_throws=normalize_hand(p_throws),
            game_types=game_types,
        )
        df = con.execute(
            f"""
            SELECT
                ab.batter_id,
                {player_name_expr('Batter', 'ab.batter_id')} AS batter_name,
                COUNT(*) AS pa,
                ROUND(AVG(ab.xwoba), 3) AS xwoba,
                ROUND(SUM(CASE WHEN ab.final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS k_pct,
                ROUND(SUM(CASE WHEN ab.final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS bb_pct,
                ROUND(SUM(CASE WHEN ab.final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS avg
            FROM at_bats ab
            LEFT JOIN players pl ON pl.player_id = ab.batter_id
            WHERE {where_sql}
            GROUP BY ab.batter_id, {player_name_expr('Batter', 'ab.batter_id')}
            HAVING COUNT(*) >= {min_pa}
            ORDER BY xwoba DESC
            LIMIT {limit}
            """
        ).df()
        return inject_player_names(df_to_json(df), id_key="batter_id", name_key="batter_name")

    pitch_where = build_filters(
        "p",
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        start_date=resolved["start_date"],
        end_date=resolved["end_date"],
        balls=balls,
        strikes=strikes,
        outs_col="outs_when_up",
        outs=outs,
        stand=normalize_hand(stand),
        p_throws=normalize_hand(p_throws),
        game_types=game_types,
    )

    df = con.execute(
        f"""
        WITH count_pas AS (
            SELECT DISTINCT
                CONCAT(p.game_pk, '_', p.at_bat_number) AS at_bat_key,
                p.batter_id
            FROM pitches p
            WHERE {pitch_where}
        )
        SELECT
            cp.batter_id,
            {player_name_expr('Batter', 'cp.batter_id')} AS batter_name,
            COUNT(*) AS pa,
            ROUND(AVG(ab.xwoba), 3) AS xwoba,
            ROUND(SUM(CASE WHEN ab.final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS k_pct,
            ROUND(SUM(CASE WHEN ab.final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS bb_pct,
            ROUND(SUM(CASE WHEN ab.final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS avg
        FROM count_pas cp
        INNER JOIN at_bats ab ON ab.at_bat_id = cp.at_bat_key
        LEFT JOIN players pl ON pl.player_id = cp.batter_id
        GROUP BY cp.batter_id, {player_name_expr('Batter', 'cp.batter_id')}
        HAVING COUNT(*) >= {min_pa}
        ORDER BY xwoba DESC
        LIMIT {limit}
        """
    ).df()
    return inject_player_names(df_to_json(df), id_key="batter_id", name_key="batter_name")


@app.get("/api/leaderboard/stuff")
def api_stuff_leaderboard(
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    pitch_type: Optional[str] = None,
    min_pitches: int = Query(20, ge=1),
    season_type: str = Query("both", pattern="^(regular|postseason|both)$"),
    window: str = Query("season", pattern="^(season|career|last7)$"),
    limit: int = Query(10, ge=1, le=100),
):
    """Pitch quality leaderboard proxied from whiff rate + velo + movement."""
    con = get_con()
    resolved = resolve_window(con, window, season, season_start, season_end)
    game_types = season_type_to_game_types(season_type)

    if window == "season" and resolved["season"] is not None and resolved["season_start"] == resolved["season_end"] and season_type == "both":
        df = stuff_plus_proxy(con, resolved["season"], pitch_type, min_pitches, game_types=game_types).head(limit)
        return inject_player_names(df_to_json(df), id_key="pitcher_id", name_key="pitcher_name")

    where_sql = build_filters(
        "p",
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        start_date=resolved["start_date"],
        end_date=resolved["end_date"],
        game_types=game_types,
        pitch_type=pitch_type,
    )
    df = con.execute(
        f"""
        SELECT
            p.pitcher_id,
            {player_name_expr('Pitcher', 'p.pitcher_id')} AS pitcher_name,
            p.pitch_type,
            COUNT(*) AS pitches,
            ROUND(AVG(p.release_speed), 1) AS avg_velo,
            ROUND(AVG(p.release_spin_rate), 0) AS avg_spin,
            ROUND(AVG(ABS(p.pfx_x)), 2) AS avg_h_break,
            ROUND(AVG(p.pfx_z), 2) AS avg_v_break,
            ROUND(SUM(CASE WHEN p.description IN ('swinging_strike', 'swinging_strike_blocked') THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS whiff_pct,
            ROUND(SUM(CASE WHEN p.description IN ('called_strike', 'swinging_strike', 'swinging_strike_blocked', 'foul', 'foul_tip') THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS csw_pct
        FROM pitches p
        LEFT JOIN players pl ON pl.player_id = p.pitcher_id
        WHERE {where_sql}
          AND p.pitch_type IS NOT NULL
        GROUP BY p.pitcher_id, {player_name_expr('Pitcher', 'p.pitcher_id')}, p.pitch_type
        HAVING COUNT(*) >= {min_pitches}
        ORDER BY whiff_pct DESC
        LIMIT {limit}
        """
    ).df()
    return inject_player_names(df_to_json(df), id_key="pitcher_id", name_key="pitcher_name")


# ---------------------------------------------------------------------------
# Legacy-compatible endpoint
# ---------------------------------------------------------------------------

@app.get("/api/count-state/outcome-matrix-legacy")
def api_outcome_matrix_legacy(season: Optional[int] = None):
    con = get_con()
    season = season or latest_data_context(con)["latest_season"]
    return df_to_json(at_bat_outcome_by_count(con, season=season))


# ---------------------------------------------------------------------------
# Admin — freshness check and background sync trigger
# ---------------------------------------------------------------------------

@app.get("/api/admin/freshness")
def api_admin_freshness():
    """Return the latest loaded game date for the current season and how stale it is."""
    con = get_con()
    today = date.today()
    current_season = today.year
    row = con.execute(
        "SELECT MAX(game_date) FROM pitches WHERE season = ?", [current_season]
    ).fetchone()
    latest_date = row[0] if row and row[0] else None
    if latest_date is None:
        days_stale = None
        up_to_date = False
    else:
        latest = latest_date if isinstance(latest_date, date) else date.fromisoformat(str(latest_date))
        days_stale = (today - latest).days
        up_to_date = days_stale == 0
    return {
        "season": current_season,
        "latest_game_date": str(latest_date) if latest_date else None,
        "today": str(today),
        "days_stale": days_stale,
        "up_to_date": up_to_date,
    }


def _run_auto_update_subprocess():
    """Spawn ETL auto-update as a subprocess (avoids DuckDB write/read conflict)."""
    with _SYNC_LOCK:
        if _SYNC_STATE["status"] == "running":
            return
        _SYNC_STATE.update({"status": "running", "started_at": datetime.utcnow().isoformat(), "error": None, "last_result": None})
    try:
        result = subprocess.run(
            [sys.executable, "etl/pipeline.py", "auto-update"],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        )
        with _SYNC_LOCK:
            if result.returncode == 0:
                _SYNC_STATE.update({"status": "done", "completed_at": datetime.utcnow().isoformat(), "last_result": result.stdout.strip()})
            else:
                _SYNC_STATE.update({"status": "error", "completed_at": datetime.utcnow().isoformat(), "error": result.stderr.strip()})
    except Exception as exc:
        with _SYNC_LOCK:
            _SYNC_STATE.update({"status": "error", "completed_at": datetime.utcnow().isoformat(), "error": str(exc)})


@app.post("/api/admin/sync")
def api_admin_sync():
    """Trigger a background auto-update (pull missing games, refresh Parquet). Non-blocking."""
    with _SYNC_LOCK:
        if _SYNC_STATE["status"] == "running":
            return {"started": False, "reason": "sync already running"}
    thread = threading.Thread(target=_run_auto_update_subprocess, daemon=True)
    thread.start()
    return {"started": True}


@app.get("/api/admin/sync-status")
def api_admin_sync_status():
    """Return the current state of the background sync job."""
    with _SYNC_LOCK:
        return dict(_SYNC_STATE)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.main:app", host="0.0.0.0", port=8000)
