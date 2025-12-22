from __future__ import annotations

import csv
import io
import sqlite3
from datetime import datetime
from typing import List, Optional

import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

DB_PATH = "tracker.db"

app = FastAPI(title="Paint Touch Tracker API")

default_origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]
extra_origins = [
    origin.strip()
    for origin in os.getenv("CORS_ORIGINS", "").split(",")
    if origin.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=default_origins + extra_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class GameIn(BaseModel):
    client_id: str
    name: str
    opponent: Optional[str] = ""
    game_date: str


class PossessionIn(BaseModel):
    client_id: str
    number: int
    quarter: int
    paint_touch: bool
    outcome: str
    points: Optional[int] = None
    notes: Optional[str] = ""
    timestamp: str


class SyncPayload(BaseModel):
    game: GameIn
    possessions: List[PossessionIn]


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                opponent TEXT,
                game_date TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS possessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id TEXT UNIQUE NOT NULL,
                game_id INTEGER NOT NULL,
                number INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                paint_touch INTEGER NOT NULL,
                outcome TEXT NOT NULL,
                points INTEGER,
                notes TEXT,
                timestamp TEXT NOT NULL,
                FOREIGN KEY(game_id) REFERENCES games(id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_possessions_game_id ON possessions(game_id)"
        )
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(possessions)").fetchall()
        }
        if "points" not in columns:
            conn.execute("ALTER TABLE possessions ADD COLUMN points INTEGER")


@app.on_event("startup")
def startup() -> None:
    init_db()


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.get("/games")
def list_games() -> List[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM games ORDER BY created_at DESC").fetchall()
    return [dict(row) for row in rows]


@app.post("/games")
def create_game(game: GameIn) -> dict:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT id FROM games WHERE client_id = ?", (game.client_id,)
        ).fetchone()
        if existing:
            return {"id": existing["id"], "created": False}

        conn.execute(
            """
            INSERT INTO games (client_id, name, opponent, game_date, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                game.client_id,
                game.name,
                game.opponent,
                game.game_date,
                datetime.utcnow().isoformat(),
            ),
        )
        game_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    return {"id": game_id, "created": True}


@app.post("/games/{game_id}/possessions")
def add_possessions(game_id: int, possessions: List[PossessionIn]) -> dict:
    if not possessions:
        return {"inserted": 0}

    with get_conn() as conn:
        game = conn.execute("SELECT id FROM games WHERE id = ?", (game_id,)).fetchone()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")

        inserted = 0
        for possession in possessions:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO possessions
                    (client_id, game_id, number, quarter, paint_touch, outcome, points, notes, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    possession.client_id,
                    game_id,
                    possession.number,
                    possession.quarter,
                    1 if possession.paint_touch else 0,
                    possession.outcome,
                    possession.points,
                    possession.notes,
                    possession.timestamp,
                ),
            )
            if cursor.rowcount:
                inserted += 1
    return {"inserted": inserted}


@app.post("/sync")
def sync(payload: SyncPayload) -> dict:
    game_response = create_game(payload.game)
    game_id = game_response["id"]
    result = add_possessions(game_id, payload.possessions)
    return {
        "game_id": game_id,
        "created": game_response["created"],
        "inserted_possessions": result["inserted"],
    }


@app.get("/games/{game_id}/possessions.csv")
def export_csv(game_id: int) -> StreamingResponse:
    with get_conn() as conn:
        game = conn.execute("SELECT * FROM games WHERE id = ?", (game_id,)).fetchone()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
        rows = conn.execute(
            """
            SELECT number, quarter, paint_touch, points, outcome, notes, timestamp
            FROM possessions
            WHERE game_id = ?
            ORDER BY number ASC
            """,
            (game_id,),
        ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "possession_number",
            "quarter",
            "paint_touch",
            "points",
            "outcome",
            "notes",
            "timestamp",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row["number"],
                row["quarter"],
                "yes" if row["paint_touch"] else "no",
                row["points"] if row["points"] is not None else "",
                row["outcome"],
                row["notes"] or "",
                row["timestamp"],
            ]
        )

    output.seek(0)
    filename = f"{game['name'].replace(' ', '_')}_{game['game_date']}.csv"
    headers = {"Content-Disposition": f"attachment; filename={filename}"}
    return StreamingResponse(output, media_type="text/csv", headers=headers)
