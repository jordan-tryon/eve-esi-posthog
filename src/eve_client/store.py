"""SQLite persistence layer for character snapshots."""

import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class SnapshotStore:
    def __init__(self, db_path: str = "eve_snapshots.db"):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                character_id     INTEGER NOT NULL,
                character_name   TEXT,
                corporation_id   INTEGER,
                captured_at      TEXT NOT NULL,
                wallet_balance   REAL,
                solar_system_id  INTEGER,
                system_name      TEXT,
                station_id       INTEGER,
                structure_id     INTEGER,
                ship_type_id     INTEGER,
                ship_name        TEXT,
                ship_group_id    INTEGER,
                security_status  REAL,
                total_sp         INTEGER,
                unallocated_sp   INTEGER,
                isk_hour         REAL,
                isk_hour_bounty  REAL,
                isk_hour_trade   REAL,
                isk_hour_industry REAL,
                isk_hour_other   REAL,
                risk_level       TEXT,
                activity_type    TEXT,
                recent_losses    INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS wallet_journal (
                journal_id   INTEGER PRIMARY KEY,
                character_id INTEGER NOT NULL,
                date         TEXT NOT NULL,
                ref_type     TEXT NOT NULL,
                amount       REAL NOT NULL,
                balance      REAL,
                description  TEXT
            );

            CREATE TABLE IF NOT EXISTS killmails (
                killmail_id     INTEGER PRIMARY KEY,
                character_id    INTEGER NOT NULL,
                killmail_hash   TEXT NOT NULL,
                kill_time       TEXT NOT NULL,
                is_loss         INTEGER NOT NULL DEFAULT 0,
                ship_type_id    INTEGER,
                solar_system_id INTEGER
            );

            CREATE TABLE IF NOT EXISTS skills (
                character_id  INTEGER NOT NULL,
                skill_id      INTEGER NOT NULL,
                skill_name    TEXT,
                trained_level INTEGER,
                active_level  INTEGER,
                skillpoints   INTEGER,
                updated_at    TEXT NOT NULL,
                PRIMARY KEY (character_id, skill_id)
            );
        """)
        self.conn.commit()

    # --- Snapshots ---

    def save_snapshot(self, snap: dict):
        cols = ", ".join(snap.keys())
        placeholders = ", ".join("?" * len(snap))
        self.conn.execute(
            f"INSERT INTO snapshots ({cols}) VALUES ({placeholders})",
            list(snap.values()),
        )
        self.conn.commit()

    def get_last_snapshot(self, character_id: int) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM snapshots WHERE character_id=? ORDER BY captured_at DESC LIMIT 1",
            (character_id,),
        ).fetchone()
        return dict(row) if row else None

    def get_snapshots(self, character_id: int, limit: int = 168) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM snapshots WHERE character_id=? ORDER BY captured_at DESC LIMIT ?",
            (character_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    # --- Wallet journal ---

    def save_journal_entries(self, character_id: int, entries: list[dict]):
        for e in entries:
            self.conn.execute(
                """INSERT OR IGNORE INTO wallet_journal
                   (journal_id, character_id, date, ref_type, amount, balance, description)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    e["id"],
                    character_id,
                    e["date"],
                    e["ref_type"],
                    e["amount"],
                    e.get("balance"),
                    e.get("description", ""),
                ),
            )
        self.conn.commit()

    def get_journal_entries_between(self, character_id: int, since: str, until: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM wallet_journal WHERE character_id=? AND date >= ? AND date < ? ORDER BY date",
            (character_id, since, until),
        ).fetchall()
        return [dict(r) for r in rows]

    # --- Killmails ---

    def get_unseen_killmail_ids(self, character_id: int, candidate_ids: list[int]) -> list[int]:
        if not candidate_ids:
            return []
        placeholders = ",".join("?" * len(candidate_ids))
        seen = {
            r[0]
            for r in self.conn.execute(
                f"SELECT killmail_id FROM killmails WHERE killmail_id IN ({placeholders})",
                candidate_ids,
            ).fetchall()
        }
        return [i for i in candidate_ids if i not in seen]

    def save_killmails(self, character_id: int, killmails: list[dict]):
        for km in killmails:
            self.conn.execute(
                """INSERT OR IGNORE INTO killmails
                   (killmail_id, character_id, killmail_hash, kill_time, is_loss, ship_type_id, solar_system_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    km["killmail_id"],
                    character_id,
                    km["killmail_hash"],
                    km["kill_time"],
                    km["is_loss"],
                    km.get("ship_type_id"),
                    km.get("solar_system_id"),
                ),
            )
        self.conn.commit()

    def get_recent_losses(self, character_id: int, since: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) FROM killmails WHERE character_id=? AND is_loss=1 AND kill_time >= ?",
            (character_id, since),
        ).fetchone()
        return row[0] if row else 0

    # --- Skills ---

    def save_skills(self, character_id: int, skills: list[dict]):
        now = _utcnow()
        for s in skills:
            self.conn.execute(
                """INSERT OR REPLACE INTO skills
                   (character_id, skill_id, skill_name, trained_level, active_level, skillpoints, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    character_id,
                    s["skill_id"],
                    s.get("skill_name"),
                    s["trained_skill_level"],
                    s["active_skill_level"],
                    s["skillpoints_in_skill"],
                    now,
                ),
            )
        self.conn.commit()

    def get_skills(self, character_id: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM skills WHERE character_id=? ORDER BY skill_name",
            (character_id,),
        ).fetchall()
        return [dict(r) for r in rows]
