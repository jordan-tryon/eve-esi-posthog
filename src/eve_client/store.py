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
                recent_losses    INTEGER DEFAULT 0,
                online           INTEGER,
                last_login       TEXT,
                last_logout      TEXT
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                character_id     INTEGER NOT NULL,
                started_at       TEXT NOT NULL,
                ended_at         TEXT,
                ship_type_id     INTEGER,
                ship_name        TEXT,
                solar_system_id  INTEGER,
                system_name      TEXT,
                security_status  REAL,
                isk_earned       REAL
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

            CREATE TABLE IF NOT EXISTS character_tokens (
                character_id    INTEGER PRIMARY KEY,
                character_name  TEXT NOT NULL,
                access_token    TEXT NOT NULL,
                refresh_token   TEXT NOT NULL,
                expires_at      REAL NOT NULL,
                registered_at   TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS characters (
                character_id    INTEGER PRIMARY KEY,
                character_name  TEXT NOT NULL,
                corporation_id  INTEGER,
                alliance_id     INTEGER,
                security_status REAL,
                portrait_url    TEXT,
                added_at        TEXT NOT NULL,
                last_synced_at  TEXT
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

            CREATE TABLE IF NOT EXISTS wallet_transactions (
                transaction_id INTEGER PRIMARY KEY,
                character_id   INTEGER NOT NULL,
                date           TEXT NOT NULL,
                type_id        INTEGER NOT NULL,
                type_name      TEXT,
                quantity       INTEGER NOT NULL,
                unit_price     REAL NOT NULL,
                is_buy         INTEGER NOT NULL,
                location_id    INTEGER,
                journal_ref_id INTEGER
            );

            CREATE TABLE IF NOT EXISTS market_orders (
                order_id       INTEGER PRIMARY KEY,
                character_id   INTEGER NOT NULL,
                type_id        INTEGER NOT NULL,
                type_name      TEXT,
                region_id      INTEGER,
                location_id    INTEGER,
                is_buy_order   INTEGER NOT NULL,
                price          REAL NOT NULL,
                volume_remain  INTEGER NOT NULL,
                volume_total   INTEGER NOT NULL,
                issued         TEXT,
                state          TEXT NOT NULL,
                synced_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS market_history (
                type_id     INTEGER NOT NULL,
                region_id   INTEGER NOT NULL,
                date        TEXT NOT NULL,
                average     REAL,
                highest     REAL,
                lowest      REAL,
                volume      INTEGER,
                order_count INTEGER,
                PRIMARY KEY (type_id, region_id, date)
            );

            CREATE TABLE IF NOT EXISTS tracked_items (
                character_id INTEGER NOT NULL,
                type_id      INTEGER NOT NULL,
                type_name    TEXT,
                region_id    INTEGER NOT NULL DEFAULT 10000002,
                added_at     TEXT NOT NULL,
                PRIMARY KEY (character_id, type_id, region_id)
            );

            CREATE TABLE IF NOT EXISTS asset_items (
                character_id INTEGER NOT NULL,
                captured_at  TEXT NOT NULL,
                type_id      INTEGER NOT NULL,
                type_name    TEXT,
                quantity     INTEGER NOT NULL,
                PRIMARY KEY (character_id, captured_at, type_id)
            );

            CREATE TABLE IF NOT EXISTS clone_cache (
                character_id  INTEGER PRIMARY KEY,
                implant_count INTEGER,
                implant_value REAL,
                training_json TEXT,
                fetched_at    TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS industry_jobs (
                job_id                 INTEGER PRIMARY KEY,
                character_id           INTEGER NOT NULL,
                installer_id           INTEGER,
                activity_id            INTEGER NOT NULL,
                blueprint_id           INTEGER,
                blueprint_type_id      INTEGER,
                blueprint_location_id  INTEGER,
                output_location_id     INTEGER,
                facility_id            INTEGER,
                product_type_id        INTEGER,
                runs                   INTEGER,
                cost                   REAL,
                licensed_runs          INTEGER,
                probability            REAL,
                status                 TEXT NOT NULL,
                duration               INTEGER,
                start_date             TEXT,
                end_date               TEXT,
                pause_date             TEXT,
                completed_date         TEXT,
                completed_character_id INTEGER,
                successful_runs        INTEGER,
                synced_at              TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS blueprints (
                item_id             INTEGER PRIMARY KEY,
                character_id        INTEGER NOT NULL,
                type_id             INTEGER NOT NULL,
                type_name           TEXT,
                location_id         INTEGER,
                location_flag       TEXT,
                quantity            INTEGER,
                time_efficiency     INTEGER,
                material_efficiency INTEGER,
                runs                INTEGER,
                synced_at           TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS contracts (
                contract_id           INTEGER PRIMARY KEY,
                character_id          INTEGER NOT NULL,
                issuer_id             INTEGER,
                issuer_corporation_id INTEGER,
                assignee_id           INTEGER,
                acceptor_id           INTEGER,
                type                  TEXT,
                status                TEXT,
                title                 TEXT,
                for_corporation       INTEGER,
                availability          TEXT,
                price                 REAL,
                reward                REAL,
                collateral            REAL,
                buyout                REAL,
                volume                REAL,
                days_to_complete      INTEGER,
                start_location_id     INTEGER,
                end_location_id       INTEGER,
                date_issued           TEXT,
                date_expired          TEXT,
                date_accepted         TEXT,
                date_completed        TEXT,
                synced_at             TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS pi_colonies (
                character_id    INTEGER NOT NULL,
                planet_id       INTEGER NOT NULL,
                planet_name     TEXT,
                planet_type     TEXT,
                type_id         INTEGER,
                solar_system_id INTEGER,
                owner_id        INTEGER,
                upgrade_level   INTEGER,
                num_pins        INTEGER,
                last_update     TEXT,
                next_expiry     TEXT,
                fetched_at      TEXT NOT NULL,
                PRIMARY KEY (character_id, planet_id)
            );
        """)
        self.conn.commit()
        self._migrate()

    def _migrate(self):
        """Add columns that didn't exist in earlier schema versions."""
        existing = {row[1] for row in self.conn.execute("PRAGMA table_info(snapshots)").fetchall()}
        for col, typedef in [
            ("online",           "INTEGER"),
            ("last_login",       "TEXT"),
            ("last_logout",      "TEXT"),
            ("estimated_value",  "REAL"),
            ("wealth_isk_hour",  "REAL"),
            ("at_risk_value",    "REAL"),
        ]:
            if col not in existing:
                self.conn.execute(f"ALTER TABLE snapshots ADD COLUMN {col} {typedef}")

        skill_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(skills)").fetchall()}
        if "group_name" not in skill_cols:
            self.conn.execute("ALTER TABLE skills ADD COLUMN group_name TEXT")

        tracked_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(tracked_items)").fetchall()}
        if "type_name" not in tracked_cols:
            self.conn.execute("ALTER TABLE tracked_items ADD COLUMN type_name TEXT")
        if "history_synced_at" not in tracked_cols:
            self.conn.execute("ALTER TABLE tracked_items ADD COLUMN history_synced_at TEXT")

        session_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(sessions)").fetchall()}
        for col, typedef in [
            ("assets_gained_value", "REAL"),
            ("assets_lost_value", "REAL"),
            ("session_type", "TEXT"),
        ]:
            if col not in session_cols:
                self.conn.execute(f"ALTER TABLE sessions ADD COLUMN {col} {typedef}")

        # Performance indexes
        self.conn.executescript("""
            CREATE INDEX IF NOT EXISTS idx_snapshots_char_time
                ON snapshots(character_id, captured_at);
            CREATE INDEX IF NOT EXISTS idx_journal_char_date
                ON wallet_journal(character_id, date);
            CREATE INDEX IF NOT EXISTS idx_sessions_char_time
                ON sessions(character_id, started_at);
            CREATE INDEX IF NOT EXISTS idx_killmails_char_time
                ON killmails(character_id, kill_time);
            CREATE INDEX IF NOT EXISTS idx_transactions_char_date
                ON wallet_transactions(character_id, date);
            CREATE INDEX IF NOT EXISTS idx_orders_char
                ON market_orders(character_id);
            CREATE INDEX IF NOT EXISTS idx_industry_jobs_char_end
                ON industry_jobs(character_id, end_date);
            CREATE INDEX IF NOT EXISTS idx_industry_jobs_char_status
                ON industry_jobs(character_id, status);
            CREATE INDEX IF NOT EXISTS idx_blueprints_char
                ON blueprints(character_id);
            CREATE INDEX IF NOT EXISTS idx_contracts_char_status
                ON contracts(character_id, status);
            CREATE INDEX IF NOT EXISTS idx_contracts_char_issued
                ON contracts(character_id, date_issued);
            CREATE INDEX IF NOT EXISTS idx_pi_colonies_char
                ON pi_colonies(character_id);
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

    def get_snapshots_since(self, character_id: int, since: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM snapshots WHERE character_id=? AND captured_at >= ? ORDER BY captured_at DESC",
            (character_id, since),
        ).fetchall()
        return [dict(r) for r in rows]

    # --- Wallet journal ---

    def save_journal_entries(self, character_id: int, entries: list[dict]):
        self.conn.executemany(
            """INSERT OR IGNORE INTO wallet_journal
               (journal_id, character_id, date, ref_type, amount, balance, description)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [
                (e["id"], character_id, e["date"], e["ref_type"],
                 e["amount"], e.get("balance"), e.get("description", ""))
                for e in entries
            ],
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
        self.conn.executemany(
            """INSERT INTO skills
               (character_id, skill_id, trained_level, active_level, skillpoints, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(character_id, skill_id) DO UPDATE SET
                 trained_level = excluded.trained_level,
                 active_level  = excluded.active_level,
                 skillpoints   = excluded.skillpoints,
                 updated_at    = excluded.updated_at""",
            [
                (character_id, s["skill_id"], s["trained_skill_level"],
                 s["active_skill_level"], s["skillpoints_in_skill"], now)
                for s in skills
            ],
        )
        self.conn.commit()

    def get_skills(self, character_id: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM skills WHERE character_id=? ORDER BY group_name, skill_name",
            (character_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_skills_map(self, character_id: int) -> dict:
        """Returns {skill_id: trained_level} for fast lookup."""
        rows = self.conn.execute(
            "SELECT skill_id, trained_level FROM skills WHERE character_id=?",
            (character_id,)
        ).fetchall()
        return {r[0]: r[1] for r in rows}

    def update_skill_names(self, character_id: int, names: dict):
        """Update skill_name for known skill_ids. names = {skill_id: name}"""
        self.conn.executemany(
            "UPDATE skills SET skill_name=? WHERE character_id=? AND skill_id=?",
            [(name, character_id, skill_id) for skill_id, name in names.items()],
        )
        self.conn.commit()

    def update_skill_groups(self, character_id: int, groups: dict):
        """Update group_name for known skill_ids. groups = {skill_id: group_name}"""
        self.conn.executemany(
            "UPDATE skills SET group_name=? WHERE character_id=? AND skill_id=?",
            [(gname, character_id, skill_id) for skill_id, gname in groups.items()],
        )
        self.conn.commit()

    # --- Tokens ---

    def save_token(self, character_id: int, character_name: str, token: dict):
        now = _utcnow()
        self.conn.execute(
            """INSERT OR REPLACE INTO character_tokens
               (character_id, character_name, access_token, refresh_token, expires_at, registered_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (character_id, character_name, token["access_token"],
             token["refresh_token"], token.get("expires_at", 0), now),
        )
        self.conn.execute(
            """INSERT OR IGNORE INTO characters
               (character_id, character_name, added_at)
               VALUES (?, ?, ?)""",
            (character_id, character_name, now),
        )
        self.conn.commit()

    def update_token(self, character_id: int, token: dict) -> dict:
        self.conn.execute(
            """UPDATE character_tokens
               SET access_token=?, refresh_token=?, expires_at=?
               WHERE character_id=?""",
            (token["access_token"], token["refresh_token"],
             token.get("expires_at", 0), character_id),
        )
        self.conn.commit()
        return token

    def get_token(self, character_id: int) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM character_tokens WHERE character_id=?", (character_id,)
        ).fetchone()
        return dict(row) if row else None

    # --- Clone cache ---

    def get_clone_cache(self, character_id: int) -> dict | None:
        row = self.conn.execute(
            "SELECT implant_count, implant_value, training_json, fetched_at FROM clone_cache WHERE character_id=?",
            (character_id,)
        ).fetchone()
        if not row:
            return None
        import json
        return {
            "implant_count": row[0],
            "implant_value": row[1],
            "training":      json.loads(row[2]) if row[2] else None,
            "fetched_at":    row[3],
        }

    def set_clone_cache(self, character_id: int, implant_count: int, implant_value: float, training: dict | None):
        import json
        from datetime import datetime, timezone
        self.conn.execute(
            """INSERT OR REPLACE INTO clone_cache (character_id, implant_count, implant_value, training_json, fetched_at)
               VALUES (?, ?, ?, ?, ?)""",
            (character_id, implant_count, implant_value,
             json.dumps(training) if training else None,
             datetime.now(timezone.utc).isoformat())
        )
        self.conn.commit()

    def get_all_character_ids(self) -> list[int]:
        rows = self.conn.execute("SELECT character_id FROM character_tokens").fetchall()
        return [r[0] for r in rows]

    def upsert_character(self, character_id: int, info: dict):
        self.conn.execute(
            """INSERT OR REPLACE INTO characters
               (character_id, character_name, corporation_id, alliance_id,
                security_status, portrait_url, added_at, last_synced_at)
               VALUES (?, ?, ?, ?, ?, ?, COALESCE(
                   (SELECT added_at FROM characters WHERE character_id=?), ?
               ), ?)""",
            (character_id, info.get("name"), info.get("corporation_id"),
             info.get("alliance_id"), info.get("security_status"),
             f"https://images.evetech.net/characters/{character_id}/portrait?size=128",
             character_id, _utcnow(), _utcnow()),
        )
        self.conn.commit()

    def get_all_characters(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM characters ORDER BY last_synced_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_character(self, character_id: int) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM characters WHERE character_id=?", (character_id,)
        ).fetchone()
        return dict(row) if row else None

    # --- Sessions ---

    def save_session_start(self, character_id: int, started_at: str, ship_type_id: int | None,
                           ship_name: str | None, solar_system_id: int | None,
                           system_name: str | None, security_status: float | None,
                           session_type: str | None = None) -> int:
        existing = self.conn.execute(
            "SELECT id FROM sessions WHERE character_id=? AND started_at=?",
            (character_id, started_at),
        ).fetchone()
        if existing:
            return existing[0]
        cur = self.conn.execute(
            """INSERT INTO sessions (character_id, started_at, ship_type_id, ship_name,
               solar_system_id, system_name, security_status, session_type)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (character_id, started_at, ship_type_id, ship_name,
             solar_system_id, system_name, security_status, session_type),
        )
        self.conn.commit()
        return cur.lastrowid

    def close_session(self, character_id: int, started_at: str, ended_at: str, isk_earned: float):
        # Close the matched session with computed ISK
        self.conn.execute(
            """UPDATE sessions SET ended_at=?, isk_earned=?
               WHERE character_id=? AND started_at=? AND ended_at IS NULL""",
            (ended_at, isk_earned, character_id, started_at),
        )
        # Close any other stale open sessions for this character
        self.conn.execute(
            """UPDATE sessions SET ended_at=?, isk_earned=0
               WHERE character_id=? AND ended_at IS NULL""",
            (ended_at, character_id),
        )
        self.conn.commit()

    def get_open_session(self, character_id: int) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM sessions WHERE character_id=? AND ended_at IS NULL ORDER BY started_at DESC LIMIT 1",
            (character_id,),
        ).fetchone()
        return dict(row) if row else None

    def save_asset_snapshot(self, character_id: int, captured_at: str,
                            aggregated: dict[int, int], names: dict[int, str] | None = None):
        """Save a full aggregated asset snapshot (type_id → quantity)."""
        names = names or {}
        self.conn.executemany(
            """INSERT OR REPLACE INTO asset_items (character_id, captured_at, type_id, type_name, quantity)
               VALUES (?, ?, ?, ?, ?)""",
            [(character_id, captured_at, tid, names.get(tid), qty)
             for tid, qty in aggregated.items()],
        )
        # Keep only the last 20 snapshots per character to avoid unbounded growth
        self.conn.execute(
            """DELETE FROM asset_items WHERE character_id=? AND captured_at NOT IN (
               SELECT DISTINCT captured_at FROM asset_items
               WHERE character_id=? ORDER BY captured_at DESC LIMIT 20)""",
            (character_id, character_id),
        )
        self.conn.commit()

    def get_last_asset_snapshot(self, character_id: int) -> dict[int, int] | None:
        """Return the most recent aggregated inventory as {type_id: quantity}."""
        row = self.conn.execute(
            "SELECT captured_at FROM asset_items WHERE character_id=? ORDER BY captured_at DESC LIMIT 1",
            (character_id,),
        ).fetchone()
        if not row:
            return None
        rows = self.conn.execute(
            "SELECT type_id, quantity FROM asset_items WHERE character_id=? AND captured_at=?",
            (character_id, row[0]),
        ).fetchall()
        return {r[0]: r[1] for r in rows}

    def accumulate_session_asset_values(self, character_id: int, started_at: str,
                                         gained: float, lost: float):
        """Add to the running asset gained/lost totals for an open session."""
        self.conn.execute(
            """UPDATE sessions
               SET assets_gained_value = COALESCE(assets_gained_value, 0) + ?,
                   assets_lost_value   = COALESCE(assets_lost_value, 0)   + ?
               WHERE character_id=? AND started_at=? AND ended_at IS NULL""",
            (gained, lost, character_id, started_at),
        )
        self.conn.commit()

    def get_sessions(self, character_id: int, limit: int = 10) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM sessions WHERE character_id=? ORDER BY started_at DESC LIMIT ?",
            (character_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    # --- Wallet transactions ---

    def save_transactions(self, entries: list[dict]):
        for e in entries:
            self.conn.execute(
                """INSERT OR REPLACE INTO wallet_transactions
                   (transaction_id, character_id, date, type_id, type_name,
                    quantity, unit_price, is_buy, location_id, journal_ref_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    e["transaction_id"],
                    e["character_id"],
                    e["date"],
                    e["type_id"],
                    e.get("type_name"),
                    e["quantity"],
                    e["unit_price"],
                    1 if e.get("is_buy") else 0,
                    e.get("location_id"),
                    e.get("journal_ref_id"),
                ),
            )
        self.conn.commit()

    def get_transactions(self, character_id: int, days: int = 30) -> list[dict]:
        from datetime import timedelta
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = self.conn.execute(
            """SELECT * FROM wallet_transactions
               WHERE character_id=? AND date >= ?
               ORDER BY date DESC""",
            (character_id, since),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_latest_transaction_id(self, character_id: int) -> int | None:
        row = self.conn.execute(
            "SELECT MAX(transaction_id) FROM wallet_transactions WHERE character_id=?",
            (character_id,),
        ).fetchone()
        return row[0] if row else None

    # --- Market orders ---

    def save_orders(self, orders: list[dict]):
        now = _utcnow()
        for o in orders:
            self.conn.execute(
                """INSERT OR REPLACE INTO market_orders
                   (order_id, character_id, type_id, type_name, region_id,
                    location_id, is_buy_order, price, volume_remain, volume_total,
                    issued, state, synced_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    o["order_id"],
                    o["character_id"],
                    o["type_id"],
                    o.get("type_name"),
                    o.get("region_id"),
                    o.get("location_id"),
                    1 if o.get("is_buy_order") else 0,
                    o["price"],
                    o["volume_remain"],
                    o["volume_total"],
                    o.get("issued"),
                    o["state"],
                    now,
                ),
            )
        self.conn.commit()

    def get_orders(self, character_id: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM market_orders WHERE character_id=? ORDER BY issued DESC",
            (character_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    # --- Market history ---

    def clear_market_history(self, type_id: int, region_id: int):
        self.conn.execute(
            "DELETE FROM market_history WHERE type_id=? AND region_id=?",
            (type_id, region_id),
        )
        self.conn.commit()

    def save_market_history(self, rows: list[dict]):
        for r in rows:
            self.conn.execute(
                """INSERT OR IGNORE INTO market_history
                   (type_id, region_id, date, average, highest, lowest, volume, order_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    r["type_id"],
                    r["region_id"],
                    r["date"],
                    r.get("average"),
                    r.get("highest"),
                    r.get("lowest"),
                    r.get("volume"),
                    r.get("order_count"),
                ),
            )
        self.conn.commit()

    def get_market_history(self, type_id: int, region_id: int, days: int = 30) -> list[dict]:
        from datetime import timedelta
        since = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
        rows = self.conn.execute(
            """SELECT * FROM market_history
               WHERE type_id=? AND region_id=? AND date >= ?
               ORDER BY date ASC""",
            (type_id, region_id, since),
        ).fetchall()
        return [dict(r) for r in rows]

    # --- Tracked items ---

    def add_tracked_item(self, character_id: int, type_id: int, region_id: int = 10000002, type_name: str | None = None):
        self.conn.execute(
            """INSERT INTO tracked_items (character_id, type_id, type_name, region_id, added_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(character_id, type_id, region_id) DO UPDATE SET type_name=excluded.type_name""",
            (character_id, type_id, type_name, region_id, _utcnow()),
        )
        self.conn.commit()

    def remove_tracked_item(self, character_id: int, type_id: int, region_id: int | None = None):
        if region_id is not None:
            self.conn.execute(
                "DELETE FROM tracked_items WHERE character_id=? AND type_id=? AND region_id=?",
                (character_id, type_id, region_id),
            )
        else:
            self.conn.execute(
                "DELETE FROM tracked_items WHERE character_id=? AND type_id=?",
                (character_id, type_id),
            )
        self.conn.commit()

    def set_history_synced_at(self, character_id: int, type_id: int, region_id: int, ts: str):
        self.conn.execute(
            "UPDATE tracked_items SET history_synced_at=? WHERE character_id=? AND type_id=? AND region_id=?",
            (ts, character_id, type_id, region_id),
        )
        self.conn.commit()

    def get_tracked_items(self, character_id: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM tracked_items WHERE character_id=? ORDER BY added_at DESC",
            (character_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    # --- Industry jobs ---

    def save_industry_jobs(self, jobs: list[dict]):
        for j in jobs:
            self.conn.execute(
                """INSERT OR REPLACE INTO industry_jobs
                   (job_id, character_id, installer_id, activity_id, blueprint_id,
                    blueprint_type_id, blueprint_location_id, output_location_id,
                    facility_id, product_type_id, runs, cost, licensed_runs,
                    probability, status, duration, start_date, end_date, pause_date,
                    completed_date, completed_character_id, successful_runs, synced_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    j["job_id"],
                    j["character_id"],
                    j.get("installer_id"),
                    j["activity_id"],
                    j.get("blueprint_id"),
                    j.get("blueprint_type_id"),
                    j.get("blueprint_location_id"),
                    j.get("output_location_id"),
                    j.get("facility_id"),
                    j.get("product_type_id"),
                    j.get("runs"),
                    j.get("cost"),
                    j.get("licensed_runs"),
                    j.get("probability"),
                    j["status"],
                    j.get("duration"),
                    j.get("start_date"),
                    j.get("end_date"),
                    j.get("pause_date"),
                    j.get("completed_date"),
                    j.get("completed_character_id"),
                    j.get("successful_runs"),
                    j.get("synced_at") or _utcnow(),
                ),
            )
        self.conn.commit()

    def get_industry_jobs(self, character_id: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM industry_jobs WHERE character_id=? ORDER BY end_date DESC",
            (character_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    # --- Blueprints ---

    def save_blueprints(self, character_id: int, blueprints: list[dict]):
        """Wholesale replace — a blueprint that's no longer owned (sold, moved to
        another character) should disappear, not linger as a stale row."""
        now = _utcnow()
        self.conn.execute("DELETE FROM blueprints WHERE character_id=?", (character_id,))
        for b in blueprints:
            self.conn.execute(
                """INSERT OR REPLACE INTO blueprints
                   (item_id, character_id, type_id, type_name, location_id,
                    location_flag, quantity, time_efficiency, material_efficiency,
                    runs, synced_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    b["item_id"],
                    character_id,
                    b["type_id"],
                    b.get("type_name"),
                    b.get("location_id"),
                    b.get("location_flag"),
                    b.get("quantity"),
                    b.get("time_efficiency"),
                    b.get("material_efficiency"),
                    b.get("runs"),
                    now,
                ),
            )
        self.conn.commit()

    def get_blueprints(self, character_id: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM blueprints WHERE character_id=? ORDER BY type_name",
            (character_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    # --- Contracts ---

    def save_contracts(self, contracts: list[dict]):
        for c in contracts:
            self.conn.execute(
                """INSERT OR REPLACE INTO contracts
                   (contract_id, character_id, issuer_id, issuer_corporation_id,
                    assignee_id, acceptor_id, type, status, title, for_corporation,
                    availability, price, reward, collateral, buyout, volume,
                    days_to_complete, start_location_id, end_location_id,
                    date_issued, date_expired, date_accepted, date_completed, synced_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    c["contract_id"],
                    c["character_id"],
                    c.get("issuer_id"),
                    c.get("issuer_corporation_id"),
                    c.get("assignee_id"),
                    c.get("acceptor_id"),
                    c.get("type"),
                    c.get("status"),
                    c.get("title"),
                    1 if c.get("for_corporation") else 0,
                    c.get("availability"),
                    c.get("price"),
                    c.get("reward"),
                    c.get("collateral"),
                    c.get("buyout"),
                    c.get("volume"),
                    c.get("days_to_complete"),
                    c.get("start_location_id"),
                    c.get("end_location_id"),
                    c.get("date_issued"),
                    c.get("date_expired"),
                    c.get("date_accepted"),
                    c.get("date_completed"),
                    c.get("synced_at") or _utcnow(),
                ),
            )
        self.conn.commit()

    def get_contracts(self, character_id: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM contracts WHERE character_id=? ORDER BY date_issued DESC",
            (character_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    # --- PI colonies ---

    def save_pi_colonies(self, character_id: int, colonies: list[dict]):
        """Wholesale replace — an abandoned/departed colony should disappear."""
        now = _utcnow()
        self.conn.execute("DELETE FROM pi_colonies WHERE character_id=?", (character_id,))
        for p in colonies:
            self.conn.execute(
                """INSERT OR REPLACE INTO pi_colonies
                   (character_id, planet_id, planet_name, planet_type, type_id,
                    solar_system_id, owner_id, upgrade_level, num_pins, last_update,
                    next_expiry, fetched_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    character_id,
                    p["planet_id"],
                    p.get("planet_name"),
                    p.get("planet_type"),
                    p.get("type_id"),
                    p.get("solar_system_id"),
                    p.get("owner_id"),
                    p.get("upgrade_level"),
                    p.get("num_pins"),
                    p.get("last_update"),
                    p.get("next_expiry"),
                    now,
                ),
            )
        self.conn.commit()

    def get_pi_colonies(self, character_id: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM pi_colonies WHERE character_id=? ORDER BY next_expiry ASC",
            (character_id,),
        ).fetchall()
        return [dict(r) for r in rows]
