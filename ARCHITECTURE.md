# Architecture — eve-esi-posthog

A multi-character EVE Online tracker. Pulls per-character data from CCP's ESI API
on a schedule, persists it to SQLite, streams events to PostHog, and serves a
server-rendered dashboard (FastAPI + Jinja2) with a few JS-driven chart tabs.

## Tech stack

- **Runtime**: Python 3.11+, managed with `uv` (`uv.lock`, `pyproject.toml`)
- **Web**: FastAPI + Uvicorn, Jinja2 templates, Tailwind via CDN (no build step)
- **Scheduling**: APScheduler `BackgroundScheduler`, in-process (per-app-instance, not distributed)
- **Storage**: SQLite (`eve_snapshots.db`), WAL mode, accessed directly via `sqlite3` — no ORM, no migration framework (ad hoc `ALTER TABLE` on boot)
- **External APIs**: EVE SSO (OAuth2 via `authlib`), EVE ESI (`esi.evetech.net`), PostHog (analytics ingestion)
- **Charts**: ApexCharts (character/trading pages), Chart.js (orphaned `dashboard.html` only — see Notable Issues)

## Entry points

| File | Purpose |
|---|---|
| `app.py` | The actual product: FastAPI web server + APScheduler-driven multi-character sync loop. Run via `python app.py` or `restart.sh`. |
| `main.py` | Legacy single-character CLI. Runs one-shot auth + `run_character_pipeline` (see `pipeline.py`). Not used by the web app. |
| `backfill.py` | Standalone script to reconstruct ~6 months of weekly ISK snapshots from wallet journal history. **Currently broken** against the current `EveAuth` API — see Notable Issues. |
| `restart.sh` | Zero-downtime-ish restart: SIGTERM the PID in `.app.pid`, wait up to 5s, SIGKILL if needed, relaunch `python app.py`, poll `:8080/` until it answers. |

## High-level data flow

```
                         EVE SSO (OAuth2)
                               │
                     ┌─────────▼─────────┐
  Browser  ───────▶  │   FastAPI (app.py) │  ───────▶  Jinja2 templates
                     │  + APScheduler      │            (server-rendered HTML)
                     └────┬──────────┬────┘
                          │          │
             every 10 min │          │ on-demand (API routes)
                          ▼          ▼
              ┌────────────────┐  ┌──────────────┐
              │ hourly_pipeline │  │ trading_pipeline │
              │ asset_tracker   │  │ (wallet txns,    │
              │ (session-only)  │  │  market orders)  │
              └───────┬────────┘  └──────┬───────────┘
                      │                  │
                      ▼                  ▼
                 ESIClient  ───────▶  esi.evetech.net
                      │
                      ▼
              SnapshotStore (SQLite, eve_snapshots.db)
                      │
                      ▼
                  Analytics (PostHog capture)
```

## Core components (`src/eve_client/`)

### `auth.py` — `EveAuth`
Multi-character EVE SSO OAuth2. `CHARACTER_SCOPES` lists ~20 ESI scopes requested
at authorization time (location, skills, wallet, clones, killmails, assets,
orders, industry, contracts, fittings).

- `get_auth_url()` — builds the SSO authorize URL.
- `exchange_code(code)` — exchanges an auth code for tokens, calls
  `/oauth/verify` to resolve `character_id`/`character_name`, persists via
  `store.save_token`. Returns `character_id`.
- `get_valid_token(character_id)` — returns a live access token, refreshing
  (via `_refresh`) if within 60s of expiry. Tokens are per-character and keyed
  off `character_id` throughout — this is what makes the app multi-character.

### `esi.py` — `ESIClient`
Thin wrapper over `httpx.Client` (persistent, connection-reused, thread-safe)
around `esi.evetech.net/latest`. One method per endpoint (location, ship,
skills, skillqueue, clones, implants, wallet, wallet journal, wallet
transactions, character orders + history, contracts, killmails, assets,
market prices/history/orders, universe names/types/groups/systems).

Notable behaviors:
- `get_wallet_journal_all`, `get_assets_all`, `get_contracts` follow ESI's
  `X-Pages` header to fetch every page.
- `get_wallet_transactions_all(since_id=...)` cursor-paginates backwards by
  `transaction_id` and stops once it reaches `since_id` — this is how
  `trading_pipeline` does incremental sync.
- `get_universe_names` batches ID→name resolution (max 1000/call per ESI limit).
- In-instance caches (`_type_cache`, `_group_cache`, `_system_cache`) — cheap
  memoization for the lifetime of one `ESIClient`, not shared across requests.
- `get_jita_sell_prices` fetches live Jita 4-4 sell-order book per type_id in
  parallel (`ThreadPoolExecutor`, 10 workers) — used by `asset_tracker` to
  value session gains/losses more accurately than the market-wide adjusted price.

### `hourly_pipeline.py` — `run_hourly_snapshot(character_id, auth, analytics, store)`
The main per-character sync, run every 10 minutes by the scheduler (see
"Scheduling" below — despite the module name, it runs every 10 min, not
hourly). Steps:

1. Fetches 8 ESI endpoints in parallel (`ThreadPoolExecutor(max_workers=9)`):
   public info, wallet balance, wallet journal (page 1 only), location, ship,
   skills, online status, all assets. Market prices are fetched too, but
   cached process-wide for 30 minutes (`_PRICES_CACHE`/`_PRICES_EXPIRES`
   module globals) since they change slowly and are expensive to refetch per
   character.
2. Resolves solar system info + ship type/group in a second small parallel batch.
3. Upserts the `characters` row.
4. **Session tracking**: diffs `last_login`/`last_logout` against the previous
   snapshot. A new login opens a `sessions` row (deduped against near-identical
   timestamps within 120s — ESI's `last_login` can jitter slightly between
   calls). A new logout closes the open session and computes ISK earned during
   it from the journal (excluding `ESCROW_RETURN_TYPES`). Also self-heals: if
   the character is offline and a `sessions` row is still open, it force-closes it.
5. **Killmails**: fetches recent killmails, diffs against already-seen IDs
   (`get_unseen_killmail_ids`), fetches full detail only for new ones, flags
   losses (`victim.character_id == character_id`), pushes a PostHog
   `ship_loss` event per loss.
6. Saves journal entries and skills; resolves and caches skill names/group
   names for any skill rows still missing them (incremental — only queries
   `WHERE skill_name IS NULL` / `group_name IS NULL`).
7. **Metrics**: computes `isk_hour` (wallet-balance-delta based — true net
   flow) plus a category breakdown (`isk_hour_bounty`, `_trade`, `_industry`,
   `_other`) from `compute_isk_rates` over the trailing 24h journal.
   `activity_type` comes from `detect_activity_type`, overridden by
   `detect_session_type(ship_group_id)` when the ship implies Mining or
   Exploration (income for those flows through market transactions or is
   invisible to the journal, so ship type is a more reliable signal).
8. **Estimated account value** = wallet balance + Σ(asset qty × market
   `average_price`, falling back to `adjusted_price`). **Wealth ISK/hr** = Δ
   estimated value between snapshots / elapsed hours (captures unrealized
   gains, not just wallet flow).
9. **At-risk value**: if the character is in space (no station/structure),
   sums the ship hull value + everything located directly on the ship
   (`location_id == ship_item_id`). Feeds `compute_risk_level`.
10. Saves the full snapshot row, pushes a PostHog `hourly_snapshot` event, flushes.

### `trading_pipeline.py` — `run_trading_sync(character_id, auth, store, esi)`
Runs immediately after `hourly_pipeline` in the scheduled job (see `app.py`
`_run_full_sync`), and separately on-demand via `/api/c/{id}/sync-trading`.

- Incrementally fetches new wallet transactions via
  `get_wallet_transactions_all(since_id=store.get_latest_transaction_id(...))`.
- Resolves unknown `type_id`s to names (batched, 1000/call).
- Fetches active + historical character market orders; historical order
  `state` is normalized to `cancelled`/`expired` only. Type names are resolved
  by first checking already-known names in `market_orders` and
  `wallet_transactions` (avoids redundant ESI calls), only hitting
  `universe/names` for genuinely unknown IDs.
- Saves everything via `store.save_transactions` / `store.save_orders`.

### `asset_tracker.py` — `run_asset_tracking(character_id, auth, store)`
Runs every 10 minutes (separate scheduler job from the hourly sync), but is a
no-op unless there's an open session (`store.get_open_session`) — it only
tracks item gain/loss *during active play*, not idle time.

- Aggregates on-ship inventory (`_aggregate_inventory`) by `type_id`, scoped
  to items whose location resolves back to the currently-flown ship
  (`ship_item_id`) via `INVENTORY_FLAGS` (cargo, drone bay, ore holds, etc.) —
  explicitly excludes station `Hangar` so pre-session stockpiles don't
  contaminate session P&L.
- Diffs against the last saved snapshot (`asset_items` table, capped at the
  most recent 20 snapshots per character) to get gained/lost quantities per type.
- Guards against false mass-loss events: if the new snapshot has <60% of the
  previous snapshot's *distinct type count*, it skips the delta for that tick
  (this is a schema/filter-transition artifact guard, not a real loss) and
  just re-baselines.
- Values gained/lost items at live Jita 4-4 sell price (falling back to the
  shared `hourly_pipeline` price cache or a fresh `adjusted_price` fetch), and
  accumulates the totals onto the open session's
  `assets_gained_value`/`assets_lost_value` columns.

### `metrics.py`
Pure functions, no I/O — the only module with meaningfully unit-testable logic.

- `compute_isk_rates` / `detect_activity_type` — classify journal entries into
  true ISK *faucets* only (`BOUNTY_TYPES`, `MISSION_TYPES`, `OPPORTUNITY_TYPES`).
  Trading (`TRADE_TYPES`) and industry (`INDUSTRY_TYPES`) are explicitly
  excluded as ISK *exchanges*, not creation. `ESCROW_RETURN_TYPES` filters out
  your own ISK coming back from cancelled/failed contracts so it doesn't
  double-count as income.
- `compute_risk_level` — LOW/MEDIUM/HIGH from security status, escalated for
  capital ships (`CAPITAL_GROUP_IDS`), expensive fits in highsec (gank risk,
  ≥60M ISK at risk), and repeated recent losses.
- `compute_trade_pnl` — FIFO-matched buy/sell P&L per `type_id` over a
  transaction window, with journal-derived fees (`brokers_fee` +
  `transaction_tax`) allocated proportionally by sell ISK.
- `compute_cancelled_order_losses` — surfaces cancelled/expired *sell* orders
  as capital that didn't convert to ISK.
- `detect_session_type` — classifies a session as Mining/Exploration/Combat
  from the ship's group ID at session start.

### `analytics.py` — `Analytics`
Thin wrapper over the `posthog` SDK. One `capture_*` method per event type
(`character_snapshot`, `character_skills`, `character_skill` [one per skill],
`character_wallet`, `session_start`, `session_end`, `ship_loss`,
`hourly_snapshot`), plus `identify_character` (PostHog `$identify` with
`$set` for name/corp/alliance/security status) and `flush()`. `distinct_id`
is always the EVE `character_id` as a string.

### `pipeline.py` — `run_character_pipeline`
Only used by the legacy CLI (`main.py`). Single-character, single-shot: pulls
public info, skills (+ name resolution, prints a table to stdout), wallet,
location, ship, and pushes them all to PostHog. Does **not** write to
`SnapshotStore` — this path is analytics-only and disconnected from the web
app's persistence/dashboard entirely.

### `store.py` — `SnapshotStore`
SQLite persistence, `sqlite3` directly (row factory = `sqlite3.Row`), WAL mode.
Schema is created with `CREATE TABLE IF NOT EXISTS` and evolved in `_migrate()`
via `ALTER TABLE ... ADD COLUMN` guarded by `PRAGMA table_info` checks — there
is no migration history/versioning, just idempotent-on-boot patching.

**Tables:**

| Table | Key columns | Notes |
|---|---|---|
| `snapshots` | `character_id, captured_at, wallet_balance, solar_system_id, station_id/structure_id, ship_type_id/name/group_id, security_status, total_sp, isk_hour(+_bounty/_trade/_industry/_other), risk_level, activity_type, recent_losses, online, last_login, last_logout, estimated_value, wealth_isk_hour, at_risk_value` | Core time-series table, one row per sync (~every 10 min/char). `estimated_value`, `wealth_isk_hour`, `at_risk_value` were added via migration. |
| `sessions` | `character_id, started_at, ended_at, ship_type_id/name, solar_system_id, system_name, security_status, isk_earned, assets_gained_value, assets_lost_value, session_type` | One row per play session, opened/closed by `hourly_pipeline`, enriched by `asset_tracker`. |
| `wallet_journal` | `journal_id (PK), character_id, date, ref_type, amount, balance, description` | Raw ESI wallet journal, append-only (`INSERT OR IGNORE`). |
| `wallet_transactions` | `transaction_id (PK), character_id, date, type_id, type_name, quantity, unit_price, is_buy, location_id, journal_ref_id` | Market buy/sell fills. |
| `market_orders` | `order_id (PK), character_id, type_id, type_name, region_id, location_id, is_buy_order, price, volume_remain/total, issued, state, synced_at` | Active + historical orders; `state` ∈ active/cancelled/expired. |
| `market_history` | `(type_id, region_id, date)` PK, `average, highest, lowest, volume, order_count` | Daily region market history for tracked items. |
| `tracked_items` | `(character_id, type_id, region_id)` PK, `type_name, added_at, history_synced_at` | User's watchlist for the trading page's market-history charts. |
| `killmails` | `killmail_id (PK), character_id, killmail_hash, kill_time, is_loss, ship_type_id, solar_system_id` | Both kills and losses; `is_loss` distinguishes. |
| `skills` | `(character_id, skill_id)` PK, `skill_name, trained_level, active_level, skillpoints, updated_at, group_name` | Upserted on every sync via `ON CONFLICT`. |
| `characters` | `character_id (PK), character_name, corporation_id, alliance_id, security_status, portrait_url, added_at, last_synced_at` | One row per tracked character — drives the landing page roster. |
| `character_tokens` | `character_id (PK), character_name, access_token, refresh_token, expires_at, registered_at` | OAuth token storage. **Tokens are stored in plaintext SQLite** — see Notable Issues. |
| `asset_items` | `(character_id, captured_at, type_id)` PK, `type_name, quantity` | On-ship inventory snapshots for `asset_tracker`'s delta logic; pruned to the last 20 snapshots/character. |
| `clone_cache` | `character_id (PK), implant_count, implant_value, training_json, fetched_at` | TTL cache (1h online / 24h offline) for the clone/implant API to reduce ESI calls. |

Indexes: `snapshots(character_id, captured_at)`, `wallet_journal(character_id,
date)`, `sessions(character_id, started_at)`, `killmails(character_id,
kill_time)`, `wallet_transactions(character_id, date)`,
`market_orders(character_id)`.

## `app.py` — web layer

### Scheduling
- `BackgroundScheduler` (APScheduler), started in the FastAPI `lifespan`
  context manager, one job pair per character:
  - `sync_{character_id}` → `_run_full_sync` → `run_hourly_snapshot` then
    `run_trading_sync`, every 10 minutes.
  - `assets_{character_id}` → `run_asset_tracking`, every 10 minutes
    (independent job, so it can no-op cheaply when there's no open session).
- `_make_sync_fn` throttles: if the character was offline on the last
  snapshot, the full sync only actually runs once ~55+ minutes have elapsed
  (saves ESI calls / rate limit budget for idle characters). Manual
  `POST /api/c/{id}/sync` bypasses this throttle.
- In-memory `_sync_jobs` / `_trading_sync_jobs` dicts track `{running, last,
  error}` per character for the status endpoints and UI spinners — **this
  state is process-local and lost on restart**, and won't work correctly
  behind multiple app instances (see Notable Issues).
- New characters get jobs registered immediately on OAuth callback
  (`/auth/callback` → `register_character_job(character_id, run_now=True)`).

### Routes

**Pages** (Jinja2, `HTMLResponse`):
- `GET /` — landing/roster page: all tracked characters + their latest
  snapshot, plus the 20 most recent losses across all characters.
- `GET /c/{character_id}` — character detail: current snapshot, last 168
  hourly snapshots, last 20 sessions, last 20 losses, sync status, skills.
  Tabs (client-side): Overview, Fitting, Ships, Skills, plus a Sessions view
  and a Day/Week/Month/Year range toggle on the ISK chart.
- `GET /skills/{character_id}` — skill list.
- `GET /trading/{character_id}` — trading dashboard. Tabs: Overview, Markets, Inventory.

**Auth:**
- `GET /auth/start` — redirects to EVE SSO.
- `GET /auth/callback`, `GET /callback` — OAuth callback, registers the
  character's scheduler jobs, redirects to `/c/{id}`.

**JSON API** (all under `/api/c/{character_id}/...` unless noted):
- `GET /api/characters`, `GET /current` — roster / latest snapshot.
- `GET /history?days&granularity` — snapshot history, optionally aggregated
  to day/month buckets (`_aggregate_snaps` averages the rate fields, keeps
  the last raw row's other fields per bucket).
- `GET /isk-timeline?hours` — bucketed ISK-earned/loot-value time series
  (`_build_isk_buckets`) plus derived timeline events (jumps, dock/undock,
  ship changes, login/logout — `_extract_timeline_events`) and per-session ISK
  breakdown (`_build_session_isk`). This is the richest computed endpoint;
  all of its logic lives in `app.py` rather than `metrics.py`.
- `GET /fitting` — current ship's fitted modules by slot group, valued at
  market price, fetched live from ESI (not cached/stored).
- `GET /optimal`, `GET /ships` — skill-readiness scoring for the current fit
  or every owned ship. Extracts skill requirements from ESI dogma attributes
  (`_extract_skill_reqs`), categorizes them into Navigation/Damage/Utility
  buckets by skill group (`_build_performance`), scores 0–100 per category
  against the character's trained levels. Also computed live, not cached.
- `POST /sync`, `GET /status` — manual full-sync trigger + polling status.
- `POST /sync-trading`, `GET /trading-status` — same, trading-only.
- `GET /escrow` — ISK locked in buy orders, courier collateral/rewards,
  auction bids — computed live from ESI orders + contracts.
- `GET /clone` — implant value + currently-training skill, TTL-cached (see
  `clone_cache` table above).
- `POST /track-item`, `DELETE /track-item/{type_id}`, `GET
  /market-history/{type_id}` — watchlist management for the trading page;
  fetches and stores 30-day market history per tracked item.
- `GET /trading-data` — P&L (`compute_trade_pnl`), cancelled-order losses,
  active orders, tracked items — the trading Overview tab's main payload.
- `GET /inventory` — all hangar (not fitted/cargo) assets grouped by
  location, with resolved names and estimated value. Resolves NPC station
  names and player structure names (per-structure ESI calls — see Notable
  Issues) separately from item type names.

## Notable issues / rough edges

- **`dashboard.html` is orphaned.** It exists in `templates/`, references
  Chart.js and `/api/history` + `/api/fitting`, but no route in `app.py`
  renders it. Either dead code from before the ApexCharts migration (see git
  log: "Migrate trading charts from Chart.js to ApexCharts") or a page that's
  meant to come back but currently doesn't.
- **`backfill.py` is likely broken against the current `EveAuth`.** It
  constructs `EveAuth(...)` with no `store=`, then calls `auth._token`
  (no such attribute exists on `EveAuth` — that was presumably from an older,
  single-character version) and `auth.get_valid_token()` with no
  `character_id` argument, but the current signature is
  `get_valid_token(self, character_id)`. Running this script as-is will raise.
- **`pipeline.py`/`main.py` is a disconnected legacy path.** It duplicates
  much of what `hourly_pipeline.py` does but only pushes to PostHog — never
  touches `SnapshotStore` — so it can't feed the dashboard. Likely predates
  the multi-character web app.
- **In-memory sync-status state.** `_sync_jobs`/`_trading_sync_jobs` in
  `app.py` are plain module-level dicts — fine for a single-process deploy,
  but they reset on restart (a sync that was "running" will just vanish) and
  wouldn't be correct if ever run with multiple worker processes/instances.
- **OAuth tokens stored in plaintext** in `character_tokens.access_token` /
  `refresh_token` — no encryption at rest. Acceptable for a single-user local
  app, worth flagging if this is ever exposed multi-tenant or on a shared host.
- **No tests.** `pytest`/`pytest-asyncio` are declared as dev dependencies in
  `pyproject.toml` but there's no `tests/` directory. `metrics.py` (pure
  functions) is the obvious first candidate.
- **No containerization/CI.** No `Dockerfile`, `docker-compose.yml`, or CI
  workflow in the repo. Deployment model is "run `restart.sh` on a host with
  Python + uv installed," tracked via a bare PID file (`.app.pid`).
- **No `SnapshotStore` schema versioning.** `_migrate()` is a hand-rolled,
  cumulative set of `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`-style checks —
  works fine at this scale, but has no down-migrations and no way to know
  "what version is this DB at" beyond reading the migration code itself.
- **Per-structure ESI calls in `/api/c/{id}/inventory`.** Player structure
  names are resolved one `GET /universe/structures/{id}/` call per unique
  structure, serially, each opening/closing its own `ESIClient` — fine for a
  handful of structures, would be slow with many.
