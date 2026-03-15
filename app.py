"""Combined web server + hourly scheduler entry point."""

import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

load_dotenv()

from src.eve_client.analytics import Analytics
from src.eve_client.auth import EveAuth
from src.eve_client.esi import ESIClient
from src.eve_client.hourly_pipeline import run_hourly_snapshot
from src.eve_client.store import SnapshotStore

# ── Globals ──────────────────────────────────────────────────────────────────

CHARACTER_ID = int(os.environ["EVE_CHARACTER_ID"])

auth = EveAuth(
    client_id=os.environ["EVE_CLIENT_ID"],
    client_secret=os.environ["EVE_CLIENT_SECRET"],
    callback_url=os.environ["EVE_CALLBACK_URL"],
)
analytics = Analytics(
    api_key=os.environ["POSTHOG_API_KEY"],
    host=os.environ.get("POSTHOG_HOST", "https://us.i.posthog.com"),
)
store = SnapshotStore("eve_snapshots.db")
scheduler = BackgroundScheduler()
_sync_lock = threading.Lock()
_sync_status: dict = {"running": False, "last": None, "error": None}

# ── Scheduler ────────────────────────────────────────────────────────────────

def _scheduled_sync():
    if not auth._token:
        print("[scheduler] No token — skipping sync.")
        return
    with _sync_lock:
        _sync_status["running"] = True
        _sync_status["error"] = None
    try:
        run_hourly_snapshot(CHARACTER_ID, auth, analytics, store)
        _sync_status["last"] = datetime.now(timezone.utc).isoformat()
    except Exception as e:
        _sync_status["error"] = str(e)
        print(f"[scheduler] Error: {e}")
    finally:
        _sync_status["running"] = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    if auth._token:
        scheduler.add_job(
            _scheduled_sync,
            trigger=IntervalTrigger(hours=1),
            id="hourly_sync",
            next_run_time=datetime.now(timezone.utc),
        )
        scheduler.start()
        print("[app] Scheduler started — first sync running now.")
    else:
        print("[app] No EVE token found. Visit http://localhost:8000/auth/start to authenticate.")
    yield
    if scheduler.running:
        scheduler.shutdown(wait=False)


# ── FastAPI ───────────────────────────────────────────────────────────────────

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")


def _is_authed() -> bool:
    return auth._token is not None


# ── Auth routes ───────────────────────────────────────────────────────────────

@app.get("/auth/start")
def auth_start():
    return RedirectResponse(auth.get_auth_url())


@app.get("/auth/callback")
def auth_callback(code: str):
    auth.exchange_code(code)
    # Start scheduler now that we have a token
    if not scheduler.running:
        scheduler.add_job(
            _scheduled_sync,
            trigger=IntervalTrigger(hours=1),
            id="hourly_sync",
            next_run_time=datetime.now(timezone.utc),
        )
        scheduler.start()
    return RedirectResponse("/")


# ── Page routes ───────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    if not _is_authed():
        return templates.TemplateResponse("auth.html", {"request": request})
    current = store.get_last_snapshot(CHARACTER_ID) or {}
    snapshots = store.get_snapshots(CHARACTER_ID, limit=168)
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "current": current,
        "snapshots": snapshots,
        "character_id": CHARACTER_ID,
        "sync_status": _sync_status,
    })


@app.get("/skills", response_class=HTMLResponse)
def skills_page(request: Request):
    if not _is_authed():
        return RedirectResponse("/auth/start")
    current = store.get_last_snapshot(CHARACTER_ID) or {}
    skill_rows = store.get_skills(CHARACTER_ID)
    return templates.TemplateResponse("skills.html", {
        "request": request,
        "current": current,
        "skills": skill_rows,
        "character_id": CHARACTER_ID,
    })


# ── API routes ────────────────────────────────────────────────────────────────

@app.get("/api/current")
def api_current():
    return store.get_last_snapshot(CHARACTER_ID) or {}


@app.get("/api/history")
def api_history(hours: int = 168):
    snapshots = store.get_snapshots(CHARACTER_ID, limit=hours)
    snapshots.reverse()  # chronological for charts
    return snapshots


@app.post("/api/sync")
def api_sync(background_tasks: BackgroundTasks):
    if not _is_authed():
        return JSONResponse({"error": "not authenticated"}, status_code=401)
    if _sync_status["running"]:
        return JSONResponse({"status": "already_running"})
    background_tasks.add_task(_scheduled_sync)
    return JSONResponse({"status": "started"})


@app.get("/api/status")
def api_status():
    return _sync_status


@app.get("/api/fitting")
def api_fitting():
    if not _is_authed():
        return JSONResponse({"error": "not authenticated"}, status_code=401)
    from collections import defaultdict
    token = auth.get_valid_token()
    esi = ESIClient(access_token=token)

    ship = esi.get_ship(CHARACTER_ID)
    ship_item_id = ship["ship_item_id"]
    type_info = esi.get_type_info(ship["ship_type_id"])

    all_assets = esi.get_assets_all(CHARACTER_ID)
    fitted = [a for a in all_assets if a.get("location_id") == ship_item_id]

    type_ids = list({a["type_id"] for a in fitted})
    names = {
        e["id"]: e["name"]
        for e in esi.get_universe_names(type_ids)
        if e.get("category") == "inventory_type"
    }

    SLOT_GROUPS = {
        "high":  [f"HiSlot{i}"  for i in range(8)],
        "mid":   [f"MedSlot{i}" for i in range(8)],
        "low":   [f"LoSlot{i}"  for i in range(8)],
        "rig":   [f"RigSlot{i}" for i in range(3)],
        "drone": ["DroneBay"],
        "cargo": ["Cargo"],
    }

    slots = defaultdict(list)
    for item in fitted:
        flag = item.get("location_flag", "Other")
        name = names.get(item["type_id"], str(item["type_id"]))
        qty  = item.get("quantity", 1)
        for group, flags in SLOT_GROUPS.items():
            if flag in flags:
                slots[group].append({"name": name, "qty": qty, "flag": flag})
                break
        else:
            slots["other"].append({"name": name, "qty": qty, "flag": flag})

    return {
        "ship_name":    ship["ship_name"],
        "ship_type":    type_info.get("name"),
        "ship_type_id": ship["ship_type_id"],
        "slots":        dict(slots),
    }


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
