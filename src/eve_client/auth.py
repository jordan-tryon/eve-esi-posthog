"""EVE SSO OAuth2 authentication."""

import json
import os
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
from authlib.integrations.httpx_client import OAuth2Client

ESI_SSO_BASE = "https://login.eveonline.com"
TOKEN_URL = f"{ESI_SSO_BASE}/v2/oauth/token"
AUTH_URL = f"{ESI_SSO_BASE}/v2/oauth/authorize"
VERIFY_URL = f"{ESI_SSO_BASE}/oauth/verify"

TOKEN_FILE = Path(".tokens.json")

CHARACTER_SCOPES = [
    "publicData",
    "esi-calendar.respond_calendar_events.v1",
    "esi-calendar.read_calendar_events.v1",
    "esi-location.read_location.v1",
    "esi-location.read_ship_type.v1",
    "esi-location.read_online.v1",
    "esi-skills.read_skills.v1",
    "esi-skills.read_skillqueue.v1",
    "esi-wallet.read_character_wallet.v1",
    "esi-clones.read_clones.v1",
    "esi-clones.read_implants.v1",
    "esi-characters.read_contacts.v1",
    "esi-characters.write_contacts.v1",
    "esi-characters.read_loyalty.v1",
    "esi-characters.read_medals.v1",
    "esi-characters.read_standings.v1",
    "esi-characters.read_agents_research.v1",
    "esi-characters.read_blueprints.v1",
    "esi-characters.read_corporation_roles.v1",
    "esi-characters.read_fatigue.v1",
    "esi-characters.read_notifications.v1",
    "esi-characters.read_titles.v1",
    "esi-characters.read_fw_stats.v1",
    "esi-characters.read_freelance_jobs.v1",
    "esi-killmails.read_killmails.v1",
    "esi-assets.read_assets.v1",
    "esi-markets.read_character_orders.v1",
    "esi-markets.structure_markets.v1",
    "esi-industry.read_character_jobs.v1",
    "esi-industry.read_character_mining.v1",
    "esi-contracts.read_character_contracts.v1",
    "esi-fittings.read_fittings.v1",
    "esi-fittings.write_fittings.v1",
    "esi-planets.manage_planets.v1",
    "esi-fleets.read_fleet.v1",
    "esi-fleets.write_fleet.v1",
    "esi-ui.open_window.v1",
    "esi-ui.write_waypoint.v1",
    "esi-search.search_structures.v1",
    "esi-structures.read_character.v1",
    "esi-activities.read_character.v1",
    "esi-mail.read_mail.v1",
    "esi-mail.send_mail.v1",
    "esi-mail.organize_mail.v1",
]


class EveAuth:
    def __init__(self, client_id: str, client_secret: str, callback_url: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.callback_url = callback_url
        self._token: dict | None = self._load_token()

    def get_auth_url(self) -> str:
        client = OAuth2Client(
            client_id=self.client_id,
            redirect_uri=self.callback_url,
            scope=" ".join(CHARACTER_SCOPES),
        )
        url, _ = client.create_authorization_url(AUTH_URL)
        return url

    def run_auth_flow(self) -> dict:
        """Open browser, spin up local callback server, exchange code automatically."""
        parsed = urlparse(self.callback_url)
        port = parsed.port or 8080
        code_holder: dict = {}

        class CallbackHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                qs = parse_qs(urlparse(self.path).query)
                code_holder["code"] = qs.get("code", [None])[0]
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(b"<h2>Auth complete! You can close this tab.</h2>")

            def log_message(self, *args):
                pass  # silence request logs

        server = HTTPServer(("localhost", port), CallbackHandler)
        server.timeout = 120

        url = self.get_auth_url()
        print(f"Opening browser for EVE SSO login...\n  {url}")
        webbrowser.open(url)
        print("Waiting for callback...")

        server.handle_request()  # blocks until one request comes in
        server.server_close()

        code = code_holder.get("code")
        if not code:
            raise RuntimeError("No code received from EVE SSO callback.")
        return self.exchange_code(code)

    def exchange_code(self, code: str) -> dict:
        client = OAuth2Client(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.callback_url,
        )
        token = client.fetch_token(TOKEN_URL, code=code)
        self._token = dict(token)
        self._save_token(self._token)
        return self._token

    def get_valid_token(self) -> str:
        if not self._token:
            raise RuntimeError("No token — run auth flow first.")
        if self._is_expired():
            self._refresh()
        return self._token["access_token"]

    def _is_expired(self) -> bool:
        expires_at = self._token.get("expires_at", 0)
        return time.time() >= expires_at - 60

    def _refresh(self):
        client = OAuth2Client(
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        token = client.refresh_token(TOKEN_URL, refresh_token=self._token["refresh_token"])
        self._token = dict(token)
        self._save_token(self._token)

    def _save_token(self, token: dict):
        TOKEN_FILE.write_text(json.dumps(token, indent=2))

    def _load_token(self) -> dict | None:
        if TOKEN_FILE.exists():
            return json.loads(TOKEN_FILE.read_text())
        return None
