"""EVE SSO OAuth2 authentication — multi-character."""

import time
from urllib.parse import parse_qs, urlparse

import httpx
from authlib.integrations.httpx_client import OAuth2Client

ESI_SSO_BASE = "https://login.eveonline.com"
TOKEN_URL    = f"{ESI_SSO_BASE}/v2/oauth/token"
AUTH_URL     = f"{ESI_SSO_BASE}/v2/oauth/authorize"

CHARACTER_SCOPES = [
    "publicData",
    "esi-location.read_location.v1",
    "esi-location.read_ship_type.v1",
    "esi-location.read_online.v1",
    "esi-skills.read_skills.v1",
    "esi-skills.read_skillqueue.v1",
    "esi-wallet.read_character_wallet.v1",
    "esi-clones.read_clones.v1",
    "esi-clones.read_implants.v1",
    "esi-characters.read_contacts.v1",
    "esi-characters.read_standings.v1",
    "esi-characters.read_agents_research.v1",
    "esi-characters.read_blueprints.v1",
    "esi-characters.read_fatigue.v1",
    "esi-characters.read_notifications.v1",
    "esi-characters.read_titles.v1",
    "esi-killmails.read_killmails.v1",
    "esi-assets.read_assets.v1",
    "esi-markets.read_character_orders.v1",
    "esi-industry.read_character_jobs.v1",
    "esi-industry.read_character_mining.v1",
    "esi-contracts.read_character_contracts.v1",
    "esi-fittings.read_fittings.v1",
    "esi-planets.manage_planets.v1",
]


class EveAuth:
    """Handles EVE SSO OAuth2 for multiple characters, backed by the DB store."""

    def __init__(self, client_id: str, client_secret: str, callback_url: str, store=None):
        self.client_id     = client_id
        self.client_secret = client_secret
        self.callback_url  = callback_url
        self.store         = store  # SnapshotStore — None for CLI-only use

    def get_auth_url(self) -> str:
        client = OAuth2Client(
            client_id=self.client_id,
            redirect_uri=self.callback_url,
            scope=" ".join(CHARACTER_SCOPES),
        )
        url, _ = client.create_authorization_url(AUTH_URL)
        return url

    def exchange_code(self, code: str) -> int:
        """Exchange auth code → tokens. Saves to DB. Returns character_id."""
        client = OAuth2Client(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.callback_url,
        )
        token = dict(client.fetch_token(TOKEN_URL, code=code))

        # Verify token to get character info
        with httpx.Client() as http:
            resp = http.get(
                "https://login.eveonline.com/oauth/verify",
                headers={"Authorization": f"Bearer {token['access_token']}"},
            )
            resp.raise_for_status()
            info = resp.json()

        character_id   = int(info["CharacterID"])
        character_name = info["CharacterName"]

        if self.store:
            self.store.save_token(character_id, character_name, token)

        print(f"[auth] Registered: {character_name} ({character_id})")
        return character_id

    def get_valid_token(self, character_id: int) -> str:
        """Return a valid access token for character, refreshing if needed."""
        if not self.store:
            raise RuntimeError("No store attached — cannot look up tokens.")

        tok = self.store.get_token(character_id)
        if not tok:
            raise RuntimeError(f"No token for character {character_id}")

        if time.time() >= tok["expires_at"] - 60:
            tok = self._refresh(character_id, tok["refresh_token"])

        return tok["access_token"]

    def _refresh(self, character_id: int, refresh_token: str) -> dict:
        client = OAuth2Client(client_id=self.client_id, client_secret=self.client_secret)
        token  = dict(client.refresh_token(TOKEN_URL, refresh_token=refresh_token))
        if self.store:
            self.store.update_token(character_id, token)
        return token
