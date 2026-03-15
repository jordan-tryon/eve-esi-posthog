"""ESI API client."""

import httpx

ESI_BASE = "https://esi.evetech.net/latest"
DEFAULT_DATASOURCE = "tranquility"


class ESIClient:
    def __init__(self, access_token: str):
        self._headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "User-Agent": "eve-esi-posthog/0.1 (contact: your@email.com)",
        }

    def _get(self, path: str, params: dict | None = None) -> dict | list:
        url = f"{ESI_BASE}{path}"
        p = {"datasource": DEFAULT_DATASOURCE, **(params or {})}
        with httpx.Client() as client:
            resp = client.get(url, headers=self._headers, params=p, timeout=30)
            resp.raise_for_status()
            return resp.json()

    def get_public_info(self, character_id: int) -> dict:
        return self._get(f"/characters/{character_id}/")

    def get_attributes(self, character_id: int) -> dict:
        return self._get(f"/characters/{character_id}/attributes/")

    def get_skills(self, character_id: int) -> dict:
        return self._get(f"/characters/{character_id}/skills/")

    def get_skillqueue(self, character_id: int) -> list:
        return self._get(f"/characters/{character_id}/skillqueue/")

    def get_clones(self, character_id: int) -> dict:
        return self._get(f"/characters/{character_id}/clones/")

    def get_implants(self, character_id: int) -> list:
        return self._get(f"/characters/{character_id}/implants/")

    def get_location(self, character_id: int) -> dict:
        return self._get(f"/characters/{character_id}/location/")

    def get_ship(self, character_id: int) -> dict:
        return self._get(f"/characters/{character_id}/ship/")

    def get_wallet(self, character_id: int) -> float:
        return self._get(f"/characters/{character_id}/wallet/")

    def get_universe_names(self, ids: list[int]) -> list[dict]:
        """Resolve a list of type/entity IDs to names. Max 1000 per call."""
        url = f"{ESI_BASE}/universe/names/"
        with httpx.Client() as client:
            resp = client.post(url, json=ids, params={"datasource": DEFAULT_DATASOURCE}, timeout=30)
            resp.raise_for_status()
            return resp.json()

    def get_wallet_journal(self, character_id: int, page: int = 1) -> list:
        return self._get(f"/characters/{character_id}/wallet/journal/", {"page": page})

    def get_wallet_journal_all(self, character_id: int) -> list:
        """Fetch every page of wallet journal, following X-Pages header."""
        url = f"{ESI_BASE}/characters/{character_id}/wallet/journal/"
        all_entries = []
        page = 1
        while True:
            with httpx.Client() as client:
                resp = client.get(
                    url,
                    headers=self._headers,
                    params={"datasource": DEFAULT_DATASOURCE, "page": page},
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                all_entries.extend(data)
                total_pages = int(resp.headers.get("X-Pages", 1))
                print(f"  [esi] journal page {page}/{total_pages} ({len(data)} entries)")
                if page >= total_pages:
                    break
                page += 1
        return all_entries

    def get_killmails_recent(self, character_id: int) -> list:
        return self._get(f"/characters/{character_id}/killmails/recent/")

    def get_killmail(self, killmail_id: int, killmail_hash: str) -> dict:
        return self._get(f"/killmails/{killmail_id}/{killmail_hash}/")

    def get_system_info(self, system_id: int) -> dict:
        return self._get(f"/universe/systems/{system_id}/")

    def get_type_info(self, type_id: int) -> dict:
        return self._get(f"/universe/types/{type_id}/")

    def get_online(self, character_id: int) -> dict:
        return self._get(f"/characters/{character_id}/online/")

    def get_assets(self, character_id: int, page: int = 1) -> list:
        return self._get(f"/characters/{character_id}/assets/", {"page": page})

    def get_assets_all(self, character_id: int) -> list:
        url = f"{ESI_BASE}/characters/{character_id}/assets/"
        all_assets = []
        page = 1
        while True:
            with httpx.Client() as client:
                resp = client.get(url, headers=self._headers,
                                  params={"datasource": DEFAULT_DATASOURCE, "page": page}, timeout=30)
                resp.raise_for_status()
                all_assets.extend(resp.json())
                if page >= int(resp.headers.get("X-Pages", 1)):
                    break
                page += 1
        return all_assets
