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
        self._type_cache: dict[int, dict] = {}
        self._group_cache: dict[int, dict] = {}
        # Persistent client — reuses TCP connections, thread-safe
        self._client = httpx.Client(
            headers=self._headers,
            params={"datasource": DEFAULT_DATASOURCE},
            timeout=30,
        )

    def close(self):
        self._client.close()

    def _get(self, path: str, params: dict | None = None) -> dict | list:
        url = f"{ESI_BASE}{path}"
        resp = self._client.get(url, params=params or {})
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
        resp = self._client.post(url, json=ids)
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
            resp = self._client.get(url, params={"page": page})
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
        if type_id not in self._type_cache:
            self._type_cache[type_id] = self._get(f"/universe/types/{type_id}/")
        return self._type_cache[type_id]

    def get_group_info(self, group_id: int) -> dict:
        if group_id not in self._group_cache:
            self._group_cache[group_id] = self._get(f"/universe/groups/{group_id}/")
        return self._group_cache[group_id]

    def get_online(self, character_id: int) -> dict:
        return self._get(f"/characters/{character_id}/online/")

    def get_market_prices(self) -> list:
        """Public endpoint — returns adjusted_price and average_price per type_id."""
        return self._get("/markets/prices/")

    def get_assets(self, character_id: int, page: int = 1) -> list:
        return self._get(f"/characters/{character_id}/assets/", {"page": page})

    def get_assets_all(self, character_id: int) -> list:
        url = f"{ESI_BASE}/characters/{character_id}/assets/"
        all_assets = []
        page = 1
        while True:
            resp = self._client.get(url, params={"page": page})
            resp.raise_for_status()
            all_assets.extend(resp.json())
            if page >= int(resp.headers.get("X-Pages", 1)):
                break
            page += 1
        return all_assets

    def get_wallet_transactions(self, character_id: int, from_id: int | None = None) -> list:
        params = {}
        if from_id is not None:
            params["from_id"] = from_id
        return self._get(f"/characters/{character_id}/wallet/transactions/", params=params)

    def get_wallet_transactions_all(self, character_id: int, since_id: int | None = None) -> list:
        """Cursor-paginate until we reach since_id or exhaust results."""
        results, from_id = [], None
        while True:
            page = self.get_wallet_transactions(character_id, from_id)
            if not page:
                break
            for entry in page:
                if since_id is not None and entry["transaction_id"] <= since_id:
                    return results
                results.append(entry)
            from_id = min(e["transaction_id"] for e in page) - 1
        return results

    def get_character_orders(self, character_id: int) -> list:
        return self._get(f"/characters/{character_id}/orders/")

    def get_character_orders_history(self, character_id: int) -> list:
        return self._get(f"/characters/{character_id}/orders/history/")

    def get_market_history(self, region_id: int, type_id: int) -> list:
        return self._get(f"/markets/{region_id}/history/", params={"type_id": type_id})
