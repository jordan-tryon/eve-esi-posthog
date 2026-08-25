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
        self._system_cache: dict[int, dict] = {}
        self._planet_cache: dict[int, dict] = {}
        self._schematic_cache: dict[int, dict] = {}
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
        if system_id not in self._system_cache:
            self._system_cache[system_id] = self._get(f"/universe/systems/{system_id}/")
        return self._system_cache[system_id]

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

    def get_contracts(self, character_id: int) -> list:
        """Fetch all character contracts, paginated."""
        url = f"{ESI_BASE}/characters/{character_id}/contracts/"
        all_contracts = []
        page = 1
        while True:
            resp = self._client.get(url, params={"page": page})
            resp.raise_for_status()
            all_contracts.extend(resp.json())
            if page >= int(resp.headers.get("X-Pages", 1)):
                break
            page += 1
        return all_contracts

    def get_market_history(self, region_id: int, type_id: int) -> list:
        return self._get(f"/markets/{region_id}/history/", params={"type_id": type_id})

    def get_industry_jobs(self, character_id: int, include_completed: bool = True) -> list:
        """Not paginated — ESI returns the full job list in one call."""
        return self._get(
            f"/characters/{character_id}/industry/jobs/",
            params={"include_completed": include_completed},
        )

    def get_blueprints_all(self, character_id: int) -> list:
        """Fetch every page of owned blueprints, following X-Pages header."""
        url = f"{ESI_BASE}/characters/{character_id}/blueprints/"
        all_bps = []
        page = 1
        while True:
            resp = self._client.get(url, params={"page": page})
            resp.raise_for_status()
            all_bps.extend(resp.json())
            if page >= int(resp.headers.get("X-Pages", 1)):
                break
            page += 1
        return all_bps

    def get_contract_items(self, character_id: int, contract_id: int) -> list:
        return self._get(f"/characters/{character_id}/contracts/{contract_id}/items/")

    def get_contract_bids(self, character_id: int, contract_id: int) -> list:
        """Only valid for auction-type contracts — ESI 404s for other types."""
        return self._get(f"/characters/{character_id}/contracts/{contract_id}/bids/")

    def get_planets(self, character_id: int) -> list:
        """PI colony summary list. Not paginated."""
        return self._get(f"/characters/{character_id}/planets/")

    def get_planet_detail(self, character_id: int, planet_id: int) -> dict:
        """Full colony detail: pins (incl. extractor expiry_time), links, routes."""
        return self._get(f"/characters/{character_id}/planets/{planet_id}/")

    def get_universe_planet(self, planet_id: int) -> dict:
        """Public endpoint — planet name + numeric type_id. Cached for process lifetime."""
        if planet_id not in self._planet_cache:
            self._planet_cache[planet_id] = self._get(f"/universe/planets/{planet_id}/")
        return self._planet_cache[planet_id]

    def get_schematic_info(self, schematic_id: int) -> dict:
        """Public endpoint — PI schematic name (e.g. 'Coolant'). Cached for process lifetime."""
        if schematic_id not in self._schematic_cache:
            self._schematic_cache[schematic_id] = self._get(f"/universe/schematics/{schematic_id}/")
        return self._schematic_cache[schematic_id]

    def get_structure_info(self, structure_id: int) -> dict:
        """Player-structure name resolution. Requires esi-universe.read_structures.v1
        and the token's character having docking access — ESI 403s otherwise."""
        return self._get(f"/universe/structures/{structure_id}/")

    def get_jita_sell_prices(self, type_ids: list[int]) -> dict[int, float]:
        """Best sell price at Jita 4-4 (station 60003760, region 10000002) per type_id."""
        if not type_ids:
            return {}
        JITA_REGION  = 10000002
        JITA_STATION = 60003760

        from concurrent.futures import ThreadPoolExecutor

        def _fetch(type_id: int):
            try:
                orders = self._get(f"/markets/{JITA_REGION}/orders/",
                                   params={"type_id": type_id, "order_type": "sell"})
                prices = [o["price"] for o in orders if o.get("location_id") == JITA_STATION]
                return type_id, min(prices) if prices else None
            except Exception:
                return type_id, None

        result: dict[int, float] = {}
        with ThreadPoolExecutor(max_workers=10) as pool:
            for type_id, price in pool.map(_fetch, type_ids):
                if price is not None:
                    result[type_id] = price
        return result
