"""Orchestrates ESI data fetching and PostHog ingestion."""

from .analytics import Analytics
from .esi import ESIClient


def run_character_pipeline(character_id: int, esi: ESIClient, analytics: Analytics):
    print(f"[pipeline] Fetching character {character_id}...")

    public_info = esi.get_public_info(character_id)
    analytics.identify_character(character_id, public_info)

    skills = esi.get_skills(character_id)
    analytics.capture_skills(character_id, skills)

    # Resolve skill names and push full skill list
    skill_list = skills.get("skills", [])
    skill_ids = [s["skill_id"] for s in skill_list]
    print(f"[pipeline] Resolving {len(skill_ids)} skill names...")
    name_map = {
        entry["id"]: entry["name"]
        for entry in esi.get_universe_names(skill_ids)
        if entry.get("category") == "inventory_type"
    }
    for skill in skill_list:
        skill["skill_name"] = name_map.get(skill["skill_id"], str(skill["skill_id"]))

    # Print skill list to terminal
    print(f"\n{'Skill':<45} {'Level':>5} {'SP':>12}")
    print("-" * 65)
    for skill in sorted(skill_list, key=lambda s: s["skill_name"]):
        print(f"{skill['skill_name']:<45} {skill['trained_skill_level']:>5} {skill['skillpoints_in_skill']:>12,}")
    print(f"\nTotal SP: {skills.get('total_sp', 0):,}  |  Unallocated: {skills.get('unallocated_sp', 0):,}\n")

    analytics.capture_skill_list(character_id, skill_list)

    wallet = esi.get_wallet(character_id)
    analytics.capture_wallet(character_id, wallet)

    # Full snapshot with all available data
    snapshot = {
        **public_info,
        "total_sp": skills.get("total_sp"),
        "unallocated_sp": skills.get("unallocated_sp"),
        "wallet_balance": wallet,
    }

    try:
        location = esi.get_location(character_id)
        snapshot["solar_system_id"] = location.get("solar_system_id")
        snapshot["station_id"] = location.get("station_id")
        snapshot["structure_id"] = location.get("structure_id")
    except Exception as e:
        print(f"[pipeline] Could not fetch location: {e}")

    try:
        ship = esi.get_ship(character_id)
        snapshot["ship_type_id"] = ship.get("ship_type_id")
        snapshot["ship_name"] = ship.get("ship_name")
    except Exception as e:
        print(f"[pipeline] Could not fetch ship: {e}")

    analytics.capture_character_snapshot(character_id, snapshot)
    analytics.flush()

    print(f"[pipeline] Done. Character: {public_info.get('name')}")
