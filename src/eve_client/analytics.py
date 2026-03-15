"""PostHog analytics integration."""

from posthog import Posthog


class Analytics:
    def __init__(self, api_key: str, host: str = "https://us.i.posthog.com"):
        self.client = Posthog(api_key, host=host)

    def capture_character_snapshot(self, character_id: int, data: dict):
        self.client.capture(
            distinct_id=str(character_id),
            event="character_snapshot",
            properties=data,
        )

    def capture_skills(self, character_id: int, skills: dict):
        self.client.capture(
            distinct_id=str(character_id),
            event="character_skills",
            properties={
                "total_sp": skills.get("total_sp"),
                "unallocated_sp": skills.get("unallocated_sp"),
                "skill_count": len(skills.get("skills", [])),
            },
        )

    def capture_skill_list(self, character_id: int, skill_rows: list[dict]):
        """One event per skill with name, level, and SP."""
        for skill in skill_rows:
            self.client.capture(
                distinct_id=str(character_id),
                event="character_skill",
                properties={
                    "skill_id": skill["skill_id"],
                    "skill_name": skill.get("skill_name", str(skill["skill_id"])),
                    "trained_level": skill["trained_skill_level"],
                    "active_level": skill["active_skill_level"],
                    "skillpoints": skill["skillpoints_in_skill"],
                },
            )

    def capture_wallet(self, character_id: int, balance: float):
        self.client.capture(
            distinct_id=str(character_id),
            event="character_wallet",
            properties={"balance_isk": balance},
        )

    def identify_character(self, character_id: int, public_info: dict):
        self.client.capture(
            distinct_id=str(character_id),
            event="$identify",
            properties={
                "$set": {
                    "name": public_info.get("name"),
                    "corporation_id": public_info.get("corporation_id"),
                    "alliance_id": public_info.get("alliance_id"),
                    "birthday": public_info.get("birthday"),
                    "security_status": public_info.get("security_status"),
                    "race_id": public_info.get("race_id"),
                }
            },
        )

    def capture_session_start(self, character_id: int, props: dict):
        self.client.capture(
            distinct_id=str(character_id),
            event="session_start",
            properties=props,
        )

    def capture_session_end(self, character_id: int, props: dict):
        self.client.capture(
            distinct_id=str(character_id),
            event="session_end",
            properties=props,
        )

    def capture_ship_loss(self, character_id: int, props: dict):
        self.client.capture(
            distinct_id=str(character_id),
            event="ship_loss",
            properties=props,
        )

    def capture_hourly_snapshot(self, character_id: int, snap: dict):
        props = {k: v for k, v in snap.items() if k not in ("character_id", "captured_at")}
        self.client.capture(
            distinct_id=str(character_id),
            event="hourly_snapshot",
            properties=props,
        )

    def flush(self):
        self.client.flush()
