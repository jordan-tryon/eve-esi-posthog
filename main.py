"""Entry point — auth flow + pipeline run."""

import os
import sys

from dotenv import load_dotenv

load_dotenv()

from src.eve_client.auth import EveAuth
from src.eve_client.esi import ESIClient
from src.eve_client.analytics import Analytics
from src.eve_client.pipeline import run_character_pipeline


def main():
    auth = EveAuth(
        client_id=os.environ["EVE_CLIENT_ID"],
        client_secret=os.environ["EVE_CLIENT_SECRET"],
        callback_url=os.environ["EVE_CALLBACK_URL"],
    )
    analytics = Analytics(
        api_key=os.environ["POSTHOG_API_KEY"],
        host=os.environ.get("POSTHOG_HOST", "https://us.i.posthog.com"),
    )

    # If no stored token, run the auth flow
    if auth._token is None:
        auth.run_auth_flow()
        print("Token saved.\n")

    token = auth.get_valid_token()
    esi = ESIClient(access_token=token)

    character_id = int(os.environ["EVE_CHARACTER_ID"])
    run_character_pipeline(character_id, esi, analytics)


if __name__ == "__main__":
    main()
