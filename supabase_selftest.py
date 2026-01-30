from __future__ import annotations

import os
import time
from dotenv import find_dotenv, load_dotenv
from supabase import ClientOptions, create_client


def main() -> None:
    dotenv_path = find_dotenv(".env", usecwd=True) or find_dotenv(".env", usecwd=False)
    if not dotenv_path:
        raise RuntimeError(".env not found")
    load_dotenv(dotenv_path)

    url = os.environ["SUPABASE_URL"]
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ["SUPABASE_ANON_KEY"]
    api_key = os.environ["BUBBLE_API_KEY"]
    table = os.getenv("SUPABASE_TABLE", "a_dis")

    sb = create_client(url, key, options=ClientOptions(headers={"x-bubble-key": api_key}))

    row = {
        "typename": "selftest",
        "username": "selftest",
        "createtime": "2026-01-31T00:00:00Z",
        "content": "selftest",
        "url": "selftest",
        "id": f"selftest-{int(time.time())}",
    }

    resp = sb.table(table).insert(row, returning="representation").execute()
    print("insert_error", getattr(resp, "error", None))
    print("insert_data", getattr(resp, "data", None))

    sel = sb.table(table).select("*").eq("id", row["id"]).execute()
    print("select_error", getattr(sel, "error", None))
    print("select_data", getattr(sel, "data", None))


if __name__ == "__main__":
    main()
