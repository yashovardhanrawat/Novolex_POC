# lakehouse_writer.py
# Writes generated SQL + metadata as a JSON file to Fabric Lakehouse
# Storage path: Files/bot_queries/<timestamp>_<query_id>.json

import os
import json
import uuid
import requests
import msal
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# ── Credentials ───────────────────────────────────────────────────────────────
TENANT_ID    = os.getenv("FABRIC_TENANT_ID")
CLIENT_ID    = os.getenv("FABRIC_CLIENT_ID")
WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID")
LAKEHOUSE_ID = os.getenv("FABRIC_LAKEHOUSE_ID")

# ── OneLake REST API ──────────────────────────────────────────────────────────
ONELAKE_BASE   = "https://onelake.dfs.fabric.microsoft.com"
SCOPES         = ["https://storage.azure.com/user_impersonation"]
BOT_QUERY_PATH = "Files/bot_queries"

# ── Token cache (module level so it persists across calls) ───────────────────
_msal_app = None


def get_msal_app():
    global _msal_app
    if _msal_app is None:
        _msal_app = msal.PublicClientApplication(
            client_id=CLIENT_ID,
            authority=f"https://login.microsoftonline.com/{TENANT_ID}"
        )
    return _msal_app


def get_access_token() -> str:
    app = get_msal_app()

    # Check cache first
    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
        if result and "access_token" in result:
            return result["access_token"]

    # Device code flow
    flow = app.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        raise Exception(f"Failed to create device flow: {flow}")

    print("\n" + flow["message"])
    print("Waiting for login...\n")

    result = app.acquire_token_by_device_flow(flow)

    if "access_token" not in result:
        raise Exception(f"Failed to get token: {result.get('error_description')}")

    return result["access_token"]


def write_query_to_lakehouse(
    user_query: str,
    sql: str,
    tables_used: list[str],
    asked_by: str = "Teams User",
) -> dict:
    """
    Writes a JSON record to Fabric Lakehouse under Files/bot_queries/
    """
    query_id  = str(uuid.uuid4())[:8]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename  = f"{timestamp}_{query_id}.json"

    # ── Build the payload ─────────────────────────────────────────────────────
    payload = {
        "query_id":    query_id,
        "timestamp":   timestamp,
        "asked_by":    asked_by,
        "user_query":  user_query,
        "sql":         sql,
        "tables_used": tables_used,
        "status":      "pending",
    }

    content = json.dumps(payload, indent=2).encode("utf-8")

    # ── Upload to OneLake ─────────────────────────────────────────────────────
    token = get_access_token()

    headers = {
        "Authorization":  f"Bearer {token}",
        "x-ms-version":   "2021-06-08",
    }

    file_path = f"{WORKSPACE_ID}/{LAKEHOUSE_ID}/{BOT_QUERY_PATH}/{filename}"
    base_url  = f"{ONELAKE_BASE}/{file_path}"

    # Step 1 — Create empty file
    create_resp = requests.put(
        base_url,
        headers=headers,
        params={"resource": "file"},
    )
    if create_resp.status_code not in (200, 201):
        raise Exception(f"Failed to create file: {create_resp.text}")

    # Step 2 — Append content
    append_resp = requests.patch(
        base_url,
        headers={**headers, "Content-Type": "application/octet-stream"},
        params={"action": "append", "position": "0"},
        data=content,
    )
    if append_resp.status_code not in (200, 202):
        raise Exception(f"Failed to append content: {append_resp.text}")

    # Step 3 — Flush (commit)
    flush_resp = requests.patch(
        base_url,
        headers=headers,
        params={"action": "flush", "position": str(len(content))},
    )
    if flush_resp.status_code not in (200, 202):
        raise Exception(f"Failed to flush file: {flush_resp.text}")

    print(f"✅ Written to Lakehouse: {BOT_QUERY_PATH}/{filename}")

    return {
        "query_id":  query_id,
        "timestamp": timestamp,
        "path":      f"{BOT_QUERY_PATH}/{filename}",
    }


# ── Quick test ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    result = write_query_to_lakehouse(
        user_query  = "Show me total sales by product for last 3 months",
        sql         = "SELECT p.ProductName, SUM(s.Quantity) as TotalQty FROM sales s JOIN products p ON s.ProductID = p.ProductID WHERE s.SalesDate >= DATEADD(month, -3, GETDATE()) GROUP BY p.ProductName",
        tables_used = ["sales", "products"],
        asked_by    = "Yashovardhan",
    )
    print(result)