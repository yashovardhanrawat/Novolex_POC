# main.py
# FastAPI server — entry point for the AiRo Bot
# Receives activity from Azure Bot Service and routes to bot.py

import os
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response
from botbuilder.core import (
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
)
from botbuilder.schema import Activity
from bot import AiRoBot

load_dotenv()

# ── Bot Framework Adapter ─────────────────────────────────────────────────────
settings = BotFrameworkAdapterSettings(
    app_id       = os.getenv("MicrosoftAppId"),
    app_password = os.getenv("MicrosoftAppPassword"),
    channel_auth_tenant = os.getenv("MicrosoftAppTenantId"),
)
adapter = BotFrameworkAdapter(settings)
bot     = AiRoBot()

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="AiRo Bot")


@app.get("/")
async def health():
    return {"status": "AiRo Bot is running ✅"}


@app.post("/api/messages")
async def messages(request: Request):
    if request.headers.get("Content-Type") != "application/json":
        return Response(status_code=415)

    body     = await request.json()
    activity = Activity().deserialize(body)
    auth     = request.headers.get("Authorization", "")

    async def call_bot(turn_context: TurnContext):
        await bot.dispatch(turn_context)

    await adapter.process_activity(activity, auth, call_bot)
    return Response(status_code=201)