# bot.py
# Teams Activity handler
# Receives user message from Teams, calls agent to generate SQL,
# replies in Teams with the generated SQL + explanation

from botbuilder.core import ActivityHandler, TurnContext
from agent import generate_sql


class AiRoBot(ActivityHandler):

    async def on_message_activity(self, turn_context: TurnContext):
        user_message = turn_context.activity.text.strip()
        user_name    = turn_context.activity.from_property.name or "Teams User"

        # ── Show typing indicator ─────────────────────────────────────────────
        await turn_context.send_activity("⏳ Generating SQL query, please wait...")

        # ── Generate SQL via LLM ──────────────────────────────────────────────
        result = generate_sql(
            user_query = user_message,
            asked_by   = user_name,
        )

        sql         = result.get("sql", "")
        explanation = result.get("explanation", "")
        tables_used = result.get("tables_used", [])
        path        = result.get("path", "")

        # ── Build reply ───────────────────────────────────────────────────────
        if not sql:
            reply = (
                f"❌ Could not generate SQL for your query.\n\n"
                f"**Reason:** {explanation}"
            )
        else:
            reply = (
                f"**Your question:** {user_message}\n\n"
                f"**Generated SQL:**\n```sql\n{sql}\n```\n\n"
                f"**What it does:** {explanation}\n\n"
                f"**Tables used:** {', '.join(tables_used)}\n\n"
                f"**Stored in Lakehouse:** `{path}`"
            )

        await turn_context.send_activity(reply)

    async def on_members_added_activity(self, members_added, turn_context: TurnContext):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity(
                    " Hi! I am **AiRo Bot**.\n\n"
                    "Ask me any business question and I will generate a SQL query for you.\n\n"
                    "Example: *Show me total sales by product for last 3 months*"
                )