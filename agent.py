# agent.py
# Core LLM logic — takes user query, reads schema, generates SQL
# Uses Groq (Llama 3.3 70B) via LangGraph

import os
from dotenv import load_dotenv
from groq import Groq
from schema import get_schema_for_llm
from lakehouse_writer import write_query_to_lakehouse

load_dotenv()

# ── Groq client ───────────────────────────────────────────────────────────────
client = Groq(api_key=os.getenv("GROQ_API_KEY"))
model  = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

# ── System prompt ─────────────────────────────────────────────────────────────
SYSTEM_PROMPT = f"""
You are AiRo SQL Bot, an assistant that converts business questions into SQL queries.

{get_schema_for_llm()}

Rules:
1. Return ONLY a JSON object with exactly these keys:
   - "sql": the SQL query as a string (no markdown, no code fences)
   - "tables_used": a list of table names used in the query
   - "explanation": one sentence explaining what the query does
2. Use standard SQL syntax.
3. Always use explicit column names, never SELECT *.
4. Use JOINs based on the relationships defined in the schema.
5. If the question cannot be answered with the available tables,
   set sql to "" and explain why in the explanation field.
""".strip()


def generate_sql(user_query: str, asked_by: str = "Teams User") -> dict:
    """
    Takes a natural language question, generates SQL using the LLM,
    stores the result in the Fabric Lakehouse, and returns the full result.

    Parameters
    ----------
    user_query : natural language question from the Teams user
    asked_by   : Teams user name or ID

    Returns
    -------
    dict with keys: sql, tables_used, explanation, query_id, path
    """
    import json, re

    # ── Call Groq LLM ─────────────────────────────────────────────────────────
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_query},
        ],
        temperature=0.1,
        max_tokens=512,
    )

    raw = response.choices[0].message.content.strip()

    # Strip markdown fences if model adds them anyway
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    # ── Parse response ────────────────────────────────────────────────────────
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {
            "sql":         "",
            "tables_used": [],
            "explanation": f"LLM returned an unparseable response: {raw}",
            "query_id":    None,
            "path":        None,
        }

    sql         = parsed.get("sql", "").strip()
    tables_used = parsed.get("tables_used", [])
    explanation = parsed.get("explanation", "").strip()

    # ── Store in Lakehouse ────────────────────────────────────────────────────
    lakehouse_result = {}
    if sql:
        lakehouse_result = write_query_to_lakehouse(
            user_query  = user_query,
            sql         = sql,
            tables_used = tables_used,
            asked_by    = asked_by,
        )

    return {
        "sql":         sql,
        "tables_used": tables_used,
        "explanation": explanation,
        "query_id":    lakehouse_result.get("query_id"),
        "path":        lakehouse_result.get("path"),
    }


# ── Quick test ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    test_query = "Show me total sales quantity by product for the last 3 months"
    print(f"User query: {test_query}\n")

    result = generate_sql(test_query, asked_by="Yashovardhan")

    print(f"SQL:\n{result['sql']}\n")
    print(f"Tables used: {result['tables_used']}")
    print(f"Explanation: {result['explanation']}")
    print(f"Stored at: {result['path']}")