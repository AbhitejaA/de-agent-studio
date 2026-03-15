"""
analyst.py — Analyst Agent for de-agent-studio
Classifies requests as new or existing
Produces structured spec for Builder Agent
"""

import anthropic
import os
import json
from dotenv import load_dotenv
from tools.rag_tools import search_knowledge_base

load_dotenv()

# ─────────────────────────────────────────
# SYSTEM PROMPT
# ─────────────────────────────────────────
ANALYST_SYSTEM_PROMPT = """You are a Senior Data Engineering Analyst at Peacock TV
with deep expertise in SQL, PySpark, Airflow, and Databricks.
You understand Peacock's data architecture, table naming conventions,
business KPIs, and reporting requirements.

When you receive a pipeline requirement:

Step 1 — Search existing knowledge base:
→ always search for existing pipelines or tables related to this request
→ if similar pipeline exists: classify as "existing", retrieve the code
→ if nothing found: classify as "new"

Step 2 — If NEW development:
→ identify required input tables from the request
→ determine the transformation logic needed
→ determine appropriate schedule (convert to cron)
→ determine output table name following naming conventions
→ determine language: SQL for queries, PySpark for transformations

Step 3 — If EXISTING update:
→ retrieve existing code from knowledge base
→ identify exactly what needs to change
→ describe the modification clearly

Step 4 — If requirement is ambiguous:
→ do not guess silently
→ state your assumptions clearly
→ ask user to confirm before proceeding

Always return response as JSON only, no other text:
{
    "type": "new" or "existing",
    "pipeline_name": "snake_case_name",
    "input_tables": ["table1", "table2"],
    "output_table": "output_table_name",
    "schedule": "cron expression e.g. 0 7 * * *",
    "logic": "plain English description of transformation logic",
    "language": "SQL or PySpark",
    "existing_file": "filename if existing, null if new",
    "assumptions": ["assumption 1", "assumption 2"],
    "explanation": "brief explanation of decisions made"
}"""

# ─────────────────────────────────────────
# TOOL DEFINITIONS
# ─────────────────────────────────────────
ANALYST_TOOLS = [
    {
        "name": "search_knowledge_base",
        "description": "Search existing ChromaDB knowledge base to find relevant pipelines or code. Call this BEFORE classifying request as new or existing. Returns relevant files if found.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query to find relevant existing pipelines e.g. 'subscriber churn pipeline' or 'weekly content report'"
                }
            },
            "required": ["query"]
        }
    }
]

# ─────────────────────────────────────────
# TOOL EXECUTOR
# ─────────────────────────────────────────
def execute_analyst_tool(tool_name: str, tool_input: dict) -> str:
    if tool_name == "search_knowledge_base":
        return search_knowledge_base(tool_input["query"])
    return f"Unknown tool: {tool_name}"

# ─────────────────────────────────────────
# ANALYST AGENT
# ─────────────────────────────────────────
def run_analyst(user_request: str) -> dict:
    """
    Run Analyst Agent on user request.
    Returns structured spec as Python dict.
    """
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    
    messages = [{"role": "user", "content": user_request}]

    while True:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=ANALYST_SYSTEM_PROMPT,
            tools=ANALYST_TOOLS,
            messages=messages
        )

        if response.stop_reason == "tool_use":
            messages.append({
                "role": "assistant",
                "content": response.content
            })
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result = execute_analyst_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result
                    })
            messages.append({
                "role": "user",
                "content": tool_results
            })

        elif response.stop_reason == "end_turn":
            reply = "".join(
                block.text for block in response.content
                if hasattr(block, "text")
            )
            # parse JSON response
            try:
                spec = json.loads(reply)
            except json.JSONDecodeError:
                # if Claude added extra text, extract JSON
                import re
                json_match = re.search(r'\{.*\}', reply, re.DOTALL)
                if json_match:
                    spec = json.loads(json_match.group())
                else:
                    spec = {"error": "Could not parse analyst response", "raw": reply}
            return spec