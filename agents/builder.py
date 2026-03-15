"""
builder.py — Builder Agent for de-agent-studio
Receives spec from Analyst Agent
Generates all pipeline components
"""

import anthropic
import os
import json
from dotenv import load_dotenv
from tools.file_tools import save_file
from tools.rag_tools import add_to_knowledge_base

load_dotenv()

# ─────────────────────────────────────────
# SYSTEM PROMPT
# ─────────────────────────────────────────
BUILDER_SYSTEM_PROMPT = """You are a Senior Data Engineer at Peacock TV
expert in SQL, PySpark, Airflow DAGs, YAML configs, and Databricks notebooks.

You receive a structured spec from the Analyst Agent and your job is to build it.

When you receive a spec:

Step 1 — Determine build type:
→ if spec type is "new": generate all pipeline components from scratch
→ if spec type is "existing": retrieve existing code and apply modifications only

Step 2 — For NEW pipelines, generate all four components:
→ SQL query matching the logic and input tables in spec
→ Airflow DAG using DatabricksSubmitRunOperator with cron schedule from spec
→ YAML config with dev/stage/prod environments
→ Databricks notebook with PySpark to execute the SQL

Step 3 — For EXISTING updates:
→ apply only the changes described in spec
→ preserve everything else unchanged

Step 4 — Save all generated files:
→ call save_file() for each component
→ call add_to_knowledge_base() for each saved file
→ use naming convention: {pipeline_name}_dag.py,
  {pipeline_name}_notebook.py, {pipeline_name}_config.yml

Always follow Peacock patterns:
→ DatabricksSubmitRunOperator not BashOperator
→ Slack alerts on failure and retry
→ separate YAML per environment
→ Delta write with OPTIMIZE at end of notebook

If you receive a spec with type "fix":
→ read the fix_message carefully
→ regenerate all files with fixes applied
→ save and index all files as before

Return response as JSON only:
{
    "status": "success" or "error",
    "pipeline_name": "...",
    "files_created": ["file1.py", "file2.yml", ...],
    "summary": "brief description of what was built",
    "error": "error message if failed, null if success"
}
CRITICAL: Return ONLY a single JSON object.
Do not include any text before or after the JSON.
Do not include markdown code blocks.
Do not include multiple JSON objects.
The entire response must be parseable by json.loads().
"""

# ─────────────────────────────────────────
# TOOL DEFINITIONS
# ─────────────────────────────────────────
BUILDER_TOOLS = [
    {
        "name": "save_file",
        "description": "Save generated pipeline file to correct folder. Call this for each component generated — DAG, config, notebook, SQL.",
        "input_schema": {
            "type": "object",
            "properties": {
                "filename": {
                    "type": "string",
                    "description": "Filename with extension e.g. subscribers_dag.py"
                },
                "content": {
                    "type": "string",
                    "description": "Complete file content to save"
                }
            },
            "required": ["filename", "content"]
        }
    },
    {
        "name": "add_to_knowledge_base",
        "description": "Index saved file in ChromaDB knowledge base. Call this after save_file() for each component so Analyst can find it in future.",
        "input_schema": {
            "type": "object",
            "properties": {
                "filename": {
                    "type": "string",
                    "description": "Filename of the saved file"
                },
                "content": {
                    "type": "string",
                    "description": "Content of the file to index"
                }
            },
            "required": ["filename", "content"]
        }
    }
]

# ─────────────────────────────────────────
# TOOL EXECUTOR
# ─────────────────────────────────────────
def execute_builder_tool(tool_name: str, tool_input: dict) -> str:
    if tool_name == "save_file":
        return save_file(tool_input["filename"], tool_input["content"])
    elif tool_name == "add_to_knowledge_base":
        return add_to_knowledge_base(tool_input["filename"], tool_input["content"])
    return f"Unknown tool: {tool_name}"

# ─────────────────────────────────────────
# BUILDER AGENT
# ─────────────────────────────────────────
def run_builder(analyst_spec: dict) -> dict:
    """
    Run Builder Agent with Analyst spec.
    Generates all pipeline components.
    Returns build result as dict.
    """
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    # convert spec to string for Claude
    spec_message = f"Build this pipeline:\n\n{json.dumps(analyst_spec, indent=2)}"
    messages = [{"role": "user", "content": spec_message}]

    while True:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=8192,
            system=BUILDER_SYSTEM_PROMPT,
            tools=BUILDER_TOOLS,
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
                    result = execute_builder_tool(block.name, block.input)
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
            try:
                result = json.loads(reply)
            except json.JSONDecodeError:
                import re
                # try to find JSON between first { and last }
                start = reply.find('{')
                end = reply.rfind('}')
                if start != -1 and end != -1 and end > start:
                    try:
                        result = json.loads(reply[start:end+1])
                    except json.JSONDecodeError:
                        result = {
                            "status": "error",
                            "pipeline_name": "",
                            "files_created": [],
                            "summary": "",
                            "error": "Could not parse builder response"
                        }
                else:
                    result = {
                        "status": "error",
                        "pipeline_name": "",
                        "files_created": [],
                        "summary": "",
                        "error": "No JSON found in builder response"
                    }
            return result