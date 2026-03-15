# 🏗️ DE Agent Studio

Multi-agent AI system that builds and reviews Airflow data pipelines using 3 specialized Claude agents.

## What it does
Describe a pipeline requirement in plain English → three AI agents collaborate to build, generate, and review production-ready pipeline code.

## The 3 Agents

| Agent | Role |
|-------|------|
| 🔍 Analyst | Classifies request as new or existing, searches knowledge base, produces structured spec |
| 🔨 Builder | Receives spec, generates SQL, Airflow DAG, YAML config, and Databricks notebook |
| 🔎 QA | Independently reviews all files, returns structured report with Critical/Warning/Suggestion findings |

## Tech Stack
- Claude API (Anthropic) — all three agents
- Python, Streamlit — UI and orchestration
- ChromaDB — RAG knowledge base for existing pipelines
- Airflow, Databricks, Snowflake patterns

## Run Locally
```bash
git clone https://github.com/AbhitejaA/de-agent-studio
cd de-agent-studio
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```