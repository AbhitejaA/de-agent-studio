"""
app.py — Coordinator + Streamlit UI for de-agent-studio
Orchestrates Analyst, Builder, and QA agents
"""

import streamlit as st
import json
import os
from dotenv import load_dotenv
from agents.analyst import run_analyst
from agents.builder import run_builder
from agents.qa import run_qa
from tools.file_tools import list_all_files
from tools.rag_tools import list_indexed_files

load_dotenv()

# ─────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────
st.set_page_config(
    page_title="DE Agent Studio",
    page_icon="🏗️",
    layout="wide"
)

# ─────────────────────────────────────────
# CUSTOM CSS
# ─────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Inter:wght@400;500;600&display=swap');
    .stApp { background-color: #0d1117; color: #e6edf3; font-family: 'Inter', sans-serif; }
    [data-testid="stSidebar"] { background-color: #161b22; border-right: 1px solid #30363d; }
    .agent-card {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.82rem;
    }
    .agent-running { border-color: #f0883e; }
    .agent-done    { border-color: #3fb950; }
    .agent-waiting { border-color: #30363d; }
    .critical  { color: #f85149; }
    .warning   { color: #f0883e; }
    .suggestion{ color: #58a6ff; }
    .passed    { color: #3fb950; }
    .sidebar-label {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.7rem;
        color: #8b949e;
        letter-spacing: 1px;
        text-transform: uppercase;
        display: block;
        margin-bottom: 0.5rem;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────
defaults = {
    "analyst_result": None,
    "builder_result": None,
    "qa_result": None,
    "current_request": None,
    "stage": "idle",  # idle, analysing, building, reviewing, done
    "activity_log": []
}
for key, val in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = val

# ─────────────────────────────────────────
# COORDINATOR
# ─────────────────────────────────────────
def coordinate(user_request: str):
    """Orchestrate all three agents sequentially."""

    # Step 1 — Analyst Agent
    st.session_state.stage = "analysing"
    st.session_state.activity_log.append("🔍 Analyst Agent: analysing requirement...")
    
    analyst_result = run_analyst(user_request)
    st.session_state.analyst_result = analyst_result
    st.session_state.activity_log.append(
        f"✅ Analyst Agent: classified as '{analyst_result.get('type', 'unknown')}' — {analyst_result.get('pipeline_name', '')}"
    )

    # Step 2 — Builder Agent
    st.session_state.stage = "building"
    st.session_state.activity_log.append("🔨 Builder Agent: generating pipeline files...")

    builder_result = run_builder(analyst_result)
    st.session_state.builder_result = builder_result
    st.session_state.activity_log.append(
        f"✅ Builder Agent: created {len(builder_result.get('files_created', []))} files"
    )

    # Step 3 — QA Agent
    st.session_state.stage = "reviewing"
    st.session_state.activity_log.append("🔎 QA Agent: reviewing pipeline...")

    qa_result = run_qa(builder_result, user_request)
    st.session_state.qa_result = qa_result
    st.session_state.activity_log.append(
        f"✅ QA Agent: review complete — {qa_result.get('overall_status', 'unknown').upper()}"
    )

    st.session_state.stage = "done"
    st.session_state.current_request = user_request

# ─────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────
with st.sidebar:
    st.markdown('<span class="sidebar-label">📁 Generated Files</span>', unsafe_allow_html=True)
    all_files = list_all_files()
    if all_files:
        for folder, files in all_files.items():
            st.markdown(
                f'<span style="color:#8b949e;font-size:0.75rem">{folder}/</span>',
                unsafe_allow_html=True
            )
            for f in files:
                filepath = os.path.join(folder, f)
                try:
                    with open(filepath, "r") as fh:
                        content = fh.read()
                    icon = "🐍" if f.endswith(".py") else "⚙️" if f.endswith(".yml") else "🗄️"
                    st.download_button(
                        label=f"{icon} {f}",
                        data=content,
                        file_name=f,
                        mime="text/plain",
                        key=f"dl_{f}"
                    )
                except Exception:
                    st.caption(f)
    else:
        st.caption("No files yet")

    st.divider()

    st.markdown('<span class="sidebar-label">🧠 Knowledge Base</span>', unsafe_allow_html=True)
    indexed = list_indexed_files()
    if indexed:
        for f in indexed:
            st.markdown(
                f'<span style="color:#3fb950;font-family:JetBrains Mono,monospace;font-size:0.75rem">● {f}</span>',
                unsafe_allow_html=True
            )
    else:
        st.caption("Empty")

    st.divider()

    if st.button("🗑️ Clear Session", use_container_width=True):
        for key, val in defaults.items():
            st.session_state[key] = val
        st.rerun()

# ─────────────────────────────────────────
# MAIN UI
# ─────────────────────────────────────────
st.markdown("""
<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:1.25rem 1.5rem;margin-bottom:1.5rem;display:flex;align-items:center">
    <div>
        <p style="font-family:JetBrains Mono,monospace;font-size:1.6rem;font-weight:600;color:#58a6ff;margin:0">
            🏗️ DE Agent Studio
        </p>
        <p style="font-family:JetBrains Mono,monospace;font-size:0.8rem;color:#8b949e;margin:0">
            // Multi Agent Pipeline Builder
        </p>
    </div>
    <span style="margin-left:auto;background:#1f6feb22;border:1px solid #1f6feb;color:#58a6ff;padding:3px 12px;border-radius:20px;font-size:0.7rem;font-family:JetBrains Mono,monospace">
        3 AGENTS · CLAUDE
    </span>
</div>
""", unsafe_allow_html=True)

# Input
user_request = st.text_area(
    "Describe your pipeline requirement:",
    placeholder="e.g. Build a daily pipeline to find top 10 subscribers by watch time, run every day at 7am",
    height=100
)

col1, col2 = st.columns([1, 4])
with col1:
    build_button = st.button("🚀 Build Pipeline", use_container_width=True)

if build_button and user_request:
    # reset previous results
    for key, val in defaults.items():
        st.session_state[key] = val

    # run coordinator
    with st.status("Running agents...", expanded=True) as status:
        st.write("🔍 Analyst Agent: analysing requirement...")
        analyst_result = run_analyst(user_request)
        st.session_state.analyst_result = analyst_result
        st.write(f"✅ Analyst done — {analyst_result.get('pipeline_name', '')}")
        
        st.write("🔨 Builder Agent: generating pipeline files...")
        builder_result = run_builder(analyst_result)
        st.session_state.builder_result = builder_result
        st.write(f"✅ Builder done — {len(builder_result.get('files_created', []))} files created")
        
        st.write("🔎 QA Agent: reviewing pipeline...")
        qa_result = run_qa(builder_result, user_request)
        st.session_state.qa_result = qa_result
        st.write(f"✅ QA done — {qa_result.get('overall_status', '').upper()}")
        
        status.update(label="✅ All agents complete!", state="complete")

    st.session_state.stage = "done"
    st.session_state.current_request = user_request
    st.rerun()

# ─────────────────────────────────────────
# RESULTS
# ─────────────────────────────────────────
if st.session_state.stage == "done":

    # Activity log
    st.markdown("#### 📋 Agent Activity")
    for log in st.session_state.activity_log:
        st.markdown(
            f'<div class="agent-card agent-done">{log}</div>',
            unsafe_allow_html=True
        )

    st.divider()

    # Two columns — Analyst spec + Builder result
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 🔍 Analyst Spec")
        if st.session_state.analyst_result:
            spec = st.session_state.analyst_result
            st.markdown(f"**Type:** `{spec.get('type', '')}`")
            st.markdown(f"**Pipeline:** `{spec.get('pipeline_name', '')}`")
            st.markdown(f"**Schedule:** `{spec.get('schedule', '')}`")
            st.markdown(f"**Tables:** `{', '.join(spec.get('input_tables', []))}`")
            st.markdown(f"**Logic:** {spec.get('logic', '')}")
            if spec.get('assumptions'):
                st.markdown("**Assumptions:**")
                for a in spec.get('assumptions', []):
                    st.markdown(f"→ {a}")

    with col2:
        st.markdown("#### 🔨 Builder Result")
        if st.session_state.builder_result:
            result = st.session_state.builder_result
            status_color = "#3fb950" if result.get('status') == 'success' else "#f85149"
            st.markdown(
                f'<span style="color:{status_color}">● {result.get("status", "").upper()}</span>',
                unsafe_allow_html=True
            )
            st.markdown(f"**Pipeline:** `{result.get('pipeline_name', '')}`")
            st.markdown("**Files created:**")
            for f in result.get('files_created', []):
                st.markdown(f"→ `{f}`")
            st.markdown(f"**Summary:** {result.get('summary', '')}")

    st.divider()

    # QA Report
    st.markdown("#### 🔎 QA Report")
    if st.session_state.qa_result:
        qa = st.session_state.qa_result
        status = qa.get('overall_status', 'unknown')
        status_color = "#3fb950" if status == "pass" else "#f85149"

        st.markdown(
            f'<span style="color:{status_color};font-size:1.1rem">● {status.upper()}</span>',
            unsafe_allow_html=True
        )
        st.markdown(f"_{qa.get('summary', '')}_")

        # Critical
        if qa.get('critical'):
            st.markdown("**🔴 Critical Issues:**")
            for issue in qa['critical']:
                st.markdown(
                    f'<div class="agent-card critical">📁 {issue.get("file", "")}<br>'
                    f'❌ {issue.get("issue", "")}<br>'
                    f'💡 {issue.get("suggestion", "")}</div>',
                    unsafe_allow_html=True
                )

        # Warnings
        if qa.get('warnings'):
            st.markdown("**🟡 Warnings:**")
            for issue in qa['warnings']:
                st.markdown(
                    f'<div class="agent-card warning">📁 {issue.get("file", "")}<br>'
                    f'⚠️ {issue.get("issue", "")}<br>'
                    f'💡 {issue.get("suggestion", "")}</div>',
                    unsafe_allow_html=True
                )

        # Suggestions
        if qa.get('suggestions'):
            st.markdown("**🟢 Suggestions:**")
            for issue in qa['suggestions']:
                st.markdown(
                    f'<div class="agent-card suggestion">📁 {issue.get("file", "")}<br>'
                    f'💡 {issue.get("issue", "")}<br>'
                    f'✨ {issue.get("suggestion", "")}</div>',
                    unsafe_allow_html=True
                )

    st.divider()

    # Apply fixes section
    st.markdown("#### 🔧 Apply Fixes")
    fix_request = st.text_input(
        "What would you like to fix?",
        placeholder="e.g. Apply all critical fixes, or: fix the join issue in the SQL"
    )
    if st.button("Apply Fixes") and fix_request:
        with st.spinner("Applying fixes..."):
            # Build clear fix instruction for Builder
            fix_message = f"""
            You previously built this pipeline:
            {json.dumps(st.session_state.analyst_result, indent=2)}

            The QA Agent found these critical issues:
            {json.dumps(st.session_state.qa_result.get('critical', []), indent=2)}

            The user wants to apply these fixes:
            {fix_request}

            Please regenerate all pipeline files with fixes applied.
            Return the same JSON format as before.
            """
            builder_result = run_builder({
                "fix_message": fix_message,
                "pipeline_name": st.session_state.analyst_result.get('pipeline_name'),
                "type": "fix"
            })
            st.session_state.builder_result = builder_result
            st.session_state.activity_log.append(f"🔧 Fixes applied: {fix_request}")

            # Run QA again on fixed files
            if builder_result.get('status') == 'success':
                qa_result = run_qa(builder_result, st.session_state.current_request)
                st.session_state.qa_result = qa_result
                st.session_state.activity_log.append(
                    f"✅ QA re-run complete — {qa_result.get('overall_status', '').upper()}"
                )
        st.rerun()