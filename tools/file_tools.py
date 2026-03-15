"""
file_tools.py — File operations for de-agent-studio
Save, read, and list generated pipeline files
"""

import os

# ─────────────────────────────────────────
# FOLDER MAPPING
# ─────────────────────────────────────────
def get_folder(filename: str) -> str:
    """Determine correct folder based on filename."""
    if filename.endswith("_dag.py"):
        return "pipelines"
    elif filename.endswith("_notebook.py"):
        return "notebooks"
    elif filename.endswith(".yml"):
        return "configs"
    elif filename.endswith(".sql"):
        return "pipelines"
    else:
        return "pipelines"


# ─────────────────────────────────────────
# SAVE FILE
# ─────────────────────────────────────────
def save_file(filename: str, content: str) -> str:
    """Save generated file to correct folder."""
    folder = get_folder(filename)
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)
    with open(filepath, "w") as f:
        f.write(content)
    return f"Saved: {filepath}"


# ─────────────────────────────────────────
# READ FILE
# ─────────────────────────────────────────
def read_file(filename: str) -> str:
    """Read file content from any output folder."""
    for folder in ["pipelines", "notebooks", "configs"]:
        filepath = os.path.join(folder, filename)
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                return f.read()
    return f"File not found: {filename}"


# ─────────────────────────────────────────
# LIST FILES
# ─────────────────────────────────────────
def list_all_files() -> dict:
    """List all files across all output folders."""
    result = {}
    for folder in ["pipelines", "notebooks", "configs"]:
        if os.path.exists(folder):
            files = os.listdir(folder)
            if files:
                result[folder] = sorted(files)
    return result