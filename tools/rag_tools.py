"""
rag.py — RAG knowledge base for DE Copilot
Stores and searches generated files using ChromaDB
"""

import chromadb
import os

# ─────────────────────────────────────────
# SETUP — create ChromaDB client
# ─────────────────────────────────────────
client = chromadb.PersistentClient(path="./knowledge_base")
collection = client.get_or_create_collection(
    name="de_agent_studio_knowledge",
    metadata={"heuristic": "cosine"}
)


# ─────────────────────────────────────────
# CHUNKING
# ─────────────────────────────────────────
def chunk_document(text: str, chunk_size: int = 500, overlap: int = 50) -> list[str]:
    """
    Split document into overlapping chunks.
    overlap ensures context isn't lost at boundaries.
    """
    words = text.split()
    chunks = []
    start = 0

    while start < len(words):
        end = start + chunk_size
        chunk = " ".join(words[start:end])
        chunks.append(chunk)
        start += chunk_size - overlap

    return chunks


# ─────────────────────────────────────────
# ADD DOCUMENT
# ─────────────────────────────────────────
def add_to_knowledge_base(filename: str, content: str) -> str:
    """
    Add a document to ChromaDB knowledge base.
    Chunks the document first, adds context to each chunk.
    """
    chunks = chunk_document(content)

    documents = []
    ids = []
    metadatas = []

    for i, chunk in enumerate(chunks):
        # Contextual retrieval — add filename context to each chunk
        contextual_chunk = f"From file: {filename}\n\n{chunk}"
        chunk_id = f"{filename}_chunk_{i}"

        documents.append(contextual_chunk)
        ids.append(chunk_id)
        metadatas.append({
            "filename": filename,
            "chunk_index": i,
            "total_chunks": len(chunks)
        })

    # Avoid duplicate IDs — delete existing if present
    try:
        collection.delete(ids=ids)
    except Exception:
        pass

    collection.add(
        documents=documents,
        ids=ids,
        metadatas=metadatas
    )

    return f"Added {filename} to knowledge base ({len(chunks)} chunks)"


# ─────────────────────────────────────────
# SEARCH
# ─────────────────────────────────────────
def search_knowledge_base(query: str, n_results: int = 3) -> str:
    """
    Search knowledge base for relevant chunks.
    Returns formatted string ready to inject into prompt.
    """
    count = collection.count()
    if count == 0:
        return "Knowledge base is empty. No files indexed yet."

    results = collection.query(
        query_texts=[query],
        n_results=min(n_results, count)
    )

    if not results["documents"][0]:
        return "No relevant documents found."

    # Format results for prompt injection
    formatted = "Relevant context from knowledge base:\n\n"
    for i, (doc, metadata) in enumerate(zip(
        results["documents"][0],
        results["metadatas"][0]
    )):
        formatted += f"--- Source: {metadata['filename']} ---\n"
        formatted += f"{doc}\n\n"

    return formatted


# ─────────────────────────────────────────
# LIST INDEXED FILES
# ─────────────────────────────────────────
def list_indexed_files() -> list[str]:
    """
    Return list of unique filenames in knowledge base.
    """
    count = collection.count()
    if count == 0:
        return []

    results = collection.get()
    filenames = list(set([
        m["filename"] for m in results["metadatas"]
    ]))
    return sorted(filenames)


# ─────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────
if __name__ == "__main__":
    # Test with a sample document
    test_content = """
    Pipeline: top_10_subscribers
    Schedule: 0 7 * * * (every day at 7am)
    Input tables: subscribers, watch_history
    Output table: top_10_subscribers_daily
    
    This pipeline generates the top 10 subscribers
    by total watch time every day at 7am.
    Uses DatabricksSubmitRunOperator.
    Slack alerts on failure and retry.
    """

    print(add_to_knowledge_base("test_pipeline.txt", test_content))
    print("\nIndexed files:", list_indexed_files())
    print("\nSearch result:")
    print(search_knowledge_base("what pipeline runs at 7am?"))