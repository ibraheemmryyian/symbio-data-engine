"""
Symbio Data Engine - ChromaDB Vector Store
==========================================
Semantic search over documents using embeddings.
"""

import logging
from typing import Optional

import chromadb
from chromadb.config import Settings

import config

logger = logging.getLogger(__name__)

# Global client (initialized lazily)
_chroma_client: Optional[chromadb.ClientAPI] = None

# Collection names
COLLECTIONS = {
    "documents": "symbio_documents",
    "waste": "symbio_waste_listings",
    "carbon": "symbio_carbon_reports",
    "symbiosis": "symbio_exchanges",
}


def get_client() -> chromadb.ClientAPI:
    """Get or create the ChromaDB client."""
    global _chroma_client
    
    if _chroma_client is None:
        try:
            # Try HTTP client first (for Docker deployment)
            _chroma_client = chromadb.HttpClient(
                host=config.CHROMA_HOST,
                port=config.CHROMA_PORT,
            )
            # Test connection
            _chroma_client.heartbeat()
            logger.info(f"Connected to ChromaDB at {config.CHROMA_HOST}:{config.CHROMA_PORT}")
        except Exception as e:
            logger.warning(f"HTTP client failed ({e}), falling back to persistent client")
            # Fall back to persistent local client
            config.CHROMA_PERSIST_DIR.mkdir(parents=True, exist_ok=True)
            _chroma_client = chromadb.PersistentClient(
                path=str(config.CHROMA_PERSIST_DIR),
                settings=Settings(anonymized_telemetry=False),
            )
            logger.info(f"Using persistent ChromaDB at {config.CHROMA_PERSIST_DIR}")
    
    return _chroma_client


def get_vectorstore(collection_name: str = "documents") -> chromadb.Collection:
    """
    Get or create a ChromaDB collection.
    
    Args:
        collection_name: Key from COLLECTIONS dict or custom name
    
    Returns:
        ChromaDB Collection object
    """
    client = get_client()
    
    # Resolve collection name from alias
    full_name = COLLECTIONS.get(collection_name, collection_name)
    
    collection = client.get_or_create_collection(
        name=full_name,
        metadata={"hnsw:space": "cosine"},  # Use cosine similarity
    )
    
    return collection


def init_vectorstore() -> None:
    """Initialize all ChromaDB collections."""
    client = get_client()
    
    for alias, name in COLLECTIONS.items():
        collection = client.get_or_create_collection(
            name=name,
            metadata={"hnsw:space": "cosine"},
        )
        logger.info(f"Collection '{alias}' ({name}): {collection.count()} documents")
    
    logger.info("ChromaDB initialized successfully")


def add_documents(
    collection_name: str,
    documents: list[str],
    metadatas: list[dict],
    ids: list[str],
    batch_size: int = 1000,
) -> None:
    """
    Add documents to a collection.
    
    🛡️ Chunks large batches to prevent OOM errors.
    
    Args:
        collection_name: Collection to add to
        documents: List of text content
        metadatas: List of metadata dicts
        ids: List of unique IDs
        batch_size: Max documents per batch (default 1000)
    """
    collection = get_vectorstore(collection_name)
    
    total = len(documents)
    
    # 🛡️ MEMORY SAFETY: Process in chunks
    for i in range(0, total, batch_size):
        end = min(i + batch_size, total)
        collection.add(
            documents=documents[i:end],
            metadatas=metadatas[i:end],
            ids=ids[i:end],
        )
        logger.debug(f"Added batch {i//batch_size + 1}: {end - i} documents")
    
    logger.info(f"Added {total} documents to {collection_name}")


def add_embeddings(
    collection_name: str,
    embeddings: list[list[float]],
    metadatas: list[dict],
    ids: list[str],
    documents: list[str] = None,
) -> None:
    """
    Add pre-computed embeddings to a collection.
    
    Args:
        collection_name: Collection to add to
        embeddings: List of embedding vectors
        metadatas: List of metadata dicts
        ids: List of unique IDs
        documents: Optional original text (for display)
    """
    collection = get_vectorstore(collection_name)
    
    collection.add(
        embeddings=embeddings,
        metadatas=metadatas,
        ids=ids,
        documents=documents,
    )
    
    logger.info(f"Added {len(embeddings)} embeddings to {collection_name}")


def search(
    collection_name: str,
    query: str,
    n_results: int = 10,
    where: dict = None,
    where_document: dict = None,
) -> dict:
    """
    Semantic search in a collection.
    
    Args:
        collection_name: Collection to search
        query: Search query text
        n_results: Number of results to return
        where: Metadata filter
        where_document: Document content filter
    
    Returns:
        Dict with ids, documents, metadatas, distances
    """
    collection = get_vectorstore(collection_name)
    
    results = collection.query(
        query_texts=[query],
        n_results=n_results,
        where=where,
        where_document=where_document,
    )
    
    return results


def search_by_embedding(
    collection_name: str,
    embedding: list[float],
    n_results: int = 10,
    where: dict = None,
) -> dict:
    """
    Search by pre-computed embedding vector.
    
    Args:
        collection_name: Collection to search
        embedding: Query embedding vector
        n_results: Number of results to return
        where: Metadata filter
    
    Returns:
        Dict with ids, documents, metadatas, distances
    """
    collection = get_vectorstore(collection_name)
    
    results = collection.query(
        query_embeddings=[embedding],
        n_results=n_results,
        where=where,
    )
    
    return results


def get_document(collection_name: str, doc_id: str) -> Optional[dict]:
    """
    Get a specific document by ID.
    
    Args:
        collection_name: Collection to search
        doc_id: Document ID
    
    Returns:
        Dict with document, metadata, or None if not found
    """
    collection = get_vectorstore(collection_name)
    
    result = collection.get(ids=[doc_id], include=["documents", "metadatas"])
    
    if result["ids"]:
        return {
            "id": result["ids"][0],
            "document": result["documents"][0] if result["documents"] else None,
            "metadata": result["metadatas"][0] if result["metadatas"] else None,
        }
    
    return None


def delete_documents(collection_name: str, ids: list[str]) -> None:
    """Delete documents by ID."""
    collection = get_vectorstore(collection_name)
    collection.delete(ids=ids)
    logger.info(f"Deleted {len(ids)} documents from {collection_name}")


def get_collection_stats() -> dict:
    """Get statistics for all collections."""
    client = get_client()
    
    stats = {}
    for alias, name in COLLECTIONS.items():
        try:
            collection = client.get_collection(name)
            stats[alias] = {
                "name": name,
                "count": collection.count(),
            }
        except Exception:
            stats[alias] = {
                "name": name,
                "count": 0,
                "error": "Collection not found",
            }
    
    return stats


def reset_collection(collection_name: str) -> None:
    """Delete and recreate a collection."""
    client = get_client()
    full_name = COLLECTIONS.get(collection_name, collection_name)
    
    try:
        client.delete_collection(full_name)
        logger.warning(f"Deleted collection {full_name}")
    except Exception:
        pass
    
    client.create_collection(
        name=full_name,
        metadata={"hnsw:space": "cosine"},
    )
    logger.info(f"Recreated collection {full_name}")
