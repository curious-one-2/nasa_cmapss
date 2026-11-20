import chromadb
from chromadb.utils import embedding_functions


def test_semantic_search():
    db_path = "data/vector_store"
    collection_name = "asrs_incidents"
    
    print(f"Connecting to ChromaDB at {db_path}...")
    client = chromadb.PersistentClient(path=db_path)

    # Initialize Ollama embedding function
    ollama_ef = embedding_functions.OllamaEmbeddingFunction(
        url="http://localhost:11434/api/embeddings",
        model_name="nomic-embed-text"
    )

    collection = client.get_collection(
        name=collection_name,
        embedding_function=ollama_ef
    )

    # Test search query targeting a specific failure mode
    query_text = "tailpipe fire and burning oil leak"
    print(f"\nRunning semantic query: '{query_text}'\n")

    results = collection.query(
        query_texts=[query_text],
        n_results=2
    )

    # Display matching results
    for i, (doc_id, narrative, distance) in enumerate(zip(
        results['ids'][0],
        results['documents'][0],
        results['distances'][0]
    )):
        print(f"--- Match {i+1} [ID: {doc_id} | Distance: {distance:.4f}] ---")
        print(f"{narrative}\n")


if __name__ == "__main__":
    test_semantic_search()