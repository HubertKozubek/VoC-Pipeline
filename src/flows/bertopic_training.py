from datetime import datetime
import numpy as np
from pathlib import Path
from prefect import flow, task, get_run_logger
from umap import UMAP
from hdbscan import HDBSCAN
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import CountVectorizer

from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired
from bertopic.vectorizers import ClassTfidfTransformer

from voc.storage.factory import get_storage
from voc.storage.types import StorageType


@task
def load_embeddings(app_id: str, storage_type: StorageType):
    logger = get_run_logger()
    logger.info(f"Loading embeddings for app {app_id}")
    
    storage = get_storage(storage_type, app_id, base_path="data/embeddings")
    
    data = storage.load()
    if not data:
        raise ValueError(f"No embeddings found for app {app_id} in data/embeddings")
        
    logger.info(f"Loaded {len(data)} embedding records")
    
    docs = [item["sentence"] for item in data]

    embeddings = np.array([item["embedding"] for item in data])
    
    return docs, embeddings


@task
def save_model(topic_model: BERTopic, app_id: str):
    logger = get_run_logger()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_path = Path(f"data/models/{app_id}_{timestamp}")
    
    model_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Saving model to {model_path}")
    topic_model.save(str(model_path), serialization="safetensors", save_ctfidf=True, save_embedding_model=SentenceTransformer("all-MiniLM-L6-v2"))
    logger.info("Model saved successfully")


@flow(name="BerTopic Training")
def train_bertopic(app_id: str):
    
    # Step 1 - Load embeddings
    docs, embeddings = load_embeddings(app_id, StorageType.PARQUET)

    # Step 2 - Reduce dimensionality
    umap_model = UMAP(n_neighbors=15, n_components=5, min_dist=0.0, metric='cosine')

    # Step 3 - Cluster reduced embeddings
    hdbscan_model = HDBSCAN(min_cluster_size=15, metric='euclidean', cluster_selection_method='eom', prediction_data=True)

    # Step 4 - Tokenize topics
    vectorizer_model = CountVectorizer(stop_words="english")

    # Step 5 - Create topic representation
    ctfidf_model = ClassTfidfTransformer()

    # Step 6 - (Optional) Fine-tune topic representations with
    # a `bertopic.representation` model
    representation_model = KeyBERTInspired()

    # All steps together
    topic_model = BERTopic(
        embedding_model=SentenceTransformer("all-MiniLM-L6-v2"), # Explicitly use the model used for embeddings
        umap_model=umap_model,                    # Step 2 - Reduce dimensionality
        hdbscan_model=hdbscan_model,              # Step 3 - Cluster reduced embeddings
        vectorizer_model=vectorizer_model,        # Step 4 - Tokenize topics
        ctfidf_model=ctfidf_model,                # Step 5 - Extract topic words
        # representation_model=representation_model # Step 6 - (Optional) Fine-tune topic representations
    )
    
    topic_model.fit(docs, embeddings)
    
    save_model(topic_model, app_id)

if __name__ == "__main__":
    train_bertopic(app_id="2393760")