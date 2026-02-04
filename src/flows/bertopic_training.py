from datetime import datetime
import numpy as np
from pathlib import Path
from prefect import flow, task, get_run_logger
from prefect.assets import materialize
from umap import UMAP
from hdbscan import HDBSCAN
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import CountVectorizer

from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired
from bertopic.vectorizers import ClassTfidfTransformer

from voc.storage.factory import get_storage
from voc.storage.types import StorageType
from voc.data_models import SentenceDTO


@task
def load_sentences(app_id: str, storage_type: StorageType, storage_config: dict) -> list[SentenceDTO]:
    logger = get_run_logger()
    logger.info(f"Loading sentences for app {app_id} from {storage_type}")
    
    storage = get_storage(storage_type, config=storage_config)
    
    sentences = storage.get_sentences(app_id)
    if not sentences:
        raise ValueError(f"No sentences found for app {app_id}")
        
    logger.info(f"Loaded {len(sentences)} sentence records")
    return sentences

@materialize(
    "model://bertopic"
)
def save_model(topic_model: BERTopic, app_id: str):
    logger = get_run_logger()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_path = Path(f"data/models/{app_id}_{timestamp}")
    
    model_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Saving model to {model_path}")
    topic_model.save(str(model_path), serialization="safetensors", save_ctfidf=True, save_embedding_model=SentenceTransformer("all-MiniLM-L6-v2"))
    logger.info("Model saved successfully")


@task
def train_model(
    sentences: list[SentenceDTO],
    umap_params: dict,
    hdbscan_params: dict,
    vectorizer_params: dict,
) -> BERTopic:
    logger = get_run_logger()
    logger.info("Initializing and training BERTopic model...")

    docs = [s.sentence for s in sentences]
    # Check if we have embeddings
    embeddings = None
    if sentences and sentences[0].embedding:
         embeddings = np.array([s.embedding for s in sentences])
         logger.info(f"Using pre-calculated embeddings with shape {embeddings.shape}")
    else:
        logger.info("No pre-calculated embeddings found. BERTopic will calculate them.")

    umap_model = UMAP(**umap_params)
    hdbscan_model = HDBSCAN(**hdbscan_params)
    vectorizer_model = CountVectorizer(**vectorizer_params)
    ctfidf_model = ClassTfidfTransformer()
    embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

    # Initialize BERTopic
    topic_model = BERTopic(
        embedding_model=embedding_model,
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        vectorizer_model=vectorizer_model,
        ctfidf_model=ctfidf_model,
    )

    topic_model.fit(docs, embeddings)
    logger.info("BERTopic model training completed.")
    
    return topic_model


@flow(name="BerTopic Training")
def train_bertopic(
    app_id: str,
    umap_params: dict,
    hdbscan_params: dict,
    vectorizer_params: dict,
    storage_type: StorageType,
    storage_config: dict = {}
):
    
    # Step 1 - Load sentences
    sentences = load_sentences(app_id, storage_type, storage_config)

    # Step 2 - Train model
    topic_model = train_model(
        sentences,
        umap_params=umap_params, 
        hdbscan_params=hdbscan_params, 
        vectorizer_params=vectorizer_params
    )
    
    # Step 3 - Save model
    save_model(topic_model, app_id)

if __name__ == "__main__":
    umap_params = {
        "n_neighbors": 15,
        "n_components": 5,
        "min_dist": 0.0,
        "metric": "cosine",
    }
    hdbscan_params = {
        "min_cluster_size": 7,
        "metric": "euclidean",
        "cluster_selection_method": "eom",
        "prediction_data": True,
    }
    vectorizer_params = {"stop_words": "english"}
    train_bertopic(
        app_id="2393760",
        umap_params=umap_params,
        hdbscan_params=hdbscan_params,
        vectorizer_params=vectorizer_params,
        storage_type=StorageType.POSTGRES,
        )