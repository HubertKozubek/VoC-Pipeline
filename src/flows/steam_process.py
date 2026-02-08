import asyncio
from prefect import flow, task
from prefect.assets import materialize
from prefect.artifacts import create_markdown_artifact
from prefect.logging import get_run_logger
from nltk.tokenize import sent_tokenize
from sentence_transformers import SentenceTransformer
import torch
from transformers import pipeline, AutoTokenizer
import pandas as pd
from typing import Any, Dict

from voc.storage.factory import get_storage
from voc.storage.types import StorageType
from voc.data_models import SteamReview, SentenceDTO


class ModelManager:
    _instance = None
    
    def __init__(self):
        self.logger = get_run_logger()
        self.logger.info("Initializing ModelManager...")
        device = 0 if torch.cuda.is_available() else -1
        self.logger.info(f"Using device: {device}")

        model_path = "cardiffnlp/twitter-roberta-base-sentiment-latest"
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
        self.sentiment_pipeline = pipeline("sentiment-analysis", model=model_path, tokenizer=self.tokenizer, device=device)
        
        self.embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

        self.logger.info("Models loaded successfully.")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance


@task(log_prints=True)
def load_models_task():
    ModelManager.get_instance()


@task(log_prints=True)
def load_review_batch(app_id: str, storage_type: StorageType, storage_config: dict, limit: int, offset: int) -> list[SteamReview]:
    storage = get_storage(storage_type, config=storage_config)
    reviews = storage.get_reviews_batch(app_id, limit=limit, offset=offset)
    return reviews


@task(log_prints=True)
def split_reviews(reviews: list[SteamReview], app_id: str) -> list[SentenceDTO]:
    sentences = []
    
    for review in reviews:
        rid = review.recommendationid
        text = review.review
        
        if not text:
            continue
            
        for sent in sent_tokenize(text):
            dto = SentenceDTO(
                recommendationid=rid,
                sentence=sent,
                review=text,
                app_id=app_id
            )
            sentences.append(dto)
    
    return sentences


@task(log_prints=True)
def calculate_sentiment(sentences: list[SentenceDTO]) -> list[SentenceDTO]:
    logger = get_run_logger()
    if not sentences:
        return []

    manager = ModelManager.get_instance()
    sentiment_pipeline = manager.sentiment_pipeline
    tokenizer = manager.tokenizer
    
    docs = [item.sentence for item in sentences]

    vocab_size = tokenizer.vocab_size
    max_len = 512
    
    safe_docs = []
    for i, doc in enumerate(docs):
        try:
             encoded = tokenizer(doc, truncation=True, max_length=max_len, add_special_tokens=True)
             ids = encoded['input_ids']
             if any(id >= vocab_size for id in ids):
                 logger.warning(f"Skipping doc {i} due to out-of-vocab IDs.")
                 safe_docs.append("")
             else:
                 safe_docs.append(doc)
        except Exception as e:
            logger.warning(f"Error validating doc {i}: {e}. Skipping.")
            safe_docs.append("")

    sentiments = sentiment_pipeline(safe_docs, batch_size=32, truncation=True, max_length=max_len)

    def map_label(label):
        l = label.lower()
        if 'negative' in l: return 'NEGATIVE'
        if 'neutral' in l: return 'NEUTRAL'
        if 'positive' in l: return 'POSITIVE'
        return label

    for item, s in zip(sentences, sentiments):
        item.sentiment = map_label(s['label'])
        item.score = s['score']
    
    return sentences


@task(log_prints=True)
def calculate_embeddings(sentences: list[SentenceDTO]) -> list[SentenceDTO]:
    if not sentences:
        return []
        
    # Access singleton
    manager = ModelManager.get_instance()
    embedding_model = manager.embedding_model
    
    texts = [item.sentence for item in sentences]
    
    embeddings = embedding_model.encode(texts, show_progress_bar=False)
    
    for item, embedding in zip(sentences, embeddings):
        item.embedding = embedding.tolist()
        
    return sentences


@materialize("postgres://silver/sentences", log_prints=True)
def save_processed_data(data: list[SentenceDTO], app_id: str, storage_type: StorageType, storage_config: dict):
    storage = get_storage(storage_type, config=storage_config)
    
    storage.save_sentences(data)


@flow(name="Process Batch Subflow")
def process_batch(reviews: list[SteamReview], app_id: str, storage_type: StorageType, storage_config: dict):
    if not reviews:
        return

    sentences = split_reviews(reviews, app_id)
    if not sentences:
        return
        
    sentences_sentiment = calculate_sentiment(sentences)
    sentences_embeddings = calculate_embeddings(sentences_sentiment)
    
    save_processed_data(sentences_embeddings, app_id, storage_type, storage_config)


@flow(name="Steam Reviews Process (Silver)")
def process_steam_reviews(
    app_id: str,
    storage_type: StorageType,
    storage_config: dict
):
    logger = get_run_logger()
    
    load_models_task()
    
    batch_size = 100 
    offset = 0
    total_processed = 0
    
    while True:
        reviews = load_review_batch(app_id, storage_type, storage_config, limit=batch_size, offset=offset)
        
        if not reviews:
            logger.info("No more reviews to process.")
            break
            
        process_batch(reviews, app_id, storage_type, storage_config)
        
        count = len(reviews)
        total_processed += count
        offset += count

    logger.info(f"Processing complete. Total reviews: {total_processed}")


if __name__ == "__main__":
    # "Tall Trails" for tests, small dataset ~ 500 reviews
    process_steam_reviews(app_id="2393760", storage_type=StorageType.POSTGRES, storage_config={})
    
    # Hollow Knight: Silksong
    # process_steam_reviews(app_id="1030300", storage_type=StorageType.POSTGRES, storage_config={})
