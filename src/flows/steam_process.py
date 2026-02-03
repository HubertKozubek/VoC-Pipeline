import asyncio
from prefect import flow, task
from prefect.logging import get_run_logger
from nltk.tokenize import sent_tokenize
from sentence_transformers import SentenceTransformer
import torch
from transformers import pipeline
import pandas as pd

from voc.storage.factory import get_storage
from voc.storage.types import StorageType


@task(log_prints=True)
def load_raw_reviews(app_id: str, storage_type: StorageType) -> list[dict]:
    logger = get_run_logger()
    storage = get_storage(storage_type, app_id)
    reviews = storage.load()
    logger.info(f"Loaded {len(reviews)} raw reviews for app {app_id}")
    return reviews


@task(log_prints=True)
def split_reviews(reviews: list[dict], app_id: str):
    logger = get_run_logger()
    sentences = []
    
    for review in reviews:
        rid = review.get("recommendationid")
        text = review.get("review", "")
        
        if not text:
            continue
            
        for sent in sent_tokenize(text):
            sentences.append({
                "recommendationid": rid,
                "sentence": sent
            })
    
    logger.info(f"Split {len(sentences)} sentences for app {app_id}")
    return sentences


@task(log_prints=True)
def calculate_sentiment(sentences: list[dict], app_id: str):
    logger = get_run_logger()
    
    device = 0 if torch.cuda.is_available() else -1
    logger.info(f"Using device: {device}")

    model_path = "cardiffnlp/twitter-roberta-base-sentiment-latest"
    sentiment_pipeline = pipeline("sentiment-analysis", model=model_path, tokenizer=model_path, device=device)

    docs = [item["sentence"] for item in sentences]
    logger.info(f"Calculating sentiment for {len(docs)} sentences...")

    # Batch processing
    sentiments = sentiment_pipeline(docs, batch_size=32, truncation=True)

    def map_label(label):
        l = label.lower()
        if 'negative' in l: return 'NEGATIVE'
        if 'neutral' in l: return 'NEUTRAL'
        if 'positive' in l: return 'POSITIVE'
        return label

    results = []
    for item, s in zip(sentences, sentiments):
        new_item = item.copy()
        new_item['sentiment'] = map_label(s['label'])
        new_item['score'] = s['score']
        results.append(new_item)
    
    try:
        df = pd.DataFrame(results)
        if not df.empty:
            logger.info(f"Sentiment distribution:\n{df['sentiment'].value_counts()}")
    except ImportError:
        pass

    logger.info(f"Calculated sentiment for {len(sentences)} sentences")
    return results


@task(log_prints=True)
def calculate_embeddings(sentences: list[dict], app_id: str):
    logger = get_run_logger()
    embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
    
    texts = [item["sentence"] for item in sentences]
    
    embeddings = embedding_model.encode(texts, show_progress_bar=True)
    
    results = []
    for item, embedding in zip(sentences, embeddings):
        res = item.copy()
        res["embedding"] = embedding.tolist()
        results.append(res)
        
    logger.info(f"Calculated embeddings for {len(sentences)} sentences for app {app_id}")
    return results


@task(log_prints=True)
def save_dataset(data: list[dict], app_id: str, storage_type: StorageType, base_path: str, dataset_name: str):
    logger = get_run_logger()
    storage = get_storage(storage_type, app_id, base_path=base_path)
    storage.save(data, dataset_name=dataset_name)
    logger.info(f"Saved {len(data)} records to {base_path}/{dataset_name}")


@flow(name="Steam Reviews Process (Silver)")
def process_steam_reviews(app_id: str, base_path: str, storage_type: StorageType):
    reviews = load_raw_reviews(app_id, storage_type)
    if not reviews:
        return
        
    sentences = split_reviews(reviews, app_id)
    save_dataset(sentences, app_id, storage_type, base_path, "sentences")
    
    sentences_sentiment = calculate_sentiment(sentences, app_id)
    
    sentences_embeddings = calculate_embeddings(sentences_sentiment, app_id)
    save_dataset(sentences_embeddings, app_id, storage_type, base_path, "embeddings")


if __name__ == "__main__":
    process_steam_reviews(app_id="2393760", storage_type=StorageType.PARQUET, base_path = "data/silver")
