import asyncio
from prefect import flow, task
from prefect.logging import get_run_logger
import httpx
from datetime import datetime, timedelta
from nltk.tokenize import sent_tokenize, word_tokenize
from sentence_transformers import SentenceTransformer

from voc.storage.factory import get_storage
from voc.storage.types import StorageType
from voc.ingestion.providers.steam import SteamReviewProvider, SteamReview


@task(log_prints=True)
async def fetch_steam_reviews(app_id: str, since: datetime | None = None):
    logger = get_run_logger()
    provider = SteamReviewProvider(app_id=app_id)
    review_list = [r async for r in provider.fetch_reviews(since=since)]
    logger.info(f"Fetched {len(review_list)} reviews for app {app_id}")
    return review_list


@task(log_prints=True)
def save_reviews(reviews: list[SteamReview], app_id: str, storage_type: StorageType):
    logger = get_run_logger()
    storage = get_storage(storage_type, app_id)
    storage.save_reviews(reviews)
    logger.info(f"Saved {len(reviews)} reviews for app {app_id}")

@task(log_prints=True)
def split_reviews(reviews: list[SteamReview], app_id: str, storage_type: StorageType):
    logger = get_run_logger()
    sentences = [{
        "review_id": review.recommendationid,
        "sentence": sent_tokenize(review.review)
        } for review in reviews]
    sentences = [{
        "review_id": doc["review_id"],
        "sentence": sentence
    } for doc in sentences for sentence in doc["sentence"]]
    logger.info(f"Split {len(sentences)} sentences for app {app_id}")
    return sentences
    

@task(log_prints=True)
def calculate_embeddings(sentences: list[dict], app_id: str, storage_type: StorageType):
    logger = get_run_logger()
    embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
    
    texts = [item["sentence"] for item in sentences]
    
    embeddings = embedding_model.encode(texts, show_progress_bar=True)
    
    results = []
    for item, embedding in zip(sentences, embeddings):
        results.append({
            "recommendationid": item["review_id"],
            "sentence": item["sentence"],
            "embedding": embedding.tolist()
        })
        
    logger.info(f"Calculated embeddings for {len(sentences)} sentences for app {app_id}")
    return results

@task(log_prints=True)
def save_embeddings(embeddings: list[dict], app_id: str, storage_type: StorageType):
    logger = get_run_logger()
    storage = get_storage(storage_type, app_id, base_path="data/embeddings")
    storage.save_reviews(embeddings)
    logger.info(f"Saved {len(embeddings)} embeddings for app {app_id}")


@flow(name="Steam Reviews Ingest")
async def ingest_steam_reviews(
    app_id: str,
    storage_type: StorageType,
    lookback_days: int | None = None
):
    if lookback_days:
        since = datetime.now() - timedelta(days=lookback_days)
    else:
        since = None
    
    reviews = await fetch_steam_reviews(app_id, since)
    save_reviews(reviews, app_id, storage_type)
    
    if reviews:
        sentences = split_reviews(reviews, app_id, storage_type)
        embeddings = calculate_embeddings(sentences, app_id, storage_type)
        save_embeddings(embeddings, app_id, storage_type)

if __name__ == "__main__":
    asyncio.run(ingest_steam_reviews(app_id = "2393760", storage_type = StorageType.PARQUET))


