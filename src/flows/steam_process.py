import asyncio
from prefect import flow, task
from prefect.assets import materialize
from prefect.artifacts import create_markdown_artifact
from prefect.logging import get_run_logger
from nltk.tokenize import sent_tokenize
from sentence_transformers import SentenceTransformer
import torch
from transformers import pipeline
import pandas as pd

from voc.storage.factory import get_storage
from voc.storage.types import StorageType
from voc.data_models import SteamReview, SentenceDTO


@materialize(
    asset_deps="postgres://bronze/reviews"
)
def load_raw_reviews(app_id: str, storage_type: StorageType, storage_config: dict) -> list[SteamReview]:
    logger = get_run_logger()
    storage = get_storage(storage_type, config=storage_config)
    reviews = storage.get_reviews(app_id)
    logger.info(f"Loaded {len(reviews)} raw reviews for app {app_id}")
    return reviews


@task(log_prints=True)
def split_reviews(reviews: list[SteamReview], app_id: str) -> list[SentenceDTO]:
    logger = get_run_logger()
    sentences = []
    
    for review in reviews:
        rid = review.recommendationid
        text = review.review
        
        if not text:
            continue
            
        for sent in sent_tokenize(text):
            # Create SentenceDTO
            dto = SentenceDTO(
                recommendationid=rid,
                sentence=sent,
                review=text,
                app_id=app_id
            )
            sentences.append(dto)
    
    logger.info(f"Split {len(sentences)} sentences for app {app_id}")
    return sentences


@task(log_prints=True)
def calculate_sentiment(sentences: list[SentenceDTO], app_id: str) -> list[SentenceDTO]:
    logger = get_run_logger()
    
    device = 0 if torch.cuda.is_available() else -1
    logger.info(f"Using device: {device}")

    model_path = "cardiffnlp/twitter-roberta-base-sentiment-latest"
    sentiment_pipeline = pipeline("sentiment-analysis", model=model_path, tokenizer=model_path, device=device)

    docs = [item.sentence for item in sentences]
    logger.info(f"Calculating sentiment for {len(docs)} sentences...")

    sentiments = sentiment_pipeline(docs, batch_size=32, truncation=True)

    def map_label(label):
        l = label.lower()
        if 'negative' in l: return 'NEGATIVE'
        if 'neutral' in l: return 'NEUTRAL'
        if 'positive' in l: return 'POSITIVE'
        return label

    for item, s in zip(sentences, sentiments):
        item.sentiment = map_label(s['label'])
        item.score = s['score']
    
    try:
        # For logging mostly
        data_dicts = [{"sentiment": s.sentiment} for s in sentences]
        df = pd.DataFrame(data_dicts)
        if not df.empty:
            logger.info(f"Sentiment distribution:\n{df['sentiment'].value_counts()}")
    except ImportError:
        pass

    logger.info(f"Calculated sentiment for {len(sentences)} sentences")
    return sentences


@task(log_prints=True)
def calculate_embeddings(sentences: list[SentenceDTO], app_id: str) -> list[SentenceDTO]:
    logger = get_run_logger()
    embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
    
    texts = [item.sentence for item in sentences]
    
    embeddings = embedding_model.encode(texts, show_progress_bar=True)
    
    for item, embedding in zip(sentences, embeddings):
        item.embedding = embedding.tolist()
        
    logger.info(f"Calculated embeddings for {len(sentences)} sentences for app {app_id}")
    return sentences


@materialize("postgres://silver/sentences", log_prints=True)
def save_processed_data(data: list[SentenceDTO], app_id: str, storage_type: StorageType, storage_config: dict):
    logger = get_run_logger()
    storage = get_storage(storage_type, config=storage_config)
    storage.save_sentences(data)
    msg = f"Saved {len(data)} processed sentences for app {app_id}"
    logger.info(msg)
    
    create_markdown_artifact(
        key="silver-process-report",
        markdown=f"## Process Report\n\n{msg}\n- **Storage:** {storage_type}\n- **App ID:** {app_id}",
        description=f"Process report for {app_id}"
    )


@flow(name="Steam Reviews Process (Silver)")
def process_steam_reviews(
    app_id: str = "2393760",
    storage_type: StorageType = StorageType.POSTGRES,
    storage_config: dict = {}
):
    reviews = load_raw_reviews(app_id, storage_type, storage_config)
    if not reviews:
        return
        
    sentences = split_reviews(reviews, app_id)
    sentences_sentiment = calculate_sentiment(sentences, app_id)
    sentences_embeddings = calculate_embeddings(sentences_sentiment, app_id)
    
    # Dynamic asset key based on storage type
    asset_key = f"{storage_type}://silver/sentences"
    save_task = save_processed_data.with_options(assets=[asset_key])
    save_task(sentences_embeddings, app_id, storage_type, storage_config)


if __name__ == "__main__":
    process_steam_reviews(app_id="2393760", storage_type=StorageType.POSTGRES, storage_config={})
