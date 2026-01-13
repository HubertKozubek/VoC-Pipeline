import asyncio
import sys
from pathlib import Path
import logging
from datetime import datetime, timedelta, timezone
import polars as pl
import shutil
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.append(str(project_root / "src"))

from voc.ingestion.providers.steam import SteamReviewProvider
from voc.logging import get_logger
from voc.storage.parquet_store import ParquetStore

logger = get_logger("test_steam")

async def test_fetch():
    APP_ID = "2393760" # Random game with small number of reviews
    
    provider = SteamReviewProvider(app_id=APP_ID)
    
    logger.info(f"Testing fetch of all reviews")
    
    reviews = [r async for r in provider.fetch_reviews()]
    
    logger.info(f"Count: {len(reviews)}")
    if reviews:
        logger.info(f"Sample content: {reviews[0].review[:50]}...")

    assert len(reviews) > 100 # At the time of writing, this game has 478 (english) reviews

    since_date = datetime.now(timezone.utc) - timedelta(days=20)
    logger.info(f"Testing fetch with since={since_date}")
    
    recent_reviews = [r async for r in provider.fetch_reviews(since=since_date)]
    logger.info(f"Fetched {len(recent_reviews)} reviews since {since_date}")
    
    
    # Check type of first review
    if recent_reviews:
        first_review = recent_reviews[0]
        logger.info(f"Review type: {type(first_review)}")
    
    for r in recent_reviews:
        r_ts = datetime.fromtimestamp(r.timestamp_created, tz=timezone.utc)
        assert r_ts >= since_date, f"Review {r.recommendationid} timestamp {r_ts} is older than {since_date}"
    
    logger.info("Date filtering verification passed.")

    store = ParquetStore(base_path="test_data", app_id="2393760") # using test_data dir
    store.save_reviews(reviews)
    
    output_dir = Path("test_data/2393760")
    parquet_files = list(output_dir.glob("*.parquet"))
    
    assert len(parquet_files) > 0, "No parquet files found"
    logger.info(f"Found parquet file: {parquet_files[0]}")

    df = pl.read_parquet(parquet_files[0])
    logger.info(f"Read back {len(df)} reviews from Parquet")
    assert len(df) >= len(reviews)

    shutil.rmtree("test_data")
    logger.info("Storage verification passed.")

    # assert len(reviews) > 100 # At the time of writing, this game has 475 (english) reviews

if __name__ == "__main__":
    asyncio.run(test_fetch())
