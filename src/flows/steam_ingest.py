import asyncio
from prefect import flow, task
import httpx
from datetime import datetime, timedelta
from voc.storage.factory import get_storage
from voc.storage.types import StorageType
from voc.ingestion.providers.steam import SteamReviewProvider, SteamReview
from voc.logging import get_logger

logger = get_logger(__name__)


@task(log_prints=True)
async def fetch_steam_reviews(app_id: str, since: datetime | None = None):
    provider = SteamReviewProvider(app_id=app_id)
    review_list = [r async for r in provider.fetch_reviews(since=since)]
    logger.info(f"Fetched {len(review_list)} reviews for app {app_id}")
    return review_list


@task(log_prints=True)
def save_reviews(reviews: list[SteamReview], app_id: str, storage_type: StorageType):
    storage = get_storage(storage_type, app_id)
    storage.save_reviews(reviews)
    logger.info(f"Saved {len(reviews)} reviews for app {app_id}")


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

if __name__ == "__main__":
    asyncio.run(ingest_steam_reviews(app_id = "2393760", storage_type = StorageType.PARQUET))


