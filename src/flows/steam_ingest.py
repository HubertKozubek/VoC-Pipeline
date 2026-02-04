import asyncio
from prefect import flow, task
from prefect.assets import materialize
from prefect.artifacts import create_markdown_artifact
from prefect.logging import get_run_logger
import httpx
from datetime import datetime, timedelta

from voc.storage.factory import get_storage
from voc.storage.types import StorageType
from voc.ingestion.providers.steam import SteamReviewProvider
from voc.data_models import SteamReview


@task(log_prints=True)
async def fetch_steam_reviews(app_id: str, since: datetime | None = None):
    logger = get_run_logger()
    provider = SteamReviewProvider(app_id=app_id)
    review_list = [r async for r in provider.fetch_reviews(since=since)]
    logger.info(f"Fetched {len(review_list)} reviews for app {app_id}")
    return review_list


@materialize("postgres://bronze/reviews", log_prints=True)
def save_bronze_data(reviews: list[SteamReview], app_id: str, storage_type: StorageType, storage_config: dict):
    logger = get_run_logger()
    storage = get_storage(storage_type, config=storage_config)
    
    storage.save_reviews(reviews)
    msg = f"Saved {len(reviews)} reviews for app {app_id}"
    logger.info(msg)
    
    create_markdown_artifact(
        key="bronze-ingest-report",
        markdown=f"## Ingestion Report\n\n{msg}\n- **Storage:** {storage_type}\n- **App ID:** {app_id}",
        description=f"Ingestion report for {app_id}"
    )


@flow(name="Steam Reviews Ingest (Bronze)")
async def ingest_steam_reviews(
    app_id: str,
    storage_type: StorageType,
    lookback_days: int | None = None,
    storage_config: dict = {}
):
    if lookback_days:
        since = datetime.now() - timedelta(days=lookback_days)
    else:
        since = None
    
    reviews = await fetch_steam_reviews(app_id, since)
    
    # Dynamic asset key based on storage type
    asset_key = f"{storage_type}://bronze/reviews"
    save_task = save_bronze_data.with_options(assets=[asset_key])
    save_task(reviews, app_id, storage_type, storage_config)


if __name__ == "__main__":
    # "Tall Trails" for tests, small dataset ~ 500 reviews
    # asyncio.run(ingest_steam_reviews(app_id="2393760", storage_type=StorageType.POSTGRES, storage_config={}))
    
    # Hollow Knight: Silksong
    asyncio.run(ingest_steam_reviews(app_id="1030300", storage_type=StorageType.POSTGRES, storage_config={}))
