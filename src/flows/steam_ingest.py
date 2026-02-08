import asyncio
from prefect import flow, task
from prefect.assets import materialize
from prefect.artifacts import create_markdown_artifact
from prefect.artifacts import create_link_artifact
from prefect.logging import get_run_logger
import httpx
from datetime import datetime, timedelta

from voc.storage.factory import get_storage
from voc.storage.types import StorageType
from voc.ingestion.providers.steam import SteamReviewProvider
from voc.data_models import SteamReview


@task(log_prints=True)
def save_bronze_data(reviews: list[SteamReview], app_id: str, storage_type: StorageType, storage_config: dict):
    storage = get_storage(storage_type, config=storage_config)
    
    storage.save_reviews(reviews)


@task
async def batched_reviews(app_id: str, since: datetime | None = None, batch_size: int = 100):
    provider = SteamReviewProvider(app_id=app_id)
    batch = []
    
    async for review in provider.fetch_reviews(since=since):
        batch.append(review)
        if len(batch) >= batch_size:
            yield batch
            batch = []
            
    if batch:
        yield batch


@flow(name="Steam Reviews Ingest (Bronze)")
async def ingest_steam_reviews(
    app_id: str,
    storage_type: StorageType,
    lookback_days: int | None = None,
    storage_config: dict = {}
):
    logger = get_run_logger()
    if lookback_days:
        since = datetime.now() - timedelta(days=lookback_days)
    else:
        since = None
    

    total_fetched = 0
    logger.info(f"Starting ingestion for app {app_id}")
    
    async for batch in batched_reviews(app_id, since, batch_size=100):
        save_bronze_data(batch, app_id, storage_type, storage_config)
        total_fetched += len(batch)
        
    logger.info(f"Ingestion complete. Total reviews processed: {total_fetched}")

    asset_key = f"{storage_type}://bronze/reviews"
    create_link_artifact(
        key="bronze-ingest-report",
        link=asset_key,
        description=f"Ingestion report for {app_id}"
    )



if __name__ == "__main__":
    # "Tall Trails" for tests, small dataset ~ 500 reviews
    asyncio.run(ingest_steam_reviews(app_id="2393760", storage_type=StorageType.POSTGRES, storage_config={}))
    
    # Hollow Knight: Silksong
    # asyncio.run(ingest_steam_reviews(app_id="1030300", storage_type=StorageType.POSTGRES, storage_config={}))
