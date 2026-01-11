import asyncio
import sys
from pathlib import Path
import logging

project_root = Path(__file__).parent.parent
sys.path.append(str(project_root / "src"))

from voc.ingestion.providers.steam import SteamReviewProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_steam")

async def test_fetch():
    APP_ID = "2393760" # Random game with small number of reviews
    
    provider = SteamReviewProvider(app_id=APP_ID)
    
    logger.info(f"Testing fetch of all reviews")
    
    reviews = [r async for r in provider.fetch_reviews()]
    
    logger.info(f"Count: {len(reviews)}")
    if reviews:
        logger.info(f"Sample content: {reviews[0]['review'][:50]}...")

    assert len(reviews) > 100 # At the time of writing, this game has 475 reviews

if __name__ == "__main__":
    asyncio.run(test_fetch())
