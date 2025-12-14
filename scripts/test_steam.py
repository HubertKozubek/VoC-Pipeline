from voc.ingestion.steam import SteamReviewProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_steam")

def test_fetch():
    APP_ID = "1030300" # Hollow Knight: Silksong
    
    provider = SteamReviewProvider(app_id=APP_ID)
    
    since_24h = datetime.now(timezone.utc) - timedelta(hours=24)
    logger.info(f"Testing incremental fetch since {since_24h}")
    
    reviews = provider.fetch_reviews(since=since_24h)
    
    logger.info(f"Count: {len(reviews)}")
    if reviews:
        logger.info(f"Sample content: {reviews[0].content[:50]}...")

if __name__ == "__main__":
    test_fetch()
