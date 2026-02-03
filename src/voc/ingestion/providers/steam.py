import httpx
import logging
from typing import List, Optional, Any, Dict
from datetime import datetime, timezone
import time
from .provider import BaseReviewProvider

logger = logging.getLogger(__name__)

from pydantic import BaseModel

from voc.data_models import SteamReview

class SteamReviewProvider(BaseReviewProvider):
    BASE_URL = "https://store.steampowered.com/appreviews/{appid}"
    DEFAULT_PARAMETERS = {
        "json": 1,
        "language": "english",
        "num_per_page": 100,
        "filter": "recent",
        "cursor": "*",
        "purchase_type": "all"
    }

    def __init__(self, app_id: str, params: dict | None = None):
        self.app_id = app_id
        self.params = params or self.DEFAULT_PARAMETERS

    async def fetch_reviews(self, since: datetime | None = None) -> List[SteamReview]:
        url = self.BASE_URL.format(appid=self.app_id)
        params = self.params.copy()
        if since and params.get("filter") != "recent":
             logger.warning("Fetching reviews with 'since' parameter but filter is not 'recent'. Strict date breaking might be inaccurate.")
        
        num_reviews_total = 0

        async with httpx.AsyncClient() as client:
            while True:
                response = await client.get(url, params=params)
                response.raise_for_status()

                data = response.json()
                reviews_data = data.get("reviews", [])
                
                num_reviews_in_response = data.get("query_summary", {}).get("num_reviews", 0)
                num_reviews_total += num_reviews_in_response

                for r in reviews_data:
                    timestamp = datetime.fromtimestamp(r.get("timestamp_created", 0), tz=timezone.utc)
                    
                    if since and timestamp < since:
                        logger.info(f"Found around {num_reviews_total} reviews in total.")
                        logger.info(f"Found review older than {since}. Stopping.")
                        return

                    # Inject app_id and validate
                    r["app_id"] = self.app_id
                    try:
                        yield SteamReview(**r)
                    except Exception as e:
                        logger.warning(f"Failed to parse review {r.get('recommendationid', 'unknown')}: {e}")

                if num_reviews_in_response == 0:
                    logger.info(f"Found {num_reviews_total} reviews in total.") 
                    break

                cursor = data.get("cursor")
                if cursor:
                    params["cursor"] = cursor
