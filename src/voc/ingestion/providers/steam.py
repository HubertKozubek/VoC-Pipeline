import httpx
import logging
from typing import List, Optional, Any, Dict
from datetime import datetime, timezone
import time
from .provider import BaseReviewProvider, Review

logger = logging.getLogger(__name__)

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

    async def fetch_reviews(self, since: datetime | None = None) -> List[Review]:
        url = self.BASE_URL.format(appid=self.app_id)
        params = self.params.copy()
        
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
                    yield r

                if num_reviews_in_response == 0:
                    break

                cursor = data.get("cursor")
                if cursor:
                    params["cursor"] = cursor
