import requests
import logging
import urllib.parse
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
        "cursor": "*"
    }

    def __init__(self, app_id: str, params: dict | None = None):
        self.app_id = app_id
        self.params = params or self.DEFAULT_PARAMETERS

    async def fetch_reviews(self, since: datetime | None = None) -> List[Review]:
        url = self.BASE_URL.format(appid=self.app_id)
        params = self.params.copy()
        num_of_reviews_retrived = 100

        while num_of_reviews_retrived != 0:
            response = requests.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            reviews_data = data.get("reviews", [])
            num_of_reviews_retrived = data["query_summary"]["num_reviews"]

            for r in reviews_data:
                yield r

            cursor = urllib.parse.quote(data.get("cursor", ""))
            params["cursor"] = cursor
            
            
