import requests
import logging
from typing import List, Optional, Any, Dict
from datetime import datetime, timezone
import time
from .provider import BaseReviewProvider, Review

logger = logging.getLogger(__name__)

class SteamReviewProvider(BaseReviewProvider):
    BASE_URL = "https://store.steampowered.com/appreviews/"

    def __init__(self, app_id: str):
        self.app_id = app_id

    def fetch_reviews(self, since: datetime | None = None) -> List[Review]:
        pass
