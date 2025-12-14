from pathlib import Path
from datetime import datetime
from typing import List
from voc.ingestion.providers.provider import Review
from .storage import BaseStorage

logger = logging.getLogger(__name__)

class RawDataStorage(BaseStorage):
    def __init__(self, base_path: str = "data/raw"):
        self.base_path = Path(base_path)
    
    def save_reviews(self, reviews: List[Review]):
        pass