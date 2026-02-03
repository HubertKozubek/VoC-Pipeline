import json
from pathlib import Path
from datetime import datetime
from typing import List
import logging


logger = logging.getLogger(__name__)

from abc import ABC, abstractmethod

from voc.data_models import SteamReview, SentenceDTO

class VocStorage(ABC):
    @abstractmethod
    def save_reviews(self, reviews: List[SteamReview]):
        pass

    @abstractmethod
    def get_reviews(self, app_id: str) -> List[SteamReview]:
        pass

    @abstractmethod
    def save_sentences(self, sentences: List[SentenceDTO]):
        pass

    @abstractmethod
    def get_sentences(self, app_id: str) -> List[SentenceDTO]:
        pass