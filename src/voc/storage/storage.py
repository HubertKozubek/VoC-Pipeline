import json
from pathlib import Path
from datetime import datetime
from typing import List
import logging
from .provider import Review

logger = logging.getLogger(__name__)

from abc import ABC, abstractmethod

class BaseStorage(ABC):
    @abstractmethod
    def save_reviews(self, reviews: List[Review]):
        pass    