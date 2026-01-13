from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel


class BaseReviewProvider(ABC):
    @abstractmethod
    def fetch_reviews(self, since: Optional[datetime] = None) -> List[Dict]:
        pass
