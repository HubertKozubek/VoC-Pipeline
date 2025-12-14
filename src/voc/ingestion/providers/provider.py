from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel

class Review(BaseModel):
    review_id: str
    source_id: str = "steam"
    app_id: str
    content: str
    timestamp: datetime
    author_id: str
    recommended: bool
    votes_up: int
    votes_funny: int
    metadata: Dict[str, Any] = {}

class BaseReviewProvider(ABC):
    @abstractmethod
    def fetch_reviews(self, since: Optional[datetime] = None) -> List[Review]:
        pass
