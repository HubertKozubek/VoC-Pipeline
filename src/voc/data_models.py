from datetime import datetime
from typing import List, Optional, Any
from pydantic import BaseModel, Field

class SteamReview(BaseModel):
    recommendationid: str
    review: str
    timestamp_created: int
    votes_up: int
    votes_funny: int
    weighted_vote_score: float
    comment_count: int
    steam_purchase: bool
    received_for_free: bool
    written_during_early_access: bool
    
    author: dict = Field(default_factory=dict)
    
    # Metadata
    app_id: str
    language: str = "english"
    raw_data: Optional[dict] = None

class SentenceDTO(BaseModel):
    recommendationid: str
    sentence: str
    app_id: str
    sentiment: Optional[str] = None
    score: Optional[float] = None
    embedding: Optional[List[float]] = None
    
    model_config = {
        "extras": "ignore"  # Allow extra fields if raw data has more
    }
