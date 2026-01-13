import polars as pl
from typing import List, Dict, Any
from pathlib import Path
from datetime import datetime
from voc.logging import get_logger

logger = get_logger(__name__)

class ParquetStore:
    def __init__(self, base_path: str = "data/reviews", app_id: str = "unknown"):
        self.base_path = Path(base_path)
        self.app_id = app_id
        
    def save_reviews(self, reviews: List[Any]):
        if not reviews:
            return

        logger.info(f"Saving {len(reviews)} reviews for AppID: {self.app_id}")
        
        # Normalize to list of dicts
        data = []
        for r in reviews:
            if hasattr(r, "model_dump"):
                 r_dict = r.model_dump()
            elif isinstance(r, dict):
                 r_dict = r
            else:
                 logger.warning(f"Skipping unknown review type: {type(r)}")
                 continue
            
            # Inject app_id if missing (though SteamReview model has it now)
            if "app_id" not in r_dict:
                r_dict["app_id"] = self.app_id
            
            data.append(r_dict)
        
        # Create DataFrame
        df = pl.DataFrame(data)
        

        output_dir = self.base_path / self.app_id
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"{timestamp}.parquet"
        file_path = output_dir / filename
        

        df.write_parquet(file_path)
        
        logger.info(f"Saved to {file_path}")
