import polars as pl
from typing import List, Dict, Any
from pathlib import Path
from datetime import datetime
from voc.logging import get_logger
from voc.storage.storage import VocStorage

logger = get_logger(__name__)

from voc.data_models import SteamReview, SentenceDTO

class ParquetStore(VocStorage):
    def __init__(self, base_path: str = "data", **kwargs):
        self.base_path = Path(base_path)
        
    def _save_to_path(self, data: List[Any], relative_path: str):
        if not data: return
        
        normalized_data = []
        for r in data:
            if hasattr(r, "model_dump"):
                 r_dict = r.model_dump()
            elif isinstance(r, dict):
                 r_dict = r
            else:
                 continue
            normalized_data.append(r_dict)
        
        if not normalized_data: return

        df = pl.DataFrame(normalized_data)
        
        output_dir = self.base_path / relative_path
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"{timestamp}.parquet"
        file_path = output_dir / filename
        
        df.write_parquet(file_path)
        logger.info(f"Saved to {file_path}")

    def _load_from_path(self, relative_path: str) -> List[dict]:
        target_dir = self.base_path / relative_path
        if not target_dir.exists():
            return []
        
        try:
            df = pl.read_parquet(target_dir / "*.parquet")
            return df.to_dicts()
        except Exception as e:
            logger.error(f"Error loading parquet from {relative_path}: {e}")
            return []

    def save_reviews(self, reviews: List[SteamReview]):
        if not reviews: return
        # Bronze: data/reviews
        self._save_to_path(reviews, "reviews")

    def get_reviews(self, app_id: str) -> List[SteamReview]:
        target_dir = self.base_path / "reviews"
        if not target_dir.exists():
            return []
            
        try:
            # Load all and filter. optimize with scan_parquet if needed later.
            df = pl.read_parquet(target_dir / "*.parquet")
            if "app_id" in df.columns:
                df = df.filter(pl.col("app_id") == app_id)
            else:
                return []
                
            raw_data = df.to_dicts()
            return [SteamReview(**r) for r in raw_data if r]
        except Exception as e:
            logger.error(f"Error loading reviews: {e}")
            return []

    def save_sentences(self, sentences: List[SentenceDTO]):
        if not sentences: return
        # Silver: data/sentences
        self._save_to_path(sentences, "sentences")

    def get_sentences(self, app_id: str) -> List[SentenceDTO]:
        target_dir = self.base_path / "sentences"
        if not target_dir.exists():
            return []
            
        try:
            df = pl.read_parquet(target_dir / "*.parquet")
            if "app_id" in df.columns:
                df = df.filter(pl.col("app_id") == app_id)
            else:
                return []
                
            raw_data = df.to_dicts()
            results = []
            for s in raw_data:
                if not s: continue
                # app_id is in data now
                results.append(SentenceDTO(**s))
            return results
        except Exception as e:
            logger.error(f"Error loading sentences: {e}")
            return []
