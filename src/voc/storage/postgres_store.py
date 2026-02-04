from typing import List, Any, Dict
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import json
from datetime import datetime

from voc.logging import get_logger
from voc.storage.storage import VocStorage
from voc.data_models import SteamReview, SentenceDTO
from .models import Base, Review, Sentence

logger = get_logger(__name__)

class PostgresStore(VocStorage):
    def __init__(self, connection_string: str = "postgresql+psycopg://voc:voc@localhost:5432/voc", **kwargs):
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        
        Base.metadata.create_all(self.engine)

    def save_reviews(self, reviews: List[SteamReview]):
        if not reviews:
            return

        session = self.Session()
        try:
            values = []
            for r in reviews:
                data = r.model_dump()
                # Store full data in raw_data column
                values.append({
                    "recommendationid": str(data.get("recommendationid")),
                    "app_id": data.get("app_id"),
                    "raw_data": data,
                    "ingestion_time": datetime.utcnow()
                })

            if not values:
                return

            batch_size = 1000
            for i in range(0, len(values), batch_size):
                batch = values[i : i + batch_size]
                stmt = insert(Review).values(batch)
                
                stmt = stmt.on_conflict_do_update(
                    index_elements=[Review.recommendationid],
                    set_={
                        "raw_data": stmt.excluded.raw_data,
                        "app_id": stmt.excluded.app_id,
                        "ingestion_time": stmt.excluded.ingestion_time
                    }
                )
                session.execute(stmt)
            
            session.commit()
            logger.info(f"Saved/Upserted {len(values)} reviews")
        except Exception as e:
            logger.error(f"Error saving reviews: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def get_reviews(self, app_id: str) -> List[SteamReview]:
        session = self.Session()
        try:
            reviews = session.query(Review).filter(Review.app_id == app_id).all()
            results = []
            for r in reviews:
                d = r.raw_data if r.raw_data else {}
                
                try:
                    results.append(SteamReview(**d))
                except Exception as e:
                    logger.warning(f"Failed to parse review {r.recommendationid}: {e}")
            return results
        finally:
            session.close()

    def save_sentences(self, sentences: List[SentenceDTO]):
        if not sentences:
            return
            
        session = self.Session()
        try:
            include_fields = {"recommendationid", "sentence", "sentiment", "score", "embedding"}
            values = [s.model_dump(include=include_fields) for s in sentences]
            
            if not values:
                return
            
            batch_size = 1000
            for i in range(0, len(values), batch_size):
                batch = values[i : i + batch_size]
                session.execute(insert(Sentence), batch)
                
            session.commit()
            logger.info(f"Saved {len(values)} sentences")
        except Exception as e:
            logger.error(f"Error saving sentences: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def get_sentences(self, app_id: str) -> List[SentenceDTO]:
        session = self.Session()
        try:
            query = session.query(Sentence).join(
                Review, Sentence.recommendationid == Review.recommendationid
            ).filter(Review.app_id == app_id)
            
            rows = query.all()
            results = []
            rows = query.all()
            results = []
            for s in rows:
                dto = SentenceDTO(
                    recommendationid=s.recommendationid,
                    sentence=s.sentence,
                    sentiment=s.sentiment,
                    score=s.score,
                    embedding=s.embedding,
                    review_id=s.recommendationid,
                    app_id=app_id, # Use validity context
                )
                results.append(dto)
            return results
        finally:
            session.close()
