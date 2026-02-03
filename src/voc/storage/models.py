from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Index, ARRAY, Boolean
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()

from datetime import datetime

class Review(Base):
    __tablename__ = "reviews"

    recommendationid = Column(String, primary_key=True)
    app_id = Column(String, index=True)

    
    # Store the full raw data just in case we miss something in schema updates
    raw_data = Column(JSONB)
    ingestion_time = Column(DateTime, default=datetime.utcnow)
    
    sentences = relationship("Sentence", back_populates="review", cascade="all, delete-orphan")

class Sentence(Base):
    __tablename__ = "sentences"

    id = Column(Integer, primary_key=True, autoincrement=True)
    recommendationid = Column(String, ForeignKey("reviews.recommendationid"))
    
    sentence = Column(String)
    sentiment = Column(String)
    score = Column(Float)
    
    # Embedding vector (384 dim for all-MiniLM-L6-v2) - Storing as standard array
    embedding = Column(ARRAY(Float))
    
    review = relationship("Review", back_populates="sentences")
