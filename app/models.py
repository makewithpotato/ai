from sqlalchemy import Column, String, Text, DateTime, ForeignKey, BigInteger
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import ARRAY
from app.database import Base

class Movie(Base):
    __tablename__ = "movie"
    
    id = Column(BigInteger, primary_key=True, index=True)
    title = Column(String(255), nullable=True)
    status = Column(String(50), default="PENDING")  # PENDING, PROCEEDING[N/M], ORGANIZING, COMPLETE, FAILED_*
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    custom_prompts = Column(ARRAY(String), nullable=True)
    custom_retrievals = Column(ARRAY(String), nullable=True)
    
    # 관계 설정
    summaries = relationship("MovieManagerSummary", back_populates="movie")

class MovieManagerSummary(Base):
    __tablename__ = "moviemanager_summary"
    
    movie_id = Column(BigInteger, ForeignKey("movie.id"), primary_key=True)
    summary_id = Column(BigInteger, primary_key=True)  # 요약본의 순서
    summary_text = Column(Text, nullable=False)
    
    # 관계 설정
    movie = relationship("Movie", back_populates="summaries") 