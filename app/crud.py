from sqlalchemy.orm import Session
from sqlalchemy import desc
from app.models import Movie, MovieManagerSummary
from typing import Optional, List
import re

# ============== Movie CRUD ==============

def get_movie(db: Session, movie_id: int) -> Optional[Movie]:
    """영화 정보 조회"""
    return db.query(Movie).filter(Movie.id == movie_id).first()

def update_movie_status(db: Session, movie_id: int, status: str) -> bool:
    """영화 상태 업데이트"""
    movie = db.query(Movie).filter(Movie.id == movie_id).first()
    if movie:
        movie.status = status
        db.commit()
        db.refresh(movie)
        return True
    return False

def mark_movie_failed(db: Session, movie_id: int) -> bool:
    """영화 상태를 실패로 표시"""
    movie = db.query(Movie).filter(Movie.id == movie_id).first()
    if movie and not movie.status.startswith("FAILED_"):
        movie.status = f"FAILED_{movie.status}"
        db.commit()
        db.refresh(movie)
        return True
    return False

def get_resume_info(db: Session, movie_id: int) -> Optional[dict]:
    """재시작 정보 조회"""
    movie = get_movie(db, movie_id)
    if not movie:
        return None
    
    status = movie.status
    
    # FAILED_ 접두사 제거
    if status.startswith("FAILED_"):
        status = status.replace("FAILED_", "")
    
    # 상태 파싱
    if status == "PENDING":
        return {"current": 0, "total": 0, "stage": "pending"}
    elif status.startswith("PROCEEDING["):
        match = re.match(r"PROCEEDING\[(\d+)/(\d+)\]", status)
        if match:
            return {
                "current": int(match.group(1)),
                "total": int(match.group(2)),
                "stage": "proceeding"
            }
    elif status == "ORGANIZING":
        return {"stage": "organizing"}
    elif status == "COMPLETE":
        return {"stage": "complete"}
    
    return None

# ============== MovieManagerSummary CRUD ==============

def create_or_update_summary(db: Session, movie_id: int, summary_id: int, summary_text: str) -> MovieManagerSummary:
    """요약 생성 또는 업데이트 (덮어쓰기)"""
    # 기존 요약이 있는지 확인
    existing_summary = db.query(MovieManagerSummary)\
                        .filter(MovieManagerSummary.movie_id == movie_id)\
                        .filter(MovieManagerSummary.summary_id == summary_id)\
                        .first()
    
    if existing_summary:
        # 기존 요약 업데이트
        existing_summary.summary_text = summary_text
        db.commit()
        db.refresh(existing_summary)
        return existing_summary
    else:
        # 새 요약 생성
        summary = MovieManagerSummary(
            movie_id=movie_id,
            summary_id=summary_id,
            summary_text=summary_text
        )
        db.add(summary)
        db.commit()
        return summary

def delete_summaries_from(db: Session, movie_id: int, from_summary_id: int) -> int:
    """특정 summary_id 이후의 모든 요약 삭제 (재시작 시 사용)"""
    deleted_count = db.query(MovieManagerSummary)\
                     .filter(MovieManagerSummary.movie_id == movie_id)\
                     .filter(MovieManagerSummary.summary_id >= from_summary_id)\
                     .delete()
    db.commit()
    return deleted_count

def get_summaries(db: Session, movie_id: int) -> List[MovieManagerSummary]:
    """영화의 모든 요약 조회 (순서대로)"""
    return db.query(MovieManagerSummary)\
             .filter(MovieManagerSummary.movie_id == movie_id)\
             .order_by(MovieManagerSummary.summary_id)\
             .all()

def get_latest_summary(db: Session, movie_id: int) -> Optional[MovieManagerSummary]:
    """영화의 최신 요약 조회"""
    return db.query(MovieManagerSummary)\
             .filter(MovieManagerSummary.movie_id == movie_id)\
             .order_by(desc(MovieManagerSummary.summary_id))\
             .first()

def get_summaries_up_to(db: Session, movie_id: int, summary_id: int) -> List[MovieManagerSummary]:
    """특정 summary_id까지의 요약들 조회"""
    return db.query(MovieManagerSummary)\
             .filter(MovieManagerSummary.movie_id == movie_id)\
             .filter(MovieManagerSummary.summary_id <= summary_id)\
             .order_by(MovieManagerSummary.summary_id)\
             .all() 