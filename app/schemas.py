# app/schemas.py

from pydantic import BaseModel
from typing import List

# ─────────────────────────────────────────
# 1) Chat 관련 요청/응답
# ─────────────────────────────────────────
class ChatRequest(BaseModel):
    message: str

class ChatResponse(BaseModel):
    response: str

# ─────────────────────────────────────────
# 2) Transcribe 관련 요청/응답
# ─────────────────────────────────────────
class TranscribeRequest(BaseModel):
    s3_video_uri: str        # ex) "s3://my-bucket/videos/game.mp4"
    language_code: str = "ko-KR"

class UtteranceResponse(BaseModel):
    speaker: str
    start_time: float
    end_time: float
    text: str

class TranscribeResponse(BaseModel):
    utterances: List[UtteranceResponse]

# ─────────────────────────────────────────
# 3) Scene Detection 관련 요청/응답
# ─────────────────────────────────────────
class SceneRequest(BaseModel):
    s3_video_uri: str        # ex) "s3://my-bucket/videos/game.mp4"
    threshold: float = 30.0  # 장면 감지 임계값 (기본값: 30.0)

class SceneInfo(BaseModel):
    start_time: float
    end_time: float
    start_frame: int
    end_frame: int
    frame_image: str  # base64 encoded image

class SceneResponse(BaseModel):
    scenes: List[SceneInfo]

# ─────────────────────────────────────────
# 4) Combined Pipeline 요청/응답
# ─────────────────────────────────────────
class CombinedRequest(BaseModel):
    message: str
    s3_video_uri: str
    transcripts_bucket: str
    language_code: str = "ko-KR"

class CombinedResponse(BaseModel):
    claude_response: str
    transcript: str

# ─────────────────────────────────────────
# 5) Summarize 관련 요청/응답
# ─────────────────────────────────────────
class SummarizeRequest(BaseModel):
    utterances: List[UtteranceResponse]  # STT 결과
    scene_images: List[dict]  # {"start_time": float, "image": str} 형태로 전달

class SummarizeResponse(BaseModel):
    summary: str  # Claude의 요약 응답

# ─────────────────────────────────────────
# 6) Pipeline 요청/응답
# ─────────────────────────────────────────
class PipelineRequest(BaseModel):
    s3_video_uri: str
    language_code: str = "ko-KR"
    threshold: float = 30.0

# ─────────────────────────────────────────
# 7) MovieManager 요청/응답
# ─────────────────────────────────────────
class MovieManagerRequest(BaseModel):
    s3_folder_path: str  # S3 폴더 경로 (예: "s3://bucket/videos/")
    language_code: str = "ko-KR"
    threshold: float = 30.0

class VideoSummary(BaseModel):
    video_uri: str
    summary: str
    order: int  # 처리 순서

class MovieManagerResponse(BaseModel):
    video_summaries: List[VideoSummary]  # 각 비디오별 요약
    final_summary: str  # 전체 영상에 대한 최종 요약