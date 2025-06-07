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
    frame_url: str  # S3 presigned URL

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