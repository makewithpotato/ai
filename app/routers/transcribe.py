# app/routers/transcribe.py

from fastapi import APIRouter, HTTPException
from app.services.transcribe_service import transcribe_video
from app.schemas import TranscribeRequest, TranscribeResponse

router = APIRouter(prefix="/transcribe", tags=["transcribe"])

@router.post("", response_model=TranscribeResponse)
def transcribe_endpoint(req: TranscribeRequest):
    """
    S3 비디오 URI를 받아 AWS Transcribe 작업을 실행한 뒤,
    발화자, 시간, 대사 정보를 포함한 JSON 리스트를 반환합니다.
    """
    if not req.s3_video_uri.startswith("s3://"):
        raise HTTPException(status_code=400, detail="s3_video_uri는 's3://'로 시작해야 합니다.")
    
    try:
        utterances = transcribe_video(
            s3_uri=req.s3_video_uri,
            language_code=req.language_code
        )
        return TranscribeResponse(utterances=utterances)
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Transcribe 작업 오류: {e}")