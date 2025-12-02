from fastapi import APIRouter, HTTPException
from app.services.scene_service import scene_process
from app.schemas import SceneRequest, SceneResponse
from typing import List

router = APIRouter(prefix="/scene", tags=["scene"])

@router.post("", response_model=SceneResponse)
def detect_scenes_endpoint(req: SceneRequest):
    """
    S3 비디오 URI를 받아 주요 장면을 감지하고 각 장면의 대표 프레임을 S3에 업로드합니다.
    """
    if not req.s3_video_uri.startswith("s3://"):
        raise HTTPException(status_code=400, detail="s3_video_uri는 's3://'로 시작해야 합니다.")
    
    try:
        scenes = scene_process(
            s3_uri=req.s3_video_uri,
            threshold=req.threshold
        )
        return SceneResponse(scenes=scenes)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"장면 감지 중 오류 발생: {str(e)}") 