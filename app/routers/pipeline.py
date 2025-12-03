from fastapi import APIRouter, HTTPException
from app.services.transcribe_service import transcribe_video
from app.services.scene_service import scene_process
from app.services.summarize_service import summarize_content
from app.schemas import PipelineRequest, SummarizeResponse
import asyncio

router = APIRouter(prefix="/pipeline", tags=["pipeline"])

@router.post("", response_model=SummarizeResponse)
async def pipeline_endpoint(req: PipelineRequest):
    """
    S3 비디오 URI를 받아 STT와 장면 감지를 병렬로 처리한 뒤, Claude로 요약합니다.
    """
    if not req.s3_video_uri.startswith("s3://"):
        raise HTTPException(status_code=400, detail="s3_video_uri는 's3://'로 시작해야 합니다.")
    
    try:
        # 병렬 실행
        transcribe_task = asyncio.to_thread(transcribe_video, req.s3_video_uri, req.language_code)
        scene_task = asyncio.to_thread(scene_process, req.s3_video_uri, req.threshold)
        utterances, scenes = await asyncio.gather(transcribe_task, scene_task)
        
        # scene의 base64 이미지와 start_time만 추출
        scene_images = [
            {"start_time": scene["start_time"], "image": scene["frame_image"]}
            for scene in scenes
        ]
        
        # summarize 실행 (scene_images를 전달)
        summary = await summarize_content(utterances, scene_images)
        return SummarizeResponse(summary=summary)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"파이프라인 처리 중 오류 발생: {str(e)}") 