from fastapi import APIRouter, HTTPException
from app.services.moviemanager_service import process_multiple_videos
from app.schemas import MovieManagerRequest, MovieManagerResponse

router = APIRouter(prefix="/moviemanager", tags=["moviemanager"])

@router.post("", response_model=MovieManagerResponse)
async def moviemanager_endpoint(req: MovieManagerRequest):
    """
    여러 S3 비디오 URI를 받아 순차적으로 처리하여 각각의 요약과 최종 종합 요약을 생성합니다.
    이전 비디오의 요약이 다음 비디오 분석에 컨텍스트로 포함됩니다.
    """
    if not req.s3_video_uris:
        raise HTTPException(status_code=400, detail="s3_video_uris가 비어 있습니다.")
    
    for video_uri in req.s3_video_uris:
        if not video_uri.startswith("s3://"):
            raise HTTPException(status_code=400, detail=f"모든 video_uri는 's3://'로 시작해야 합니다: {video_uri}")
    
    try:
        result = await process_multiple_videos(
            s3_video_uris=req.s3_video_uris,
            language_code=req.language_code,
            threshold=req.threshold
        )
        return MovieManagerResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"다중 비디오 처리 중 오류 발생: {str(e)}") 