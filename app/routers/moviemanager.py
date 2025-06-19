from fastapi import APIRouter, HTTPException
from app.services.moviemanager_service import process_videos_from_folder, process_single_video
from app.schemas import MovieManagerRequest, MovieManagerResponse

router = APIRouter(prefix="/moviemanager", tags=["moviemanager"])

@router.post("", response_model=MovieManagerResponse)
async def moviemanager_endpoint(req: MovieManagerRequest):
    """
    두 가지 모드를 지원합니다:
    1. 폴더 모드: S3 폴더 경로를 받아 그 안의 모든 비디오 파일을 순차적으로 처리
    2. 단일 비디오 모드: 원본 비디오 S3 URI를 받아 동적으로 청크를 추출하며 처리
    
    이전 비디오/청크의 요약이 다음 분석에 컨텍스트로 포함됩니다.
    """
    # 모드 검증
    if req.s3_folder_path and req.s3_video_uri:
        raise HTTPException(status_code=400, detail="s3_folder_path와 s3_video_uri 중 하나만 제공해야 합니다.")
    
    if not req.s3_folder_path and not req.s3_video_uri:
        raise HTTPException(status_code=400, detail="s3_folder_path 또는 s3_video_uri 중 하나는 제공해야 합니다.")
    
    if not req.movie_id:
        raise HTTPException(status_code=400, detail="movie_id가 필요합니다.")
    
    try:
        if req.s3_video_uri:
            # 단일 비디오 모드 (동적 청크 추출)
            if not req.s3_video_uri.startswith("s3://"):
                raise HTTPException(status_code=400, detail="s3_video_uri는 's3://'로 시작해야 합니다.")
            
            print(f"🎬 단일 비디오 모드: {req.s3_video_uri}")
            result = await process_single_video(
                s3_video_uri=req.s3_video_uri,
                characters_info=req.characters_info,
                movie_id=req.movie_id,
                segment_duration=req.segment_duration,
                init=req.init,
                language_code=req.language_code,
                threshold=req.threshold
            )
            
        else:
            # 폴더 모드 (기존 방식)
            if not req.s3_folder_path.startswith("s3://"):
                raise HTTPException(status_code=400, detail="s3_folder_path는 's3://'로 시작해야 합니다.")
            
            print(f"📁 폴더 모드: {req.s3_folder_path}")
            result = await process_videos_from_folder(
                s3_folder_path=req.s3_folder_path,
                characters_info=req.characters_info,
                movie_id=req.movie_id,
                init=req.init,
                language_code=req.language_code,
                threshold=req.threshold
            )
        
        return MovieManagerResponse(**result)
        
    except Exception as e:
        error_msg = f"단일 비디오 처리 중 오류 발생: {str(e)}" if req.s3_video_uri else f"S3 폴더 비디오 처리 중 오류 발생: {str(e)}"
        raise HTTPException(status_code=500, detail=error_msg)