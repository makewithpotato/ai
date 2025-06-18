from fastapi import APIRouter, HTTPException
from app.services.moviemanager_service import process_videos_from_folder
from app.schemas import MovieManagerRequest, MovieManagerResponse

router = APIRouter(prefix="/moviemanager", tags=["moviemanager"])

@router.post("", response_model=MovieManagerResponse)
async def moviemanager_endpoint(req: MovieManagerRequest):
    """
    S3 폴더 경로를 받아 그 안의 모든 비디오 파일을 순차적으로 처리하여 
    각각의 요약과 최종 종합 요약을 생성합니다.
    이전 비디오의 요약이 다음 비디오 분석에 컨텍스트로 포함됩니다.
    """
    if not req.s3_folder_path:
        raise HTTPException(status_code=400, detail="s3_folder_path가 비어 있습니다.")
    
    if not req.s3_folder_path.startswith("s3://"):
        raise HTTPException(status_code=400, detail="s3_folder_path는 's3://'로 시작해야 합니다.")
    
    try:
        result = await process_videos_from_folder(
            s3_folder_path=req.s3_folder_path,
            characters_info=req.characters_info,
            language_code=req.language_code,
            threshold=req.threshold
        )
        return MovieManagerResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 폴더 비디오 처리 중 오류 발생: {str(e)}")