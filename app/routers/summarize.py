from fastapi import APIRouter, HTTPException
from app.services.summarize_service import summarize_content
from app.schemas import SummarizeRequest, SummarizeResponse

router = APIRouter(prefix="/summarize", tags=["summarize"])

@router.post("", response_model=SummarizeResponse)
def summarize_endpoint(req: SummarizeRequest):
    """
    STT 결과와 장면 이미지 URL을 받아 Claude를 통해 내용을 요약합니다.
    """
    if not req.utterances:
        raise HTTPException(status_code=400, detail="utterances가 비어 있습니다.")
    
    if not req.scene_urls:
        raise HTTPException(status_code=400, detail="scene_urls가 비어 있습니다.")
    
    try:
        summary = summarize_content(
            utterances=req.utterances,
            scene_urls=req.scene_urls
        )
        return SummarizeResponse(summary=summary)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"요약 생성 중 오류 발생: {str(e)}") 