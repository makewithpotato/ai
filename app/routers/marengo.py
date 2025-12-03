# app/routers/marengo.py

from fastapi import APIRouter, HTTPException
from app.services.marengo_service import init_marengo_client, embed_marengo
from app.schemas import ChatRequest, ChatResponse

router = APIRouter(prefix="/marengo", tags=["marengo"])

# init_marengo_client 하던 곳
init_marengo_client()

@router.post("", response_model=ChatResponse)
def chat_endpoint(req: ChatRequest):
    """
    사용자가 보낸 메시지를 Marengo API에 전달 후, 임베딩 벡터를 반환합니다.
    """
    try:
        result = embed_marengo("text", req.message)
        # convert List[float] to ChatResponse
        result_str = ','.join(map(str, result))
        return ChatResponse(response=result_str)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Marengo API 호출 오류: {e}")