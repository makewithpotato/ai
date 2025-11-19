# app/routers/marengo.py

from fastapi import APIRouter, HTTPException
from app.services.marengo_service import init_marengo_client, get_marengo_response
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
        result = get_marengo_response(req.message)
        print("Raw response from Marengo:", result)
        # If the result is a list of TextBlock, concatenate their text fields
        if isinstance(result, list):
            result = "".join(block.text for block in result)
        return ChatResponse(response=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Marengo API 호출 오류: {e}")