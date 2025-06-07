# app/routers/chat.py

from fastapi import APIRouter, HTTPException
from app.services.claude_service import init_claude_client, get_claude_response
from app.schemas import ChatRequest, ChatResponse

router = APIRouter(prefix="/chat", tags=["chat"])

# 애플리케이션이 시작된 직후 호출하여 Bedrock 클라이언트를 초기화해 둡니다.
# (main.py에서 startup 이벤트에 묶어서 호출해도 무방합니다.)
try:
    init_claude_client()
except Exception as e:
    # 초기화 실패 시 로깅만 하고, 실제 요청 시 에러 처리하도록 둡니다.
    print(f"[경고] Claude 클라이언트 초기화 중 오류: {e}")

@router.post("", response_model=ChatResponse)
def chat_endpoint(req: ChatRequest):
    """
    사용자가 보낸 메시지를 Claude API에 전달 후, 결과를 반환합니다.
    """
    try:
        result = get_claude_response(req.message)
        print("Raw response from Claude:", result)
        # If the result is a list of TextBlock, concatenate their text fields
        if isinstance(result, list):
            result = "".join(block.text for block in result)
        return ChatResponse(response=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Claude API 호출 오류: {e}")