# app/services/claude_service.py

import os
from anthropic import AnthropicBedrock

bedrock_client = None
CLAUDE_MODEL_ID = None

def init_claude_client():
    """
    애플리케이션 시작 시 한 번만 호출되어야 하는 함수로,
    환경변수에서 자격증명과 모델 ID를 읽어 Bedrock 클라이언트를 초기화합니다.
    """
    global bedrock_client, CLAUDE_MODEL_ID

    if bedrock_client is not None:
        return  # 이미 초기화된 경우 재할당하지 않음

    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
    CLAUDE_MODEL_ID = os.getenv("CLAUDE_MODEL_ID")

    if not (aws_key and aws_secret and CLAUDE_MODEL_ID):
        raise RuntimeError("필수 환경 변수가 설정되지 않았습니다: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, CLAUDE_MODEL_ID")

    bedrock_client = AnthropicBedrock(
        aws_access_key=aws_key,
        aws_secret_key=aws_secret,
        aws_region=aws_region,
    )

def get_claude_response(user_message: str) -> str:
    """
    Bedrock Claude API를 호출하여 텍스트 응답을 반환합니다.
    """
    if bedrock_client is None:
        raise RuntimeError("Claude Bedrock 클라이언트가 초기화되지 않았습니다.")
    response = bedrock_client.messages.create(
        model=CLAUDE_MODEL_ID,
        max_tokens=4096,
        messages=[{"role": "user", "content": user_message}]
    )
    return response.content