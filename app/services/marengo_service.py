# app/services/marengo_service.py

from dotenv import load_dotenv
import boto3
import os
import json

load_dotenv()

marengo_client = None
MARENGO_MODEL_ID = None

def init_marengo_client():
    """
    애플리케이션 시작 시 한 번만 호출되어야 하는 함수로,
    환경변수에서 자격증명과 모델 ID를 읽어 Bedrock 클라이언트를 초기화합니다.
    """
    global marengo_client, MARENGO_MODEL_ID

    if marengo_client is not None:
        return  # 이미 초기화된 경우 재할당하지 않음

    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
    MARENGO_MODEL_ID = "apac." +  os.getenv("MARENGO_MODEL_ID")

    if not (aws_key and aws_secret and MARENGO_MODEL_ID):
        raise RuntimeError("필수 환경 변수가 설정되지 않았습니다: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, MARENGO_MODEL_ID")

    marengo_client = boto3.client(service_name='bedrock-runtime',
        region_name=aws_region,
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret)

def get_marengo_response(user_message: str) -> str:
    """
    Bedrock Marengo API를 호출하여 텍스트 응답을 반환합니다.
    """

    message = {
        "inputType": "text",
        "inputText": user_message
    }

    if marengo_client is None:
        raise RuntimeError("Marengo Bedrock 클라이언트가 초기화되지 않았습니다.")
    response = marengo_client.invoke_model(
        modelId=MARENGO_MODEL_ID,
        body=json.dumps(message)
    )

    return response.content