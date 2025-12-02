# app/services/marengo_service.py

from dotenv import load_dotenv
from typing import List
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

def embed_marengo(input_type: str, input: str) -> List[float]:
    """
    Bedrock Marengo API를 호출하여 임베딩 벡터를 반환합니다.
    """

    if input_type not in ["text", "image"]:
        raise ValueError("input_type은 'text' 또는 'image'여야 합니다.")
    
    if input_type == "text":
        message = {
            "inputType": input_type,
            "inputText": input
        }

    if input_type == "image":
        message = {
            "inputType": input_type,
            "mediaSource": {
                "base64String": input
            }
        }

    if marengo_client is None:
        raise RuntimeError("Marengo Bedrock 클라이언트가 초기화되지 않았습니다.")
    response = marengo_client.invoke_model(
        modelId=MARENGO_MODEL_ID,
        body=json.dumps(message)
    )

    

    # response["body"]는 StreamingBody → .read() 필요
    result = json.loads(response["body"].read())

    embedding = result['data'][0]['embedding']
    
    print(type(embedding), len(embedding))

    return embedding