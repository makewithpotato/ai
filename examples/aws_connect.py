from dotenv import load_dotenv
import os
import boto3
import json

# .env 파일 로드
load_dotenv()

# 환경 변수 읽기
ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
REGION = os.getenv("AWS_DEFAULT_REGION")
MODEL_ID = os.getenv("CLAUDE_MODEL_ID")
INFERENCE_PROFILE_ARN = os.getenv("INFERENCE_PROFILE_ARN")

print("AWS_ACCESS_KEY_ID:", ACCESS_KEY_ID)

# Bedrock 클라이언트 생성
client = boto3.client(
    'bedrock-runtime',
    region_name=REGION,
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY
)

# Chat messages payload for Messages API
messages = [
    {"role": "system", "content": "You are a helpful assistant."},
    {"role": "user", "content": "Tell me about the benefits of using AWS Bedrock with Claude 3."}
]
payload = {
    "messages": messages,
    "maxTokensToSample": 300,
    "temperature": 0.5
}

response = client.invoke_model(
    modelId=MODEL_ID,
    inferenceProfileArn=INFERENCE_PROFILE_ARN,
    body=json.dumps(payload),
    contentType="application/vnd.aws-protocolmessages-v1.0+json",
    accept="application/json"
)

result = json.loads(response['body'].read())
print(result['completion'])