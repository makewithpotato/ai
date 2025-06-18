import os
import json
import boto3
from typing import List, Dict
import base64
import asyncio

def create_claude_prompt(utterances: List[Dict], scene_images: List[Dict]) -> str:
    """
    STT 결과와 장면 이미지의 start_time을 기반으로 Claude 프롬프트를 생성합니다.
    """
    conversation = "\n".join([
        f"[{utterance['speaker']}] {utterance['text']}"
        for utterance in utterances
    ])
    
    # 장면별 시간 정보 추가
    scene_times = "\n".join([
        f"Scene {i+1}: start_time={scene['start_time']}"
        for i, scene in enumerate(scene_images)
    ])
    
    prompt = f"""\n\n[대화 내용]\n{conversation}\n\n[장면별 시작 시각]\n{scene_times}\n\n장면들과 대사들을 보고, 화자를 유추하여 줄거리의 형태로 적어주세요."""
    return prompt

async def get_bedrock_response(utterances: List[Dict], scene_images: List[Dict]) -> str:
    bedrock = boto3.client(
        service_name='bedrock-runtime',
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )
    model_id = os.getenv("CLAUDE_MODEL_ID")

    # 텍스트 프롬프트 생성
    text_prompt = create_claude_prompt(utterances, scene_images)

    print(text_prompt)

    # 멀티모달 메시지 구성
    content = []
    for i, scene in enumerate(scene_images):
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": scene["image"]
            }
        })
    content.append({
        "type": "text",
        "text": text_prompt
    })

    request_body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4096,
        "messages": [
            {
                "role": "user",
                "content": content
            }
        ]
    }
    response = bedrock.invoke_model(
        modelId=model_id,
        body=json.dumps(request_body)
    )
    response_body = json.loads(response['body'].read())
    return response_body['content'][0]['text']

async def summarize_content(utterances: List[Dict], scene_images: List[Dict]) -> str:
    try:
        summary = await get_bedrock_response(utterances, scene_images)
        return summary
    except Exception as e:
        raise RuntimeError(f"요약 생성 중 오류 발생: {str(e)}") 