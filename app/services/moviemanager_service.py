import os
import json
import boto3
from typing import List, Dict
from app.services.transcribe_service import transcribe_video
from app.services.scene_service import get_video_scenes
import asyncio

def create_claude_prompt_with_context(utterances: List[Dict], scene_images: List[Dict], previous_summaries: List[str] = None) -> str:
    """
    이전 요약들을 포함하여 Claude 프롬프트를 생성합니다.
    """
    conversation = "\n".join([
        f"[{utterance['speaker']}] {utterance['text']}"
        for utterance in utterances
    ])
    
    scene_times = "\n".join([
        f"Scene {i+1}: start_time={scene['start_time']}"
        for i, scene in enumerate(scene_images)
    ])
    
    # 이전 요약들을 컨텍스트로 추가
    context = ""
    if previous_summaries:
        context = "\n\n[이전 영상들의 줄거리]\n" + "\n\n".join([
            f"영상 {i+1}: {summary}" 
            for i, summary in enumerate(previous_summaries)
        ]) + "\n\n"
    
    prompt = f"""다음은 연속된 비디오 시리즈의 일부입니다.{context}[현재 영상의 대화 내용]\n{conversation}\n\n[현재 영상의 장면별 시작 시각]\n{scene_times}\n\n이전 영상들의 맥락을 고려하여 현재 영상에 대해:\n1. 각 장면이 보여주는 상황을 설명해주세요\n2. 대화 내용과 연관지어 설명해주세요\n3. 이전 영상들과의 연결점이나 스토리 진행을 분석해주세요\n\n현재 영상의 내용을 요약해주세요."""
    
    return prompt

async def get_bedrock_response_with_context(utterances: List[Dict], scene_images: List[Dict], previous_summaries: List[str] = None) -> str:
    """
    이전 요약들을 컨텍스트로 포함하여 Bedrock Claude 응답을 생성합니다.
    """
    bedrock = boto3.client(
        service_name='bedrock-runtime',
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )
    model_id = os.getenv("BEDROCK_MODEL_ID")

    # 텍스트 프롬프트 생성 (이전 요약 포함)
    text_prompt = create_claude_prompt_with_context(utterances, scene_images, previous_summaries)
    
    # 디버깅: 프롬프트 출력
    print("=" * 80)
    print("📝 PROMPT INPUT:")
    print("=" * 80)
    print(text_prompt)
    print("=" * 80)

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
    claude_response = response_body['content'][0]['text']
    
    # 디버깅: 모델 답변 출력
    print("🤖 CLAUDE RESPONSE:")
    print("=" * 80)
    print(claude_response)
    print("=" * 80)
    
    return claude_response

async def create_final_summary(video_summaries: List[str]) -> str:
    """
    모든 비디오 요약을 종합하여 최종 요약을 생성합니다.
    """
    bedrock = boto3.client(
        service_name='bedrock-runtime',
        region_name=os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
    )
    model_id = os.getenv("BEDROCK_MODEL_ID") or "anthropic.claude-3-sonnet-20240229-v1:0"

    # 모든 요약을 하나로 합침
    all_summaries = "\n\n".join([
        f"영상 {i+1}:\n{summary}" 
        for i, summary in enumerate(video_summaries)
    ])
    
    prompt = f"""다음은 연속된 비디오 시리즈의 각 영상별 요약입니다:\n\n{all_summaries}\n\n위 내용을 바탕으로:\n1. 전체 스토리의 흐름을 정리해주세요\n2. 주요 등장인물과 그들의 관계를 설명해주세요\n3. 핵심 사건들과 갈등 구조를 분석해주세요\n4. 전체 영상 시리즈의 주제와 메시지를 요약해주세요\n\n최종적으로 전체 영상 시리즈에 대한 종합적인 요약을 제공해주세요."""

    # 디버깅: 최종 요약 프롬프트 출력
    print("=" * 80)
    print("🎬 FINAL SUMMARY PROMPT INPUT:")
    print("=" * 80)
    print(prompt)
    print("=" * 80)

    request_body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4096,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ]
    }
    
    response = bedrock.invoke_model(
        modelId=model_id,
        body=json.dumps(request_body)
    )
    response_body = json.loads(response['body'].read())
    final_response = response_body['content'][0]['text']
    
    # 디버깅: 최종 요약 답변 출력
    print("🎭 FINAL SUMMARY RESPONSE:")
    print("=" * 80)
    print(final_response)
    print("=" * 80)
    
    return final_response

async def process_multiple_videos(s3_video_uris: List[str], language_code: str = "ko-KR", threshold: float = 30.0) -> Dict:
    """
    여러 비디오를 순차적으로 처리하여 각각의 요약과 최종 요약을 생성합니다.
    """
    try:
        video_summaries = []
        previous_summaries = []
        
        print(f"🎥 총 {len(s3_video_uris)}개의 비디오를 순차적으로 처리합니다.")
        print("=" * 80)
        
        for i, video_uri in enumerate(s3_video_uris):
            print(f"🎬 [{i+1}/{len(s3_video_uris)}] 비디오 처리 시작: {video_uri}")
            
            # transcribe와 scene 병렬 처리
            transcribe_task = asyncio.to_thread(transcribe_video, video_uri, language_code)
            scene_task = asyncio.to_thread(get_video_scenes, video_uri, threshold)
            utterances, scenes = await asyncio.gather(transcribe_task, scene_task)
            
            print(f"✅ STT 결과: {len(utterances)}개의 발화")
            print(f"✅ 장면 감지: {len(scenes)}개의 장면")
            
            # scene의 base64 이미지와 start_time 추출
            scene_images = [
                {"start_time": scene["start_time"], "image": scene["frame_image"]}
                for scene in scenes
            ]
            
            # 이전 요약들을 컨텍스트로 포함하여 현재 비디오 요약 생성
            summary = await get_bedrock_response_with_context(utterances, scene_images, previous_summaries)
            
            video_summaries.append({
                "video_uri": video_uri,
                "summary": summary,
                "order": i + 1
            })
            
            # 다음 비디오 처리를 위해 이전 요약에 추가
            previous_summaries.append(summary)
            
            print(f"✅ [{i+1}/{len(s3_video_uris)}] 비디오 처리 완료")
            print("=" * 80)
        
        print("🎭 최종 종합 요약 생성 중...")
        # 최종 종합 요약 생성
        final_summary = await create_final_summary([vs["summary"] for vs in video_summaries])
        
        print("🎉 모든 비디오 처리 완료!")
        print("=" * 80)
        
        return {
            "video_summaries": video_summaries,
            "final_summary": final_summary
        }
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        raise RuntimeError(f"다중 비디오 처리 중 오류 발생: {str(e)}") 