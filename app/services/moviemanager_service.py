import os
import json
import boto3
import re
from typing import List, Dict
from app.services.transcribe_service import transcribe_video
from app.services.scene_service import get_video_scenes
import asyncio

def natural_sort_key(s: str) -> List:
    """
    자연스러운 정렬을 위한 키 함수
    숫자가 포함된 문자열을 올바른 순서로 정렬합니다.
    예: video_1.mp4, video_2.mp4, ..., video_10.mp4
    """
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', s)]

def get_video_files_from_s3_folder(s3_folder_path: str) -> List[str]:
    """
    S3 폴더에서 비디오 파일들을 찾아서 정렬된 URI 리스트를 반환합니다.
    """
    if not s3_folder_path.startswith("s3://"):
        raise ValueError("s3_folder_path는 's3://'로 시작해야 합니다.")
    
    # S3 폴더 경로 파싱
    path_parts = s3_folder_path.replace("s3://", "").split("/")
    bucket = path_parts[0]
    prefix = "/".join(path_parts[1:])
    
    # 마지막이 /로 끝나지 않으면 추가
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    
    s3 = boto3.client('s3')
    
    try:
        # S3 폴더 내 모든 객체 조회
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if 'Contents' not in response:
            raise ValueError(f"S3 폴더가 비어있거나 존재하지 않습니다: {s3_folder_path}")
        
        # 비디오 파일 확장자 필터링
        video_extensions = ['.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv', '.webm']
        video_files = []
        
        for obj in response['Contents']:
            key = obj['Key']
            # 폴더 자체는 제외 (키가 /로 끝나는 경우)
            if key.endswith('/'):
                continue
                
            # 비디오 파일인지 확인
            file_extension = os.path.splitext(key)[1].lower()
            if file_extension in video_extensions:
                video_uri = f"s3://{bucket}/{key}"
                video_files.append(video_uri)
        
        if not video_files:
            raise ValueError(f"S3 폴더에 비디오 파일이 없습니다: {s3_folder_path}")
        
        # 자연스러운 정렬 (숫자를 고려한 정렬)
        # 예: video_1.mp4, video_2.mp4, ..., video_10.mp4 순서로 정렬
        video_files.sort(key=natural_sort_key)
        
        print(f"📁 S3 폴더에서 {len(video_files)}개의 비디오 파일을 발견했습니다:")
        for i, video_file in enumerate(video_files):
            print(f"   {i+1}. {video_file}")
        
        return video_files
        
    except Exception as e:
        raise RuntimeError(f"S3 폴더 조회 중 오류 발생: {str(e)}")

def create_claude_prompt_with_context(utterances: List[Dict], scene_images: List[Dict], previous_summaries: List[str] = None, current_video_index: int = 0) -> str:
    """
    Rolling Context 기법으로 최근 3개 비디오 요약만 포함하여 Claude 프롬프트를 생성합니다.
    """
    conversation = "\n".join([
        f"[{utterance['speaker']}] {utterance['text']}"
        for utterance in utterances
    ])
    
    scene_times = "\n".join([
        f"Scene {i+1}: start_time={scene['start_time']}"
        for i, scene in enumerate(scene_images)
    ])
    
    # Rolling Context: 최근 3개 비디오 요약만 사용
    context = ""
    if previous_summaries:
        # 최근 3개만 선택 (현재 비디오 직전 3개)
        recent_summaries = previous_summaries[-3:]
        start_index = max(0, current_video_index - len(recent_summaries))
        
        context = "\n\n[최근 영상들의 줄거리]\n" + "\n\n".join([
            f"영상 {start_index + i + 1}: {summary}" 
            for i, summary in enumerate(recent_summaries)
        ]) + "\n\n"
        
        print(f"📚 Rolling Context: 최근 {len(recent_summaries)}개 영상의 요약을 컨텍스트로 사용 (영상 {start_index + 1}~{current_video_index})")
    
    prompt = f"""다음은 연속된 비디오 시리즈의 일부입니다.{context}[현재 영상의 대화 내용]\n{conversation}\n\n[현재 영상의 장면별 시작 시각]\n{scene_times}\n\n최근 영상들의 맥락을 고려하여 현재 영상에 대해:\n1. 각 장면이 보여주는 상황을 설명해주세요\n2. 대화 내용과 연관지어 설명해주세요\n3. 최근 영상들과의 연결점이나 스토리 진행을 분석해주세요\n\n현재 영상의 내용을 요약해주세요."""
    
    return prompt

async def get_bedrock_response_with_context(utterances: List[Dict], scene_images: List[Dict], previous_summaries: List[str] = None, current_video_index: int = 0) -> str:
    """
    Rolling Context 기법으로 최근 3개 비디오 요약만 컨텍스트로 포함하여 Bedrock Claude 응답을 생성합니다.
    """
    bedrock = boto3.client(
        service_name='bedrock-runtime',
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )
    model_id = os.getenv("CLAUDE_MODEL_ID")

    # 텍스트 프롬프트 생성 (Rolling Context 적용)
    text_prompt = create_claude_prompt_with_context(utterances, scene_images, previous_summaries, current_video_index)
    
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
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )
    model_id = os.getenv("CLAUDE_MODEL_ID")

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

async def process_videos_from_folder(s3_folder_path: str, language_code: str = "ko-KR", threshold: float = 30.0) -> Dict:
    """
    S3 폴더에서 비디오 파일들을 찾아 순차적으로 처리하여 각각의 요약과 최종 요약을 생성합니다.
    """
    try:
        # S3 폴더에서 비디오 파일들 조회
        video_uris = get_video_files_from_s3_folder(s3_folder_path)
        
        video_summaries = []
        previous_summaries = []
        
        print(f"🎥 총 {len(video_uris)}개의 비디오를 순차적으로 처리합니다.")
        print("=" * 80)
        
        for i, video_uri in enumerate(video_uris):
            print(f"🎬 [{i+1}/{len(video_uris)}] 비디오 처리 시작: {video_uri}")
            
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
            
            # Rolling Context를 적용하여 현재 비디오 요약 생성
            summary = await get_bedrock_response_with_context(utterances, scene_images, previous_summaries, i)
            
            video_summaries.append({
                "video_uri": video_uri,
                "summary": summary,
                "order": i + 1
            })
            
            # 다음 비디오 처리를 위해 이전 요약에 추가
            previous_summaries.append(summary)
            
            print(f"✅ [{i+1}/{len(video_uris)}] 비디오 처리 완료")
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
        raise RuntimeError(f"S3 폴더 비디오 처리 중 오류 발생: {str(e)}")