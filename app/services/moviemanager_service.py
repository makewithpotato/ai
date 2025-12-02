import os
import json
import boto3
import re
from typing import List, Dict
from app.services.transcribe_service import transcribe_video
from app.services.scene_service import scene_process, download_json_from_s3
from app.services.video_chunk_service import generate_video_chunks_info, extract_chunk_for_processing, cleanup_chunk_file
from app.crud import create_or_update_summary, get_summaries_up_to, delete_summaries_from, update_movie_status, mark_movie_failed, get_resume_info, get_movie, get_custom_prompts, get_custom_retrievals
from app.database import SessionLocal
import asyncio

def load_prompts() -> Dict[str, str]:
    """
    prompts.txt 파일에서 프롬프트 템플릿을 로드합니다.
    """
    prompts_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "prompts.txt")
    
    with open(prompts_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    prompts = {}
    # 줄 단위로 파싱하여 섹션을 식별
    lines = content.split('\n')
    current_section = None
    current_content = []
    
    for line in lines:
        # 섹션 헤더 식별 (줄의 시작과 끝이 []로 둘러싸인 경우)
        if line.strip().startswith('[') and line.strip().endswith(']') and not line.strip().startswith('[현재') and not line.strip().startswith('[등장'):
            # 이전 섹션 저장
            if current_section and current_content:
                prompts[current_section] = '\n'.join(current_content).strip()
            
            # 새 섹션 시작
            current_section = line.strip()[1:-1]  # [ ] 제거
            current_content = []
        else:
            # 섹션 내용 추가
            if current_section:
                current_content.append(line)
    
    # 마지막 섹션 저장
    if current_section and current_content:
        prompts[current_section] = '\n'.join(current_content).strip()
    
    print(f"📄 프롬프트 템플릿 로드 완료: {list(prompts.keys())}")
    return prompts


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

def create_claude_prompt_with_context(utterances: List[Dict], scene_images: List[Dict], characters_info: str, previous_summaries: List[str] = None, current_video_index: int = 0) -> str:
    """
    Rolling Context 기법으로 최근 3개 비디오 요약만 포함하여 Claude 프롬프트를 생성합니다.
    """
    # 프롬프트 템플릿 로드
    prompts = load_prompts()
    template = prompts.get("VIDEO_ANALYSIS_PROMPT", "")
    
    # 안전한 conversation 생성
    if utterances:
        conversation = "\n".join([
            f"[{utterance.get('speaker', 'Unknown')}] {utterance.get('text', '')}"
            for utterance in utterances if utterance and utterance.get('text')
        ])
    else:
        conversation = "(이 영상에는 대화 내용이 없습니다)"
    
    # 안전한 scene_times 생성
    if scene_images:
        scene_times = "\n".join([
            f"Scene {i+1}: start_time={scene.get('start_time', 0)}"
            for i, scene in enumerate(scene_images) if scene
        ])
    else:
        scene_times = "(이 영상에는 장면 정보가 없습니다)"
    
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
    
    # 템플릿에 변수 삽입
    prompt = template.format(
        characters_info=characters_info,
        context=context,
        conversation=conversation,
        scene_times=scene_times
    )
    
    return prompt

async def get_bedrock_response_with_context(utterances: List[Dict], scene_images: List[Dict], characters_info: str, previous_summaries: List[str] = None, current_video_index: int = 0) -> str:
    """
    Rolling Context 기법으로 최근 3개 비디오 요약만 컨텍스트로 포함하여 Bedrock Claude 응답을 생성합니다.
    """
    bedrock = boto3.client(
        service_name='bedrock-runtime',
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )
    model_id = os.getenv("CLAUDE_MODEL_ID")

    # 텍스트 프롬프트 생성 (Rolling Context 적용)
    text_prompt = create_claude_prompt_with_context(utterances, scene_images, characters_info, previous_summaries, current_video_index)
    
    # 디버깅: 프롬프트 출력
    print("=" * 80)
    print("📝 PROMPT INPUT:")
    print("=" * 80)
    print(text_prompt)
    print("=" * 80)

    # 멀티모달 메시지 구성
    content = []
    if scene_images:
        for i, scene in enumerate(scene_images):
            if scene and scene.get("image"):
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

def parse_final_summary(final_summary_text: str) -> Dict[str, str]:
    """
    최종 요약에서 줄거리와 평론을 분리합니다.
    
    Args:
        final_summary_text: Claude에서 받은 최종 요약 텍스트
        
    Returns:
        Dict: {"story": "줄거리", "review": "평론"}
    """
    try:
        # ####### 구분자로 분리
        parts = final_summary_text.split("#######")
        
        if len(parts) >= 2:
            story = parts[0].strip()
            review = parts[1].strip()
            
            print(f"📖 줄거리 추출 완료 (길이: {len(story)} 문자)")
            print(f"📝 평론 추출 완료 (길이: {len(review)} 문자)")
            
            return {
                "story": story,
                "review": review
            }
        else:
            # 구분자가 없는 경우 전체를 줄거리로 처리
            print("⚠️ ####### 구분자를 찾을 수 없어 전체를 줄거리로 처리합니다.")
            return {
                "story": final_summary_text.strip(),
                "review": "평론 정보가 없습니다."
            }
            
    except Exception as e:
        print(f"❌ 최종 요약 파싱 중 오류: {str(e)}")
        return {
            "story": final_summary_text.strip(),
            "review": "평론 파싱 중 오류가 발생했습니다."
        }

def collect_thumbnail_info(video_summaries: List[Dict], s3_video_uri: str = None) -> Dict[str, any]:
    """
    썸네일 정보를 수집합니다.
    
    Args:
        video_summaries: 비디오 요약 리스트 (썸네일 URL 포함)
        s3_video_uri: 원본 비디오 URI (단일 비디오 모드용)
        
    Returns:
        Dict: {"folder_uri": str, "urls": List[str]}
    """
    thumbnail_urls = []
    thumbnail_folder_uri = None
    
    try:
        # 각 요약에서 썸네일 URL 수집 (미래에 추가될 수 있음)
        for summary in video_summaries:
            if isinstance(summary, dict) and "thumbnail_urls" in summary:
                thumbnail_urls.extend(summary["thumbnail_urls"])
        
        # 단일 비디오 모드인 경우 폴더 URI 생성
        if s3_video_uri and s3_video_uri.startswith("s3://"):
            # 원본 비디오 URI에서 썸네일 폴더 경로 생성
            # 예: s3://bucket/movies/series1/episode1.mp4 → s3://scenes-bucket/movies/series1/thumbnails/
            uri_parts = s3_video_uri.replace("s3://", "").split("/")
            
            if len(uri_parts) > 1:
                # 디렉토리 부분 추출 (파일명 제외)
                directory_path = "/".join(uri_parts[1:-1])
                if directory_path:
                    # 같은 디렉토리에 thumbnails 폴더 생성
                    scenes_bucket = os.getenv("SCENES_BUCKET")
                    if scenes_bucket:
                        thumbnail_folder_uri = f"s3://{scenes_bucket}/{directory_path}/thumbnails/"
                    else:
                        print("⚠️ SCENES_BUCKET 환경변수가 설정되지 않았습니다.")
        
        print(f"📷 썸네일 정보 수집 완료:")
        print(f"   폴더 URI: {thumbnail_folder_uri}")
        print(f"   개별 URL 개수: {len(thumbnail_urls)}")
        
        return {
            "folder_uri": thumbnail_folder_uri,
            "urls": thumbnail_urls
        }
        
    except Exception as e:
        print(f"❌ 썸네일 정보 수집 중 오류: {str(e)}")
        return {
            "folder_uri": None,
            "urls": []
        }

async def get_final_scenes(custom_retrievals: List[str]) -> List[Dict]:
    """
    커스텀 검색어들을 사용하여 최종 장면 검색 결과를 생성합니다.

    1. 데이터베이스에서 movie에 임베딩 존재하는지 확인하기.
    2. 임베딩 존재하면 s3 접근하여 임베딩 벡터 리스트 가져오기.
    3. 가져온 벡터들 중 일정 이상 유사한 벡터 여러 개 선택.
    4. 선택한 벡터들에 해당하는 S3 이미지(장면) URI 반환.
    """

    pass

    # db = SessionLocal()
    # embedding_uri = get_embedding_uri(db, movie_id, 1)  # movie에 저장된 임베딩 존재하는지 확인
    # db.close()
    # if not embedding_uri:
    #     raise ValueError("해당 영화에 대한 임베딩이 존재하지 않습니다. 먼저 영화를 처리해야 합니다.")
    
    # print(f"📊 임베딩 URI: {embedding_uri}")

    # # S3에서 임베딩 벡터 리스트 가져오기
    # embeddings = download_json_from_s3(embedding_uri)



    
    

async def create_final_results(video_summaries: List[str], custom_prompts: List[str], characters_info: str) -> List[tuple]:
    """
    모든 비디오 요약을 종합하여 최종 요약을 생성합니다.
    """
    bedrock = boto3.client(
        service_name='bedrock-runtime',
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )
    model_id = os.getenv("CLAUDE_MODEL_ID")

    # 프롬프트 템플릿 로드
    pre_prompts = load_prompts()

    # 각 입력 프롬프트 가져오기.
    template = pre_prompts.get("FINAL_SUMMARY_PROMPT", "")

    # 모든 요약을 하나로 합침
    all_summaries = "\n\n".join([
        f"영상 {i+1}:\n{summary}" 
        for i, summary in enumerate(video_summaries)
    ])

    final_responses = []

    # get all prompts and answers
    for index, current_prompt in enumerate(custom_prompts):
        prompt = current_prompt + "\nthe sentence bleow describes the video.\n" + all_summaries\
        + "\nthe sentence below shows the information of the character\n" + characters_info

        # 디버깅: 최종 요약 프롬프트 출력
        print("=" * 80)
        print(f"🎬 FINAL SUMMARY PROMPT INPUT {index + 1}:")
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
        print(f"🎭 SUMMARY RESPONSE {index + 1}:")
        print("=" * 80)
        print(final_response)
        print("=" * 80)

        result_tuple = (current_prompt, final_response)
        
        final_responses.append(result_tuple)

    return final_responses


async def process_single_video(s3_video_uri: str, characters_info: str, movie_id: int, 
                              segment_duration: int = 600, init: bool = False, 
                              language_code: str = "ko-KR", threshold: float = 30.0) -> Dict:
    """
    원본 비디오 파일을 받아서 동적으로 청크를 추출하며 순차적으로 처리하여 각각의 요약과 최종 요약을 생성합니다.
    
    Args:
        s3_video_uri: 원본 비디오 S3 URI
        characters_info: 등장인물 정보
        movie_id: 영화 ID (데이터베이스 저장용)
        segment_duration: 각 세그먼트의 길이 (초 단위, 기본값: 10분)
        init: True이면 처음부터 시작, False이면 마지막 상태부터 재시작
        language_code: 언어 코드
        threshold: 장면 감지 임계값
    
    Returns:
        Dict: 처리 결과
    """
    try:
        print(f"🎬 원본 비디오 동적 청크 처리 시작")
        print(f"   원본 URI: {s3_video_uri}")
        print(f"   Movie ID: {movie_id}")
        print(f"   세그먼트 길이: {segment_duration}초 ({segment_duration/60:.1f}분)")
        print("=" * 80)
        
        # 청크 정보 생성 (실제 파일 생성 없이 메타데이터만)
        chunks_info = generate_video_chunks_info(s3_video_uri, segment_duration)
        total_chunks = len(chunks_info)
        
        # init 파라미터에 따른 처리
        start_from = 0
        
        if init:
            print(f"🔄 init=True: 처음부터 새로 시작합니다. Movie ID: {movie_id}")
            # 기존 요약들 모두 삭제
            db = SessionLocal()
            deleted_count = delete_summaries_from(db, movie_id, 1)  # summary_id 1부터 모두 삭제
            update_movie_status(db, movie_id, "PENDING")  # 상태를 PENDING으로 리셋
            db.close()
            print(f"🗑️ 기존 요약 {deleted_count}개 삭제 완료")
            print(f"📊 Movie 상태 리셋: PENDING")
            
        else:
            # 재시작 정보 확인
            db = SessionLocal()
            resume_info = get_resume_info(db, movie_id)
            db.close()
            
            if resume_info:
                if resume_info.get("stage") == "organizing" or resume_info.get("stage") == "complete":
                    if resume_info.get("stage") == "complete":
                        print(f"⚠️ 이미 완료된 작업입니다. Movie ID: {movie_id}")
                        print(f"💡 처음부터 다시 시작하려면 init=true로 설정하세요.")
                    print(f"🔄 ORGANIZING 단계에서 재시작합니다. Movie ID: {movie_id}")
                    start_from = total_chunks  # 모든 청크 건너뛰고 최종 요약으로
                    
                elif resume_info.get("stage") == "proceeding":
                    current = resume_info.get("current", 0)
                    total = resume_info.get("total", 0)
                    print(f"🔄 PROCEEDING[{current}/{total}] 단계에서 재시작합니다. Movie ID: {movie_id}")
                    start_from = current  # 현재 진행된 위치부터 시작
            else:
                print(f"🆕 새로운 작업을 시작합니다. Movie ID: {movie_id}")
        
        # 변수 초기화
        video_summaries = []
        previous_summaries = []
        
        if start_from > 0 and start_from < total_chunks:  # PROCEEDING 재시작인 경우
            # 기존 요약들을 로드
            db = SessionLocal()
            existing_summaries = get_summaries_up_to(db, movie_id, start_from)
            db.close()
            
            for summary in existing_summaries:
                chunk_info = chunks_info[summary.summary_id - 1] if summary.summary_id <= len(chunks_info) else None
                video_summaries.append({
                    "video_uri": f"chunk_{summary.summary_id}_{chunk_info['start']:.0f}s-{chunk_info['end']:.0f}s" if chunk_info else f"chunk_{summary.summary_id}",
                    "summary": summary.summary_text,
                    "order": summary.summary_id,
                    "summary_id": summary.summary_id
                })
                previous_summaries.append(summary.summary_text)
            
            print(f"📚 PROCEEDING 재시작: 기존 요약 {len(existing_summaries)}개 로드 완료")
        elif start_from >= total_chunks:  # ORGANIZING 재시작인 경우
            # 기존 청크 요약들을 모두 로드
            db = SessionLocal()
            existing_summaries = get_summaries_up_to(db, movie_id, total_chunks)
            db.close()
            
            for summary in existing_summaries:
                if summary.summary_id <= total_chunks:  # 최종 요약 제외
                    chunk_info = chunks_info[summary.summary_id - 1] if summary.summary_id <= len(chunks_info) else None
                    video_summaries.append({
                        "video_uri": f"chunk_{summary.summary_id}_{chunk_info['start']:.0f}s-{chunk_info['end']:.0f}s" if chunk_info else f"chunk_{summary.summary_id}",
                        "summary": summary.summary_text,
                        "order": summary.summary_id,
                        "summary_id": summary.summary_id
                    })
            
            print(f"📚 ORGANIZING: 기존 청크 요약 {len(video_summaries)}개 로드 완료")
        
        # 상태를 PROCEEDING으로 업데이트 (시작)
        if start_from < total_chunks:
            db = SessionLocal()
            update_movie_status(db, movie_id, f"PROCEEDING[{start_from}/{total_chunks}]")
            db.close()
            print(f"📊 Movie 상태 업데이트: PROCEEDING[{start_from}/{total_chunks}]")
        
        print(f"🎥 총 {total_chunks}개의 청크 중 {start_from + 1}번부터 처리합니다.")
        print(f"🎬 Movie ID: {movie_id}")
        print("=" * 80)
        
        # start_from 인덱스부터 청크 처리 시작
        for i in range(start_from, total_chunks):
            chunk_info = chunks_info[i]
            current_chunk = i + 1
            
            # 각 청크 처리 시작 시 상태 업데이트
            db = SessionLocal()
            update_movie_status(db, movie_id, f"PROCEEDING[{current_chunk}/{total_chunks}]")
            db.close()
            print(f"📊 Movie 상태 업데이트: PROCEEDING[{current_chunk}/{total_chunks}]")
            
            print(f"🎬 [{current_chunk}/{total_chunks}] 청크 처리 시작: {chunk_info['start']:.1f}s - {chunk_info['end']:.1f}s ({chunk_info['duration']:.1f}s)")
            
            # 청크 파일 동적 추출
            chunk_file_path = None
            try:
                chunk_file_path = extract_chunk_for_processing(s3_video_uri, chunk_info)
                
                # 청크를 임시 S3에 업로드하지 않고 로컬 파일 URI로 처리
                chunk_uri = f"file://{chunk_file_path}"
                
                # transcribe process와 scene process 병렬 처리
                transcribe_task = asyncio.to_thread(transcribe_video, chunk_uri, language_code)
                scene_task = asyncio.to_thread(scene_process, chunk_uri, threshold, movie_id, s3_video_uri)
                utterances, scenes = await asyncio.gather(transcribe_task, scene_task)
                
                print(f"✅ STT 결과: {len(utterances) if utterances else 0}개의 발화")
                print(f"✅ 장면 감지: {len(scenes) if scenes else 0}개의 장면")
                
                # 빈 데이터 처리
                if not utterances:
                    utterances = []
                    print("⚠️ STT 결과가 없습니다. (무음 구간일 수 있습니다)")
                
                if not scenes:
                    scenes = []
                    print("⚠️ 장면 감지 결과가 없습니다.")
                
                # scene의 base64 이미지와 start_time 추출
                scene_images = [
                    {"start_time": scene["start_time"], "image": scene["frame_image"]}
                    for scene in scenes
                ] if scenes else []
                
                # 데이터가 없는 경우 건너뛰기
                if not utterances and not scene_images:
                    print("⚠️ STT와 장면 데이터가 모두 없어 이 청크를 건너뜁니다.")
                    continue
                
                print(f"🤖 Claude 요약 생성 시작...")
                # Rolling Context를 적용하여 현재 청크 요약 생성
                summary = await get_bedrock_response_with_context(utterances, scene_images, characters_info, previous_summaries, i)
                print(f"✅ Claude 요약 생성 완료 (길이: {len(summary)} 문자)")
                
                # 요약을 데이터베이스에 저장 (청크 순서에 맞는 summary_id 사용)
                print(f"💾 데이터베이스 저장 시작...")
                summary_id = i + 1  # 청크 순서와 동일하게 (1부터 시작)
                print(f"   할당된 Summary ID: {summary_id} (청크 순서 {i + 1})")
                save_success = save_summary_to_db(movie_id, summary_id, summary)
                
                if save_success:
                    print(f"💾 요약 저장 완료: Summary ID {summary_id}")
                else:
                    print(f"⚠️ 요약 저장 실패: Summary ID {summary_id}")
                
                video_summaries.append({
                    "video_uri": f"chunk_{current_chunk}_{chunk_info['start']:.0f}s-{chunk_info['end']:.0f}s",
                    "summary": summary,
                    "order": i + 1,
                    "summary_id": summary_id
                })
                
                # 다음 청크 처리를 위해 이전 요약에 추가
                previous_summaries.append(summary)
                
            finally:
                # 청크 임시 파일 정리
                if chunk_file_path:
                    cleanup_chunk_file(chunk_file_path)
            
            
            print(f"✅ [{current_chunk}/{total_chunks}] 청크 처리 완료")
            print("=" * 80)
        
        # 최종 요약 생성 시작 시 상태 업데이트
        db = SessionLocal()
        update_movie_status(db, movie_id, "ORGANIZING")
        db.close()
        print(f"📊 Movie 상태 업데이트: ORGANIZING")

        # 커스텀 프롬프트 가져오기
        db = SessionLocal()
        custom_prompts = get_custom_prompts(db, movie_id)
        custom_retrievals = get_custom_retrievals(db, movie_id)
        db.close()
        print(f"프롬프트 {len(custom_prompts)}개, 검색어 {len(custom_retrievals)}개 로드 완료")
        
        print("🎭 최종 프롬프트 응답 결과 생성 중...")
        # 최종 프롬프트 응답 결과 생성
        final_summary = await create_final_results([vs["summary"] for vs in video_summaries], custom_prompts, characters_info)
        print(f"✅ 최종 요약 생성 완료 (길이: {len(final_summary)} 문자)")

        # 최종 장면 검색 결과 생성 (아래 함수는 위와 다르게 직접 db 조회를 통해 정보에 접근한다.)
        #final_scenes = await get_final_scenes(custom_retrievals)
        # s3 uri들의 리스트의 딕셔너리 형태가 되어야 할 것.
        print(f"✅ 최종 장면 검색 결과 생성 완료 문자)")

        # 최종 요약도 데이터베이스에 저장 (모든 청크 다음 순서)
        print(f"💾 최종 요약 데이터베이스 저장 시작...")
        final_summary_id = total_chunks + 1  # 마지막 청크 다음 순서
        print(f"   할당된 Final Summary ID: {final_summary_id} (최종 요약)")
        final_save_success = save_summary_to_db(movie_id, final_summary_id, final_summary)
        
        if final_save_success:
            print(f"💾 최종 요약 저장 완료: Summary ID {final_summary_id}")
        else:
            print(f"⚠️ 최종 요약 저장 실패: Summary ID {final_summary_id}")
        
        # 모든 처리 완료 시 상태 업데이트
        db = SessionLocal()
        update_movie_status(db, movie_id, "COMPLETE")
        db.close()
        print(f"📊 Movie 상태 업데이트: COMPLETE")
        
        print("🎉 모든 청크 처리 완료!")
        print("=" * 80)
        
        # 최종 요약을 줄거리와 평론으로 분리 (이제 필요 없다.)
        # parsed_summary = parse_final_summary(final_summary)
        
        # 썸네일 정보 수집
        thumbnail_info = collect_thumbnail_info(video_summaries, s3_video_uri)
        
        return {
            "prompt2results": final_summary,
            "thumbnail_folder_uri": thumbnail_info["folder_uri"]
        }
        
    except Exception as e:
        # 오류 발생 시 실패 상태로 업데이트
        try:
            db = SessionLocal()
            mark_movie_failed(db, movie_id)
            db.close()
            print(f"📊 Movie 상태 업데이트: 오류로 인한 FAILED 상태")
        except:
            pass
        
        print(f"❌ 오류 발생: {str(e)}")
        raise RuntimeError(f"원본 비디오 처리 중 오류 발생: {str(e)}")

def save_summary_to_db(movie_id: int, summary_id: int, summary_text: str) -> bool:
    """
    요약을 데이터베이스에 저장합니다.
    
    Args:
        movie_id: 영화 ID
        summary_id: 요약 순서 ID
        summary_text: 요약 텍스트
    
    Returns:
        bool: 저장 성공 여부
    """
    try:
        print(f"💾 요약 저장 시도: Movie ID {movie_id}, Summary ID {summary_id}")
        print(f"   Summary Text 길이: {len(summary_text)} 문자")
        print(f"   Summary Text 미리보기: {summary_text[:100]}...")
        
        # 별도의 데이터베이스 세션 사용 (트랜잭션 롤백 방지)
        db = SessionLocal()
        
        try:
            # movie 테이블에 해당 ID가 존재하는지 확인
            movie = get_movie(db, movie_id)
            if not movie:
                print(f"❌ Movie ID {movie_id}가 존재하지 않습니다!")
                return False
            
            print(f"✅ Movie ID {movie_id} 확인됨: {movie.title}")
            
            # 요약 생성 및 저장 (덮어쓰기 지원)
            summary = create_or_update_summary(db, movie_id, summary_id, summary_text)
            
            print(f"✅ 요약 저장 완료: Movie ID {movie_id}, Summary ID {summary_id}")
            print(f"   저장된 데이터: movie_id={summary.movie_id}, summary_id={summary.summary_id}")
            return True
            
        except Exception as e:
            print(f"❌ 요약 저장 중 오류: {str(e)}")
            db.rollback()
            return False
        finally:
            db.close()
        
    except Exception as e:
        print(f"❌ 요약 저장 실패: {str(e)}")
        import traceback
        print(f"   상세 오류: {traceback.format_exc()}")
        return False

async def process_videos_from_folder(s3_folder_path: str, characters_info: str, movie_id: int, init: bool = False, language_code: str = "ko-KR", threshold: float = 30.0) -> Dict:
    """
    S3 폴더에서 비디오 파일들을 찾아 순차적으로 처리하여 각각의 요약과 최종 요약을 생성합니다.
    
    Args:
        s3_folder_path: S3 폴더 경로
        characters_info: 등장인물 정보
        movie_id: 영화 ID (데이터베이스 저장용)
        init: True이면 처음부터 시작, False이면 마지막 상태부터 재시작
        language_code: 언어 코드
        threshold: 장면 감지 임계값
    
    Returns:
        Dict: 처리 결과
    """
    try:
        # S3 폴더에서 비디오 파일들 조회 (먼저 조회해서 총 개수 확인)
        video_uris = get_video_files_from_s3_folder(s3_folder_path)
        total_videos = len(video_uris)
        
        # init 파라미터에 따른 처리
        start_from = 0
        
        if init:
            print(f"🔄 init=True: 처음부터 새로 시작합니다. Movie ID: {movie_id}")
            # 기존 요약들 모두 삭제
            db = SessionLocal()
            deleted_count = delete_summaries_from(db, movie_id, 1)  # summary_id 1부터 모두 삭제
            update_movie_status(db, movie_id, "PENDING")  # 상태를 PENDING으로 리셋
            db.close()
            print(f"🗑️ 기존 요약 {deleted_count}개 삭제 완료")
            print(f"📊 Movie 상태 리셋: PENDING")
        else:
            # 재시작 정보 확인
            db = SessionLocal()
            resume_info = get_resume_info(db, movie_id)
            db.close()
            
            if resume_info:
                if resume_info.get("stage") == "organizing" or resume_info.get("stage") == "complete":
                    if resume_info.get("stage") == "complete":
                        print(f"⚠️ 이미 완료된 작업입니다. Movie ID: {movie_id}")
                        print(f"💡 처음부터 다시 시작하려면 init=true로 설정하세요.")
                    print(f"🔄 ORGANIZING 단계에서 재시작합니다. Movie ID: {movie_id}")
                    # 모든 비디오 요약은 완료되었으므로 최종 요약만 다시 생성
                    start_from = total_videos  # 모든 비디오 건너뛰고 최종 요약으로
                    
                    # 기존 비디오 요약들을 모두 로드
                    db = SessionLocal()
                    existing_summaries = get_summaries_up_to(db, movie_id, total_videos)
                    db.close()
                    
                    for summary in existing_summaries:
                        video_summaries.append({
                            "video_uri": video_uris[summary.summary_id - 1],
                            "summary": summary.summary_text,
                            "order": summary.summary_id,
                            "summary_id": summary.summary_id
                        })
                    
                    print(f"📚 ORGANIZING: 기존 비디오 요약 {len(existing_summaries)}개 로드 완료")
                elif resume_info.get("stage") == "proceeding":
                    current = resume_info.get("current", 0)
                    total = resume_info.get("total", 0)
                    print(f"🔄 PROCEEDING[{current}/{total}] 단계에서 재시작합니다. Movie ID: {movie_id}")
                    start_from = current  # 마지막 완료된 비디오 다음부터 시작
                    print(f"📍 비디오 {start_from + 1}번부터 재시작합니다.")
            else:
                print(f"🆕 새로운 작업을 시작합니다. Movie ID: {movie_id}")
        
        # 변수 초기화 (ORGANIZING 단계에서는 이미 초기화됨)
        if 'video_summaries' not in locals():
            video_summaries = []
        if 'previous_summaries' not in locals():
            previous_summaries = []
        
        if start_from > 0 and start_from < total_videos:  # PROCEEDING 재시작인 경우
            # 기존 요약들을 로드
            db = SessionLocal()
            existing_summaries = get_summaries_up_to(db, movie_id, start_from)
            db.close()
            
            for summary in existing_summaries:
                video_summaries.append({
                    "video_uri": video_uris[summary.summary_id - 1],  # summary_id는 1부터 시작
                    "summary": summary.summary_text,
                    "order": summary.summary_id,
                    "summary_id": summary.summary_id
                })
                previous_summaries.append(summary.summary_text)
            
            print(f"📚 PROCEEDING 재시작: 기존 요약 {len(existing_summaries)}개 로드 완료")
        elif start_from >= total_videos:  # ORGANIZING 재시작인 경우
            # 기존 비디오 요약들을 모두 로드
            db = SessionLocal()
            existing_summaries = get_summaries_up_to(db, movie_id, total_videos if total_videos > 0 else 100)  # 충분히 큰 값
            db.close()
            
            for summary in existing_summaries:
                if summary.summary_id <= total_videos:  # 최종 요약 제외
                    video_summaries.append({
                        "video_uri": video_uris[summary.summary_id - 1] if summary.summary_id <= len(video_uris) else f"s3://dummy/segment_{summary.summary_id:03d}.mp4",
                        "summary": summary.summary_text,
                        "order": summary.summary_id,
                        "summary_id": summary.summary_id
                    })
            
            print(f"📚 ORGANIZING: 기존 비디오 요약 {len(video_summaries)}개 로드 완료")
        
        # 상태를 PROCEEDING으로 업데이트 (시작)
        if start_from < total_videos:
            db = SessionLocal()
            update_movie_status(db, movie_id, f"PROCEEDING[{start_from}/{total_videos}]")
            db.close()
            print(f"📊 Movie 상태 업데이트: PROCEEDING[{start_from}/{total_videos}]")
        
        print(f"🎥 총 {total_videos}개의 비디오 중 {start_from + 1}번부터 처리합니다.")
        print(f"🎬 Movie ID: {movie_id}")
        print("=" * 80)
        
        # start_from 인덱스부터 비디오 처리 시작
        for i in range(start_from, total_videos):
            video_uri = video_uris[i]
            # 각 비디오 처리 시작 시 상태 업데이트
            current_video = i + 1
            db = SessionLocal()
            update_movie_status(db, movie_id, f"PROCEEDING[{current_video}/{total_videos}]")
            db.close()
            print(f"📊 Movie 상태 업데이트: PROCEEDING[{current_video}/{total_videos}]")
            
            print(f"🎬 [{current_video}/{total_videos}] 비디오 처리 시작: {video_uri}")
            
            # transcribe와 scene 병렬 처리 (movie_id 전달)
            transcribe_task = asyncio.to_thread(transcribe_video, video_uri, language_code)
            scene_task = asyncio.to_thread(scene_process, video_uri, threshold, movie_id)
            utterances, scenes = await asyncio.gather(transcribe_task, scene_task)
            
            print(f"✅ STT 결과: {len(utterances) if utterances else 0}개의 발화")
            print(f"✅ 장면 감지: {len(scenes) if scenes else 0}개의 장면")
            
            # 빈 데이터 처리
            if not utterances:
                utterances = []
                print("⚠️ STT 결과가 없습니다. (엔딩 크레딧이나 무음 구간일 수 있습니다)")
            
            if not scenes:
                scenes = []
                print("⚠️ 장면 감지 결과가 없습니다.")
            
            # scene의 base64 이미지와 start_time 추출
            scene_images = [
                {"start_time": scene["start_time"], "image": scene["frame_image"]}
                for scene in scenes
            ] if scenes else []
            
            # 데이터가 없는 경우 건너뛰기
            if not utterances and not scene_images:
                print("⚠️ STT와 장면 데이터가 모두 없어 이 비디오를 건너뜁니다.")
                continue
            
            print(f"🤖 Claude 요약 생성 시작...")
            # Rolling Context를 적용하여 현재 비디오 요약 생성
            summary = await get_bedrock_response_with_context(utterances, scene_images, characters_info, previous_summaries, i)
            print(f"✅ Claude 요약 생성 완료 (길이: {len(summary)} 문자)")
            
            # 요약을 데이터베이스에 저장 (비디오 순서에 맞는 summary_id 사용)
            print(f"💾 데이터베이스 저장 시작...")
            summary_id = i + 1  # 비디오 순서와 동일하게 (1부터 시작)
            print(f"   할당된 Summary ID: {summary_id} (비디오 순서 {i + 1})")
            save_success = save_summary_to_db(movie_id, summary_id, summary)
            
            if save_success:
                print(f"💾 요약 저장 완료: Summary ID {summary_id}")
            else:
                print(f"⚠️ 요약 저장 실패: Summary ID {summary_id}")
            
            video_summaries.append({
                "video_uri": video_uri,
                "summary": summary,
                "order": i + 1,
                "summary_id": summary_id
            })
            
            # 다음 비디오 처리를 위해 이전 요약에 추가
            previous_summaries.append(summary)
            
            print(f"✅ [{current_video}/{total_videos}] 비디오 처리 완료")
            print("=" * 80)
        
        # 최종 요약 생성 시작 시 상태 업데이트
        db = SessionLocal()
        update_movie_status(db, movie_id, "ORGANIZING")
        db.close()
        print(f"📊 Movie 상태 업데이트: ORGANIZING")

        # 커스텀 프롬프트 가져오기
        db = SessionLocal()
        custom_prompts = get_custom_prompts(db, movie_id)
        db.close()
        print(f"프롬프트 {len(custom_prompts)}개 로드 완료 for 최종 요약 생성")
        
        print("🎭 최종 종합 요약 생성 중...")
        # 최종 프롬프트 응답 결과 생성
        final_summary = await create_final_results([vs["summary"] for vs in video_summaries], custom_prompts, characters_info)
        print(f"✅ 최종 요약 생성 완료 (길이: {len(final_summary)} 문자)")
        
        # 최종 요약도 데이터베이스에 저장 (모든 비디오 다음 순서)
        print(f"💾 최종 요약 데이터베이스 저장 시작...")
        final_summary_id = total_videos + 1  # 마지막 비디오 다음 순서
        print(f"   할당된 Final Summary ID: {final_summary_id} (최종 요약)")
        final_save_success = save_summary_to_db(movie_id, final_summary_id, final_summary)
        
        if final_save_success:
            print(f"💾 최종 요약 저장 완료: Summary ID {final_summary_id}")
        else:
            print(f"⚠️ 최종 요약 저장 실패: Summary ID {final_summary_id}")
        
        # 모든 처리 완료 시 상태 업데이트
        db = SessionLocal()
        update_movie_status(db, movie_id, "COMPLETE")
        db.close()
        print(f"📊 Movie 상태 업데이트: COMPLETE")
        
        print("🎉 모든 비디오 처리 완료!")
        print("=" * 80)
        
        # 최종 요약을 줄거리와 평론으로 분리 (이제 필요 없다.)
        parsed_summary = parse_final_summary(final_summary)
        
        # 썸네일 정보 수집 (폴더 모드에서는 폴더 URI 없음)
        thumbnail_info = collect_thumbnail_info(video_summaries, None)
        
        return {
            "final_story": parsed_summary["story"],
            "final_review": parsed_summary["review"],
            "thumbnail_folder_uri": thumbnail_info["folder_uri"]
        }
        
    except Exception as e:
        # 오류 발생 시 실패 상태로 업데이트
        try:
            db = SessionLocal()
            mark_movie_failed(db, movie_id)
            db.close()
            print(f"📊 Movie 상태 업데이트: 오류로 인한 FAILED 상태")
        except:
            pass
        
        print(f"❌ 오류 발생: {str(e)}")
        raise RuntimeError(f"S3 폴더 비디오 처리 중 오류 발생: {str(e)}")

