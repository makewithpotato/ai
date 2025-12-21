import os
import json
import boto3
import re
from typing import List, Dict
from app.services.transcribe_service import transcribe_video
from app.services.scene_service import scene_process, download_json_from_s3, delete_embeddings_and_thumbnails
from app.services.video_chunk_service import generate_video_chunks_info, extract_chunk_for_processing, cleanup_chunk_file
from app.services.marengo_service import embed_marengo
from app.crud import (
    create_or_update_summary, 
    get_summaries_up_to, 
    delete_summaries_from,
    update_movie_status, 
    mark_movie_failed,
    get_resume_info, 
    get_movie, 
    get_custom_prompts, 
    get_custom_retrievals, 
    get_embedding_uri,
    set_embedding_uri
)
from app.database import SessionLocal
import asyncio
import numpy as np

def load_prompts(language: str = "kor") -> Dict[str, str]:
    """
    prompts.txt 파일에서 프롬프트 템플릿을 로드합니다.
    """
    if language == "eng":
        prompts_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "prompts_eng.txt")
    else:
        prompts_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "prompts.txt")
    
    with open(prompts_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    prompts = {}
    # 줄 단위로 파싱하여 섹션을 식별
    lines = content.split('\n')
    current_section = None
    current_content = []
    
    for line in lines:
        # 섹션 헤더 식별 (줄의 시작과 끝이 <<>>로 둘러싸인 경우)
        if line.strip().startswith('<<') and line.strip().endswith('>>'):
            # 이전 섹션 저장
            if current_section and current_content:
                prompts[current_section] = '\n'.join(current_content).strip()
            
            # 새 섹션 시작
            current_section = line.strip()[2:-2]  # << >> 제거
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

def create_claude_prompt_with_context(utterances: List[Dict], scene_images: List[Dict], characters_info: str, previous_summaries: List[str] = None, current_video_index: int = 0, prompt_language: str = "kor", custom_utterance = None, with_cw=True, retrieval_queries: List[str] = None) -> str:
    """
    Rolling Context 기법으로 최근 3개 비디오 요약만 포함하여 Claude 프롬프트를 생성합니다.
    장면과 대사를 시간대별로 연결하여 제공합니다.
    """
    # 프롬프트 템플릿 로드
    prompts = load_prompts(prompt_language)
    template = prompts.get("VIDEO_ANALYSIS_PROMPT", "")
    
    if custom_utterance:
        conversation = custom_utterance

    else: 
        # 안전한 conversation 생성
        if utterances:
            conversation = "\n".join([
                f"[{utterance.get('speaker', 'Unknown')}] {utterance.get('text', '')}"
                for utterance in utterances if utterance and utterance.get('text')
            ])
        else:
            conversation = "(이 영상에는 대화 내용이 없습니다)"
    
    # 안전한 scene_times 생성 -> 장면과 대사를 시간대별로 연결
    scene_dialogue_mapping = ""
    if scene_images and utterances:
        scene_info_list = []
        for i, scene in enumerate(scene_images):
            if scene:
                scene_start = scene.get('start_time', 0)
                scene_end = scene_start + 5  # 장면 길이를 5초로 가정 (또는 scene에 end_time이 있다면 사용)
                
                # 해당 장면 시간대의 대사 찾기
                scene_utterances = [
                    utt for utt in utterances
                    if utt.get('start_time', 0) < scene_end and utt.get('end_time', 0) > scene_start
                ]
                
                dialogue_texts = []
                for utt in scene_utterances:
                    speaker = utt.get('speaker', 'Unknown')
                    text = utt.get('text', '')
                    if text:
                        dialogue_texts.append(f"[{speaker}] {text}")
                
                dialogue = " / ".join(dialogue_texts) if dialogue_texts else "(대사 없음)"
                
                scene_info_list.append(
                    f"Scene {i}: 시간={scene_start:.1f}s, 대사: {dialogue}"
                )
        
        scene_dialogue_mapping = "\n".join(scene_info_list)
    elif scene_images:
        # utterances가 없는 경우 기존 방식
        scene_dialogue_mapping = "\n".join([
            f"Scene {i}: 시간={scene.get('start_time', 0):.1f}s"
            for i, scene in enumerate(scene_images) if scene
        ])
    else:
        scene_dialogue_mapping = "(이 영상에는 장면 정보가 없습니다)"
    
    # Rolling Context: 최근 3개 비디오 요약만 사용
    context = ""
    if previous_summaries and with_cw:
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
        scene_times=scene_dialogue_mapping
    )
    
    # retrieval_queries가 있는 경우 추가 프롬프트
    if retrieval_queries:
        retrieval_section = "\n\n=== 장면 검색 요청 ===\n"
        retrieval_section += "사용자가 다음 검색어로 장면을 찾고 싶어합니다:\n"
        for idx, query in enumerate(retrieval_queries, 1):
            retrieval_section += f"{idx}. {query}\n"
        retrieval_section += "\n위 장면 목록에서 각 검색어와 가장 관련된 장면 번호들을 선택해주세요.\n"
        retrieval_section += "응답 마지막에 다음 형식으로 추가해주세요:\n"
        retrieval_section += "[SCENE_SELECTION]\n"
        for idx, query in enumerate(retrieval_queries, 1):
            retrieval_section += f"{idx}. {query}: Scene 번호 (쉼표로 구분, 예: 0, 3, 7)\n"
        retrieval_section += "[/SCENE_SELECTION]"
        
        prompt += retrieval_section
    
    return prompt

async def translate_with_claude(text_list: list[str]) -> list[str]:
    """
    자동으로 비영어권 텍스트면 영어로 번역합니다.
    args:
        text_list: 번역할 텍스트 리스트
    returns:
        list[str]: 번역된 텍스트 리스트
    """

    bedrock = boto3.client(
        service_name='bedrock-runtime',
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )
    model_id = os.getenv("CLAUDE_MODEL_ID")

    # convert text to string by list comprehension
    prompt = """Translate the following text to English.
    If its already in English, just repeat it.
    just output the translated text without any extra explanation.
    split each output text with '###' symbol."""
    
    prompt += "\n\n" + " ### ".join(text_list)

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
    translated_text = response_body['content'][0]['text']

    # 디버깅: 모델 답변 출력
    print("🤖 TRANSLATED RESPONSE:")
    print("=" * 80)
    print(translated_text)
    print("=" * 80)

    # 파싱 ### 구분자로 분리
    try:
        translated_list = [part.strip() for part in translated_text.split("###")]
        translated_list = [part for part in translated_list if part]  # 빈 문자열 제거
        print(f"✅ 번역된 텍스트 개수: {len(translated_list)}")
        if len(translated_list) != len(text_list):
            raise ValueError("번역된 텍스트 개수가 입력 텍스트 개수와 일치하지 않습니다.")
        return translated_list
    except Exception as e:
        print(f"❌ 번역 파싱 중 오류: {str(e)}")
        return text_list  # 오류 시 원본 텍스트 반환


async def get_bedrock_response_with_context(utterances: List[Dict], scene_images: List[Dict], characters_info: str, previous_summaries: List[str] = None, current_video_index: int = 0, prompt_language: str = "kor", custom_utterance = None, with_cw=True, retrieval_queries: List[str] = None) -> tuple[str, Dict[str, List[int]]]:
    """
    Rolling Context 기법으로 최근 3개 비디오 요약만 컨텍스트로 포함하여 Bedrock Claude 응답을 생성합니다.
    retrieval_queries가 있으면 장면 선택 결과도 함께 반환합니다.
    
    Returns:
        tuple[str, Dict[str, List[int]]]: (요약 텍스트, 검색어별 선택된 장면 인덱스)
    """
    bedrock = boto3.client(
        service_name='bedrock-runtime',
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )
    model_id = os.getenv("CLAUDE_MODEL_ID")

    # 텍스트 프롬프트 생성 (Rolling Context 적용)
    text_prompt = create_claude_prompt_with_context(utterances, scene_images, characters_info, previous_summaries, current_video_index, prompt_language=prompt_language, custom_utterance=custom_utterance, with_cw=with_cw, retrieval_queries=retrieval_queries)
    
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
    
    # 장면 선택 결과 파싱
    scene_selections = {}
    if retrieval_queries:
        # [SCENE_SELECTION] ... [/SCENE_SELECTION] 섹션 찾기
        import re
        selection_match = re.search(r'\[SCENE_SELECTION\](.*?)\[/SCENE_SELECTION\]', claude_response, re.DOTALL)
        if selection_match:
            selection_text = selection_match.group(1)
            print("\n📌 장면 선택 결과 파싱:")
            
            for idx, query in enumerate(retrieval_queries, 1):
                # 각 검색어에 대한 장면 번호 찾기
                # 해당 줄만 매칭하도록 수정 (줄바꿈 전까지만, 공백도 줄바꿈 제외)
                pattern = f"{idx}\\.\\s*{re.escape(query)}:[ \\t]*([^\\n]*)"
                match = re.search(pattern, selection_text)
                if match:
                    scene_numbers_str = match.group(1).strip()
                    # 빈 문자열이 아닌 경우에만 숫자 추출
                    if scene_numbers_str:
                        # 숫자만 추출
                        scene_numbers = [int(n) for n in re.findall(r'\d+', scene_numbers_str)]
                        if scene_numbers:
                            scene_selections[query] = scene_numbers
                            print(f"  {query}: Scene {scene_numbers}")
                        else:
                            scene_selections[query] = []
                            print(f"  {query}: 선택된 장면 없음 (숫자 없음)")
                    else:
                        scene_selections[query] = []
                        print(f"  {query}: 선택된 장면 없음 (빈 응답)")
                else:
                    scene_selections[query] = []
                    print(f"  {query}: 선택된 장면 없음")
            
            # 응답에서 [SCENE_SELECTION] 섹션 제거
            claude_response = re.sub(r'\[SCENE_SELECTION\].*?\[/SCENE_SELECTION\]', '', claude_response, flags=re.DOTALL).strip()
    else:
        # retrieval_queries가 없는 경우 빈 딕셔너리 반환
        scene_selections = {}
    
    return claude_response, scene_selections

def parse_final_summary(final_summary_text: str, expected_len: int) -> Dict[str, str]:
    """
    최종 요약에서 줄거리와 평론을 분리합니다.
    
    Args:
        final_summary_text: Claude에서 받은 최종 요약 텍스트
        expected_len: 예상되는 분리된 부분의 개수 (예: 2)
        
    Returns:
        Dict: {"story": "줄거리", "review": "평론"}
    """
    try:
        # ####### 구분자로 분리
        parts = final_summary_text.split("#######")
        
        # 오류 처리
        if len(parts) != expected_len:
            raise ValueError(f"예상된 부분 개수({expected_len})와 실제 개수({len(parts)})가 일치하지 않습니다.")
        
        return parts
            
    except Exception as e:
        print(f"❌ 최종 요약 파싱 중 오류: {str(e)}")

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

async def get_final_scenes(custom_retrievals: List[str], movie_id: int, video_summaries: List[Dict] = None) -> Dict[str, List[str]]:
    """
    커스텀 검색어들을 사용하여 최종 장면 검색 결과를 생성합니다.
    
    새로운 방식:
    1. video_summaries에 저장된 LLM의 장면 선택 결과를 수집
    2. 선택된 장면들의 임베딩 벡터를 가져와서 코사인 유사도 계산
    3. 유사도가 높은 top-3 반환

    Args:
        custom_retrievals: 커스텀 검색어 리스트
        movie_id: 영화 ID
        video_summaries: 비디오 요약 정보 (scene_selections 포함)
    
    Returns:
        Dict[str, List[str]]: 검색어별 장면 URI 리스트
    """
    
    if not video_summaries:
        print("⚠️ video_summaries가 제공되지 않았습니다.")
        return {}

    db = SessionLocal()
    embedding_uri = get_embedding_uri(db, movie_id)
    db.close()
    
    if not embedding_uri:
        return {}  # 임베딩이 없으면 빈 결과 반환
    
    print(f"📊 임베딩 URI: {embedding_uri}")

    # S3에서 임베딩 벡터 딕셔너리(JSON) 다운로드
    uri2embedding_dict = download_json_from_s3(embedding_uri)
    print(f"✅ S3에서 임베딩 벡터 데이터 다운로드 완료 (총 {len(uri2embedding_dict)}개 항목)")

    uri_list = list(uri2embedding_dict.keys())
    scene_feat_list = list(uri2embedding_dict.values())
    scene_feat_matrix = np.array(scene_feat_list)
    
    # 정규화
    scene_feat_matrix = scene_feat_matrix / np.linalg.norm(scene_feat_matrix, axis=1, keepdims=True)
    
    result = {}

    # custom_retrievals가 영어가 아닌 경우 bedrock 요청 통해 번역
    print("🌐 커스텀 검색어 번역 처리 중...")
    translated_retrievals = await translate_with_claude(custom_retrievals)
    
    # 각 청크에서 LLM이 선택한 장면 문자열 수집
    for i, retrieval in enumerate(custom_retrievals):
        print(f"\n🔍 검색어 처리 중: '{retrieval}'")
        
        selected_scene_strings = []
        
        # 모든 청크를 순회하며 해당 검색어에 대해 선택된 장면 수집
        for vs in video_summaries:
            scene_selections = vs.get("scene_selections", {})
            if retrieval in scene_selections:
                chunk_selected = scene_selections[retrieval]  # chunk_n_scene_m 형태의 문자열 리스트
                selected_scene_strings.extend(chunk_selected)
        
        if not selected_scene_strings:
            print(f"⚠️ LLM이 '{retrieval}'에 관련된 장면을 찾지 못했습니다.")
            selected_scene_strings = []
        
        # chunk_n_scene_m 문자열을 URI와 매칭
        selected_uris_from_llm = []
        for scene_str in selected_scene_strings:
            # URI 리스트에서 해당 문자열을 포함하는 URI 찾기
            matched_uris = [uri for uri in uri_list if scene_str in uri]
            if matched_uris:
                selected_uris_from_llm.append(matched_uris[0])  # 첫 번째 매칭 URI 사용
                print(f"   {scene_str} → {matched_uris[0]}")
            else:
                print(f"   ⚠️ {scene_str}에 매칭되는 URI 없음")
        
        if not selected_uris_from_llm:
            print(f"⚠️ 매칭된 URI가 없습니다.")
        
        print(f"📋 LLM이 선택한 장면: {len(selected_uris_from_llm)}개")
        
        # 검색어 임베딩
        text_vector = embed_marengo("text", translated_retrievals[i])
        text_vector = np.array(text_vector) / np.linalg.norm(text_vector)
        
        # LLM이 선택한 장면이 3개 미만인 경우
        if len(selected_uris_from_llm) < 3:
            needed_count = 3 - len(selected_uris_from_llm)
            print(f"⚠️ LLM 선택 장면이 3개 미만입니다. LLM 선택 {len(selected_uris_from_llm)}개 + 유사도 분석 {needed_count}개")
            
            # LLM이 선택한 장면들의 URI를 먼저 추가
            selected_uris = selected_uris_from_llm.copy()
            
            # LLM이 선택하지 않은 나머지 장면들
            remaining_uris = [uri for uri in uri_list if uri not in selected_uris_from_llm]
            remaining_indices = [uri_list.index(uri) for uri in remaining_uris]
            
            if remaining_indices:
                # 나머지 장면들에 대해 유사도 계산
                remaining_feats = scene_feat_matrix[remaining_indices]
                remaining_similarities = np.dot(remaining_feats, text_vector)
                
                # 필요한 개수만큼 top-k 선택
                top_k = min(needed_count, len(remaining_indices))
                top_k_indices = np.argsort(-remaining_similarities)[:top_k]
                
                # 추가 장면 URI 추가
                additional_uris = [uri_list[remaining_indices[idx]] for idx in top_k_indices]
                selected_uris.extend(additional_uris)
                
            result[retrieval] = selected_uris
            print(f"✅ 최종 선택: LLM {len(selected_uris_from_llm)}개 + 유사도 {len(selected_uris) - len(selected_uris_from_llm)}개 = 총 {len(result[retrieval])}개")
        else:
            # 선택된 장면들 중 벡터 유사도 높은 top-3 선택
            selected_uris = selected_uris_from_llm.copy()
            selected_indices = [uri_list.index(uri) for uri in selected_uris_from_llm]
            selected_feats = scene_feat_matrix[selected_indices]
            
            # 코사인 유사도 계산
            similarities = np.dot(selected_feats, text_vector)
            
            # top-3 선택
            top_k = min(3, len(selected_uris))
            top_k_indices = np.argsort(-similarities)[:top_k]
            
            result[retrieval] = [selected_uris[idx] for idx in top_k_indices]
            print(f"✅ LLM 선택 장면에서 최종 선택된 장면: {len(result[retrieval])}개")
    
    return result

    
    

async def create_final_results(video_summaries: List[str], custom_prompts: List[str], characters_info: str, prompt_language: str = "kor") -> List[tuple]:
    """
    모든 비디오 요약을 종합하여 최종 요약을 생성합니다.
    """
    bedrock = boto3.client(
        service_name='bedrock-runtime',
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )
    model_id = os.getenv("CLAUDE_MODEL_ID")

    # 프롬프트 템플릿 로드
    pre_prompts = load_prompts(prompt_language)

    # 각 입력 프롬프트 가져오기.
    template = pre_prompts.get("FINAL_SUMMARY_PROMPT", "")

    # 모든 요약을 하나로 합침
    all_summaries = "\n\n".join([
        f"영상 {i+1}:\n{summary}" 
        for i, summary in enumerate(video_summaries)
    ])

    # 커스텀 프롬프트 목록 형태의 string으로 변환
    custom_prompt_list = "\n".join(
        f"{idx + 1}. {item}" for idx, item in enumerate(custom_prompts)
        )

    # 여러 프롬프트를 묶어서 한 번에 보내기
    # 형식이 고정된 응답을 내도록 설계 필요
    # 가져온 프롬프트 템플릿에 video_summaries, custom_prompts, characters_info 삽입
    prompt = template.format(
        all_summaries=all_summaries,
        characters_info=characters_info,
        custom_prompt_list=custom_prompt_list
    )

    final_responses = []

    # get all prompts and answers
    # for index, current_prompt in enumerate(custom_prompts):
    #     prompt = current_prompt + "\nthe sentence bleow describes the video.\n" + all_summaries\
    #     + "\nthe sentence below shows the information of the character\n" + characters_info

    #     # 디버깅: 최종 요약 프롬프트 출력
    #     print("=" * 80)
    #     print(f"🎬 FINAL SUMMARY PROMPT INPUT {index + 1}:")
    #     print("=" * 80)
    #     print(prompt)
    #     print("=" * 80)

    #     request_body = {
    #         "anthropic_version": "bedrock-2023-05-31",
    #         "max_tokens": 4096,
    #         "messages": [
    #             {
    #                 "role": "user",
    #                 "content": [
    #                     {
    #                         "type": "text",
    #                         "text": prompt
    #                     }
    #                 ]
    #             }
    #         ]
    #     }

    #     response = bedrock.invoke_model(
    #     modelId=model_id,
    #     body=json.dumps(request_body)
    #     )

    #     response_body = json.loads(response['body'].read())
    #     final_response = response_body['content'][0]['text']
        
    #     # 디버깅: 최종 요약 답변 출력
    #     print(f"🎭 SUMMARY RESPONSE {index + 1}:")
    #     print("=" * 80)
    #     print(final_response)
    #     print("=" * 80)

    #     result_tuple = (current_prompt, final_response)
        
    #     final_responses.append(result_tuple)

    # 디버깅: 최종 요약 프롬프트 출력
    print("=" * 80)
    print(f"🎬 FINAL SUMMARY PROMPT INPUT:")
    print("=" * 80)
    print(prompt)
    print("=" * 80)

    # 프롬프트 보내기
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
    print(f"🎭 FINAL SUMMARY RESPONSE:")
    print("=" * 80)
    print(final_response)
    print("=" * 80)

    parsed_response_list = parse_final_summary(final_response, len(custom_prompts))

    # 응답 파싱해서 List[tuple] 형태로 반환
    for current_prompt, parsed_response in zip(custom_prompts, parsed_response_list):
        result_tuple = (current_prompt, parsed_response)
        final_responses.append(result_tuple)

    return final_responses


async def process_single_video(s3_video_uri: str, characters_info: str, movie_id: int, 
                              segment_duration: int = 600, init: bool = False, 
                              language_code: str = "ko-KR", threshold: float = 30.0, prompt_language: str = "kor") -> Dict:
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
        # 청크 정보 생성 (실제 파일 생성 없이 메타데이터만)
        chunks_info, segment_duration = generate_video_chunks_info(s3_video_uri)
        total_chunks = len(chunks_info)

        print(f"🎬 원본 비디오 동적 청크 처리 시작")
        print(f"   원본 URI: {s3_video_uri}")
        print(f"   Movie ID: {movie_id}")
        print(f"   세그먼트 길이: {segment_duration}초 ({segment_duration/60:.1f}분)")
        print("=" * 80)
        
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
            
            # S3에 있는 embeddings.json과 thumbnails 폴더 삭제
            print("🗑️ S3 정리 시작...")
            delete_embeddings_and_thumbnails(movie_id, s3_video_uri)

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

        # 커스텀 프롬프트 가져오기
        db = SessionLocal()
        custom_prompts = get_custom_prompts(db, movie_id)
        custom_retrievals = get_custom_retrievals(db, movie_id)
        db.close()
        print(f"프롬프트 {len(custom_prompts)}개, 검색어 {len(custom_retrievals)}개 로드 완료")
        
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
                scene_task = asyncio.to_thread(scene_process, chunk_uri, threshold, movie_id, current_chunk, s3_video_uri)

                utterances, (scenes, saved_uri) = await asyncio.gather(transcribe_task, scene_task)

                if saved_uri:
                    db = SessionLocal()
                    set_embedding_uri(db, movie_id, saved_uri)  # 임베딩 URI 저장
                    db.close()
                    print(f"✅ 장면 임베딩 URI 저장 완료: {saved_uri}")
                else:
                    print(f"⚠️ 장면 임베딩 URI가 반환되지 않았습니다.")
                
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
                # 검색어도 함께 전달하여 LLM이 관련 장면 선택
                summary, scene_selections = await get_bedrock_response_with_context(
                    utterances, scene_images, characters_info, previous_summaries, i, 
                    prompt_language, retrieval_queries=custom_retrievals
                )
                print(f"✅ Claude 요약 생성 완료 (길이: {len(summary)} 문자)")
                
                # scene_selections를 chunk_n_scene_m 형태의 문자열로 변환
                adjusted_scene_selections = {}
                for query, indices in scene_selections.items():
                    scene_strings = [f"chunk_{current_chunk}_scene_{idx + 1}" for idx in indices]
                    adjusted_scene_selections[query] = scene_strings
                    print(f"   '{query}': 장면 {indices} → {scene_strings}")
                
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
                    "summary_id": summary_id,
                    "scenes": scenes,  # 장면 정보 저장
                    "utterances": utterances,  # STT 정보 저장
                    "scene_selections": adjusted_scene_selections  # chunk_n_scene_m 형태로 저장
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

        # 프롬프트가 너무 많다면 10개로 제한
        if len(custom_prompts) > 10:
            custom_prompts = custom_prompts[:10]
            print(f"⚠️ 프롬프트 개수가 너무 많아 10개로 제한합니다.")
        if len(custom_retrievals) > 10:
            custom_retrievals = custom_retrievals[:10]
            print(f"⚠️ 검색어 개수가 너무 많아 10개로 제한합니다.")
        
        print("🎭 최종 프롬프트 응답 결과 생성 중...")

        # 최종 프롬프트 응답 결과 생성
        final_summary = await create_final_results([vs["summary"] for vs in video_summaries], custom_prompts, characters_info, prompt_language)
        print(f"✅ 최종 요약 생성 완료")     

        # 최종 장면 검색 결과 생성 (LLM 선택 + 벡터 유사도)
        # s3 uri들의 리스트의 딕셔너리 형태가 되어야 할 것.
        final_scenes = await get_final_scenes(custom_retrievals, movie_id, video_summaries)
        
        # 빈 딕셔너리가 아닌 경우에만 출력
        if final_scenes:
            print(f"✅ 최종 장면 검색 결과 생성 완료")
            print(f"{final_scenes}")
        else:
            print(f"⚠️ 최종 장면 검색 결과가 없습니다.")

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
        
        # 썸네일 정보 수집
        thumbnail_info = collect_thumbnail_info(video_summaries, s3_video_uri)
        
        return {
            "prompt2results": final_summary,
            "retrieval2uris": final_scenes,
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
            summary, _ = await get_bedrock_response_with_context(utterances, scene_images, characters_info, previous_summaries, i)
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

