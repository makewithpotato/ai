import os
import json
import boto3
import re
from typing import List, Dict
from app.services.transcribe_service import transcribe_video
from app.services.scene_service import get_video_scenes
from app.crud import create_or_update_summary, get_summaries, get_summaries_up_to, delete_summaries_from, update_movie_status, mark_movie_failed, get_resume_info, get_movie
from app.database import SessionLocal
import asyncio

def load_prompts() -> Dict[str, str]:
    """
    prompts.txt 파일에서 프롬프트 템플릿을 로드합니다.
    """
    try:
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
        
    except FileNotFoundError:
        print("⚠️ prompts.txt 파일을 찾을 수 없습니다. 기본 프롬프트를 사용합니다.")
        return {
            "VIDEO_ANALYSIS_PROMPT": "[등장인물 정보]\n{characters_info}\n\n다음은 연속된 비디오 시리즈의 일부입니다.{context}[현재 영상의 대화 내용]\n{conversation}\n\n[현재 영상의 장면별 시작 시각]\n{scene_times}\n\n등장인물 정보와 최근 영상들의 맥락을 고려하여 현재 영상에 대해:\n1. 각 장면이 보여주는 상황을 설명해주세요\n2. 대화 내용과 연관지어 설명해주세요\n3. 최근 영상들과의 연결점이나 스토리 진행을 분석해주세요\n\n현재 영상의 내용을 요약해주세요.",
            "FINAL_SUMMARY_PROMPT": "[등장인물 정보]\n{characters_info}\n\n다음은 연속된 비디오 시리즈의 각 영상별 요약입니다:\n\n{all_summaries}\n\n등장인물 정보와 위 내용을 바탕으로:\n1. 전체 스토리의 흐름을 정리해주세요\n2. 주요 등장인물과 그들의 관계를 설명해주세요\n3. 핵심 사건들과 갈등 구조를 분석해주세요\n4. 전체 영상 시리즈의 주제와 메시지를 요약해주세요\n\n최종적으로 전체 영상 시리즈에 대한 종합적인 요약을 제공해주세요."
        }
    except Exception as e:
        print(f"⚠️ 프롬프트 로드 중 오류: {str(e)}. 기본 프롬프트를 사용합니다.")
        return {
            "VIDEO_ANALYSIS_PROMPT": "[등장인물 정보]\n{characters_info}\n\n다음은 연속된 비디오 시리즈의 일부입니다.{context}[현재 영상의 대화 내용]\n{conversation}\n\n[현재 영상의 장면별 시작 시각]\n{scene_times}\n\n등장인물 정보와 최근 영상들의 맥락을 고려하여 현재 영상에 대해:\n1. 각 장면이 보여주는 상황을 설명해주세요\n2. 대화 내용과 연관지어 설명해주세요\n3. 최근 영상들과의 연결점이나 스토리 진행을 분석해주세요\n\n현재 영상의 내용을 요약해주세요.",
            "FINAL_SUMMARY_PROMPT": "[등장인물 정보]\n{characters_info}\n\n다음은 연속된 비디오 시리즈의 각 영상별 요약입니다:\n\n{all_summaries}\n\n등장인물 정보와 위 내용을 바탕으로:\n1. 전체 스토리의 흐름을 정리해주세요\n2. 주요 등장인물과 그들의 관계를 설명해주세요\n3. 핵심 사건들과 갈등 구조를 분석해주세요\n4. 전체 영상 시리즈의 주제와 메시지를 요약해주세요\n\n최종적으로 전체 영상 시리즈에 대한 종합적인 요약을 제공해주세요."
        }

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

async def create_final_summary(video_summaries: List[str], characters_info: str) -> str:
    """
    모든 비디오 요약을 종합하여 최종 요약을 생성합니다.
    """
    bedrock = boto3.client(
        service_name='bedrock-runtime',
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )
    model_id = os.getenv("CLAUDE_MODEL_ID")

    # 프롬프트 템플릿 로드
    prompts = load_prompts()
    template = prompts.get("FINAL_SUMMARY_PROMPT", "")

    # 모든 요약을 하나로 합침
    all_summaries = "\n\n".join([
        f"영상 {i+1}:\n{summary}" 
        for i, summary in enumerate(video_summaries)
    ])
    
    # 템플릿에 변수 삽입
    prompt = template.format(
        characters_info=characters_info,
        all_summaries=all_summaries
    )

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
        start_from = 0  # 기본값: 처음부터 시작
        
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
                if resume_info.get("stage") == "complete":
                    print(f"⚠️ 이미 완료된 작업입니다. Movie ID: {movie_id}")
                    print(f"💡 처음부터 다시 시작하려면 init=true로 설정하세요.")
                    # 기존 결과 반환 (필요시 구현)
                    raise RuntimeError("이미 완료된 작업입니다. init=true로 재시작하세요.")
                elif resume_info.get("stage") == "organizing":
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
            
            # transcribe와 scene 병렬 처리
            transcribe_task = asyncio.to_thread(transcribe_video, video_uri, language_code)
            scene_task = asyncio.to_thread(get_video_scenes, video_uri, threshold)
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
        
        print("🎭 최종 종합 요약 생성 중...")
        # 최종 종합 요약 생성
        final_summary = await create_final_summary([vs["summary"] for vs in video_summaries], characters_info)
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
        
        return {
            "video_summaries": video_summaries,
            "final_summary": final_summary,
            "final_summary_id": final_summary_id
        }
        
    except Exception as e:
        # 오류 발생 시 실패 상태로 업데이트
        try:
            mark_movie_failed(db, movie_id)
            db.close()
            print(f"📊 Movie 상태 업데이트: 오류로 인한 FAILED 상태")
        except:
            pass
        
        print(f"❌ 오류 발생: {str(e)}")
        raise RuntimeError(f"S3 폴더 비디오 처리 중 오류 발생: {str(e)}")

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

