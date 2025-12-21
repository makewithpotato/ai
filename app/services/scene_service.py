import os
import tempfile
import boto3
from typing import List, Dict, Optional
import cv2
from scenedetect import detect, ContentDetector
from app.services.marengo_service import embed_marengo
import numpy as np
import base64
import uuid
import json

def match_utterances_to_scene(scene_start: float, scene_end: float, utterances: List[Dict]) -> str:
    """
    장면의 시간 범위에 해당하는 STT 텍스트를 추출하여 결합합니다.
    
    Args:
        scene_start: 장면 시작 시간 (초)
        scene_end: 장면 종료 시간 (초)
        utterances: STT 발화 정보 리스트 [{"speaker": str, "start_time": float, "end_time": float, "text": str}, ...]
    
    Returns:
        str: 해당 장면에 포함된 모든 대사를 결합한 텍스트
    """
    if not utterances:
        return ""
    
    matched_texts = []
    
    for utterance in utterances:
        utt_start = utterance.get('start_time', 0)
        utt_end = utterance.get('end_time', 0)
        text = utterance.get('text', '')
        
        # 발화가 장면 시간 범위와 겹치는지 확인
        # 겹침 조건: 발화 시작이 장면 끝 이전이고, 발화 끝이 장면 시작 이후
        if utt_start < scene_end and utt_end > scene_start:
            if text:
                matched_texts.append(text)
    
    # 모든 매칭된 텍스트를 공백으로 연결
    combined_text = " ".join(matched_texts).strip()
    
    return combined_text

def get_output_bucket() -> str:
    """
    환경 변수에서 출력 버킷 이름을 가져옵니다.
    """
    output_bucket = os.getenv("SCENES_BUCKET")
    if not output_bucket:
        raise ValueError("환경 변수 SCENES_BUCKET이 설정되지 않았습니다.")
    return output_bucket

def download_video_from_s3(s3_uri: str) -> str:
    """
    S3에서 비디오를 다운로드하여 임시 파일로 저장합니다.
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError("s3_uri는 's3://'로 시작해야 합니다.")
    
    # S3 URI 파싱
    bucket = s3_uri.split('/')[2]
    key = '/'.join(s3_uri.split('/')[3:])
    
    # S3 클라이언트 생성
    s3 = boto3.client('s3')
    
    # 임시 파일 생성
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
    
    try:
        # S3에서 비디오 다운로드
        s3.download_file(bucket, key, temp_file.name)
        return temp_file.name
    except Exception as e:
        # 임시 파일 삭제
        os.unlink(temp_file.name)
        raise e
    
def download_json_from_s3(s3_uri: str) -> Dict:
    """
    S3에서 JSON 파일을 다운로드하여 파싱합니다.
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError("s3_uri는 's3://'로 시작해야 합니다.")
    
    # S3 URI 파싱
    bucket = s3_uri.split('/')[2]
    key = '/'.join(s3_uri.split('/')[3:])
    
    # S3 클라이언트 생성
    s3 = boto3.client('s3')
    
    try:
        # S3에서 JSON 파일 다운로드
        response = s3.get_object(Bucket=bucket, Key=key)
        json_data = response['Body'].read().decode('utf-8')
        return json.loads(json_data)
    except Exception as e:
        raise e

def frame_to_base64(frame: np.ndarray, max_size_mb: float = 4.5) -> str:
    """
    OpenCV 프레임을 base64 문자열로 변환 (간단한 버전)
    """
    max_size_bytes = max_size_mb * 1024 * 1024
    
    # 먼저 적절한 품질로 인코딩
    encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 90]
    _, buffer = cv2.imencode('.jpg', frame, encode_param)
    
    # 크기가 크면 리사이징
    if len(buffer) > max_size_bytes:
        height, width = frame.shape[:2]
        scale = (max_size_bytes / len(buffer)) ** 0.5
        new_size = (int(width * scale * 0.9), int(height * scale * 0.9))
        resized = cv2.resize(frame, new_size, interpolation=cv2.INTER_AREA)
        _, buffer = cv2.imencode('.jpg', resized, encode_param)
    
    return base64.b64encode(buffer).decode('utf-8')

def save_frame_to_s3(frame: np.ndarray, prefix: str = "scenes") -> str:
    """
    프레임을 S3에 업로드하고 URL을 반환합니다.
    """
    # S3 클라이언트 생성
    s3 = boto3.client('s3')
    
    # 출력 버킷 가져오기
    output_bucket = get_output_bucket()
    
    # 임시 파일에 프레임 저장
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.jpg')
    cv2.imwrite(temp_file.name, frame)
    
    try:
        # S3에 업로드할 키 생성
        key = f"{prefix}/{uuid.uuid4()}.jpg"
        
        # S3에 업로드
        s3.upload_file(temp_file.name, output_bucket, key)
        
        # URL 생성 (1시간 동안 유효한 presigned URL)
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': output_bucket, 'Key': key},
            ExpiresIn=3600
        )
        
        return url
    finally:
        # 임시 파일 삭제
        os.unlink(temp_file.name)

def detect_and_embed_scenes(video_path: str, threshold: float = 30.0, max_scenes_count: int = 20, movie_id: int = None, chunk_id: int = None, original_uri: str = None) -> tuple[List[Dict], Optional[str]]:
    """
    비디오에서 주요 장면을 감지하고 각 장면의 대표 프레임을 base64로 반환합니다.
    품질이 좋은 프레임은 S3 thumbnails/ 경로에도 저장합니다.
    장면이 20개 초과일 경우, 시간별로 균일하게 분포하도록 최대 20개로 제한합니다.
    """
    # 장면 감지
    scene_list = detect(video_path, ContentDetector(threshold=threshold))
    
    print(f"🎬 감지된 총 장면 수: {len(scene_list)}개")
    
    # 장면이 max_scenes_count 초과일 경우, 시간별로 균일하게 분포하도록 먼저 제한
    if len(scene_list) > max_scenes_count:
        print(f"📊 장면 수가 {max_scenes_count}개를 초과하여 interval 기반 선택 적용")
        
        # 시간 범위 계산
        total_duration = scene_list[-1][1].get_seconds() - scene_list[0][0].get_seconds()
        interval = total_duration / max_scenes_count
        
        # interval 기반으로 장면 선택
        selected_scenes = []
        for i in range(max_scenes_count):
            target_time = scene_list[0][0].get_seconds() + i * interval
            closest_scene = min(scene_list, key=lambda x: abs(x[0].get_seconds() - target_time))
            if closest_scene not in selected_scenes:  # 중복 방지
                selected_scenes.append(closest_scene)
        
        scene_list = selected_scenes
        print(f"✅ {len(scene_list)}개 장면으로 제한됨")
    
    # 비디오 열기
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    
    scenes = []
    video_name = os.path.basename(video_path)
    
    for scene_index, scene in enumerate(scene_list):
        # 장면의 중간 프레임 선택
        middle_frame = int((scene[0].frame_num + scene[1].frame_num) / 2)
        cap.set(cv2.CAP_PROP_POS_FRAMES, middle_frame)
        ret, frame = cap.read()

        if not ret:
            print(f"⚠️ Scene {scene_index + 1}: 프레임 읽기 실패")
            continue

        quality_check = check_frame_quality(frame)

        print(f"🔍 Scene {scene_index + 1} 품질 검사:")
        print(f"   밝기: {quality_check['brightness']:.1f} ({'✅' if quality_check['brightness_ok'] else '❌'})")
        print(f"   선명도: {quality_check['sharpness']:.1f} ({'✅' if quality_check['sharpness_ok'] else '❌'})")
        
        if quality_check['is_good_quality']:
            # 프레임을 base64로 변환 (Bedrock 전송용)
            frame_image = frame_to_base64(frame)
            
            # 프레임을 복사하여 저장 (S3 저장용)
            frame_copy = frame.copy()
            
            scene_data = {
                "start_time": scene[0].get_seconds(),
                "end_time": scene[1].get_seconds(),
                "start_frame": scene[0].frame_num,
                "end_frame": scene[1].frame_num,
                "frame_image": frame_image,
                "frame": frame_copy
            }
            
            scenes.append(scene_data)
        else:
            print(f"   ⚠️ 품질 부족으로 제외됨")
    
    cap.release()
    
    print(f"✅ 최종 선택된 장면: {len(scenes)}개 (품질 검사 통과)")

    embed_uri_pairs = {}
    saved_uri: Optional[str] = None

    for scene_index, scene_data in enumerate(scenes):
        try:
            # scene retrieval 과정 수행 필요
            # marengo_service에서 aws bedrock marengo embed model 호출하여 임베딩을 받아오는 함수 사용
            # 임베딩을 썸네일과 함께 S3에 저장, DB에 메타데이터 저장.
            
            # scene_data에 저장된 원본 프레임 사용
            scene_frame = scene_data.get("frame")
            if scene_frame is None:
                print(f"⚠️ Scene {scene_index + 1}: 프레임이 없습니다. 건너뜁니다.")
                continue
            
            thumbnail_url = save_thumbnail_to_s3(scene_frame, movie_id, chunk_id, scene_index + 1, original_uri)
            scene_data['thumbnail_url'] = thumbnail_url

            embedded_vector = embed_marengo("image", scene_data["frame_image"])
            embed_uri_pairs[thumbnail_url] = embedded_vector
            
            # 메모리 절약을 위해 프레임 데이터 제거
            del scene_data['frame']
            
        except Exception as e:
            print(f"❌ Scene {scene_index + 1} 처리 중 오류: {str(e)}")

    if embed_uri_pairs:
        saved_uri = save_json_to_s3(embed_uri_pairs, movie_id, video_name, original_uri=original_uri)
        print(f"✅ 총 {len(embed_uri_pairs)}개 장면 임베딩 완료 및 S3 저장 완료.")
    
    return scenes, saved_uri

def scene_process(uri: str, threshold: float = 30.0, movie_id: int = None, chunk_id: int = None, original_uri: str = None) -> tuple[List[Dict], str]:
    """
    전체 장면 처리 프로세스입니다. 다음과 같은 과정을 거칩니다.
    1. 해당 비디오를 청크로 분할합니다.
    2. 분할한 비디오 청크에서 pySceneDetect를 사용하여 장면을 감지합니다.
    3. 우수한 장면은 S3에 저장되며, marengo를 통한 임베딩 역시 수행 후 저장됩니다. (임베딩 메타데이터는 DB에 저장)
    
    Args:
        uri: S3 URI (s3://) 또는 로컬 파일 URI (file://)
        threshold: 장면 감지 임계값
        movie_id: 영화 ID
        chunk_id: 비디오 청크 ID (단일 비디오 모드에서 사용)
        original_uri: 원본 비디오 URI (썸네일 경로 결정용, 단일 비디오 모드에서 사용)
        
    Returns:
        List[Dict]: 장면 정보 리스트
    """
    try:
        video_path = None
        should_cleanup = False
        
        # original_uri가 없으면 현재 uri를 사용
        if original_uri is None:
            original_uri = uri
        
        if uri.startswith("file://"):
            # 로컬 파일인 경우
            video_path = uri[7:]  # "file://" 제거
            if not os.path.exists(video_path):
                raise ValueError(f"로컬 파일이 존재하지 않습니다: {video_path}")
            should_cleanup = False  # 로컬 파일은 삭제하지 않음
            
        elif uri.startswith("s3://"):
            # S3 URI인 경우 다운로드
            video_path = download_video_from_s3(uri)
            should_cleanup = True  # 다운로드한 임시 파일은 삭제
            
        else:
            raise ValueError("URI는 's3://' 또는 'file://'로 시작해야 합니다.")
        
        try:
            # 다운로드받은 영상 장면 감지 직후 임베딩
            scenes, saved_uri = detect_and_embed_scenes(video_path, threshold, movie_id=movie_id, chunk_id=chunk_id, original_uri=original_uri)
            return scenes, saved_uri
        finally:
            # 임시 파일 삭제 (S3에서 다운로드한 경우만)
            if should_cleanup and video_path and os.path.exists(video_path):
                os.unlink(video_path)
                
    except Exception as e:
        raise RuntimeError(f"장면 감지 중 오류 발생: {str(e)}")

def check_frame_quality(frame: np.ndarray) -> Dict[str, float]:
    """
    프레임의 품질을 검사합니다.
    
    Args:
        frame: OpenCV 프레임 (numpy array)
    
    Returns:
        Dict: 품질 지표들 (brightness, sharpness, is_good_quality)
    """
    # 그레이스케일 변환
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    
    # 1. 밝기 검사 (평균 밝기)
    brightness = np.mean(gray)
    
    # 2. 선명도 검사 (Laplacian variance)
    laplacian_var = cv2.Laplacian(gray, cv2.CV_64F).var()
    
    # 3. 품질 판정 (기준 완화)
    # 밝기: 30-220 범위가 적절 (기존 50-200에서 완화)
    # 선명도: Laplacian variance > 50이 선명함 (기존 100에서 완화)
    brightness_ok = 30 <= brightness <= 220
    sharpness_ok = laplacian_var > 10
    
    is_good_quality = brightness_ok and sharpness_ok
    
    return {
        "brightness": brightness,
        "sharpness": laplacian_var,
        "brightness_ok": brightness_ok,
        "sharpness_ok": sharpness_ok,
        "is_good_quality": is_good_quality
    }

def save_json_to_s3(dict_data: dict, movie_id: int, video_name: str, original_uri: str = None) -> str:
    """
    uri-임베딩 쌍을 원본 비디오와 같은 디렉토리의 embeddings/ 폴더에 저장합니다.
    기존 데이터가 있으면 병합하여 누적 저장합니다.
    
    Args:
        dict_data: 저장할 JSON 데이터
        movie_id: 영화 ID
        video_name: 비디오 파일명
        original_uri: 원본 비디오 URI (디렉토리 구조 유지용)
    
    Returns:
        str: S3 URL
    """
    # S3 클라이언트 생성
    s3 = boto3.client('s3')
    
    # 출력 버킷 가져오기
    output_bucket = get_output_bucket()
    
    try:
        # 썸네일 저장 경로 결정
        if original_uri and original_uri.startswith("s3://"):
            # 원본 비디오 URI에서 디렉토리 구조 추출
            # 예: s3://bucket/movies/series1/episode1.mp4 → movies/series1/embeddings/
            uri_parts = original_uri.replace("s3://", "").split("/")
            bucket_from_uri = uri_parts[0]
            
            if len(uri_parts) > 1:
                # 디렉토리 부분 추출 (파일명 제외)
                directory_path = "/".join(uri_parts[1:-1])
                if directory_path:
                    # 같은 디렉토리에 embeddings 폴더 생성
                    embeddings_dir = f"{directory_path}/embeddings"
                else:
                    # 루트 디렉토리인 경우
                    embeddings_dir = "embeddings"
            else:
                # 버킷 루트인 경우
                embeddings_dir = "embeddings"
        else:
            # original_uri가 없거나 S3 URI가 아닌 경우 기본 경로 사용
            embeddings_dir = f"embeddings/{movie_id}"
        
        # 파일명 생성
        filename = "embeddings.json"
        
        # 최종 S3 키 생성
        key = f"{embeddings_dir}/{filename}"
        uri = f"s3://{output_bucket}/{key}"
        
        # 기존 데이터 병합 (있으면 다운로드)
        merged_data = dict_data.copy()
        try:
            response = s3.get_object(Bucket=output_bucket, Key=key)
            existing_data = json.loads(response['Body'].read().decode('utf-8'))
            print(f"📥 기존 임베딩 데이터 {len(existing_data)}개 발견, 병합 중...")
            # 기존 데이터를 먼저 넣고 새 데이터로 업데이트 (중복 시 새 데이터 우선)
            merged_data = {**existing_data, **dict_data}
            print(f"📊 병합 완료: 기존 {len(existing_data)}개 + 신규 {len(dict_data)}개 = 총 {len(merged_data)}개")
        except s3.exceptions.NoSuchKey:
            print(f"📝 기존 임베딩 파일 없음, 새로 생성")
        except Exception as e:
            print(f"⚠️ 기존 데이터 로드 실패 (무시하고 새로 저장): {str(e)}")
        
        # JSON 데이터를 문자열로 변환
        json_data = json.dumps(merged_data)
        
        # S3에 업로드
        s3.put_object(Body=json_data, Bucket=output_bucket, Key=key, ContentType='application/json')
        
        print(f"✅ 임베딩 저장 완료: {uri}")
        print(f"   경로: {key}")
        print(f"   총 임베딩 개수: {len(merged_data)}개")
        return uri
        
    except Exception as e:
        print(f"❌ 임베딩 저장 실패: {str(e)}")
        raise e
def delete_embeddings_and_thumbnails(movie_id: int, s3_video_uri: str = None) -> bool:
    """
    S3에서 embeddings.json 파일과 thumbnails 폴더를 삭제합니다.
    
    Args:
        movie_id: 영화 ID
        s3_video_uri: 원본 비디오 URI (디렉토리 구조 결정용)
    
    Returns:
        bool: 삭제 성공 여부
    """
    try:
        s3 = boto3.client('s3')
        output_bucket = get_output_bucket()
        
        # 디렉토리 경로 결정
        if s3_video_uri and s3_video_uri.startswith("s3://"):
            uri_parts = s3_video_uri.replace("s3://", "").split("/")
            if len(uri_parts) > 1:
                directory_path = "/".join(uri_parts[1:-1])
                if directory_path:
                    embeddings_dir = f"{directory_path}/embeddings"
                    thumbnails_dir = f"{directory_path}/thumbnails"
                else:
                    embeddings_dir = "embeddings"
                    thumbnails_dir = "thumbnails"
            else:
                embeddings_dir = "embeddings"
                thumbnails_dir = "thumbnails"
        else:
            embeddings_dir = f"embeddings/{movie_id}"
            thumbnails_dir = f"thumbnails/{movie_id}"
        
        deleted_count = 0
        
        # embeddings.json 파일 삭제
        embeddings_key = f"{embeddings_dir}/embeddings.json"
        try:
            s3.delete_object(Bucket=output_bucket, Key=embeddings_key)
            print(f"🗑️ embeddings.json 삭제 완료: {embeddings_key}")
            deleted_count += 1
        except s3.exceptions.NoSuchKey:
            print(f"ℹ️ embeddings.json 파일 없음: {embeddings_key}")
        except Exception as e:
            print(f"⚠️ embeddings.json 삭제 실패: {str(e)}")
        
        # thumbnails 폴더의 모든 파일 삭제
        try:
            # 폴더 내 모든 객체 조회
            response = s3.list_objects_v2(Bucket=output_bucket, Prefix=thumbnails_dir + "/")
            
            if 'Contents' in response:
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                
                if objects_to_delete:
                    # 배치 삭제
                    delete_response = s3.delete_objects(
                        Bucket=output_bucket,
                        Delete={'Objects': objects_to_delete}
                    )
                    deleted_count += len(objects_to_delete)
                    print(f"🗑️ thumbnails 폴더 삭제 완료: {len(objects_to_delete)}개 파일")
                else:
                    print(f"ℹ️ thumbnails 폴더가 비어있음")
            else:
                print(f"ℹ️ thumbnails 폴더 없음: {thumbnails_dir}")
        except Exception as e:
            print(f"⚠️ thumbnails 폴더 삭제 실패: {str(e)}")
        
        print(f"✅ S3 정리 완료: 총 {deleted_count}개 항목 삭제")
        return True
        
    except Exception as e:
        print(f"❌ S3 삭제 중 오류: {str(e)}")
        return False
def save_thumbnail_to_s3(frame: np.ndarray, movie_id: int, chunk_id: int, scene_index: int, original_uri: str = None) -> str:
    """
    썸네일 후보 프레임을 원본 비디오와 같은 디렉토리의 thumbnails/ 폴더에 저장합니다.
    
    Args:
        frame: OpenCV 프레임
        movie_id: 영화 ID
        scene_index: 장면 인덱스
        original_uri: 원본 비디오 URI (디렉토리 구조 유지용)
    
    Returns:
        str: S3 URL
    """
    # S3 클라이언트 생성
    s3 = boto3.client('s3')
    
    # 출력 버킷 가져오기
    output_bucket = get_output_bucket()
    
    # 임시 파일에 프레임 저장
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.jpg')
    
    # JPEG 품질을 높게 설정하여 저장
    cv2.imwrite(temp_file.name, frame, [cv2.IMWRITE_JPEG_QUALITY, 90])
    
    try:
        # 썸네일 저장 경로 결정
        if original_uri and original_uri.startswith("s3://"):
            # 원본 비디오 URI에서 디렉토리 구조 추출
            # 예: s3://bucket/movies/series1/episode1.mp4 → movies/series1/thumbnails/
            uri_parts = original_uri.replace("s3://", "").split("/")
            bucket_from_uri = uri_parts[0]
            
            if len(uri_parts) > 1:
                # 디렉토리 부분 추출 (파일명 제외)
                directory_path = "/".join(uri_parts[1:-1])
                if directory_path:
                    # 같은 디렉토리에 thumbnails 폴더 생성
                    thumbnail_dir = f"{directory_path}/thumbnails"
                else:
                    # 루트 디렉토리인 경우
                    thumbnail_dir = "thumbnails"
            else:
                # 버킷 루트인 경우
                thumbnail_dir = "thumbnails"
        else:
            # original_uri가 없거나 S3 URI가 아닌 경우 기본 경로 사용
            thumbnail_dir = f"thumbnails/{movie_id}"
        
        # 파일명 생성
        filename = f"chunk_{chunk_id}_scene_{scene_index}.jpg"
        
        # 최종 S3 키 생성
        key = f"{thumbnail_dir}/{filename}"
        
        # S3에 업로드
        s3.upload_file(temp_file.name, output_bucket, key)
        
        # 공개 URL 생성 (또는 presigned URL)
        url = f"https://{output_bucket}.s3.amazonaws.com/{key}"
        
        print(f"✅ 썸네일 저장 완료: {url}")
        print(f"   경로: {key}")
        return url
        
    except Exception as e:
        print(f"❌ 썸네일 저장 실패: {str(e)}")
        raise e
    finally:
        # 임시 파일 삭제
        os.unlink(temp_file.name) 