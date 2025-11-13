import os
import tempfile
import boto3
from typing import List, Dict
import cv2
from scenedetect import detect, ContentDetector
import numpy as np
import base64
import uuid

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

def frame_to_base64(frame: np.ndarray) -> str:
    """
    OpenCV 프레임을 base64 문자열로 변환합니다.
    """
    _, buffer = cv2.imencode('.jpg', frame)
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

def detect_scenes(video_path: str, threshold: float = 30.0, max_scenes_count: int = 20, movie_id: int = None, original_uri: str = None) -> List[Dict]:
    """
    비디오에서 주요 장면을 감지하고 각 장면의 대표 프레임을 base64로 반환합니다.
    품질이 좋은 프레임은 S3 thumbnails/ 경로에도 저장합니다.
    장면이 20개 초과일 경우, 시간별로 균일하게 분포하도록 최대 20개로 제한합니다.
    """
    # 장면 감지
    scene_list = detect(video_path, ContentDetector(threshold=threshold))
    
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
        
        if ret:
            # 프레임을 base64로 변환 (Bedrock 전송용 - 모든 프레임)
            frame_image = frame_to_base64(frame)
            
            scene_data = {
                "start_time": scene[0].get_seconds(),
                "end_time": scene[1].get_seconds(),
                "start_frame": scene[0].frame_num,
                "end_frame": scene[1].frame_num,
                "frame_image": frame_image
            }
            
            scenes.append(scene_data)
    
    cap.release()

    # 장면이 20개 초과일 경우, 시간별로 균일하게 분포하도록 최대 20개로 제한
    if len(scenes) > max_scenes_count:
        total_duration = scenes[-1]["end_time"] - scenes[0]["start_time"]
        interval = total_duration / max_scenes_count
        selected_scenes = []
        for i in range(max_scenes_count):
            target_time = scenes[0]["start_time"] + i * interval
            closest_scene = min(scenes, key=lambda x: abs(x["start_time"] - target_time))
            selected_scenes.append(closest_scene)
        scenes = selected_scenes

    # 품질 검사 및 S3 저장 (최대 20개 장면에 대해서만 수행)
    if movie_id is not None:
        print(f"🔍 최대 {len(scenes)}개 장면에 대해 품질 검사 수행...")
        for scene_index, scene_data in enumerate(scenes):
            try:
                # base64 이미지를 다시 프레임으로 변환
                frame_bytes = base64.b64decode(scene_data["frame_image"])
                frame_array = np.frombuffer(frame_bytes, dtype=np.uint8)
                frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
                
                quality_check = check_frame_quality(frame)
                
                print(f"🔍 Scene {scene_index + 1} 품질 검사:")
                print(f"   밝기: {quality_check['brightness']:.1f} ({'✅' if quality_check['brightness_ok'] else '❌'})")
                print(f"   선명도: {quality_check['sharpness']:.1f} ({'✅' if quality_check['sharpness_ok'] else '❌'})")
                
                if quality_check['is_good_quality']:
                    thumbnail_url = save_thumbnail_to_s3(frame, movie_id, video_name, scene_index + 1, original_uri)
                    scene_data['thumbnail_url'] = thumbnail_url
                    print(f"✅ Scene {scene_index + 1}: 품질 양호 → S3 저장 완료")
                else:
                    print(f"⚠️ Scene {scene_index + 1}: 품질 부족 → S3 저장 생략")
                    
            except Exception as e:
                print(f"❌ Scene {scene_index + 1} 처리 중 오류: {str(e)}")

    return scenes

def get_video_scenes(uri: str, threshold: float = 30.0, movie_id: int = None, original_uri: str = None) -> List[Dict]:
    """
    비디오의 주요 장면을 감지하고 각 장면의 대표 프레임을 base64로 반환합니다.
    품질이 좋은 프레임은 S3 thumbnails/ 경로에도 저장합니다.
    
    Args:
        uri: S3 URI (s3://) 또는 로컬 파일 URI (file://)
        threshold: 장면 감지 임계값
        movie_id: 영화 ID
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
            # 장면 감지 (movie_id와 original_uri 전달)
            scenes = detect_scenes(video_path, threshold, movie_id=movie_id, original_uri=original_uri)
            return scenes
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
    sharpness_ok = laplacian_var > 30
    
    is_good_quality = brightness_ok and sharpness_ok
    
    return {
        "brightness": brightness,
        "sharpness": laplacian_var,
        "brightness_ok": brightness_ok,
        "sharpness_ok": sharpness_ok,
        "is_good_quality": is_good_quality
    }

def save_thumbnail_to_s3(frame: np.ndarray, movie_id: int, video_name: str, scene_index: int, original_uri: str = None) -> str:
    """
    썸네일 후보 프레임을 원본 비디오와 같은 디렉토리의 thumbnails/ 폴더에 저장합니다.
    
    Args:
        frame: OpenCV 프레임
        movie_id: 영화 ID
        video_name: 비디오 파일명
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
        video_basename = os.path.splitext(os.path.basename(video_name))[0]
        filename = f"{video_basename}_scene_{scene_index}.jpg"
        
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