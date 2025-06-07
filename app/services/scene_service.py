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

def detect_scenes(video_path: str, threshold: float = 30.0) -> List[Dict]:
    """
    비디오에서 주요 장면을 감지하고 각 장면의 대표 프레임을 S3에 업로드합니다.
    """
    # 장면 감지
    scene_list = detect(video_path, ContentDetector(threshold=threshold))
    
    # 비디오 열기
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    
    scenes = []
    for scene in scene_list:
        # 장면의 중간 프레임 선택
        middle_frame = int((scene[0].frame_num + scene[1].frame_num) / 2)
        cap.set(cv2.CAP_PROP_POS_FRAMES, middle_frame)
        ret, frame = cap.read()
        
        if ret:
            # 프레임을 S3에 업로드하고 URL 받기
            frame_url = save_frame_to_s3(frame)
            
            scenes.append({
                "start_time": scene[0].get_seconds(),
                "end_time": scene[1].get_seconds(),
                "start_frame": scene[0].frame_num,
                "end_frame": scene[1].frame_num,
                "frame_url": frame_url
            })
    
    cap.release()
    return scenes

def get_video_scenes(s3_uri: str, threshold: float = 30.0) -> List[Dict]:
    """
    S3에 있는 비디오의 주요 장면을 감지하고 각 장면의 대표 프레임을 S3에 업로드합니다.
    """
    try:
        # S3에서 비디오 다운로드
        video_path = download_video_from_s3(s3_uri)
        
        try:
            # 장면 감지
            scenes = detect_scenes(video_path, threshold)
            return scenes
        finally:
            # 임시 파일 삭제
            os.unlink(video_path)
    except Exception as e:
        raise RuntimeError(f"장면 감지 중 오류 발생: {str(e)}") 