import os
import tempfile
import subprocess
import boto3
from typing import List, Dict
import uuid

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

def get_video_duration_from_s3(s3_uri: str) -> float:
    """
    S3 비디오의 총 재생 시간을 초 단위로 반환합니다.
    메타데이터만 확인하므로 전체 파일을 다운로드하지 않습니다.
    """
    try:
        # presigned URL을 통해 ffprobe로 메타데이터만 조회
        s3 = boto3.client('s3')
        bucket = s3_uri.split('/')[2]
        key = '/'.join(s3_uri.split('/')[3:])
        
        # presigned URL 생성 (1시간 유효)
        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=3600
        )
        
        cmd = [
            'ffprobe', 
            '-v', 'quiet', 
            '-show_entries', 'format=duration', 
            '-of', 'default=noprint_wrappers=1:nokey=1', 
            presigned_url
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return float(result.stdout.strip())
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"비디오 길이 측정 실패: {str(e)}")
    except ValueError as e:
        raise RuntimeError(f"비디오 길이 파싱 실패: {str(e)}")

def extract_video_chunk_from_s3(s3_uri: str, start_seconds: int, duration_seconds: int) -> str:
    """
    S3 비디오에서 특정 시간 구간만 추출하여 임시 파일로 저장합니다.
    
    Args:
        s3_uri: 원본 비디오 S3 URI
        start_seconds: 시작 시간 (초)
        duration_seconds: 구간 길이 (초)
    
    Returns:
        str: 추출된 청크 파일의 로컬 경로
    """
    try:
        # S3 presigned URL 생성
        s3 = boto3.client('s3')
        bucket = s3_uri.split('/')[2]
        key = '/'.join(s3_uri.split('/')[3:])
        
        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=3600
        )
        
        # 출력 파일 생성
        output_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
        output_path = output_file.name
        output_file.close()
        
        # ffmpeg 명령어: 특정 구간만 추출
        cmd = [
            'ffmpeg',
            '-ss', str(start_seconds),  # 시작 시간
            '-i', presigned_url,        # 입력 (presigned URL)
            '-t', str(duration_seconds), # 지속 시간
            '-c', 'copy',               # 재인코딩 없이 복사 (빠름)
            '-avoid_negative_ts', 'make_zero',  # 타임스탬프 조정
            '-y',                       # 덮어쓰기
            output_path
        ]
        
        print(f"🎬 청크 추출 중: {start_seconds}초~{start_seconds + duration_seconds}초")
        print(f"   명령어: ffmpeg -ss {start_seconds} -i [URL] -t {duration_seconds} -c copy {os.path.basename(output_path)}")
        
        # ffmpeg 실행
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # 파일 크기 확인
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
            print(f"✅ 청크 추출 완료: {os.path.basename(output_path)} ({file_size:.1f}MB)")
            return output_path
        else:
            raise RuntimeError("추출된 청크 파일이 비어있거나 생성되지 않았습니다.")
            
    except subprocess.CalledProcessError as e:
        # 임시 파일 정리
        if 'output_path' in locals() and os.path.exists(output_path):
            os.unlink(output_path)
        raise RuntimeError(f"비디오 청크 추출 실패: {str(e)}\nstderr: {e.stderr}")
    except Exception as e:
        # 임시 파일 정리
        if 'output_path' in locals() and os.path.exists(output_path):
            os.unlink(output_path)
        raise RuntimeError(f"비디오 청크 추출 중 오류: {str(e)}")

def cleanup_chunk_file(file_path: str):
    """
    청크 임시 파일을 정리합니다.
    """
    try:
        if os.path.exists(file_path):
            os.unlink(file_path)
            print(f"🗑️ 청크 파일 삭제: {os.path.basename(file_path)}")
    except Exception as e:
        print(f"⚠️ 청크 파일 삭제 실패: {file_path} - {str(e)}")

def generate_video_chunks_info(s3_uri: str, segment_duration: int = 600) -> List[Dict]:
    """
    원본 비디오를 기반으로 청크 정보 리스트를 생성합니다.
    실제 파일을 생성하지 않고 메타데이터만 반환합니다.
    
    Args:
        s3_uri: 원본 비디오 S3 URI
        segment_duration: 각 청크의 길이 (초)
    
    Returns:
        List[Dict]: 청크 정보 리스트 [{"start": 0, "duration": 600, "order": 1}, ...]
    """
    try:
        # 총 비디오 길이 확인
        total_duration = get_video_duration_from_s3(s3_uri)
        print(f"📹 원본 비디오 길이: {total_duration:.1f}초 ({total_duration/60:.1f}분)")
        
        chunks = []
        start_time = 0
        chunk_order = 1
        
        while start_time < total_duration:
            # 남은 시간이 segment_duration보다 작으면 남은 시간만큼
            chunk_duration = min(segment_duration, total_duration - start_time)
            
            chunks.append({
                "start": start_time,
                "duration": chunk_duration,
                "order": chunk_order,
                "end": start_time + chunk_duration
            })
            
            start_time += chunk_duration
            chunk_order += 1
        
        print(f"📁 총 {len(chunks)}개의 청크로 분할 예정 (각 최대 {segment_duration/60:.1f}분)")
        
        return chunks
        
    except Exception as e:
        raise RuntimeError(f"비디오 청크 정보 생성 실패: {str(e)}")

def extract_chunk_for_processing(s3_uri: str, chunk_info: Dict) -> str:
    """
    처리를 위해 특정 청크를 추출합니다.
    
    Args:
        s3_uri: 원본 비디오 S3 URI
        chunk_info: 청크 정보 (start, duration, order 포함)
    
    Returns:
        str: 추출된 청크 파일의 로컬 경로
    """
    return extract_video_chunk_from_s3(
        s3_uri=s3_uri,
        start_seconds=int(chunk_info["start"]),
        duration_seconds=int(chunk_info["duration"])
    ) 