# app/services/transcribe_service.py

import os
import boto3
import time
import uuid
import requests
import tempfile
from typing import List, Dict

class Utterance:
    def __init__(self, speaker: str, start_time: float, end_time: float, text: str):
        self.speaker = speaker
        self.start_time = start_time
        self.end_time = end_time
        self.text = text

    def to_dict(self) -> Dict:
        return {
            "speaker": self.speaker,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "text": self.text
        }

def safe_float_convert(value: str) -> float:
    """문자열을 float로 안전하게 변환합니다."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def upload_local_file_to_s3(local_path: str) -> str:
    """
    로컬 파일을 S3에 임시 업로드하고 S3 URI를 반환합니다.
    """
    try:
        s3 = boto3.client('s3')
        bucket = os.getenv("TRANSCRIPTS_BUCKET")
        if not bucket:
            raise ValueError("환경 변수 TRANSCRIPTS_BUCKET이 설정되지 않았습니다.")
        
        # 임시 키 생성
        temp_key = f"temp_videos/{uuid.uuid4()}.mp4"
        
        # S3에 업로드
        s3.upload_file(local_path, bucket, temp_key)
        
        # S3 URI 생성
        s3_uri = f"s3://{bucket}/{temp_key}"
        print(f"🔄 로컬 파일을 S3에 임시 업로드: {s3_uri}")
        
        return s3_uri
        
    except Exception as e:
        raise RuntimeError(f"로컬 파일 S3 업로드 실패: {str(e)}")

def cleanup_temp_s3_file(s3_uri: str):
    """
    임시로 업로드된 S3 파일을 삭제합니다.
    """
    try:
        if not s3_uri.startswith("s3://"):
            return
            
        s3 = boto3.client('s3')
        bucket = s3_uri.split('/')[2]
        key = '/'.join(s3_uri.split('/')[3:])
        
        # temp_videos/ 경로에 있는 파일만 삭제 (안전장치)
        if key.startswith("temp_videos/"):
            s3.delete_object(Bucket=bucket, Key=key)
            print(f"🗑️ 임시 S3 파일 삭제: {s3_uri}")
        
    except Exception as e:
        print(f"⚠️ 임시 S3 파일 삭제 실패: {s3_uri} - {str(e)}")

def transcribe_video(uri: str, language_code: str = "en-US") -> List[Dict]:
    """
    AWS Transcribe를 통해 비디오를 음성 텍스트로 변환하고,
    발화자, 시간, 대사 정보를 포함한 JSON 리스트를 반환합니다.
    
    Args:
        uri: S3 URI (s3://) 또는 로컬 파일 URI (file://)
        language_code: 언어 코드
    
    Returns:
        List[Dict]: 발화 정보 리스트
    """
    temp_s3_uri = None
    
    try:
        # URI 타입에 따른 처리
        if uri.startswith("file://"):
            # 로컬 파일인 경우 S3에 임시 업로드
            local_path = uri[7:]  # "file://" 제거
            if not os.path.exists(local_path):
                raise ValueError(f"로컬 파일이 존재하지 않습니다: {local_path}")
            
            temp_s3_uri = upload_local_file_to_s3(local_path)
            s3_uri = temp_s3_uri
            
        elif uri.startswith("s3://"):
            # S3 URI인 경우 그대로 사용
            s3_uri = uri
            
        else:
            raise ValueError("URI는 's3://' 또는 'file://'로 시작해야 합니다.")

        transcribe = boto3.client(
            'transcribe',
            region_name=os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
        )

        output_bucket = os.getenv("TRANSCRIPTS_BUCKET")
        if not output_bucket:
            raise ValueError("환경 변수 TRANSCRIPTS_BUCKET이 설정되지 않았습니다.")

        job_name = f"transcribe-job-{uuid.uuid4()}"
        transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': s3_uri},
            MediaFormat='mp4',
            LanguageCode=language_code,
            OutputBucketName=output_bucket,
            OutputKey=f"transcripts/{job_name}.json",
            Settings={
                'ShowSpeakerLabels': True,
                'MaxSpeakerLabels': 5  # 최대 5명의 발화자로 제한
            }
        )

        # 완료될 때까지 5초 간격으로 폴링
        while True:
            status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
            job_status = status['TranscriptionJob']['TranscriptionJobStatus']
            if job_status in ['COMPLETED', 'FAILED']:
                break
            time.sleep(5)

        if job_status == 'COMPLETED':
            # presigned URL로부터 JSON을 가져와 발화 정보 파싱
            result_url = status['TranscriptionJob']['Transcript']['TranscriptFileUri']
            response = requests.get(result_url)
            transcript_json = response.json()
            
            utterances = []
            
            # speaker_labels.segments에서 발화자 정보 추출
            if 'speaker_labels' in transcript_json['results'] and 'segments' in transcript_json['results']['speaker_labels']:
                segments = transcript_json['results']['speaker_labels']['segments']
                items = transcript_json['results']['items']
                
                # 각 세그먼트에 대해 발화 정보 생성
                for segment in segments:
                    start_time = safe_float_convert(segment.get('start_time', '0'))
                    end_time = safe_float_convert(segment.get('end_time', '0'))
                    
                    # 해당 세그먼트의 시간 범위에 있는 items 찾기
                    segment_items = [
                        item for item in items 
                        if safe_float_convert(item.get('start_time', '0')) >= start_time 
                        and safe_float_convert(item.get('end_time', '0')) <= end_time
                    ]
                    
                    # items에서 텍스트 추출
                    segment_text = ' '.join([
                        item['alternatives'][0]['content']
                        for item in segment_items
                        if 'alternatives' in item and item['alternatives']
                    ])
                    
                    utterance = Utterance(
                        speaker=segment.get('speaker_label', 'unknown'),
                        start_time=start_time,
                        end_time=end_time,
                        text=segment_text
                    )
                    utterances.append(utterance.to_dict())

            return utterances
        else:
            raise RuntimeError(f"Transcription job {job_name} failed")
            
    except Exception as e:
        raise RuntimeError(f"Transcribe 처리 중 오류: {str(e)}")
        
    finally:
        # 임시 S3 파일 정리
        if temp_s3_uri:
            cleanup_temp_s3_file(temp_s3_uri)